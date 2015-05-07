/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef COMMON_DATATRANSFER_SASL_H_
#define COMMON_DATATRANSFER_SASL_H_

#include "sasl_authenticator.h"
#include "common/continuation/asio.h"
#include "common/continuation/protobuf.h"

#include "libhdfs++/options.h"

#include "datatransfer.h"
#include "datatransfer.pb.h"

namespace hdfs {

template <class Stream>
class DataTransferSaslStream {
 public:
  DataTransferSaslStream(const BlockReaderOptions &options,
                         const std::shared_ptr<Stream> &stream,
                         const std::string &username,
                         const std::string &password)
      : stream_(stream)
      , options_(options)
      , authenticator_(username, password)
  {}

  template<class Handler>
  void Handshake(const Handler &next);

  template <typename MutableBufferSequence, typename ReadHandler>
  ASIO_INITFN_RESULT_TYPE(ReadHandler,
      void (asio::error_code, std::size_t))
  async_read_some(const MutableBufferSequence& buffers,
                  ASIO_MOVE_ARG(ReadHandler) handler) {
    return stream_->async_read_some(buffers, handler);
  }

  template <typename ConstBufferSequence, typename WriteHandler>
  ASIO_INITFN_RESULT_TYPE(WriteHandler,
      void (asio::error_code, std::size_t))
  async_write_some(const ConstBufferSequence& buffers,
      ASIO_MOVE_ARG(WriteHandler) handler)
  {
    return stream_->async_write_some(buffers, handler);
  }

 private:
  DataTransferSaslStream(const DataTransferSaslStream&) = delete;
  DataTransferSaslStream &operator=(const DataTransferSaslStream &) = delete;
  std::shared_ptr<Stream> stream_;
  BlockReaderOptions options_;
  DigestMD5Authenticator authenticator_;
  struct ReadSaslMessageContinuation;
  struct AuthenticatorContinuation;
};

namespace DataTransferSaslStreamUtil {

typedef hadoop::hdfs::DataTransferEncryptorMessageProto SaslMessage;

Status ConvertToStatus(const SaslMessage *msg, std::string *payload);
void PrepareInitialHandshake(SaslMessage *msg);

}

template<class Stream>
struct DataTransferSaslStream<Stream>::AuthenticatorContinuation
    : continuation::Continuation {
  AuthenticatorContinuation(DigestMD5Authenticator *authenticator,
                     BlockReaderOptions *options,
                     const std::string *request,
                     hadoop::hdfs::DataTransferEncryptorMessageProto *msg)
      : authenticator_(authenticator)
      , options_(options)
      , request_(request)
      , msg_(msg)
  {}

  virtual void Run(const Next& next) override {
    std::string response;
    Status status = authenticator_->EvaluateResponse(*request_, &response);
    msg_->Clear();
    if (status.ok()) {
      // TODO: Handle encryption scheme
      msg_->set_payload(response);
      msg_->set_status(hadoop::hdfs::DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_SUCCESS);
    } else {
      msg_->set_status(hadoop::hdfs::DataTransferEncryptorMessageProto_DataTransferEncryptorStatus_ERROR);
    }
    next(Status::OK());
  }

 private:
  DigestMD5Authenticator *authenticator_;
  BlockReaderOptions *options_;
  const std::string *request_;
  hadoop::hdfs::DataTransferEncryptorMessageProto *msg_;
};

template<class Stream>
struct DataTransferSaslStream<Stream>::ReadSaslMessageContinuation
    : continuation::Continuation {
  ReadSaslMessageContinuation(Stream *stream, std::string *data)
      : stream_(stream)
      , data_(data)
  {}

  virtual void Run(const Next& next) override {
    read_pb_.reset(
        new continuation::ReadDelimitedPBMessageContinuation<Stream, 1024>(stream_, &resp_));
    auto handler = [this,next](const Status &status) {
      if (status.ok()) {
        Status new_stat = DataTransferSaslStreamUtil::ConvertToStatus(&resp_, data_);
        next(new_stat);
      } else {
        next(status);
      }
    };
    read_pb_->Run(handler);
  }

 private:
  Stream *stream_;
  std::string *data_;
  hadoop::hdfs::DataTransferEncryptorMessageProto resp_;
  std::unique_ptr<continuation::Continuation> read_pb_;
};

template <class Stream>
template <class Handler>
void DataTransferSaslStream<Stream>::Handshake(const Handler &next) {
  using hadoop::hdfs::DataTransferEncryptorMessageProto;
  struct State {
    int magic_number;
    DataTransferEncryptorMessageProto request0;
    std::string response0;
    DataTransferEncryptorMessageProto request1;
    std::string response1;
    std::shared_ptr<Stream> stream;
  };
  auto s = std::make_shared<State>();
  s->stream = stream_;
  s->magic_number = htonl(kDataTransferSasl);
  DataTransferSaslStreamUtil::PrepareInitialHandshake(&s->request0);

  auto prog = continuation::Write(stream_, asio::buffer(reinterpret_cast<const char*>(&s->magic_number), sizeof(s->magic_number)))
          >>= WriteDelimitedPBMessage(stream_.get(), &s->request0)
          >>= ReadSaslMessageContinuation(stream_.get(), &s->response0)
          >>= AuthenticatorContinuation(&authenticator_, &options_, &s->response0, &s->request1)
          >>= WriteDelimitedPBMessage(stream_.get(), &s->request1)
          >>= ReadSaslMessageContinuation(stream_.get(), &s->response1);

  // TODO: Check whether the server and the client matches the QOP

  auto m = std::shared_ptr<decltype(prog)>(new decltype(prog)(std::move(prog)));
  m->Run([m,s,next](const Status &status) {
      next(status);
    });
}

}

#endif
