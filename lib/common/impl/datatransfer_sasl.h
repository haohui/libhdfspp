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
#ifndef COMMON_IMPL_DATATRANSFER_SASL_H_
#define COMMON_IMPL_DATATRANSFER_SASL_H_

#include "libhdfs++/options.h"

#include "common/monad/monad.h"
#include "common/monad/asio.h"
#include "common/protobuf_util.h"

#include "datatransfer.pb.h"

namespace hdfs {

namespace DataTransferSaslStreamUtil {

typedef hadoop::hdfs::DataTransferEncryptorMessageProto SaslMessage;

Status ConvertToStatus(const SaslMessage *msg, std::string *payload);
void PrepareInitialHandshake(SaslMessage *msg);

}

template<class Stream>
struct DataTransferSaslStream<Stream>::AuthenticatorMonad : monad::Monad<> {
  AuthenticatorMonad(DigestMD5Authenticator *authenticator,
                     BlockReaderOptions *options,
                     const std::string *request,
                     hadoop::hdfs::DataTransferEncryptorMessageProto *msg)
      : authenticator_(authenticator)
      , options_(options)
      , request_(request)
      , msg_(msg)
  {}

  template<class Next>
  void Run(const Next& next) {
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

  HDFS_MONAD_DEFAULT_MOVE_CONSTRUCTOR(AuthenticatorMonad);
  HDFS_MONAD_DISABLE_COPY_CONSTRUCTOR(AuthenticatorMonad);
 private:
  DigestMD5Authenticator *authenticator_;
  BlockReaderOptions *options_;
  const std::string *request_;
  hadoop::hdfs::DataTransferEncryptorMessageProto *msg_;
};

template<class Stream>
struct DataTransferSaslStream<Stream>::ReadSaslMessageMonad : monad::Monad<> {
  ReadSaslMessageMonad(Stream *stream, std::string *data)
      : stream_(stream)
      , data_(data)
  {}
  HDFS_MONAD_DEFAULT_MOVE_CONSTRUCTOR(ReadSaslMessageMonad);
  HDFS_MONAD_DISABLE_COPY_CONSTRUCTOR(ReadSaslMessageMonad);

  template<class Next>
  void Run(const Next& next) {
    auto read_pb = std::make_shared<ReadMonad>(stream_, &resp_);
    auto handler = [this,read_pb, next](const Status &status) {
      if (status.ok()) {
        Status new_stat = DataTransferSaslStreamUtil::ConvertToStatus(&resp_, data_);
        next(new_stat);
      } else {
        next(status);
      }
    };
    read_pb->Run(handler);
  }

 private:
  Stream *stream_;
  std::string *data_;
  hadoop::hdfs::DataTransferEncryptorMessageProto resp_;
  typedef ReadPBMessageMonad<Stream, hadoop::hdfs::DataTransferEncryptorMessageProto, 1024> ReadMonad;
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

  auto prog = monad::Write(stream_, asio::buffer(reinterpret_cast<const char*>(&s->magic_number), sizeof(s->magic_number)))
          >>= WriteDelimitedPBMessage(stream_.get(), &s->request0)
          >>= ReadSaslMessageMonad(stream_.get(), &s->response0)
          >>= AuthenticatorMonad(&authenticator_, &options_, &s->response0, &s->request1)
          >>= WriteDelimitedPBMessage(stream_.get(), &s->request1)
          >>= ReadSaslMessageMonad(stream_.get(), &s->response1);

  // TODO: Check whether the server and the client matches the QOP

  auto m = std::shared_ptr<decltype(prog)>(new decltype(prog)(std::move(prog)));
  m->Run([m,s,next](const Status &status) {
      next(status);
    });
}

}

#endif
