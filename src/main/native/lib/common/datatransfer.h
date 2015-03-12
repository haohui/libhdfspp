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
#ifndef COMMON_DATA_TRANSFER_H_
#define COMMON_DATA_TRANSFER_H_

#include "sasl_authenticator.h"
#include "libhdfs++/options.h"

#include <asio/read.hpp>
#include <memory>

namespace hdfs {

enum {
  kDataTransferVersion = 28,
  kDataTransferSasl = 0xdeadbeef,
};

enum Operation {
  kWriteBlock = 80,
  kReadBlock  = 81,
};

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
  struct ReadSaslMessageMonad;
  struct AuthenticatorMonad;
};

}

#include "impl/datatransfer_sasl.h"

#endif
