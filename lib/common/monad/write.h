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
#ifndef LIB_COMMON_MONAD_WRITE_H_
#define LIB_COMMON_MONAD_WRITE_H_

#include "monad.h"
#include "libhdfs++/status.h"

#include <asio/write.hpp>

namespace hdfs {
namespace monad {

template<class Stream, class ConstBufferSequence>
struct WriteMonad : Monad<> {
  WriteMonad(std::shared_ptr<Stream> stream, const ConstBufferSequence& buffer)
      : stream_(stream)
      , buffer_(buffer)
  {}

  WriteMonad(WriteMonad &&) = default;
  WriteMonad& operator=(WriteMonad&&) = default;

  template<class Next>
  void Run(const Next& next) {
    auto handler = [next, this](const asio::error_code &ec, size_t) {
      Status status;
      if (ec) {
        status = Status(ec.value(), ec.message().c_str());
      }
      next(status);
    };
    asio::async_write(*stream_, buffer_, handler);
  }

 private:
  std::shared_ptr<Stream> stream_;
  ConstBufferSequence buffer_;

  WriteMonad(const WriteMonad&) = delete;
  WriteMonad& operator=(const WriteMonad&) = delete;
};


template<class Stream, class ConstBufferSequence>
WriteMonad<Stream, ConstBufferSequence> Write(std::shared_ptr<Stream> stream, const ConstBufferSequence &buffer) {
  return WriteMonad<Stream, ConstBufferSequence>(stream, buffer);
}

}
}

#endif
