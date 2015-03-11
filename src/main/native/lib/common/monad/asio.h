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
#ifndef LIB_COMMON_MONAD_ASIO_H_
#define LIB_COMMON_MONAD_ASIO_H_

#include "monad.h"
#include "common/util.h"

#include "libhdfs++/status.h"

#include <asio/read.hpp>
#include <asio/write.hpp>

namespace hdfs {
namespace monad {

template<class Stream, class BufferOp>
struct ReadMonad : Monad<> {
  ReadMonad(Stream *stream, const BufferOp &get_buffer)
      : stream_(stream)
      , get_buffer_(get_buffer)
  {}

  HDFS_MONAD_DEFAULT_MOVE_CONSTRUCTOR(ReadMonad);
  HDFS_MONAD_DISABLE_COPY_CONSTRUCTOR(ReadMonad);

  template<class Next>
  void Run(const Next& next) {
    auto handler = [next, this](const asio::error_code &ec, size_t)
                   { next(ToStatus(ec)); };
    asio::async_read(*stream_, get_buffer_(), handler);
  }

 private:
  Stream *stream_;
  BufferOp get_buffer_;
};

template<class Stream, class BufferOp>
struct WriteMonad : Monad<> {
  WriteMonad(Stream *stream, const BufferOp& buffer)
      : stream_(stream)
      , get_buffer_(buffer)
  {}

  HDFS_MONAD_DEFAULT_MOVE_CONSTRUCTOR(WriteMonad);
  HDFS_MONAD_DISABLE_COPY_CONSTRUCTOR(WriteMonad);

  template<class Next>
  void Run(const Next& next) {
    auto handler = [next, this](const asio::error_code &ec, size_t)
                   { next(ToStatus(ec)); };
    asio::async_write(*stream_, get_buffer_(), handler);
  }

 private:
  Stream *stream_;
  BufferOp get_buffer_;
};

namespace detail {

template<class Buffer>
struct EarlyBufferBinder {
  explicit EarlyBufferBinder(const Buffer &buffer)
      : buffer_(buffer)
  {}
  const Buffer &operator()() const { return buffer_; }
  Buffer buffer_;
};

}

template<class Stream, class ConstBufferSequence>
using EarlyBindWriteOperation = WriteMonad<Stream, detail::EarlyBufferBinder<ConstBufferSequence> >;

template<class Stream, class MutableBufferSequence>
using EarlyBindReadOperation = ReadMonad<Stream, detail::EarlyBufferBinder<MutableBufferSequence> >;

template<class Stream, class BufferOp>
WriteMonad<Stream, BufferOp> LateBindWrite(Stream *stream, const BufferOp &get_buffer) {
  return WriteMonad<Stream, BufferOp>(stream, get_buffer);
}

template<class Stream, class ConstBufferSequence>
EarlyBindWriteOperation<Stream, ConstBufferSequence>
Write(Stream *stream, const ConstBufferSequence &buffer) {
  return LateBindWrite(stream, detail::EarlyBufferBinder<ConstBufferSequence>(buffer));
}

template<class Stream, class BufferOp>
ReadMonad<Stream, BufferOp> LateBindRead(Stream *stream, const BufferOp &get_buffer) {
  return ReadMonad<Stream, BufferOp>(stream, get_buffer);
}

template<class Stream, class MutableBufferSequence>
EarlyBindReadOperation<Stream, MutableBufferSequence>
Read(Stream *stream, const MutableBufferSequence &buffer) {
  return LateBindRead(stream, detail::EarlyBufferBinder<MutableBufferSequence>(buffer));
}

}
}

#endif
