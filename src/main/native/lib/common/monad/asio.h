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

#include <asio/connect.hpp>
#include <asio/read.hpp>
#include <asio/write.hpp>
#include <asio/ip/tcp.hpp>

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

template<class Socket, class Iterator>
struct ConnectMonad : Monad<> {
  ConnectMonad(Socket *socket, Iterator begin, Iterator end, Iterator *connected_endpoint)
      : socket_(socket)
      , begin_(begin)
      , end_(end)
      , connected_endpoint_(connected_endpoint)
  {}

  HDFS_MONAD_DEFAULT_MOVE_CONSTRUCTOR(ConnectMonad);
  HDFS_MONAD_DISABLE_COPY_CONSTRUCTOR(ConnectMonad);

  template<class Next>
  void Run(const Next& next) {
    auto handler = [this,next](const asio::error_code &ec, Iterator it) {
      if (connected_endpoint_) {
        *connected_endpoint_ = it;
      }
      next(ToStatus(ec));
    };
    asio::async_connect(*socket_, begin_, end_, handler);
  }
 private:
  Socket *socket_;
  Iterator begin_;
  Iterator end_;
  Iterator *connected_endpoint_;
};

struct ResolveMonad : Monad<> {
  ResolveMonad(::asio::io_service *io_service,
               const std::string &host,
               const std::string &service)
      : resolver_(*io_service)
      , query_(host, service)
  {}

  HDFS_MONAD_DEFAULT_MOVE_CONSTRUCTOR(ResolveMonad);
  HDFS_MONAD_DISABLE_COPY_CONSTRUCTOR(ResolveMonad);

  template<class Next>
  void Run(const Next& next) {
    auto handler = [next](const asio::error_code &ec,
                          asio::ip::tcp::resolver::iterator it)
                   { next(ToStatus(ec), it); };
    resolver_.async_resolve(query_, handler);
  }

 private:
  ::asio::ip::tcp::resolver resolver_;
  ::asio::ip::tcp::resolver::query query_;
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
static inline WriteMonad<Stream, BufferOp> LateBindWrite(Stream *stream, const BufferOp &get_buffer) {
  return WriteMonad<Stream, BufferOp>(stream, get_buffer);
}

template<class Stream, class ConstBufferSequence>
EarlyBindWriteOperation<Stream, ConstBufferSequence>
static inline Write(Stream *stream, const ConstBufferSequence &buffer) {
  return LateBindWrite(stream, detail::EarlyBufferBinder<ConstBufferSequence>(buffer));
}

template<class Stream, class BufferOp>
static inline ReadMonad<Stream, BufferOp> LateBindRead(Stream *stream, const BufferOp &get_buffer) {
  return ReadMonad<Stream, BufferOp>(stream, get_buffer);
}

template<class Stream, class MutableBufferSequence>
EarlyBindReadOperation<Stream, MutableBufferSequence>
static inline Read(Stream *stream, const MutableBufferSequence &buffer) {
  return LateBindRead(stream, detail::EarlyBufferBinder<MutableBufferSequence>(buffer));
}

static inline ResolveMonad &&Resolve(::asio::io_service *io_service, const std::string &host,
                    const std::string &service) {
  return std::move(Resolve(io_service, host, service));
}

template<class Socket, class Iterator>
ConnectMonad<Socket, Iterator>
static inline Connect(Socket *socket, Iterator begin, Iterator end) {
  return ConnectMonad<Socket, Iterator>(socket, begin, end, nullptr);
}

}
}

#endif
