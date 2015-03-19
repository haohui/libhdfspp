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
#ifndef FS_INPUTSTREAM_IMPL_H_
#define FS_INPUTSTREAM_IMPL_H_

#include "common/monad/asio.h"
#include "reader/block_reader.h"

#include <functional>
#include <future>

namespace hdfs {

struct InputStreamImpl::HandshakeMonad : monad::Monad<> {
  typedef RemoteBlockReader<::asio::ip::tcp::socket> Reader;
  HandshakeMonad(Reader *reader, const std::string &client_name,
               const hadoop::common::TokenProto *token,
               const hadoop::hdfs::ExtendedBlockProto *block,
               uint64_t length, uint64_t offset)
      : reader_(reader)
      , client_name_(client_name)
      , length_(length)
      , offset_(offset)
  {
    if (token) {
      token_.reset(new hadoop::common::TokenProto());
      token_->CheckTypeAndMergeFrom(*token);
    }
    block_.CheckTypeAndMergeFrom(*block);
  }

  HDFS_MONAD_DEFAULT_MOVE_CONSTRUCTOR(HandshakeMonad);
  HDFS_MONAD_DISABLE_COPY_CONSTRUCTOR(HandshakeMonad);

  template<class Next>
  void Run(const Next& next) {
    reader_->async_connect(client_name_, token_.get(), &block_, length_, offset_, next);
  }

 private:
  Reader *reader_;
  const std::string client_name_;
  std::unique_ptr<hadoop::common::TokenProto> token_;
  hadoop::hdfs::ExtendedBlockProto block_;
  uint64_t length_;
  uint64_t offset_;
};

template<class MutableBufferSequence>
struct InputStreamImpl::ReadBlockMonad : monad::Monad<> {
  typedef RemoteBlockReader<::asio::ip::tcp::socket> Reader;
  ReadBlockMonad(Reader *reader, MutableBufferSequence buffer,
                 size_t *transferred)
      : reader_(reader)
      , buffer_(buffer)
      , transferred_(transferred)
  {}

  HDFS_MONAD_DEFAULT_MOVE_CONSTRUCTOR(ReadBlockMonad);
  HDFS_MONAD_DISABLE_COPY_CONSTRUCTOR(ReadBlockMonad);

  template<class Next>
  void Run(const Next& next) {
    reader_->async_read_some(buffer_, [this,next](const Status &status, size_t transferred) {
        *transferred_ = transferred;
        next(status);
      });
  }

 private:
  Reader *reader_;
  MutableBufferSequence buffer_;
  size_t *transferred_;
};


template<class MutableBufferSequence, class Handler>
void InputStreamImpl::AsyncPreadSome(
    size_t offset, const MutableBufferSequence &buffers,
    const Handler &handler) {
  using ::hadoop::hdfs::LocatedBlockProto;
  namespace ip = ::asio::ip;
  using ::asio::ip::tcp;

  auto it = std::find_if(
      blocks_.begin(), blocks_.end(),
      [offset](const LocatedBlockProto &p) {
        return p.offset() <= offset && offset <= p.offset() + p.b().numbytes();
      });

  if (it == blocks_.end()) {
    handler(Status::InvalidArgument("Cannot find corresponding blocks"), 0);
    return;
  } else if (!it->locs_size()) {
    handler(Status::ResourceUnavailable("No datanodes available"), 0);
    return;
  }

  uint64_t offset_within_block = offset - it->offset();
  uint64_t size_within_block =
      std::min<uint64_t>(it->b().numbytes(), asio::buffer_size(buffers));

  struct State {
    tcp::socket conn;
    RemoteBlockReader<tcp::socket> reader;
    LocatedBlockProto block;
    std::vector<tcp::endpoint> endpoints;
    size_t transferred;

    State(::asio::io_service *io_service, const LocatedBlockProto &block)
        : conn(*io_service)
        , reader(BlockReaderOptions(), &conn)
        , block(block)
        , transferred(0)
    {
      for (auto & loc : block.locs()) {
        auto datanode = loc.id();
        endpoints.push_back(tcp::endpoint(ip::address::from_string(datanode.ipaddr()), datanode.xferport()));
      }
    }
  };

  auto s = std::make_shared<State>(&fs_->rpc_engine().io_service(), *it);
  auto prog = monad::Connect(&s->conn, s->endpoints.begin(), s->endpoints.end())
          >>= HandshakeMonad(&s->reader, fs_->rpc_engine().client_name(), nullptr,
                             &s->block.b(), size_within_block, offset_within_block)
          >>= ReadBlockMonad<::asio::mutable_buffers_1>(&s->reader, buffers, &s->transferred);

  auto m = std::shared_ptr<decltype(prog)>(new decltype(prog)(std::move(prog)));
  m->Run([m,s,this,handler](const Status &status) {
      handler(status, s->transferred);
    });
}

}

#endif
