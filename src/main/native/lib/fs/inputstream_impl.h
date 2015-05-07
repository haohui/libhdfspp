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

#include "reader/block_reader.h"

#include "common/continuation/asio.h"
#include "common/continuation/protobuf.h"

#include <functional>
#include <future>

namespace hdfs {

struct InputStreamImpl::HandshakeContinuation : continuation::Continuation {
  typedef RemoteBlockReader<::asio::ip::tcp::socket> Reader;
  HandshakeContinuation(Reader *reader, const std::string &client_name,
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

  virtual void Run(const Next& next) override {
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
struct InputStreamImpl::ReadBlockContinuation : continuation::Continuation {
  typedef RemoteBlockReader<::asio::ip::tcp::socket> Reader;
  ReadBlockContinuation(Reader *reader, MutableBufferSequence buffer,
                 size_t *transferred)
      : reader_(reader)
      , buffer_(buffer)
      , buffer_size_(asio::buffer_size(buffer))
      , transferred_(transferred)
  {}

  virtual void Run(const Next& next) override {
    *transferred_ = 0;
    next_ = next;
    OnReadData(Status::OK(), 0);
  }

 private:
  Reader *reader_;
  MutableBufferSequence buffer_;
  const size_t buffer_size_;
  size_t *transferred_;
  std::function<void(const Status &)> next_;

  void OnReadData(const Status &status, size_t transferred) {
    using std::placeholders::_1;
    using std::placeholders::_2;
    *transferred_ += transferred;
    if (!status.ok()) {
      next_(status);
    } else if (*transferred_ >= buffer_size_) {
      next_(status);
    } else {
      reader_->async_read_some(
          asio::buffer(buffer_ + *transferred_, buffer_size_ - *transferred_),
          std::bind(&ReadBlockContinuation::OnReadData, this, _1, _2));
    }
  }
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
        return p.offset() <= offset && offset < p.offset() + p.b().numbytes();
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
      std::min<uint64_t>(it->b().numbytes() - offset_within_block, asio::buffer_size(buffers));

  struct State {
    std::unique_ptr<tcp::socket> conn;
    std::shared_ptr<RemoteBlockReader<tcp::socket> > reader;
    LocatedBlockProto block;
    std::vector<tcp::endpoint> endpoints;
    size_t transferred;
  };

  auto m = continuation::Pipeline<State>::Create();
  auto &s = m->state();
  s.conn.reset(new tcp::socket(fs_->rpc_engine().io_service()));
  s.reader = std::make_shared<RemoteBlockReader<tcp::socket> >(BlockReaderOptions(), s.conn.get());
  s.block = *it;
  for (auto &loc : it->locs()) {
    auto datanode = loc.id();
    s.endpoints.push_back(tcp::endpoint(ip::address::from_string(datanode.ipaddr()), datanode.xferport()));
  }

  m->Push(continuation::Connect(s.conn.get(), s.endpoints.begin(), s.endpoints.end()))
      .Push(new HandshakeContinuation(s.reader.get(), fs_->rpc_engine().client_name(), nullptr,
                                   &s.block.b(), size_within_block, offset_within_block))
      .Push(new ReadBlockContinuation<::asio::mutable_buffers_1>(
          s.reader.get(), asio::buffer(buffers, size_within_block), &s.transferred));

  m->Run([handler](const Status &status, const State &state) {
      handler(status, state.transferred);
    });
}

}

#endif
