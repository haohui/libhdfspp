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
#ifndef LIB_RPC_IMPL_RPC_CONNECTION_H_
#define LIB_RPC_IMPL_RPC_CONNECTION_H_

#include "RpcHeader.pb.h"

#include "common/util.h"
#include "common/monad/monad.h"
#include "common/monad/bind.h"
#include "common/monad/asio.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <asio/connect.hpp>

namespace hdfs {

struct RpcConnection::RpcSend : monad::Monad<> {
  RpcSend(RpcConnection *parent, const std::shared_ptr<RpcRequestBase> &req)
      : parent_(parent)
      , req_(req)
  {}

  template<class Next>
  void Run(const Next& next) {
    SerializeRpcRequest();
    auto asio_buf_op = [this]() { return asio::buffer(buf_); };
    typedef monad::WriteMonad<NextLayer, decltype(asio_buf_op)> WriteOperation;
    auto write_monad = std::make_shared<WriteOperation>(&parent_->next_layer(), asio_buf_op);
    write_monad->Run([write_monad,next](const Status &status) { next(status); });
  }

  HDFS_MONAD_DEFAULT_MOVE_CONSTRUCTOR(RpcSend);
  HDFS_MONAD_DISABLE_COPY_CONSTRUCTOR(RpcSend);

 private:
  RpcConnection *parent_;
  const std::string method_name_;
  std::string buf_;
  std::shared_ptr<RpcRequestBase> req_;

  typedef ::google::protobuf::MessageLite Message;
  void SerializeRpcRequest();
};


struct RpcConnection::RpcRecv : monad::Monad<> {
  RpcRecv(RpcConnection *parent, const std::shared_ptr<::google::protobuf::MessageLite> &resp)
      : parent_(parent)
      , resp_(resp)
  {}

  template<class Next>
  void Run(const Next& next) {
    len_ = 0;
    auto prog = monad::Read(&parent_->next_layer(),
                            ::asio::buffer(reinterpret_cast<char*>(&len_), sizeof(len_)))
            >>= monad::Inline(std::bind(&RpcRecv::ReserveBuffer, this))
            >>= monad::LateBindRead(&parent_->next_layer(),
                                    [this]() { return ::asio::buffer(data_); })
            >>= monad::Inline(std::bind(&RpcRecv::ParseRpcPacket, this));

    auto m = std::shared_ptr<decltype(prog)>(new decltype(prog)(std::move(prog)));
    m->Run([m,next](const Status &status) { next(status); });
  }

  HDFS_MONAD_DEFAULT_MOVE_CONSTRUCTOR(RpcRecv);
  HDFS_MONAD_DISABLE_COPY_CONSTRUCTOR(RpcRecv);

 private:
  RpcConnection *parent_;
  std::shared_ptr<::google::protobuf::MessageLite> resp_;
  int len_;
  std::vector<char> data_;
  Status ReserveBuffer();
  Status ReceiveRpcPacket();
  Status ParseRpcPacket();
};

template <class Handler>
void RpcConnection::Connect(const ::asio::ip::tcp::endpoint &server, const Handler &handler) {
  next_layer_.async_connect(server, [handler](const ::asio::error_code &ec) {
      handler(ToStatus(ec));
    });
}

template <class Handler>
void RpcConnection::Handshake(const Handler &handler) {
  typedef monad::EarlyBindWriteOperation<NextLayer, ::asio::const_buffers_1> WriteOperation;
  class State {
   public:
    std::string payload_;
    WriteOperation monad_;
    State(std::string &&payload, NextLayer *next_layer)
        : payload_(payload)
        , monad_(std::move(monad::Write(next_layer, asio::buffer(payload_))))
    {}
  };

  std::string payload;
  PrepareHandshakePacket(&payload);
  auto s = std::make_shared<State>(std::move(payload), &next_layer_);
  s->monad_.Run([s, handler](const Status &status) { handler(status); });
}

template <class Handler>
void RpcConnection::AsyncRpc(const std::shared_ptr<RpcRequestBase> &req,
                             const std::shared_ptr<::google::protobuf::MessageLite> &resp,
                             const Handler &handler) {
  auto prog = RpcSend(this, req) >>= RpcRecv(this, resp);
  auto m = std::shared_ptr<decltype(prog)>(new decltype(prog)(std::move(prog)));
  m->Run([m,handler](const Status &status) { handler(status); });
}

}

#endif
