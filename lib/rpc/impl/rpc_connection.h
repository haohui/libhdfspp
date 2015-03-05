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

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <asio/connect.hpp>
#include <asio/write.hpp>

namespace hdfs {

class RpcConnection::RequestBase {
 public:
  const std::string method_name_;
  const std::shared_ptr<::google::protobuf::MessageLite> req_;
  const std::shared_ptr<::google::protobuf::MessageLite> resp_;
  const int call_id_;
  // ::asio::deadline_timer timeout_timer_;
  RequestBase(RpcConnection *parent,
              const char *method_name,
              const std::shared_ptr<::google::protobuf::MessageLite> &req,
              const std::shared_ptr<::google::protobuf::MessageLite> &resp);
  virtual void InvokeCallback(const Status &status) = 0;
  virtual ~RequestBase();
};

template <class Handler>
class RpcConnection::Request : public RequestBase {
 public:
  Request(RpcConnection *parent,
          const char *method_name,
          const std::shared_ptr<::google::protobuf::MessageLite> &req,
          const std::shared_ptr<::google::protobuf::MessageLite> &resp,
          const Handler &handler)
      : RequestBase(parent, method_name, req, resp)
      , handler_(handler)
  {}
  void InvokeCallback(const Status &status) { handler_(status); }
  Handler handler_;
};

template <class Handler>
void RpcConnection::Connect(const ::asio::ip::tcp::endpoint &server, const Handler &handler) {
  next_layer_.async_connect(server, [handler](const ::asio::error_code &ec) {
      handler(ToStatus(ec));
    });
}

template <class Handler>
void RpcConnection::Handshake(const Handler &handler) {
  auto handshake_packet = std::make_shared<std::string>();
  PrepareHandshakePacket(handshake_packet.get());

  ::asio::async_write(next_layer(), asio::buffer(*handshake_packet),
                      [handshake_packet, handler](const ::asio::error_code &ec, size_t)
                      { handler(ToStatus(ec)); });
}

template <class Handler>
void RpcConnection::AsyncRpc(const char *method_name,
                             const std::shared_ptr<::google::protobuf::MessageLite> &req,
                             const std::shared_ptr<::google::protobuf::MessageLite> &resp,
                             const Handler &handler) {
  auto r = new Request<Handler>(this, method_name, req, resp, handler);
  pending_requests_.push_back(std::shared_ptr<RequestBase>(r));
  StartWriteLoop();
}

}

#endif
