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
  int call_id() const { return call_id_; }
  ::asio::deadline_timer &timer() { return timer_; }
  const std::string &payload() const { return payload_; }

  virtual ~RequestBase();
  virtual void OnResponseArrived(
      ::google::protobuf::io::CodedInputStream *is,
      const Status &status) = 0;

 protected:
  int call_id_;
  ::asio::deadline_timer timer_;
  std::string payload_;

  RequestBase(RpcConnection *parent, const std::string &method_name,
              const std::string &request);
  RequestBase(RpcConnection *parent, const std::string &method_name,
              const ::google::protobuf::MessageLite *request);
};

template <class Handler>
class RpcConnection::Request : public RequestBase {
 public:
  Request(RpcConnection *parent, const std::string &method_name,
          const std::string &request,
          Handler &&handler)
      : RequestBase(parent, method_name, request)
      , handler_(std::move(handler))
  {}

  Request(RpcConnection *parent, const std::string &method_name,
          const ::google::protobuf::MessageLite *request,
          Handler &&handler)
      : RequestBase(parent, method_name, request)
      , handler_(std::move(handler))
  {}

  virtual void OnResponseArrived(::google::protobuf::io::CodedInputStream *is,
                                 const Status &status) override
  { handler_(is, status); }

  Handler handler_;
};

template <class Iterator, class Handler>
void RpcConnection::Connect(Iterator begin, Iterator end, const Handler &handler) {
  ::asio::async_connect(next_layer_, begin, end,
                        [handler](const ::asio::error_code &ec, Iterator) {
                          handler(ToStatus(ec));
                        });
}

template <class Handler>
void RpcConnection::Handshake(const Handler &handler) {
  auto handshake_packet = PrepareHandshakePacket();

  ::asio::async_write(next_layer(), asio::buffer(*handshake_packet),
                      [handshake_packet, handler](const ::asio::error_code &ec, size_t)
                      { handler(ToStatus(ec)); });
}


template <class Handler>
void RpcConnection::AsyncRpc(const std::string &method_name,
                             const ::google::protobuf::MessageLite *req,
                             std::shared_ptr<::google::protobuf::MessageLite> resp,
                             const Handler &handler) {
  auto wrapped_handler = [resp,handler](::google::protobuf::io::CodedInputStream *is, const Status &status) {
    if (status.ok()) {
      ReadDelimitedPBMessage(is, resp.get());
    }
    handler(status);
  };

  auto r = new Request<decltype(wrapped_handler)>(this, method_name, req, std::move(wrapped_handler));
  pending_requests_.push_back(std::shared_ptr<RequestBase>(r));
  StartWriteLoop();
}

template <class Handler>
void RpcConnection::AsyncRawRpc(const std::string &method_name,
                                const std::string &req,
                                std::shared_ptr<std::string> resp,
                                const Handler &handler) {
  auto wrapped_handler = [this,resp,handler](::google::protobuf::io::CodedInputStream *is, const Status &status) {
    if (status.ok()) {
      uint32_t size = 0;
      is->ReadVarint32(&size);
      auto limit = is->PushLimit(size);
      is->ReadString(resp.get(), limit);
      is->PopLimit(limit);
    }
    handler(status);
  };

  auto r = new Request<decltype(wrapped_handler)>(this, method_name, req, std::move(wrapped_handler));
  pending_requests_.push_back(std::shared_ptr<RequestBase>(r));
  StartWriteLoop();
}

}

#endif
