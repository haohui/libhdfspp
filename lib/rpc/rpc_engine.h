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
#ifndef LIB_RPC_ENGINE_H_
#define LIB_RPC_ENGINE_H_

#include "libhdfs++/status.h"

#include <google/protobuf/message_lite.h>

#include <asio/ip/tcp.hpp>

#include <atomic>
#include <memory>
#include <vector>

namespace hdfs {

class RpcEngine;

struct RpcRequestBase {
  const std::string method_name;
  const std::unique_ptr<::google::protobuf::MessageLite> req;
  RpcRequestBase(const char *method_name,
                 std::unique_ptr<::google::protobuf::MessageLite> &&req);
  virtual ~RpcRequestBase();
};

class RpcConnection {
 public:
  typedef ::asio::ip::tcp::socket NextLayer;
  RpcConnection(RpcEngine *engine);
  template <class Handler>
  void Handshake(const Handler &handler);
  template <class Handler>
  void Connect(const ::asio::ip::tcp::endpoint &server, const Handler &handler);
  void Shutdown();

  template <class Handler>
  void AsyncRpc(const std::shared_ptr<RpcRequestBase> &req,
                const std::shared_ptr<::google::protobuf::MessageLite> &resp,
                const Handler &handler);
  NextLayer &next_layer()
  { return next_layer_; }

 private:
  RpcEngine * const engine_;
  enum {
    kCallIdAuthorizationFailed = -1,
    kCallIdInvalid = -2,
    kCallIdConnectionContext = -3,
    kPingCallId = -4
  };
  NextLayer next_layer_;

  struct RpcSend;
  struct RpcRecv;

  void PrepareHandshakePacket(std::string *result);
};

class RpcEngine {
 public:
  enum {
    kRpcVersion = 9
  };

  RpcEngine(::asio::io_service *io_service,
            const char *client_name,
            const char *protocol_name, int protocol_version);

  template <class Handler>
  void AsyncRpc(const char *method_name, const ::google::protobuf::MessageLite &req,
                const Handler &&handler);

  Status Connect(const ::asio::ip::tcp::endpoint &server);
  void Shutdown();

  int NextCallId()
  { return ++call_id_; }

  const std::string &client_name() const { return client_name_; }
  const std::string &protocol_name() const { return protocol_name_; }
  int protocol_version() const { return protocol_version_; }
  RpcConnection &connection() { return conn_; }
  ::asio::io_service *io_service() { return io_service_; }
 private:
  ::asio::io_service *io_service_;
  const std::string client_name_;
  const std::string protocol_name_;
  const int protocol_version_;
  std::atomic_int call_id_;
  RpcConnection conn_;

  std::vector<std::shared_ptr<RpcRequestBase> > requests_;
  struct AcquireConnection;
};

}

#include "impl/rpc_connection.h"

#endif
