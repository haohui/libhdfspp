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
#include <asio/deadline_timer.hpp>

#include <atomic>
#include <memory>
#include <unordered_map>
#include <vector>

namespace hdfs {

class RpcEngine;

class RpcConnection {
 public:
  typedef ::asio::ip::tcp::socket NextLayer;
  RpcConnection(RpcEngine *engine);
  template <class Iterator, class Handler>
  void Connect(Iterator begin, Iterator end, const Handler &handler);
  template <class Handler>
  void Handshake(const Handler &handler);
  void Shutdown();

  template <class Handler>
  void AsyncRpc(const std::string &method_name,
                const ::google::protobuf::MessageLite *req,
                std::shared_ptr<::google::protobuf::MessageLite> resp,
                const Handler &handler);

  template <class Handler>
  void AsyncRawRpc(const std::string &method_name,
                   const std::string &request,
                   std::shared_ptr<std::string> resp,
                   const Handler &handler);

  NextLayer &next_layer()
  { return next_layer_; }

  void StartReadLoop();

 private:
  class RequestBase;
  template <class Handler>
  class Request;

  RpcEngine * const engine_;
  NextLayer next_layer_;
  enum {
    kCallIdAuthorizationFailed = -1,
    kCallIdInvalid = -2,
    kCallIdConnectionContext = -3,
    kCallIdPing = -4
  };

  struct ResponseState {
    enum {
      kReadLength,
      kReadContent,
      kParseResponse,
    } state;
    unsigned length;
    std::vector<char> data;
    ResponseState();
  };
  ResponseState response_state_;

  // The request being sent over the wire
  std::shared_ptr<RequestBase> request_over_the_wire_;
  // Requests to be sent over the wire
  std::vector<std::shared_ptr<RequestBase> > pending_requests_;
  // Requests that are waiting for responses
  std::unordered_map<int, std::shared_ptr<RequestBase> > requests_on_fly_;

  template <class Handler>
  void StartRpc(std::string &&request, const Handler &handler);

  ::asio::io_service &io_service();
  std::shared_ptr<std::string> PrepareHandshakePacket();
  static std::string SerializeRpcRequest(const std::string &method_name, const ::google::protobuf::MessageLite *req);
  void HandleRpcResponse(const std::vector<char> &data);
  void OnHandleWrite(const ::asio::error_code &ec, size_t transferred);
  void OnHandleRead(const ::asio::error_code &ec, size_t transferred);
  void StartWriteLoop();
};

class RpcEngine {
 public:
  enum {
    kRpcVersion = 9
  };

  RpcEngine(::asio::io_service *io_service,
            const std::string &client_name,
            const char *protocol_name, int protocol_version);

  template <class Handler>
  void AsyncRpc(const std::string &method_name,
                const ::google::protobuf::MessageLite *req,
                const std::shared_ptr<::google::protobuf::MessageLite> &resp,
                const Handler &handler) {
    conn_.AsyncRpc(method_name, req, resp, handler);
  }

  Status Rpc(const std::string &method_name,
             const ::google::protobuf::MessageLite *req,
             const std::shared_ptr<::google::protobuf::MessageLite> &resp);
  /**
   * Send raw bytes as RPC payload. This is intended to be used in JNI
   * bindings only.
   **/
  Status RawRpc(const std::string &method_name, const std::string &req,
                std::shared_ptr<std::string> resp);
  Status Connect(const ::asio::ip::tcp::endpoint &server);
  void StartReadLoop();
  void Shutdown();

  int NextCallId()
  { return ++call_id_; }

  const std::string &client_name() const { return client_name_; }
  const std::string &protocol_name() const { return protocol_name_; }
  int protocol_version() const { return protocol_version_; }
  RpcConnection &connection() { return conn_; }
  ::asio::io_service &io_service() { return *io_service_; }

  static std::string GetRandomClientName();
 private:
  ::asio::io_service *io_service_;
  const std::string client_name_;
  const std::string protocol_name_;
  const int protocol_version_;
  std::atomic_int call_id_;
  RpcConnection conn_;
};

}

#include "impl/rpc_connection.h"

#endif
