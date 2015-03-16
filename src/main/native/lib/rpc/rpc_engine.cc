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
#include "rpc_engine.h"

#include <future>

namespace hdfs {

RpcEngine::RpcEngine(::asio::io_service *io_service,
                     const std::string &client_name,
                     const char *protocol_name, int protocol_version)
    : io_service_(io_service)
    , client_name_(client_name)
    , protocol_name_(protocol_name)
    , protocol_version_(protocol_version)
    , call_id_(0)
    , conn_(this)
{}

Status RpcEngine::Connect(const ::asio::ip::tcp::endpoint &server) {
  std::promise<Status> stat;
  conn_.Connect(server, [this,&stat](const Status &status) {
      if (!status.ok()) {
        stat.set_value(status);
        return;
      }
      conn_.Handshake([this,&stat](const Status &status) { stat.set_value(status); });
    });
  return stat.get_future().get();
}

void RpcEngine::StartReadLoop() {
  conn_.StartReadLoop();
}

void RpcEngine::Shutdown() {
  io_service_->post([this]() { conn_.Shutdown(); });
}

Status RpcEngine::Rpc(const std::string &method_name,
                      const ::google::protobuf::MessageLite *req,
                      const std::shared_ptr<::google::protobuf::MessageLite> &resp) {
  std::promise<Status> stat;
  AsyncRpc(method_name, req, resp, [&stat](const Status &status) {
      stat.set_value(status);
    });
  return stat.get_future().get();
}

Status RpcEngine::RawRpc(const std::string &method_name, const std::string &req,
                         std::shared_ptr<std::string> resp) {
  std::promise<Status> stat;
  conn_.AsyncRawRpc(method_name, req, resp, [&stat](const Status &status) {
      stat.set_value(status);
    });
  return stat.get_future().get();
}

}
