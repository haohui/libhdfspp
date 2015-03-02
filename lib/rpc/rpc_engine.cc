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

namespace hdfs {

RpcEngine::RpcEngine(::asio::io_service *io_service,
                     const char *client_name,
                     const char *protocol_name, int protocol_version)
    : io_service_(io_service)
    , client_name_(client_name)
    , protocol_name_(protocol_name)
    , protocol_version_(protocol_version)
    , call_id_(0)
    , conn_(this)
{}

void RpcEngine::Shutdown() {
  io_service_->post([this]() { conn_.Shutdown(); });
}

RpcRequestBase::RpcRequestBase(
    const char *method_name,
    std::unique_ptr<::google::protobuf::MessageLite> &&req)
    : method_name(method_name)
    , req(std::move(req))
{}

RpcRequestBase::~RpcRequestBase() {}

RpcConnection::RpcConnection(RpcEngine *engine)
    : engine_(engine)
    , next_layer_(*engine->io_service())
{}

}
