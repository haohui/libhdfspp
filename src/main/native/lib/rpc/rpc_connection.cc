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

#include "RpcHeader.pb.h"
#include "ProtobufRpcEngine.pb.h"
#include "IpcConnectionContext.pb.h"

#include "common/util.h"

#include <asio/read.hpp>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace hdfs {

namespace pb = ::google::protobuf;
namespace pbio = ::google::protobuf::io;

using namespace ::hadoop::common;
using namespace ::std::placeholders;

static void
ConstructPacket(std::string *res,
                std::initializer_list<const pb::MessageLite*> headers,
                const std::string *request) {
  int len = 0;
  std::for_each(headers.begin(), headers.end(),
                [&len](const pb::MessageLite *v) { len += DelimitedPBMessageSize(v); });
  if (request) {
    len += pbio::CodedOutputStream::VarintSize32(request->size()) + request->size();
  }

  int net_len = htonl(len);
  res->reserve(res->size() + sizeof(net_len) + len);

  pbio::StringOutputStream ss(res);
  pbio::CodedOutputStream os(&ss);
  os.WriteRaw(reinterpret_cast<const char*>(&net_len), sizeof(net_len));

  uint8_t *buf = os.GetDirectBufferForNBytesAndAdvance(len);
  assert (buf && "Cannot allocate memory");

  std::for_each(headers.begin(), headers.end(), [&buf](const pb::MessageLite *v) {
      buf = pbio::CodedOutputStream::WriteVarint32ToArray(v->ByteSize(), buf);
      buf = v->SerializeWithCachedSizesToArray(buf);
    });

  if (request) {
    buf = pbio::CodedOutputStream::WriteVarint32ToArray(request->size(), buf);
    buf = os.WriteStringToArray(*request, buf);
  }
}

static void SetRequestHeader(RpcEngine *engine, int call_id,
                              const std::string &method_name,
                              RpcRequestHeaderProto *rpc_header,
                              RequestHeaderProto *req_header) {
  rpc_header->set_rpckind(RPC_PROTOCOL_BUFFER);
  rpc_header->set_rpcop(RpcRequestHeaderProto::RPC_FINAL_PACKET);
  rpc_header->set_callid(call_id);
  rpc_header->set_clientid(engine->client_name());

  req_header->set_methodname(method_name);
  req_header->set_declaringclassprotocolname(engine->protocol_name());
  req_header->set_clientprotocolversion(engine->protocol_version());
}

RpcConnection::RequestBase::RequestBase(
    RpcConnection *parent,
    const std::string &method_name,
    const std::string &request)
    : call_id_(parent->engine_->NextCallId())
    , timer_(parent->io_service())
{
  RpcRequestHeaderProto rpc_header;
  RequestHeaderProto req_header;
  SetRequestHeader(parent->engine_, call_id_, method_name, &rpc_header, &req_header);
  ConstructPacket(&payload_, {&rpc_header, &req_header}, &request);
}

RpcConnection::RequestBase::RequestBase(
    RpcConnection *parent,
    const std::string &method_name,
    const pb::MessageLite *request)
    : call_id_(parent->engine_->NextCallId())
    , timer_(parent->io_service())
{
  RpcRequestHeaderProto rpc_header;
  RequestHeaderProto req_header;
  SetRequestHeader(parent->engine_, call_id_, method_name, &rpc_header, &req_header);
  ConstructPacket(&payload_, {&rpc_header, &req_header, request}, nullptr);
}

RpcConnection::RequestBase::~RequestBase() {}

RpcConnection::ResponseState::ResponseState()
    : state(kReadLength)
    , length(0)
{}

RpcConnection::RpcConnection(RpcEngine *engine)
    : engine_(engine)
    , next_layer_(engine->io_service())
{}

::asio::io_service &RpcConnection::io_service() {
  return engine_->io_service();
}

void RpcConnection::OnHandleWrite(const ::asio::error_code &ec, size_t) {
  request_over_the_wire_.reset();
  if (ec) {
    // TODO: Current RPC has failed -- we should abandon the
    // connection and do proper clean up
    assert(false && "Unimplemented");
  }

  if (!pending_requests_.size()) {
    return;
  }

  std::shared_ptr<RequestBase> req = pending_requests_.front();
  pending_requests_.erase(pending_requests_.begin());
  requests_on_fly_[req->call_id()] = req;
  request_over_the_wire_ = req;

  // TODO: set the timeout for the RPC request

  asio::async_write(next_layer(), asio::buffer(req->payload()),
                    std::bind(&RpcConnection::OnHandleWrite, this, _1, _2));
}

void RpcConnection::OnHandleRead(const ::asio::error_code &ec, size_t) {
  switch (ec.value()) {
    case 0:
      // No errors
      break;
    case asio::error::operation_aborted:
      // The event loop has been shut down. Ignore the error.
      return;
    default:
      assert(false && "Unimplemented");
  }

  auto s = &response_state_;
  if (s->state == ResponseState::kReadLength) {
    s->state = ResponseState::kReadContent;
    auto buf = ::asio::buffer(reinterpret_cast<char*>(&s->length),
                              sizeof(s->length));
    asio::async_read(next_layer(), buf,
                     std::bind(&RpcConnection::OnHandleRead, this, _1, _2));

  } else if (s->state == ResponseState::kReadContent) {
    s->state = ResponseState::kParseResponse;
    s->length = ntohl(s->length);
    s->data.resize(s->length);
    asio::async_read(next_layer(), ::asio::buffer(s->data),
                     std::bind(&RpcConnection::OnHandleRead, this, _1, _2));

  } else if (s->state == ResponseState::kParseResponse) {
    s->state = ResponseState::kReadLength;
    HandleRpcResponse(s->data);
    s->data.clear();
    StartReadLoop();
  }
}

void RpcConnection::StartReadLoop() {
  io_service().post(std::bind(&RpcConnection::OnHandleRead, this, ::asio::error_code(), 0));
}

void RpcConnection::StartWriteLoop() {
  io_service().post([this]() {
      if (!request_over_the_wire_) {
        OnHandleWrite(::asio::error_code(), 0);
      }});
}

void RpcConnection::HandleRpcResponse(const std::vector<char> &data) {
  pbio::ArrayInputStream ar(&data[0], data.size());
  pbio::CodedInputStream in(&ar);
  in.PushLimit(data.size());
  RpcResponseHeaderProto h;
  ReadDelimitedPBMessage(&in, &h);

  auto it = requests_on_fly_.find(h.callid());
  if (it == requests_on_fly_.end()) {
    // TODO: out of line RPC request
    assert (false && "Out of line request with unknown call id");
  }

  auto req = it->second;
  requests_on_fly_.erase(it);
  Status stat;
  if (h.has_exceptionclassname()) {
    stat = Status::Exception(h.exceptionclassname().c_str(),
                             h.errormsg().c_str());
  }
  req->OnResponseArrived(&in, stat);
}

std::shared_ptr<std::string> RpcConnection::PrepareHandshakePacket() {
  static const char kHandshakeHeader[] = {'h', 'r', 'p', 'c',
                                          RpcEngine::kRpcVersion, 0, 0};
  auto res = std::make_shared<std::string>(kHandshakeHeader, sizeof(kHandshakeHeader));

  RpcRequestHeaderProto h;
  h.set_rpckind(RPC_PROTOCOL_BUFFER);
  h.set_rpcop(RpcRequestHeaderProto::RPC_FINAL_PACKET);
  h.set_callid(kCallIdConnectionContext);
  h.set_clientid(engine_->client_name());

  IpcConnectionContextProto handshake;
  handshake.set_protocol(engine_->protocol_name());
  ConstructPacket(res.get(), {&h, &handshake}, nullptr);
  return res;
}

void RpcConnection::Shutdown() {
  next_layer_.close();
}

}
