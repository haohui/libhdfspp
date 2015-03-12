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

#include "common/protobuf_util.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <iostream>

namespace hdfs {

namespace pb = ::google::protobuf;
namespace pbio = ::google::protobuf::io;

using namespace ::hadoop::common;
using namespace ::std::placeholders;

static void ConstructRpcRequest(pbio::CodedOutputStream *os,
                                std::initializer_list<const pb::MessageLite*> msgs) {
  int len = 0;
  std::for_each(msgs.begin(), msgs.end(),
                [&len](const pb::MessageLite *v) { len += DelimitedPBMessageSize(v); });
  int net_len = htonl(len);
  os->WriteRaw(reinterpret_cast<const char*>(&net_len), sizeof(net_len));
  uint8_t *buf = os->GetDirectBufferForNBytesAndAdvance(len);
  if (buf) {
    std::for_each(msgs.begin(), msgs.end(), [&buf](const pb::MessageLite *v) {
        buf = pbio::CodedOutputStream::WriteVarint32ToArray(v->ByteSize(), buf);
        buf = v->SerializeWithCachedSizesToArray(buf);
    });
  } else {
    std::for_each(msgs.begin(), msgs.end(), [os, &buf](const pb::MessageLite *v) {
        os->WriteVarint32(v->ByteSize());
        v->SerializeWithCachedSizes(os);
    });
  }
}

RpcConnection::RequestBase::RequestBase(
    RpcConnection *parent,
    const char *method_name,
    const std::shared_ptr<::google::protobuf::MessageLite> &req,
    const std::shared_ptr<::google::protobuf::MessageLite> &resp)
    : method_name_(method_name)
    , req_(req)
    , resp_(resp)
    , call_id_(parent->engine_->NextCallId())
    , timeout_timer_(parent->io_service())
{}

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

std::shared_ptr<std::string> RpcConnection::SerializeRpcRequest(
    const std::shared_ptr<RequestBase> &req) {
  RpcRequestHeaderProto h;
  h.set_rpckind(RPC_PROTOCOL_BUFFER);
  h.set_rpcop(RpcRequestHeaderProto::RPC_FINAL_PACKET);
  h.set_callid(req->call_id_);
  h.set_clientid(engine_->client_name());

  RequestHeaderProto req_header;
  req_header.set_methodname(req->method_name_);
  req_header.set_declaringclassprotocolname(engine_->protocol_name());
  req_header.set_clientprotocolversion(engine_->protocol_version());

  auto res = std::make_shared<std::string>();
  pbio::StringOutputStream ss(res.get());
  pbio::CodedOutputStream os(&ss);
  ConstructRpcRequest(&os, {&h, &req_header, req->req_.get()});
  return res;
}

void RpcConnection::OnHandleRpcTimeout(const ::asio::error_code &ec,
		std::shared_ptr<RequestBase> req) {

	req->timeout_timer_.cancel();

	auto it = requests_on_fly_.find(req->call_id_);
	if (it != requests_on_fly_.end()) { // wait too long to get response
		//clean request waiting for response
		requests_on_fly_.erase(req->call_id_);
		std::cerr << "rpc request timed out" << std::endl;

		Status stat;
		if (!next_layer().is_open()) { // bad connection
			stat = Status::BadConnection(ec.message().c_str());
		} else {
			stat = Status::RpcTimeout(ec.message().c_str());
		}
		req->InvokeCallback(stat);
	}
}

void RpcConnection::OnHandleWrite(const ::asio::error_code &ec, size_t) {
  request_over_the_wire_.reset();
  if (ec) {
    // TODO: Current RPC has failed -- we should abandon the
    // connection and do proper clean up
    assert (false && "Unimplemented");
  }

  if (!pending_requests_.size()) {
    return;
  }

  std::shared_ptr<RequestBase> req = pending_requests_.front();
  pending_requests_.erase(pending_requests_.begin());
  requests_on_fly_[req->call_id_] = req;
  request_over_the_wire_ = req;

  // set the timeout for the RPC request
  req->timeout_timer_.expires_from_now(seconds_type(30));
  req->timeout_timer_.async_wait(
    		  std::bind(&RpcConnection::OnHandleRpcTimeout, this, ::asio::error_code(), req));

  std::shared_ptr<std::string> buf = SerializeRpcRequest(req);
  asio::async_write(next_layer(), asio::buffer(*buf),
                    std::bind(&RpcConnection::OnHandleWrite, this, _1, _2));
}

void RpcConnection::OnHandleRead(const ::asio::error_code &ec, size_t) {
  if (ec) {
    // TODO: Error in response
    assert (false && "Unimplemented");
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
  RpcResponseHeaderProto h;
  ReadDelimitedPBMessage(&in, &h);

  auto it = requests_on_fly_.find(h.callid());
  if (it == requests_on_fly_.end()) {
    // TODO: out of line RPC request
    assert (false && "Out of line request with unknown call id");
  }

  auto req = it->second;
  requests_on_fly_.erase(it);
  if (h.has_exceptionclassname()) {
    Status stat = Status::Exception(h.exceptionclassname().c_str(),
                                    h.errormsg().c_str());
    req->InvokeCallback(stat);
  } else {
    ReadDelimitedPBMessage(&in, req->resp_.get());
    req->InvokeCallback(Status::OK());
  }
}

void RpcConnection::PrepareHandshakePacket(std::string *result) {
  static const char kHandshakeHeader[] = {'h', 'r', 'p', 'c',
                                          RpcEngine::kRpcVersion, 0, 0};
  pbio::StringOutputStream ss(result);
  pbio::CodedOutputStream os(&ss);
  os.WriteRaw(kHandshakeHeader, sizeof(kHandshakeHeader));

  RpcRequestHeaderProto h;
  h.set_rpckind(RPC_PROTOCOL_BUFFER);
  h.set_rpcop(RpcRequestHeaderProto::RPC_FINAL_PACKET);
  h.set_callid(kCallIdConnectionContext);
  h.set_clientid(engine_->client_name());

  IpcConnectionContextProto handshake;
  handshake.set_protocol(engine_->protocol_name());
  ConstructRpcRequest(&os, {&h, &handshake});
}

void RpcConnection::Shutdown() {
  next_layer_.close();
}

}
