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

namespace hdfs {

namespace pb = ::google::protobuf;
namespace pbio = ::google::protobuf::io;

using namespace ::hadoop::common;

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

void RpcConnection::RpcSend::SerializeRpcRequest() {
  RpcEngine *engine = parent_->engine_;
  RpcRequestHeaderProto h;
  h.set_rpckind(RPC_PROTOCOL_BUFFER);
  h.set_rpcop(RpcRequestHeaderProto::RPC_FINAL_PACKET);
  h.set_callid(engine->NextCallId());
  h.set_clientid(engine->client_name());

  RequestHeaderProto req_header;
  req_header.set_methodname(req_->method_name);
  req_header.set_declaringclassprotocolname(engine->protocol_name());
  req_header.set_clientprotocolversion(engine->protocol_version());

  pbio::StringOutputStream ss(&buf_);
  pbio::CodedOutputStream os(&ss);
  ConstructRpcRequest(&os, {&h, &req_header, req_->req.get()});
}

Status RpcConnection::RpcRecv::ReserveBuffer() {
  len_ = ntohl(len_);
  data_.resize(len_);
  return Status::OK();
}

Status RpcConnection::RpcRecv::ParseRpcPacket() {
  pbio::ArrayInputStream ar(&data_[0], data_.size());
  pbio::CodedInputStream in(&ar);
  RpcResponseHeaderProto h;
  ReadDelimitedPBMessage(&in, &h);

  if (h.has_exceptionclassname()) {
    return Status::Exception(h.exceptionclassname().c_str(),
                             h.errormsg().c_str());
  }

  ReadDelimitedPBMessage(&in, resp_.get());
  return Status::OK();
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
