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
#include "bindings.h"

#include "me_haohui_libhdfspp_NativeRpcEngine.h"

#include "common/wrapper.h"
#include "common/util.h"
#include "rpc/rpc_engine.h"

#include <asio/ip/tcp.hpp>

using namespace ::hdfs;
using ::asio::ip::tcp;

JNIEXPORT jlong JNICALL
Java_me_haohui_libhdfspp_NativeRpcEngine_create(JNIEnv *env, jclass, jlong io_service_handle,
                                                jbyteArray jclient_name, jbyteArray jprotocol,
                                                jint version) {
  IoServiceImpl *io_service = reinterpret_cast<IoServiceImpl*>(io_service_handle);

  RpcEngine *engine = new RpcEngine(&io_service->io_service(), ReadByteString(env, jclient_name),
                                    ReadByteString(env, jprotocol).c_str(), version);
  return reinterpret_cast<uintptr_t>(engine);
}

JNIEXPORT jbyteArray JNICALL
Java_me_haohui_libhdfspp_NativeRpcEngine_connect(JNIEnv *env, jclass, jlong handle,
                                                 jstring jhost, jint port) {
  RpcEngine *self = reinterpret_cast<RpcEngine*>(handle);
  const char *host = env->GetStringUTFChars(jhost, nullptr);
  tcp::endpoint ep(asio::ip::address::from_string(host), port);
  Status status = self->Connect(ep);
  env->ReleaseStringUTFChars(jhost, host);
  return ToJavaStatusRep(env, status);
}

JNIEXPORT void JNICALL
Java_me_haohui_libhdfspp_NativeRpcEngine_startReadLoop(JNIEnv *, jclass, jlong handle) {
  RpcEngine *self = reinterpret_cast<RpcEngine*>(handle);
  self->StartReadLoop();
}

JNIEXPORT jbyteArray JNICALL
Java_me_haohui_libhdfspp_NativeRpcEngine_rpc(JNIEnv *env, jclass, jlong handle, jbyteArray jmethod,
                                             jbyteArray request, jobjectArray jstatus) {
  RpcEngine *self = reinterpret_cast<RpcEngine*>(handle);
  auto response = std::make_shared<std::string>();
  Status stat = self->RawRpc(ReadByteString(env, jmethod),
                             std::move(ReadByteString(env, request)), response);
  if (!stat.ok()) {
    SetStatusArray(env, jstatus, stat);
    return nullptr;
  }
  jbyteArray jresp = env->NewByteArray(response->size());
  void *b = env->GetPrimitiveArrayCritical(jresp, nullptr);
  memcpy(b, response->c_str(), response->size());
  env->ReleasePrimitiveArrayCritical(jresp, b, 0);
  return jresp;
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeRpcEngine_destroy(JNIEnv *, jclass, jlong handle) {
  delete reinterpret_cast<RpcEngine*>(handle);
}
