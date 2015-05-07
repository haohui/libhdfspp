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
#include "me_haohui_libhdfspp_NativeTcpConnection.h"
#include "common/wrapper.h"

#include <asio/ip/tcp.hpp>

using namespace ::hdfs;
using ::asio::ip::tcp;

JNIEXPORT jlong JNICALL Java_me_haohui_libhdfspp_NativeTcpConnection_create(JNIEnv *, jclass, jlong io_service_handle) {
  IoServiceImpl *io_service = reinterpret_cast<IoServiceImpl*>(io_service_handle);
  tcp::socket *sock = new tcp::socket(io_service->io_service());
  return reinterpret_cast<uintptr_t>(sock);
}

JNIEXPORT jbyteArray JNICALL Java_me_haohui_libhdfspp_NativeTcpConnection_connect(JNIEnv *env, jclass, jlong handle, jstring jhost, jint port) {
  tcp::socket *self = reinterpret_cast<tcp::socket*>(handle);
  const char *host = env->GetStringUTFChars(jhost, nullptr);
  tcp::endpoint ep(asio::ip::address::from_string(host), port);
  asio::error_code ec;
  self->connect(ep, ec);
  env->ReleaseStringUTFChars(jhost, host);
  if (!ec) {
    return nullptr;
  }
  assert (false && "Unimplemented");
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeTcpConnection_disconnect(JNIEnv *, jclass, jlong handle) {
  tcp::socket *self = reinterpret_cast<tcp::socket*>(handle);
  ::asio::error_code ec;
  self->close(ec);
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeTcpConnection_destroy(JNIEnv *, jclass, jlong handle) {
  tcp::socket *self = reinterpret_cast<tcp::socket*>(handle);
  delete self;
}
