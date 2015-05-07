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

#include "me_haohui_libhdfspp_NativeRemoteBlockReader.h"

#include "libhdfs++/hdfs.h"
#include "reader/block_reader.h"

#include <asio/ip/tcp.hpp>

using namespace ::hdfs;
using ::asio::ip::tcp;

JNIEXPORT jlong JNICALL Java_me_haohui_libhdfspp_NativeRemoteBlockReader_create(JNIEnv *, jclass, jlong jconn) {
  tcp::socket *conn = reinterpret_cast<tcp::socket*>(jconn);
  auto self = new std::shared_ptr<RemoteBlockReader<tcp::socket> >
              (new RemoteBlockReader<tcp::socket>(BlockReaderOptions(), conn));
  return reinterpret_cast<uintptr_t>(self);
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeRemoteBlockReader_destroy(JNIEnv *, jclass, jlong handle) {
  delete reinterpret_cast<std::shared_ptr<RemoteBlockReader<tcp::socket> >*>(handle);
}

JNIEXPORT jbyteArray JNICALL
Java_me_haohui_libhdfspp_NativeRemoteBlockReader_connect(JNIEnv *env, jclass, jlong handle,
                                                         jbyteArray jclient_name, jbyteArray jtoken,
                                                         jbyteArray jblock, jlong length, jlong offset) {
  auto &self = *reinterpret_cast<std::shared_ptr<RemoteBlockReader<tcp::socket> >*>(handle);
  std::string client_name(ReadByteString(env, jclient_name));
  hadoop::common::TokenProto token;
  if (jtoken) {
    ReadPBMessage(env, jtoken, &token);
  }
  hadoop::hdfs::ExtendedBlockProto block;
  ReadPBMessage(env, jblock, &block);
  Status stat = self->connect(client_name, jtoken ? &token : nullptr, &block, length, offset);
  return ToJavaStatusRep(env, stat);
}

JNIEXPORT jint JNICALL
Java_me_haohui_libhdfspp_NativeRemoteBlockReader_readSome(JNIEnv *env, jclass, jlong handle, jobject jdst,
                                                          jint position, jint limit, jobjectArray jstatus) {
  auto &self = *reinterpret_cast<std::shared_ptr<RemoteBlockReader<tcp::socket> >*>(handle);
  char *start = reinterpret_cast<char*>(env->GetDirectBufferAddress(jdst));
  Status stat;
  if (!start || position > limit) {
    stat = Status::InvalidArgument("Invalid buffer");
    SetStatusArray(env, jstatus, stat);
    return 0;
  }
  size_t transferred = self->read_some(asio::buffer(start + position, limit - position), &stat);
  if (!stat.ok()) {
    SetStatusArray(env, jstatus, stat);
  }
  return transferred;
}

