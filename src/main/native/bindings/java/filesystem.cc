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

#include "me_haohui_libhdfspp_NativeFileSystem.h"
#include "me_haohui_libhdfspp_NativeInputStream.h"

#include "libhdfs++/hdfs.h"

#include <asio/ip/tcp.hpp>

using namespace ::hdfs;
using ::asio::ip::tcp;

JNIEXPORT jlong JNICALL
Java_me_haohui_libhdfspp_NativeFileSystem_create(JNIEnv *env, jclass,
                                                 jlong io_service_handle,
                                                 jstring jserver,
                                                 jint port,
                                                 jobjectArray jstatus) {
  IoService *io_service = reinterpret_cast<IoService*>(io_service_handle);
  const char *server = env->GetStringUTFChars(jserver, nullptr);
  FileSystem *fsptr;
  Status stat = FileSystem::New(io_service, server, port, &fsptr);
  jlong result = 0;
  if (stat.ok()) {
    result = reinterpret_cast<uintptr_t>(fsptr);
  } else {
    SetStatusArray(env, jstatus, stat);
  }

  env->ReleaseStringUTFChars(jserver, server);
  return result;
}

JNIEXPORT jlong JNICALL
Java_me_haohui_libhdfspp_NativeFileSystem_open(JNIEnv *env, jclass,
                                               jlong handle,
                                               jstring jpath,
                                               jobjectArray jstatus) {
  FileSystem *self = reinterpret_cast<FileSystem*>(handle);
  const char *path = env->GetStringUTFChars(jpath, nullptr);
  InputStream *isptr;
  Status stat = self->Open(path, &isptr);
  jlong result = 0;
  if (stat.ok()) {
    result = reinterpret_cast<uintptr_t>(isptr);
  } else {
    SetStatusArray(env, jstatus, stat);
  }
  env->ReleaseStringUTFChars(jpath, path);
  return result;
}

JNIEXPORT void JNICALL
Java_me_haohui_libhdfspp_NativeFileSystem_destroy(JNIEnv *, jclass, jlong handle) {
  FileSystem *self = reinterpret_cast<FileSystem*>(handle);
  delete self;
}

JNIEXPORT void JNICALL
Java_me_haohui_libhdfspp_NativeInputStream_destroy(JNIEnv *, jclass, jlong handle) {
  InputStream *self = reinterpret_cast<InputStream*>(handle);
  delete self;
}

JNIEXPORT jint JNICALL
Java_me_haohui_libhdfspp_NativeInputStream_positionRead(JNIEnv *env, jclass,
                                                        jlong handle,
                                                        jobject jbuf,
                                                        jint position,
                                                        jint limit,
                                                        jlong offset,
                                                        jobjectArray jstatus) {
  InputStream *self = reinterpret_cast<InputStream*>(handle);
  char *buf = reinterpret_cast<char*>(env->GetDirectBufferAddress(jbuf));
  Status stat;
  if (!buf || position > limit) {
    stat = Status::InvalidArgument("Invalid buffer");
    SetStatusArray(env, jstatus, stat);
    return 0;
  }
  size_t read_bytes = 0;
  stat = self->PositionRead(buf + position, limit - position, offset, &read_bytes);
  if (!stat.ok()) {
    SetStatusArray(env, jstatus, stat);
  }
  return read_bytes;
}
