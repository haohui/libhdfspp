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
#include "me_haohui_libhdfspp_NativeIoService.h"

#include "libhdfs++/hdfs.h"

using namespace ::hdfs;

JNIEXPORT jlong JNICALL Java_me_haohui_libhdfspp_NativeIoService_create(JNIEnv *, jclass) {
  return reinterpret_cast<uintptr_t>(IoService::New());
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeIoService_nativeRun(JNIEnv *, jclass, jlong handle) {
  IoService *self = reinterpret_cast<IoService*>(handle);
  self->Run();
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeIoService_stop(JNIEnv *, jclass, jlong handle) {
  IoService *self = reinterpret_cast<IoService*>(handle);
  self->Stop();
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeIoService_destroy(JNIEnv *, jclass, jlong handle) {
  IoService *self = reinterpret_cast<IoService*>(handle);
  delete self;
}
