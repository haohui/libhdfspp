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
#ifndef BINDINGS_H_
#define BINDINGS_H_

#include "libhdfs++/status.h"
#include "common/util.h"

#include <google/protobuf/message_lite.h>

#include <jni.h>

namespace hdfs {

class StatusHelper {
 public:
  static std::pair<const char *, size_t> Rep(const Status &status) {
    const char *state = status.state_;
    size_t length = *reinterpret_cast<const uint32_t*>(state);
    return std::make_pair(state, length + 8);
  }
};

}

static inline jbyteArray ToJavaStatusRep(JNIEnv *env, const hdfs::Status &stat) {
  if (stat.ok()) {
    return nullptr;
  }

  auto rep = hdfs::StatusHelper::Rep(stat);
  jbyteArray arr = env->NewByteArray(rep.second);
  void *b = env->GetPrimitiveArrayCritical(arr, nullptr);
  memcpy(b, rep.first, rep.second);
  env->ReleasePrimitiveArrayCritical(arr, b, 0);
  return arr;
}

static inline void SetStatusArray(JNIEnv *env, jobjectArray jstatus, const hdfs::Status &stat) {
  jbyteArray arr = ToJavaStatusRep(env, stat);
  env->SetObjectArrayElement(jstatus, 0, arr);
}

static inline void ReadPBMessage(JNIEnv *env, jbyteArray jbytes, ::google::protobuf::MessageLite *msg) {
  void *b = env->GetPrimitiveArrayCritical(jbytes, nullptr);
  msg->ParseFromArray(b, env->GetArrayLength(jbytes));
  env->ReleasePrimitiveArrayCritical(jbytes, b, JNI_ABORT);
}

static inline std::string ReadByteString(JNIEnv *env, jbyteArray jbytes) {
  char *data = reinterpret_cast<char*>(env->GetPrimitiveArrayCritical(jbytes, nullptr));
  std::string res(data, env->GetArrayLength(jbytes));
  env->ReleasePrimitiveArrayCritical(jbytes, data, JNI_ABORT);
  return res;
}

#endif
