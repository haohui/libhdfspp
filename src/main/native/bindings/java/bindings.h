#ifndef BINDINGS_H_
#define BINDINGS_H_

#include "libhdfs++/status.h"
#include "common/util.h"

#include <google/protobuf/message_lite.h>

#include <jni.h>

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
