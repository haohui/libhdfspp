#ifndef LIB_HDFS_TEST_JNI_HELPER_H_
#define LIB_HDFS_TEST_JNI_HELPER_H_

#include <jni.h>

#include <vector>
#include <string>

namespace hdfs {

class SingleThreadJavaEnvironment {
 public:
  static SingleThreadJavaEnvironment *Construct(const std::vector<std::string> &jvm_args);
  JNIEnv *env() const { return env_; }
  ~SingleThreadJavaEnvironment();
 private:
  SingleThreadJavaEnvironment(JavaVM *jvm, JNIEnv *env);
  SingleThreadJavaEnvironment(const SingleThreadJavaEnvironment &) = delete;
  SingleThreadJavaEnvironment &operator=(const SingleThreadJavaEnvironment &) = delete;
  JavaVM *jvm_;
  JNIEnv *env_;
};

class JNIExceptionGuard {
 public:
  JNIExceptionGuard(JNIEnv *env)
      : env_(env)
  {}

  ~JNIExceptionGuard() {
    env_->ExceptionClear();
  }
 private:
  JNIEnv *env_;
};

struct JNILocalRefDeleter {
  static void Delete(JNIEnv *env, jobject ref) {
    env->DeleteLocalRef(ref);
  }
};

struct JNIGlobalRefDeleter {
  static void Delete(JNIEnv *env, jobject ref) {
    env->DeleteGlobalRef(ref);
  }
};

template <class T, class Deleter>
class UniqueJNIRef {
 public:
  UniqueJNIRef(JNIEnv *env, T ref)
      : env_(env), ref_(ref)
  {}
  ~UniqueJNIRef() {
    if (ref_) {
      Deleter::Delete(env_, ref_);
    }
  }
  T Get() const { return ref_; }
  T Release() {
    jobject r = ref_;
    ref_ = nullptr;
    return r;
  }
  T operator*() const { return ref_; }
  operator bool() const { return ref_; }

 private:
  JNIEnv *env_;
  T ref_;
  UniqueJNIRef(const UniqueJNIRef &) = delete;
  UniqueJNIRef &operator=(const UniqueJNIRef &) = delete;
};

template <class T>
using UniqueJNILocalRef = UniqueJNIRef<T, JNILocalRefDeleter>;
template <class T>
using UniqueJNIGlobalRef = UniqueJNIRef<T, JNIGlobalRefDeleter>;

}

#endif
