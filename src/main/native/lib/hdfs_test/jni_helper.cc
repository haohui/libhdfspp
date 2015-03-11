#include "jni_helper.h"
#include <string.h>

namespace hdfs {

SingleThreadJavaEnvironment::SingleThreadJavaEnvironment(JavaVM *jvm, JNIEnv *env)
    : jvm_(jvm)
    , env_(env)
{}

SingleThreadJavaEnvironment::~SingleThreadJavaEnvironment() {
  jvm_->DestroyJavaVM();
}

SingleThreadJavaEnvironment *SingleThreadJavaEnvironment::Construct(const std::vector<std::string> &jvm_args) {
  JNIEnv *env;
  JavaVM *jvm;
  std::vector<JavaVMOption> cmdline_args(jvm_args.size());
  for (size_t i = 0; i < jvm_args.size(); ++i) {
    cmdline_args[i].optionString = const_cast<char*>(jvm_args[i].c_str());
  }

  JavaVMInitArgs args;
  memset(&args, 0, sizeof(args));
  args.version = JNI_VERSION_1_6;
  args.nOptions = cmdline_args.size();
  args.options = &cmdline_args[0];

  long status = JNI_CreateJavaVM(&jvm, reinterpret_cast<void**>(&env), &args);
  return status == JNI_ERR ? nullptr : new SingleThreadJavaEnvironment(jvm, env);
}

}
