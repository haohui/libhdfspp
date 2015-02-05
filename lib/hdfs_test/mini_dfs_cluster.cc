#include "mini_dfs_cluster.h"

#include <iostream>

namespace hdfs {

static const char kBuilderName[] = "org/apache/hadoop/hdfs/MiniDFSCluster$Builder";
static const char kMiniDFSClusterName[] = "org/apache/hadoop/hdfs/MiniDFSCluster";
static const char kConfigurationName[] = "org/apache/hadoop/conf/Configuration";

MiniDFSCluster *MiniDFSCluster::Construct(const MiniDFSClusterOptions &,
                                          SingleThreadJavaEnvironment *java) {
  JNIEnv *env = java->env();
  JNIExceptionGuard guard(env);
  UniqueJNILocalRef<jclass> configuration_class(env, env->FindClass(kConfigurationName));
  UniqueJNILocalRef<jclass> builder_class(env, env->FindClass(kBuilderName));
  UniqueJNILocalRef<jclass> minidfs_class(env, env->FindClass(kMiniDFSClusterName));

  if (!configuration_class || !builder_class || !minidfs_class) {
    return nullptr;
  }

  jmethodID configuration_init = env->GetMethodID(*configuration_class, "<init>", "()V");
  jmethodID builder_init = env->GetMethodID(*builder_class, "<init>", "(Lorg/apache/hadoop/conf/Configuration;)V");
  jmethodID minidfs_init = env->GetMethodID(*minidfs_class, "<init>", "(Lorg/apache/hadoop/hdfs/MiniDFSCluster$Builder;)V");
  if (!configuration_init || !builder_init || !minidfs_init) {
    return nullptr;
  }

  UniqueJNILocalRef<jobject> configuration(env, env->NewObject(*configuration_class, configuration_init));
  if (!configuration) {
    return nullptr;
  }

  UniqueJNILocalRef<jobject> builder(env, env->NewObject(*builder_class, builder_init, *configuration));
  if (!builder) {
    return nullptr;
  }

  UniqueJNILocalRef<jobject> minidfs(env, env->NewObject(*minidfs_class, minidfs_init, *builder));
  return new MiniDFSCluster(env, &minidfs);
}

MiniDFSCluster::MiniDFSCluster(JNIEnv *env, UniqueJNILocalRef<jobject> *minidfs)
    : minidfs_(env, minidfs->Release())
{}


}
