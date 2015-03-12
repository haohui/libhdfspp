#ifndef LIB_TESTS_MINI_DFS_CLUSTER_H
#define LIB_TESTS_MINI_DFS_CLUSTER_H

#include "jni_helper.h"

#include <asio/ip/tcp.hpp>
#include <vector>

namespace hdfs {

struct MiniDFSClusterOptions {};

class MiniDFSCluster {
 public:
  static MiniDFSCluster *Construct(const MiniDFSClusterOptions &options,
                                   SingleThreadJavaEnvironment *java);
  asio::ip::tcp::endpoint GetNameNode();
 private:
  MiniDFSCluster(JNIEnv *env, UniqueJNILocalRef<jobject> *ref);
  void Shutdown();
  SingleThreadJavaEnvironment *java_;
  UniqueJNIGlobalRef<jobject> minidfs_;
};

}

#endif
