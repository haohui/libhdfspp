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
#include <iostream>
#include <string>
#include <errno.h>

#include "native_mini_dfs_cluster_wrapper.h"

NativeMiniDfsClusterWrapper* NativeMiniDfsClusterWrapper::Construct(struct NativeMiniDfsConf &conf) {
  struct NativeMiniDfsCluster* cluster;

  cluster = nmdCreate(&conf);
  if (cluster == NULL) {
    std::cerr << "Cannot start native mini-dfs, \"nmdCreat\" returns NULL" << std::endl;
    return NULL;
  }

  if (0 != nmdWaitClusterUp(cluster)) {
    std::cerr << "cannot start native mini-dfs, \"nmdWaitClusterUp\" returns none zero" << std::endl;
    ShutdownCluster(cluster);
    return NULL;
  }

  return new NativeMiniDfsClusterWrapper(cluster);
}

NativeMiniDfsClusterWrapper::NativeMiniDfsClusterWrapper(struct NativeMiniDfsCluster* clus) 
  : clus_(clus) {
}

int NativeMiniDfsClusterWrapper::GetNameNodePort() {
  return nmdGetNameNodePort(clus_);
}

void NativeMiniDfsClusterWrapper::ShutdownCluster(NativeMiniDfsCluster* clus) {
  if (clus) {
    nmdShutdown(clus);
    nmdFree(clus);
  }
}

NativeMiniDfsClusterWrapper::~NativeMiniDfsClusterWrapper() {
  ShutdownCluster(clus_);
}
