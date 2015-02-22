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
#ifndef NATIVE_DFS_TETS_HELPER_H_
#define NATIVE_DFS_TETS_HELPER_H_

#include "hdfs.h"
#include "platform.h"
#include "native_mini_dfs.h"
#include "namenode.h"

class NativeDfsTestHelper {
 public:
  static void libHdfsRead(hdfsFS fs, const char *readPath, const char *fileContents);

  static void libHdfsWrite(hdfsFS fs, const char *writePath, const char *fileContents);

  static void dumpBlockInfo(file_info &info);
};
#endif
