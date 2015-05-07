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
#ifndef FS_FILESYSTEM_H_
#define FS_FILESYSTEM_H_

#include "namenode_protocol.h"
#include "common/wrapper.h"
#include "libhdfs++/hdfs.h"

namespace hdfs {

class FileSystemImpl : public FileSystem {
 public:
  FileSystemImpl(IoService *io_service);
  Status Connect(const char *server, unsigned short port);
  virtual Status Open(const char *path, InputStream **isptr) override;
  RpcEngine &rpc_engine() { return engine_; }
 private:
  IoServiceImpl *io_service_;
  RpcEngine engine_;
  ClientNamenodeProtocol namenode_;
};

class InputStreamImpl : public InputStream {
 public:
  InputStreamImpl(FileSystemImpl *fs, const ::hadoop::hdfs::LocatedBlocksProto *blocks);
  virtual Status PositionRead(void *buf, size_t nbyte, size_t offset, size_t *read_bytes) override;
  template<class MutableBufferSequence, class Handler>
  void AsyncPreadSome(size_t offset, const MutableBufferSequence &buffers,
                      const Handler &handler);
 private:
  FileSystemImpl *fs_;
  unsigned long long file_length_;
  std::vector<::hadoop::hdfs::LocatedBlockProto> blocks_;
  struct HandshakeContinuation;
  template<class MutableBufferSequence>
  struct ReadBlockContinuation;
};

}

#include "inputstream_impl.h"

#endif
