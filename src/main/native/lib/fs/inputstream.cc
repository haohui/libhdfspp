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

#include "filesystem.h"

namespace hdfs {

using ::hadoop::hdfs::LocatedBlocksProto;

InputStream::~InputStream()
{}

InputStreamImpl::InputStreamImpl(FileSystemImpl *fs, const LocatedBlocksProto *blocks)
    : fs_(fs)
    , file_length_(blocks->filelength())
{
  for (const auto &block : blocks->blocks()) {
    blocks_.push_back(block);
  }

  if (blocks->has_lastblock() && blocks->lastblock().b().numbytes()) {
    blocks_.push_back(blocks->lastblock());
  }
}

Status InputStreamImpl::PositionRead(void *buf, size_t nbyte, size_t offset, size_t *read_bytes) {
  std::promise<Status> stat;
  auto handler = [&stat,read_bytes](const Status &status, size_t transferred) {
    *read_bytes = transferred;
    stat.set_value(status);
  };
  
  AsyncPreadSome(offset, asio::buffer(buf, nbyte), handler);
  return stat.get_future().get();
}

}
