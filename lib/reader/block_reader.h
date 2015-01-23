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
#ifndef BLOCK_READER_H_
#define BLOCK_READER_H_

#include "libhdfs++/options.h"
#include "datatransfer.pb.h"

#include <memory>

namespace hdfs {

template<class Stream>
class RemoteBlockReader {
 public:
  explicit RemoteBlockReader(const BlockReaderOptions &options,
                             std::shared_ptr<Stream> stream)
      : stream_(stream)
      , state_(kOpen)
      , options_(options)
  {}

  template<class MutableBufferSequence, class ReadHandler>
  void async_read_some(const MutableBufferSequence& buffers,
                       const ReadHandler &handler);

  template<class ConnectHandler>
  void async_connect(const std::string &client_name,
                 const hadoop::common::TokenProto *token,
                 const hadoop::hdfs::ExtendedBlockProto *block,
                 uint64_t length, uint64_t offset,
                 const ConnectHandler &handler);
 private:
  struct ReadPacketHeader;
  struct ReadChecksum;
  template<class MutableBufferSequence>
  struct ReadData;
  struct AckRead;
  enum State {
    kOpen,
    kReadPacketHeader,
    kReadData,
    kFinished,
  };

  std::shared_ptr<Stream> stream_;
  hadoop::hdfs::PacketHeaderProto header_;
  State state_;
  BlockReaderOptions options_;
  size_t packet_len_;
  size_t packet_read_bytes_;
  long long bytes_to_read_;
  std::vector<char> checksum_;
};

}

#include "impl/remote_block_reader.h"

#endif
