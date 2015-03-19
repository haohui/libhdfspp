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

#include "libhdfs++/hdfs.h"

#include <iostream>
#include <string>
#include <thread>

using namespace hdfs;

class Executor {
 public:
  Executor()
      : io_service_(IoService::New())
      , thread_(std::bind(&IoService::Run, io_service_.get()))
  {}

  IoService *io_service() { return io_service_.get(); }

  ~Executor() {
    io_service_->Stop();
    thread_.join();
  }

  std::unique_ptr<IoService> io_service_;
  std::thread thread_;
};

int main(int argc, char *argv[]) {
  if (argc != 4) {
    std::cerr
        << "Print files stored in a HDFS cluster.\n"
        << "Usage: " << argv[0] << " "
        << "<nnhost> <nnport> <file>\n";
    return 1;
  }

  Executor executor;

  FileSystem *fsptr;
  Status stat = FileSystem::New(executor.io_service(), argv[1], std::stoi(argv[2]), &fsptr);
  if (!stat.ok()) {
    std::cerr << "Cannot create the filesystem: " << stat.ToString() << std::endl;
    return 1;
  }

  std::unique_ptr<FileSystem> fs(fsptr);

  InputStream *isptr;
  stat = fs->Open(argv[3], &isptr);
  if (!stat.ok()) {
    std::cerr << "Cannot open the file: " << stat.ToString() << std::endl;
    return 1;
  }

  std::unique_ptr<InputStream> is(isptr);

  char buf[8192] = {0,};
  size_t read_bytes = 0;
  stat = is->PositionRead(buf, sizeof(buf), 0, &read_bytes);
  if (!stat.ok()) {
    std::cerr << "Read failures: " << stat.ToString() << std::endl;
  }
  std::cerr << "Read bytes:" << read_bytes << std::endl << buf << std::endl;
  return 0;
}
