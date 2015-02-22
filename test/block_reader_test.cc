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
#include "block_reader.h"
#include "native_mini_dfs_cluster_wrapper.h"
#include "native_dfs_test_helper.h"
#include "gtest/gtest.h"

#include <asio.hpp>
#include <iostream>
#include <string>
#include <ostream>

class BlockReaderTest : public testing::Test {
 protected: 
  virtual void SetUp() {
    cluster_.reset(NativeMiniDfsClusterWrapper::Construct(conf_));
    ASSERT_TRUE(cluster_ != NULL);

    fs_ = hdfsConnectNewInstance(kNameNodeHost.c_str(), cluster_->GetNameNodePort());
    ASSERT_TRUE(fs_ != NULL);
  }

  virtual void TearDown() {
    hdfsDisconnect(fs_);
    Test::TearDown();
  }

  int AsyncBlockRead(const std::string &bpid, const long blockid, const long generationstamp,
    const size_t size, const size_t offset, const std::string &dnhost, const std::string &dnport,
    std::string &contents) {

    using namespace hdfs;
    using ::asio::ip::tcp;

    asio::io_service io_service;
    contents.clear();

    hadoop::hdfs::ExtendedBlockProto block;
    block.set_poolid(bpid);
    block.set_blockid(blockid);
    block.set_generationstamp(generationstamp);

    tcp::resolver resolver(io_service);
    tcp::resolver::query query(tcp::v4(), dnhost.c_str(), dnport.c_str());
    tcp::resolver::iterator iterator = resolver.resolve(query);

    std::shared_ptr<tcp::socket> s(new tcp::socket(io_service));
    asio::connect(*s.get(), iterator);

    BlockReaderOptions options;
    RemoteBlockReader<tcp::socket> reader(options, s);
    std::unique_ptr<char[]> buf(new char[size + 1]);
    reader.async_connect("libhdfs++", nullptr, &block, size, offset, [&buf,&reader,size](const Status &status) {
        if (!status.ok()) {
          std::cerr << "Error:" << status.code() << " " << status.ToString() << std::endl;
        } else {
          reader.async_read_some(asio::buffer(buf.get(), size), [&buf,size](const Status &status, size_t transferred) {
              buf[std::min(transferred, size)] = 0;
              std::cout << "Done: " << status.code() << std::endl <<
                  "Transferred: " << transferred << std::endl << 
                  "Content: " << buf.get() << std::endl;
            });
        }
      });
    io_service.run();
    contents = buf.get();
    return 0;
  }

  struct NativeMiniDfsConf conf_ = {
    1, /* doFormat */
    1, /* webhdfsEnabled */
    50070, /* namenodeHttpPort */
    0 /* configureShortCircuit */
  };

  // Native mini dfs cluster
  std::shared_ptr<NativeMiniDfsClusterWrapper>  cluster_;
  // Native libhdfsß
  hdfsFS fs_;

  const std::string kNameNodeHost = "127.0.0.1";
};

TEST_F(BlockReaderTest, AsyncReadFirstBlock) {
  // Using libhdfs to create test file
  std::string test_file = "/tmp/AsyncReadFirstBlock.txt";
  std::string test_file_contents("Hello, libhdfspp AsyncReadFirstBlock!");
  NativeDfsTestHelper::libHdfsWrite(fs_, test_file.c_str(), test_file_contents.c_str());
  
  // Using webhdfs to getBlockLocations of the test file
  file_info info = {};
  int status = NameNode::getFileInfo(kNameNodeHost, conf_.namenodeHttpPort, test_file, info);
  ASSERT_TRUE(status == 0);
  NativeDfsTestHelper::dumpBlockInfo(info);

  // Verify conents read from libhdfspp
  std::string read_contents;

  // Read the first block in full
  block_info blockInfo = info.blocks[0];
  
  std::string bpid = blockInfo.blockPoolId;
  unsigned long long blockid = blockInfo.blockId;
  unsigned long long generationstamp = blockInfo.generationStamp;
  size_t size = info.fileLength;
  size_t offset = 0;
  std::string dnport = std::to_string(info.locations[0].xferPort);
  std::string dnhost = info.locations[0].hostName;

  EXPECT_TRUE(AsyncBlockRead(bpid, blockid, generationstamp,
    size, offset, dnhost, dnport, read_contents) == 0);
  EXPECT_TRUE(test_file_contents.compare(read_contents) == 0);
}

TEST_F(BlockReaderTest, AsyncReadPartialBlock) {
  // Using libhdfs to create test file
  std::string test_file("/tmp/AsyncReadPartialBlock.txt");
  std::string test_file_contents("Hello, libhdfspp AsyncReadPartialBlock!");
  NativeDfsTestHelper::libHdfsWrite(fs_, test_file.c_str(), test_file_contents.c_str());
  
  // Using webhdfs to getBlockLocations of the test file
  file_info info = {};
  int status = NameNode::getFileInfo(kNameNodeHost, conf_.namenodeHttpPort, test_file, info);
  ASSERT_TRUE(status == 0);
  NativeDfsTestHelper::dumpBlockInfo(info);

  // Verify conents read from libhdfspp
  std::string read_contents;

  // Read the first block partially (0 ~ length-1)
  block_info blockInfo = info.blocks[0];
  
  std::string bpid = blockInfo.blockPoolId;
  unsigned long long blockid = blockInfo.blockId;
  unsigned long long generationstamp = blockInfo.generationStamp;
  size_t size = info.fileLength - 2;
  size_t offset = 0;
  std::string dnport = std::to_string(info.locations[0].xferPort);
  std::string dnhost = info.locations[0].hostName;

  EXPECT_TRUE(AsyncBlockRead(bpid, blockid, generationstamp,
    size, offset, dnhost, dnport, read_contents) == 0);
  std::cout << read_contents << std::endl;
  for (size_t i = offset; i < size; i++) {
    std::cout << test_file_contents[i];
  }
  std::cout<<std::endl;
  EXPECT_TRUE(test_file_contents.compare(0, size, read_contents) == 0);
}

TEST_F(BlockReaderTest, DISABLED_AsyncReadFromMiddle) {
  // Using libhdfs to create test file
  std::string test_file("/tmp/AsyncReadFromMiddle.txt");
  std::string test_file_contents("Hello, libhdfspp AsyncReadFromMiddle!");
  NativeDfsTestHelper::libHdfsWrite(fs_, test_file.c_str(), test_file_contents.c_str());
  
  // Using webhdfs to getBlockLocations of the test file
  file_info info = {};
  int status = NameNode::getFileInfo(kNameNodeHost, conf_.namenodeHttpPort, test_file, info);
  ASSERT_TRUE(status == 0);
  NativeDfsTestHelper::dumpBlockInfo(info);

  // Verify conents read from libhdfspp
  std::string read_contents;

  // Read the first block from the middle
  block_info blockInfo = info.blocks[0];
  
  std::string bpid = blockInfo.blockPoolId;
  unsigned long long blockid = blockInfo.blockId;
  unsigned long long generationstamp = blockInfo.generationStamp;
  size_t size = info.fileLength;
  size_t offset = size/2;
  std::string dnport = std::to_string(info.locations[0].xferPort);
  std::string dnhost = info.locations[0].hostName;

  EXPECT_TRUE(AsyncBlockRead(bpid, blockid, generationstamp,
    size, offset, dnhost, dnport, read_contents) == 0);
  EXPECT_TRUE(test_file_contents.compare(offset, size, read_contents) == 0);
}

TEST_F(BlockReaderTest, AsyncReadAll) {
  // Using libhdfs to create test file
  std::string test_file("/tmp/AsyncReadAll.txt");
  std::string test_file_contents("Hello, libhdfspp AsyncReadAll!");
  NativeDfsTestHelper::libHdfsWrite(fs_, test_file.c_str(), test_file_contents.c_str());
  //NativeDfsTestHelper::libHdfsRead(fs_, test_file.c_str(), test_file_contents.c_str());

  // Using webhdfs to getBlockLocations of the test file
  file_info info = {};
  int status = NameNode::getFileInfo(kNameNodeHost, conf_.namenodeHttpPort, test_file, info);
  ASSERT_TRUE(status == 0);
  NativeDfsTestHelper::dumpBlockInfo(info);

  for(std::vector<block_info>::iterator it = info.blocks.begin(); 
      it != info.blocks.end(); it++) {
    block_info blockInfo = *it;

    std::string bpid = blockInfo.blockPoolId;
    unsigned long long blockid = blockInfo.blockId;
    unsigned long long generationstamp = blockInfo.generationStamp;
    size_t size = info.fileLength;
    size_t offset = 0;
    
    // Read from all datanode locations
    std::string read_contents;
    for (size_t i = 0; i < blockInfo.locations.size(); i++) {
      int locationIndex = blockInfo.locations[i];
      std::string dnport = std::to_string(info.locations[locationIndex].xferPort);
      std::string dnhost = info.locations[locationIndex].hostName;
      EXPECT_TRUE(AsyncBlockRead(bpid, blockid, generationstamp,
        size, offset, dnhost, dnport, read_contents) == 0);
      EXPECT_TRUE(test_file_contents.compare(read_contents) == 0);
    }
  }
}
