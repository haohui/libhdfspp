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
#include "native_dfs_test_helper.h"

#include <iostream>
#include <string>
#include <ostream>

void NativeDfsTestHelper::libHdfsRead(hdfsFS fs, const char *readPath, const char *fileContents) {
  char buffer[32];
  tSize num_read_bytes;
  hdfsFile readFile;

  int exists = hdfsExists(fs, readPath);
  if (exists) {
    std::cerr << "Failed to validate existence of" << readPath << std::endl;
    exit(-1);
  }

  readFile = hdfsOpenFile(fs, readPath, O_RDONLY, 0, 0, 0);
  if (!readFile) {
    std::cerr << "Failed to open " << readPath << " for reading!" << std::endl;
    exit(-1);
  }

  if (!hdfsFileIsOpenForRead(readFile)) {
    std::cerr << "hdfsFileIsOpenForRead: we just opened a file " << 
              "with O_RDONLY, and it did not show up as 'open for read'" <<std::endl;
    exit(-1);
  }

  std::cerr << "hdfsAvailable: " << hdfsAvailable(fs, readFile) << std::endl;

  // Test the direct read path
  if(hdfsSeek(fs, readFile, 0)) {
    std::cerr << "Failed to seek " << readPath << " for reading!" << std::endl;
    exit(-1);
  }
  memset(buffer, 0, sizeof(buffer));
  num_read_bytes = hdfsRead(fs, readFile, (void*)buffer,
          sizeof(buffer));
  if (strncmp(fileContents, buffer, strlen(fileContents)) != 0) {
    std::cerr << "Failed to read (direct). Expected: " << fileContents << " but got: " <<
        buffer << " (" << num_read_bytes << " bytes)" << std::endl;
    exit(-1);
  }

  std::cout << "Read (direct) following " << num_read_bytes << " bytes: " 
      << buffer << std::endl;
  hdfsCloseFile(fs, readFile);
}

void NativeDfsTestHelper::libHdfsWrite(hdfsFS fs, const char *writePath, const char *fileContents) {
  tSize num_written_bytes;
  hdfsFile writeFile;

  writeFile = hdfsOpenFile(fs, writePath, O_WRONLY|O_CREAT, 0, 0, 0);
  if(!writeFile) {
      std::cerr << "Failed to open " << writePath << " for writing!" << std::endl;
      exit(-1);
  }
  std::cout << "Opened " << writePath  << " for writing successfully..." << std::endl;
  num_written_bytes =
    hdfsWrite(fs, writeFile, (void*)fileContents,
      (tSize)(strlen(fileContents)+1));
  if (num_written_bytes != (int)strlen(fileContents) + 1) {
    std::cerr << "Failed to write correct number of bytes - expected : " <<
            (int)(strlen(fileContents) + 1) << " but got: " << (int)num_written_bytes;
    exit(-1);
  }
  std::cerr << "Wrote " << num_written_bytes << " bytes" << std::endl;;

  if (hdfsFlush(fs, writeFile)) {
      std::cerr << "Failed to 'flush' writePath " << num_written_bytes << std::endl; 
      exit(-1);
  }
  std::cout << "Flushed " << writePath <<" successfully!" << std::endl; 

  if (hdfsHFlush(fs, writeFile)) {
      std::cerr << "Failed to 'hflush' on "<< writePath << std::endl;
      exit(-1);
  }
  std::cout << "HFlushed " << writePath <<  " successfully on path " <<  std::endl;
  hdfsCloseFile(fs, writeFile);
}

void NativeDfsTestHelper::dumpBlockInfo(file_info &info) {
  std::cout << std::endl;
  for(std::vector<block_info>::iterator it = info.blocks.begin(); 
      it != info.blocks.end(); it++) {
    block_info blockInfo = *it;
    std::cout << "BlockPoolId : " << blockInfo.blockPoolId << std::endl;
    std::cout << "BlockId: " << blockInfo.blockId << std::endl;
    std::cout << "GenerationStamp: " << blockInfo.generationStamp << std::endl;
    std::cout << "StartOffset: " << blockInfo.startOffset << std::endl;
    std::cout << "NumBytes: " <<  blockInfo.numBytes << std::endl;
    std::cout << "BlockPooiId: " << blockInfo.blockPoolId << std::endl;
    std::cout << "BlockToken: " << blockInfo.blockToken << std::endl;

    std::cout << "Datanode Locations: " << std::endl;
    for (size_t i = 0; i < blockInfo.locations.size(); i++) {
      int locationIndex = blockInfo.locations[i];
      std::cout << "Locations[" << i << "].name : " 
          << info.locations[locationIndex].name << std::endl;
      std::cout << "Locations[" << i << "].ipAddr : " 
          << info.locations[locationIndex].ipAddr << std::endl;
      std::cout << "Locations[" << i << "].hostName : " 
          << info.locations[locationIndex].hostName << std::endl;
      std::cout << "Locations[" << i << "].xferPort : " 
          << info.locations[locationIndex].xferPort << std::endl;
    }
  }
  std::cout << std::endl;
}
