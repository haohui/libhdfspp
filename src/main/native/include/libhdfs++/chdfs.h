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

//  Expose a pure C interface that doesn't need any C++/C++11 header files, designed to be compatible with
//  libhdfs and libhdfs3 C APIs.  
//
//  This is still a work in progress and only contains a subset of the function calls in libhdfs/libhdfs3 for now.
//

#ifndef INCLUDE_LIBHDFSPP_CHDFS_H_
#define INCLUDE_LIBHDFSPP_CHDFS_H

#include "stdlib.h"

struct hdfsFile_struct;
struct hdfsFS_struct;

typedef hdfsFS_struct*   hdfsFS;
typedef hdfsFile_struct* hdfsFile;


/*  Library initialization routine
 *    -need to start background thread(s) to run asio::io_service
 *    -connect to specified namenode host:port
 *    -this will need to be extended to allow application to control memory management routines
 *     by passing an allocator/deleter pair as well as specify io_service thread count 
 */
extern "C" {
  hdfsFS hdfsConnect(const char *nnhost, unsigned short nnport);
}


/** 
 * hdfsDisconnect - Disconnect from the hdfs file system.
 * Disconnect from hdfs.
 * @param fs The configured filesystem handle.
 * @return Returns 0 on success, -1 on error.  
 */
extern "C" {
  int hdfsDisconnect(hdfsFS fs);
}


/* hdfsOpenFile - Open an hdfs file in given mode **read only for now**
 * @param fs    The configured filesystem handle
 * @param path  The absolute path to the file
 * @param flags Ignored, assumes O_RDONLY
 * @param bufferSize  ignored - not implemented
 * @param replication ignored - not implemented
 * @param blockSize   ignored - not implemented
 */
extern "C" {
  hdfsFile hdfsOpenFile(hdfsFS fs, const char *path, int flags, int bufferSize, short replication, int blockSize);
}


/** 
 * hdfsCloseFile - Close an open file. 
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns 0 on success, -1 on error.  
 */
extern "C" {
  int hdfsCloseFile(hdfsFS fs, hdfsFile file);
}


/** 
 * hdfsPread - Positional read of data from an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param position Position from which to read
 * @param buffer The buffer to copy read bytes into.
 * @param length The length of the buffer.
 * @return Returns the number of bytes actually read, possibly less than
 * than length;-1 on error.
 */
extern "C" {
  size_t hdfsPread(hdfsFS fs, hdfsFile file, off_t position, void *buf, size_t length);
}

/*  todo, roughly in order of priority, before write path is complete
 *    tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length);
 *    int hdfsExists(hdfsFS fs, const char *path);
 *    int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos);
 *    tOffset hdfsTell(hdfsFS fs, hdfsFile file);
 *
 */

#endif

