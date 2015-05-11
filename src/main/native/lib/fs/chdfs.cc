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

//Expose a pure C interface that doesn't need any C++/C++11 header files
//todo: 
//  Need to be able to pass in parameters to hint at resource allocation like how many threads call run on io_service
//  It would be nice to be able to pass in counters to gather statistics for filesystem operations

#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>

#include "libhdfs++/chdfs.h"
#include "libhdfs++/hdfs.h"

#include <iostream>
#include <string>
#include <thread>

//undefine these and requests will lock up
#define CHDFS_SERIALIZED_OPEN
//#define CHDFS_SERIALIZED_READ

//---------------------------------------------------------------------------------------
//  Wrap C++ calls in C
//---------------------------------------------------------------------------------------

using namespace hdfs;


//todo: switch back to std::thread
void *call_run(void *servicePtr) {
  IoService *wrappedService = reinterpret_cast<IoService*>(servicePtr);
  wrappedService->Run();
  return NULL;
}

//Copied almost directly from inputstream_test.  Replaced std::thread with pthreads temporarily
class Executor {
public:
  Executor() {
    //Create a new IoService object. This wraps the boost io_service object.
    io_service_ = std::unique_ptr<IoService>(IoService::New());
    //Call run on IoService object in a background thread, the run call should never return.
    int ret = pthread_create(&processing_thread, NULL, call_run, reinterpret_cast<void*>(io_service_.get()));
    
    if(ret != 0) {
      //strip out exceptions later
      throw std::runtime_error("unable to start pthread?");
    }
  }

  ~Executor() {
    //Stop IoService event loop and background thread.  Don't need this yet.
  }
 
  IoService *io_service() {
    return io_service_.get();
  }

  std::unique_ptr<IoService> io_service_;
  pthread_t processing_thread;  
};


struct hdfsFile_struct {
  hdfsFile_struct() : inputStream(NULL) {};
  hdfsFile_struct(InputStream *is) : inputStream(is) {};

  virtual ~hdfsFile_struct() {
    if(NULL != inputStream)
      delete inputStream;
  }

  InputStream *inputStream;
};


struct hdfsFS_struct {
  hdfsFS_struct() : fileSystem(NULL), backgroundIoService(NULL) {};
  hdfsFS_struct(FileSystem *fs, Executor *ex) : fileSystem(fs), backgroundIoService(ex) {};
  virtual ~hdfsFS_struct() {
    delete fileSystem;
    delete backgroundIoService;
  };
 
  FileSystem *fileSystem;
  Executor *backgroundIoService;
};


/*  FS initialization routine
 *    -need to start background thread(s) to run asio::io_service
 *    -connect to specified namenode host:port
 *    -this will need to be extended to allow application to control memory management routines
 *     by passing an allocator/deleter pair as well as specify io_service thread count 
 */
hdfsFS hdfsConnect(const char *nnhost, unsigned short nnport) {
  std::unique_ptr<Executor> background_io_service = std::unique_ptr<Executor>(new Executor());

  if(NULL == background_io_service) 
    return NULL;

  //connect to NN, fileSystem will be set on success
  FileSystem *fileSystem = NULL;
  Status stat = FileSystem::New(background_io_service->io_service(), nnhost, nnport, &fileSystem);
  if(!stat.ok())
    return NULL;

  //make a hdfsFS handle
  return new hdfsFS_struct(fileSystem, background_io_service.release());
}


int hdfsDisconnect(hdfsFS fs) {
  //likely other stuff to free up, just stub for short tests
  if(NULL != fs)
    delete fs;  
  else
    return -1;
  return 0;
}

#ifdef CHDFS_SERIALIZED_OPEN
  pthread_mutex_t open_lock = PTHREAD_MUTEX_INITIALIZER;
#endif
hdfsFile hdfsOpenFile(hdfsFS fs, const char *path, int flags, int bufferSize, short replication, int blockSize) {
  #ifdef CHDFS_SERIALIZED_OPEN
    pthread_mutex_lock(&open_lock);
  #endif
  //the following four params are just placeholders until write path is finished
  (void)flags;
  (void)bufferSize;
  (void)replication;
  (void)blockSize;

  //assuming we want to do a read with default settings, sufficient for now
  InputStream *isPtr = NULL;
  Status stat = fs->fileSystem->Open(path, &isPtr);
  if(!stat.ok())
    return NULL;

  #ifdef CHDFS_SERIALIZED_OPEN
    pthread_mutex_unlock(&open_lock);
  #endif
  return new hdfsFile_struct(isPtr);
}


int hdfsCloseFile(hdfsFS fs, hdfsFile file) {
  (void)fs;
  if(NULL != file)
    delete file;
  else
    return -1;
  return 0;
}

#ifdef CHDFS_SERIALIZED_READ
  pthread_mutex_t read_lock = PTHREAD_MUTEX_INITIALIZER;
#endif
size_t hdfsPread(hdfsFS fs, hdfsFile file, off_t position, void *buf, size_t length) {
  #ifdef CHDFS_SERIALIZED_READ
    pthread_mutex_lock(&read_lock);  
  #endif

  if(NULL == fs || NULL == file)
    //set errno
    return 0;   

  size_t readBytes = 0;
  Status stat = file->inputStream->PositionRead(buf, length, position, &readBytes);
  if(!stat.ok()) {
    //set errno
    return 0;
  }

  #ifdef CHDFS_SERIALIZED_READ
    pthread_mutex_unlock(&read_lock);
  #endif
  return readBytes;
}


