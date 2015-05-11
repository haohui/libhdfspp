#include "libhdfs++/chdfs.h"


#include <string>
#include <iostream>
#include <cstring>

//Simple test that can be run through valgrind to make sure the libhdfs compatible API isn't leaking memory.


hdfsFS fs = NULL;

int main(int argc, char **argv){
  if(argc != 4) {
    std::cout << "usage: ./cinputstream_test <host> <port> <path>" << std::endl;
    return 1;
  }

  fs = hdfsConnect(argv[1], std::stoi(argv[2]));
  if(NULL == fs){
    std::cout << "error: initializing filesystem returned null" << std::endl;
    return 1;
  }

  hdfsFile file = hdfsOpenFile(fs, argv[3], 0, 0, 0, 0);
  if(file == NULL) {
    std::cout << "error: could not open file" << std::endl;
    return 1;
  }

  char buf[1000];
  memset(buf, 0, 1000);

  int count = hdfsPread(fs, file, 0 /*pos*/, buf, 50 /*max bytes to read*/);
  if(count == -1) {
    std::cout << "error: could not read file" << std::endl;
    return 1;
  }

  std::cout << "read " << count << " bytes" << std::endl;
  std::cout << buf << std::endl;
  

  hdfsCloseFile(fs, file);
  hdfsDisconnect(fs);


}



