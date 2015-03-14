#include "namenode.h"
#include "iostream"
#include "stdlib.h"





int main(int argc, char **argv){
    if(argc != 5)
    {
        std::cerr << "Usage: ./namenode_test <host> <port> <file path> <offset>" << std::endl;
        return 1;
    }


    file_info info;
    int status = NameNode::getFileInfo(argv[1], atoi(argv[2]), argv[3], info);
    std::cerr << info.str() << std::endl;

    //lookup blocks for a given offset
    unsigned long long offset = (unsigned long long)atoll(argv[4]);

    std::vector<block_info> blocks;
    info.getBlocksForOffset(offset, blocks);

    for(std::vector<block_info>::iterator it = blocks.begin(); it != blocks.end(); it++)
    {
        std::cout << "\tblock at startoffset=" << it->startOffset << " size is " << it->numBytes << " bytes contains offset " << offset << std::endl;   
    } 


    return status;
}
