/*
    Provide an abstraction for fetching info from the NameNode.  For now implement with webhdfs
    and don't worry avoid HA or failover.
*/
#ifndef NAMENODE_H
#define NAMENODE_H

#include <map>
#include <vector>
#include <string>


struct block_info
{
    block_info() : startOffset(0), generationStamp(0) {};
    //fields from webhdfs
    unsigned long long startOffset;
    unsigned long long numBytes;
    unsigned long long generationStamp;
    unsigned long long blockId;
    std::string blockPoolId;
    std::string blockToken;

    //index into vector of locations in file_info
    std::vector<unsigned int> locations;  
};

struct location_info
{
    //fields from webhdfs
    std::string name;
    std::string hostName;
    std::string ipAddr;
    int xferPort;
};


struct file_info
{
    file_info() : fileLength(0), isLastBlockComplete(true), isUnderConstruction(false) {};
  
    //fields from webhdfs
    unsigned long long fileLength;
    bool isLastBlockComplete;
    bool isUnderConstruction;
  
    //Locations are kept in a vector, so that way the block_info only needs to hold on to indexes into this rather than strings for node names
    //In order for this to work nothing can be deleted from the locations vector.  May need to add an invalid flag to location
    std::vector<location_info> locations; //append only
    std::map<std::string, unsigned int> locationsByName;  //keyed by name to get index info blocks
  
    //Order this vector by startOffset field on block, webhdfs already does this.
    std::vector<block_info> blocks; //keyed by startOffset
  
    //adds a location and returns index in locations vector.  If location exists just return index
    unsigned int addLocation(const location_info& loc){
        if(locationsByName.count(loc.name) == 1)
            return locationsByName[loc.name];

        unsigned int idx = locations.size();
        locations.push_back(loc);
        locationsByName[loc.name] = idx;
        return idx;
    }
  
    std::string str();

    //find blocks that contain the offset provided
    void getBlocksForOffset(unsigned long long offset, std::vector<block_info>& out);

        
};




class NameNode
{
public:
    static int getFileInfo(std::string nnHost, int nnPort, std::string path, file_info& info);

private:

};








#endif
