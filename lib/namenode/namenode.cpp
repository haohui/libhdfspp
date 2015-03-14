#include "namenode.h"

#include "../../third_party/jsoncpp/include/json/json.h"

#include <iostream>
#include <sstream>
#include <curl/curl.h>
#include <algorithm>


size_t recv_callback(void *ptr, size_t size, size_t nmemb, std::vector<char> *buffer)
{
    //append the size*nmemb bytes of info from ptr to the buffer
    int bytes = size * nmemb;

    const char *in = reinterpret_cast<const char*>(ptr);
    if(in != NULL)
    {
        const char *end = in + bytes;
        buffer->insert(buffer->end(), in, end);
    }
    return bytes;
}


int getData(const std::string& url, std::vector<char>& buffer)
{
    //use curl to get info
    CURL *curl;
    CURLcode result;

    curl = curl_easy_init();

    //bail if init fails    
    if(!curl)
    {
        std::cerr << "failed to init curl" << std::endl;
        return 1;
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, recv_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buffer);

    //do request
    result = curl_easy_perform(curl);

    if(result != CURLE_OK)
    {
        std::cerr << "request failed.." << curl_easy_strerror(result) << std::endl;
        curl_easy_cleanup(curl);
        return 1;
    }

    //done with curl stuff
    curl_easy_cleanup(curl);
    return 0;  
}



std::string file_info::str()
{
    std::stringstream ss;

    ss << "Blocks" << std::endl;
    for(std::vector<block_info>::iterator it = blocks.begin(); it != blocks.end(); it++)
    {
        ss << "\tsize= "<< it->numBytes <<" offset = " << it->startOffset << " id=" << it->blockId << " genStamp= " << it->generationStamp;
        ss << "exists on locations [";
        for(unsigned int i=0; i<it->locations.size(); i++)
        {
            ss << it->locations[i] << ' ';
        }             
        ss << ']' << std::endl;
    }    

    ss << "Location name->idx mappings" << std::endl;
    for(std::map<std::string, unsigned int>::iterator it = locationsByName.begin(); it != locationsByName.end(); it++)
    {
        ss << '\t' << it->first << " -> " << it->second << std::endl;
    }

    ss << "Locations" << std::endl;
    int idx = 0;
    for(std::vector<location_info>::iterator it = locations.begin(); it != locations.end(); it++, idx++)
    {
        ss << "\tindex=" << idx <<" name=" << it->name << " hostname=" << it->hostName << std::endl; 
    }

    return ss.str();
}


bool block_offset_compare(const block_info& first, const block_info& second)
{
    return first.startOffset < second.startOffset;
}

//this should always return a 1 element vector because blocks don't overlap and this wraps multiple locations
void file_info::getBlocksForOffset(unsigned long long absoluteOffset, std::vector<block_info>& out)
{
    block_info offset;
    offset.startOffset = absoluteOffset;

    //comparator uses <= so that lower bound can equal the offset
    std::vector<block_info>::iterator blk = std::upper_bound(blocks.begin(), blocks.end(), offset, block_offset_compare);
    if(blk == blocks.end())
    {
        return;
    }

    //upper_bound will always return one block higher, unless it is the first block
    if(blk != blocks.begin())
    {
        blk--;
    }

    while(blk != blocks.end() && absoluteOffset - blk->startOffset < blk->numBytes)
    {
        out.push_back(*blk);
        blk++;
    }
}


static int parseData(const std::vector<char>& data, file_info& file)
{
    //root of json tree
    Json::Value root;
    Json::Reader reader;

    //try and parse json object
    bool parsed = reader.parse(&data[0], root);
    if(!parsed)
    {
        std::cerr << "It didn't parse.." << std::endl;
        return -1;
    }

    //the top level json object, this is the only named field under root
    Json::Value LocatedBlocks = root["LocatedBlocks"];

    //general file info
    file.fileLength = LocatedBlocks["fileLength"].asUInt64(); 
    file.isLastBlockComplete = LocatedBlocks["isLastBlockComplete"].asBool();
    file.isUnderConstruction = LocatedBlocks["isUnderConstruction"].asBool();

    //metadata for individual blocks
    Json::Value blocks = LocatedBlocks["locatedBlocks"];
    unsigned int blockCount = blocks.size(); //includes replicas

    for(unsigned int blockIndex=0; blockIndex<blockCount; blockIndex++)
    {
        //top level block object, has a bunch of fields
        Json::Value blockObject = blocks[blockIndex];
        Json::Value block     = blockObject["block"];    //object
        Json::Value locations = blockObject["locations"];//array
        Json::Value blockToken = blockObject["blockToken"]; // object
        block_info blk;

        //get general info for this block
        blk.blockId = block["blockId"].asUInt64();
        blk.generationStamp = block["generationStamp"].asUInt64();
        blk.numBytes = block["numBytes"].asUInt64();
        blk.startOffset = blockObject["startOffset"].asUInt64();    
        blk.blockPoolId = block["blockPoolId"].asString();
        blk.blockToken = blockToken["urlString"].asString();    

        //find all DataNodes that host this block
        unsigned int locationCount = locations.size();
        for(unsigned int locationIndex=0; locationIndex<locationCount; locationIndex++)
        {
            Json::Value location  = locations[locationIndex];

            location_info loc;
            loc.name     = location["name"].asString();
            loc.hostName = location["hostName"].asString();
            loc.ipAddr   = location["ipAddr"].asString(); 
            loc.xferPort = location["xferPort"].asInt();
            
            //try and add the location, if it already exists idx = the index in the existing locations vector
            unsigned int idx = file.addLocation(loc);
            //block needs to access location objects by index
            blk.locations.push_back(idx);
        }
        file.blocks.push_back(blk);
    }
    return 0;
}


//return 0 on success
int NameNode::getFileInfo(std::string nnHost, int nnPort, std::string absolutePath, file_info& info)
{
    //clear out file_info, only passing it in to use return codes
    info.locations.clear();
    info.locationsByName.clear();
    info.blocks.clear();


    std::stringstream url;
    url << "http://" << nnHost << ':' << nnPort << "/webhdfs/v1" << absolutePath << "?op=GET_BLOCK_LOCATIONS";
    std::cerr << "opening " << url.str() << std::endl;    

    //Json parser can't parse streams..
    std::vector<char> buf;

    //need checks on error code
    int res = getData(url.str(), buf);
    if(res != 0)
        return res;

    std::cout << std::endl;
    for (std::vector<char>::iterator it = buf.begin(); it != buf.end(); it++) {
        std::cout << *it;
    }
    std::cout << std::endl;
    
    res = parseData(buf, info);

    std::sort(info.blocks.begin(), info.blocks.end(), block_offset_compare);

    return res;
}
