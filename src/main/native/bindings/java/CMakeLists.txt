include_directories(${CMAKE_BINARY_DIR}/javah)
add_library(hdfsppjni SHARED common.cc filesystem.cc)
target_link_libraries(hdfsppjni fs common reader proto rpc ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES})
