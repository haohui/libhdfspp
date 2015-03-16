include_directories(${CMAKE_BINARY_DIR}/javah)
add_library(hdfsppjni SHARED bindings.cc)
target_link_libraries(hdfsppjni common reader proto rpc ${PROTOBUF_LIBRARIES})
