include_directories(${CMAKE_BINARY_DIR}/javah)
add_library(hdfsppjni SHARED bindings.cc)
target_link_libraries(hdfsppjni "-Wl,--whole-archive" common reader proto rpc "-Wl,--no-whole-archive" ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES})
