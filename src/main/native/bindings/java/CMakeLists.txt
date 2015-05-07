include_directories(${CMAKE_BINARY_DIR}/javah)
add_library(hdfsppjni SHARED io_service.cc rpc.cc)
target_link_libraries(hdfsppjni rpc common proto ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES})
