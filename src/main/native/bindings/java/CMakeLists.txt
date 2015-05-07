include_directories(${CMAKE_BINARY_DIR}/javah)
add_library(hdfsppjni SHARED
            block_reader.cc filesystem.cc io_service.cc
            rpc.cc tcp_connection.cc)
target_link_libraries(hdfsppjni fs reader rpc common proto ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES})
