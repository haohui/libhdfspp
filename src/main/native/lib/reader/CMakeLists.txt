add_library(reader remote_block_reader.cc)
add_dependencies(reader proto)
add_executable(remote_block_reader_test remote_block_reader_test.cc)
target_link_libraries(remote_block_reader_test reader proto common ${PROTOBUF_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
