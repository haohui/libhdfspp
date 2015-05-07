add_library(fs filesystem.cc inputstream.cc)
add_executable(inputstream_test inputstream_test.cc)
target_link_libraries(inputstream_test fs rpc reader common proto ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
