include_directories(${OPENSSL_INCLUDE_DIRS})
add_library(rpc rpc_connection.cc rpc_engine.cc)
add_dependencies(rpc proto)
add_executable(rpc_test rpc_test.cc)
target_link_libraries(rpc_test rpc common proto ${PROTOBUF_LIBRARIES} ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

