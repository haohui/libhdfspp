add_library(rpc rpc_connection.cc rpc_engine.cc)
add_executable(rpc_test rpc_test.cc)
target_link_libraries(rpc_test rpc common proto ${PROTOBUF_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
add_test(NAME rpc_test COMMAND rpc_test)
