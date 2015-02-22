find_package(CURL)

add_library(namenode namenode.cpp)
add_executable(namenode_test namenode_test.cpp)
target_link_libraries(namenode_test namenode ${CURL_LIBRARIES} ${CMAKE_SOURCE_DIR}/third_party/jsoncpp/src/lib_json/libjsoncpp.a)
