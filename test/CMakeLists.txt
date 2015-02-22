FIND_PACKAGE(JNI)
FIND_PACKAGE(CURL)

SET(MINIDFS_LIBRARIES ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/libnative_mini_dfs.a)
SET(LIBHDFS_LIBRARIES ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/target/usr/local/lib/libhdfs.a)

SET(MINIDFS_INCLUDE_DIR ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs/)

if (WIN32)
    SET(PLATFORM_INCLUDE_DIR ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs/os/windows)
else()
    SET(PLATFORM_INCLUDE_DIR ${HADOOP_TOP_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs/os/posix)
endif()

INCLUDE_DIRECTORIES(
    ${JNI_INCLUDE_DIRS} 
    ${MINIDFS_INCLUDE_DIR}
    ${PLATFORM_INCLUDE_DIR}
    ../lib/namenode
)

add_library(native_dfs_test_helper native_dfs_test_helper.cc native_mini_dfs_cluster_wrapper.cc)

add_executable(native_mini_dfs_test native_mini_dfs_test.c)
target_link_libraries(native_mini_dfs_test 
    ${MINIDFS_LIBRARIES}
    ${JNI_LIBRARIES}
)

add_executable(block_reader_test block_reader_test.cc)
target_link_libraries(block_reader_test 
    ${MINIDFS_LIBRARIES}
    ${LIBHDFS_LIBRARIES}
    ${JNI_LIBRARIES}
    ${CURL_LIBRARIES}
    reader 
    proto 
    common 
    namenode
    native_dfs_test_helper
    ${PROTOBUF_LIBRARIES}
    ${CMAKE_SOURCE_DIR}/third_party/jsoncpp/src/lib_json/libjsoncpp.a 
    gtest_main
    -v
)

add_test(NAME block_reader_gtest COMMAND block_reader_gtest)
add_test(NAME native_mini_dfs_test COMMAND native_mini_dfs_test)


