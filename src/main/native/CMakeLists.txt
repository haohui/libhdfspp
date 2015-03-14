cmake_minimum_required (VERSION 2.8) 
project (libhdfs++)


#if (NOT CMAKE_BUILD_TYPE)
#  set (CMAKE_BUILD_TYPE "Release")
#endif()
set (CMAKE_BUILD_TYPE "Debug")

enable_testing()

if (BUILD_TESTING)
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})
endif()

find_package(Protobuf REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Doxygen)
find_package(Threads)

GET_FILENAME_COMPONENT(HADOOP_TOP_DIR "${PROJECT_SOURCE_DIR}/../../../../../" REALPATH)

include_directories(third_party/asio-1.10.2/include)
include_directories(third_party/gtest-1.7.0/include)
include_directories(${PROTOBUF_INCLUDE_DIRS})
include_directories(${CMAKE_BINARY_DIR}/lib/proto)
include_directories(include)
include_directories(lib)
include_directories(lib/reader)
include_directories(lib/common)

add_definitions(-DASIO_STANDALONE)
if(UNIX)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -std=c++11 -g -fPIC -fno-strict-aliasing")
endif()

if(APPLE)
# Mac OS 10.7 and later deprecates most of the methods in OpenSSL.
# Add -Wno-deprecated-declarations to avoid the warnings.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -Wno-deprecated-declarations")
endif()

if(DOXYGEN_FOUND)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/doc/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/doc/Doxyfile @ONLY)
add_custom_target(doc ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/doc/Doxyfile
                  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                  COMMENT "Generating API documentation with Doxygen" VERBATIM)
endif(DOXYGEN_FOUND)

add_subdirectory(lib)
add_subdirectory(third_party/gtest-1.7.0)
add_subdirectory(third_party/jsoncpp)
add_subdirectory(test)
add_subdirectory(bindings/java)

