cmake_minimum_required (VERSION 2.8) 
project (libhdfs++)


#if (NOT CMAKE_BUILD_TYPE)
#  set (CMAKE_BUILD_TYPE "Release")
#endif()
set (CMAKE_BUILD_TYPE "Debug")

enable_testing()

if (BUILD_TESTING)
find_package(JNI REQUIRED)
endif()

find_package(Protobuf REQUIRED)
include_directories(third_party/asio-1.10.2/include)
include_directories(${PROTOBUF_INCLUDE_DIRS})
include_directories(${CMAKE_BINARY_DIR}/lib/proto)
include_directories(include)
include_directories(lib)

if(UNIX)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -std=c++11 -g")
endif()

if(APPLE)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
endif()

add_subdirectory(lib)
