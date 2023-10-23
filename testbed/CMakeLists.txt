cmake_minimum_required (VERSION 2.6)

project (CuckooDuo)

set(PROJECT_SOURCE_DIR ${CMAKE_SOURCE_DIR}/remote)
set(RDMA_SOURCE_DIR ${PROJECT_SOURCE_DIR}/rdma)
set(TEST_DIR ${PROJECT_SOURCE_DIR}/test)
set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/bin)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR})

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_BUILD_TYPE Release) # or debug if you need to use gdb on it.

# Some how g++ has better performance than clang++. Of course I don't know all flags for clang++.
set(CMAKE_CXX_COMPILER g++) # or clang++
#set(CMAKE_CXX_COMPILER clang++) # or clang++
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -Ofast -ffast-math -funroll-loops -march=native") # get crazy here
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -Ofast -ffast-math -funroll-loops")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")
#add_compile_options("-std=c++17")
#set(CMAKE_CXX_FLAGS "-O0  -ggdb")

find_library(IBVERBS_LIBRARY ibverbs HINTS /home/atr/local/lib)
find_library(RDMACM_LIBRARY rdmacm HINTS /home/atr/local/lib)

link_libraries(pthread ${IBVERBS_LIBRARY} ${RDMACM_LIBRARY})

include_directories("${PROJECT_SOURCE_DIR}" "/home/atr/local/include/")

add_executable(rdma_server ${RDMA_SOURCE_DIR}/rdma_common.cpp ${RDMA_SOURCE_DIR}/rdma_server.cpp)

add_executable(test_expand ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_expand.cpp)
add_executable(test_latency ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_latency.cpp)
add_executable(test_multi ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_multi.cpp)
add_executable(test_hybrid_single ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_hybrid_single.cpp)
add_executable(test_hybrid_multi ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_hybrid_multi.cpp)
