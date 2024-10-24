cmake_minimum_required (VERSION 2.6)

project (CuckooDuo)

set(PROJECT_SOURCE_DIR ${CMAKE_SOURCE_DIR}/remote)
set(RDMA_SOURCE_DIR ${PROJECT_SOURCE_DIR}/rdma)
set(TEST_DIR ${PROJECT_SOURCE_DIR}/test)
set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/bin)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_BUILD_TYPE Release) # or debug if you need to use gdb on it.

# Some how g++ has better performance than clang++. Of course I don't know all flags for clang++.
set(CMAKE_CXX_COMPILER g++) # or clang++
#set(CMAKE_CXX_COMPILER clang++) # or clang++
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -Ofast -ffast-math -funroll-loops -march=native") # get crazy here
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -Ofast -ffast-math -funroll-loops")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")
#add_compile_options("-std=gnu++14")
#set(CMAKE_CXX_FLAGS "-O0  -ggdb")

find_library(IBVERBS_LIBRARY ibverbs HINTS /home/atr/local/lib)
find_library(RDMACM_LIBRARY rdmacm HINTS /home/atr/local/lib)

link_libraries(pthread ${IBVERBS_LIBRARY} ${RDMACM_LIBRARY})

include_directories("${PROJECT_SOURCE_DIR}" "/home/atr/local/include/")

add_executable(rdma_server ${RDMA_SOURCE_DIR}/rdma_common.cpp ${RDMA_SOURCE_DIR}/rdma_server.cpp)

add_executable(test_expand ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_expand.cpp)
add_executable(test_expand2 ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_expand2.cpp)
add_executable(test_expand3 ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_expand3.cpp)

add_executable(test_latency ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_latency.cpp)
add_executable(test_multi ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_multi.cpp)
add_executable(test_multi_insert ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_multi_insert.cpp)

add_executable(test_hybrid_single ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_hybrid_single.cpp)
add_executable(test_hybrid_multi ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_hybrid_multi.cpp)
add_executable(test_hybrid_cache ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_hybrid_cache.cpp)

add_executable(test_partial_query ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_partial_query.cpp)
add_executable(test_partial_insert ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_partial_insert.cpp)

add_executable(test_bpk_latency ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_bpk_latency.cpp)

add_executable(test_workloada ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_workloada.cpp)
add_executable(test_workloadd ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_workloadd.cpp)

add_executable(test_query_cache ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_query_cache.cpp)

add_executable(test_stash ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_stash.cpp)
add_executable(test_rtt_insert ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_rtt_insert.cpp)

add_executable(test_large_latency ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_large_latency.cpp)
add_executable(test_large_multi ${RDMA_SOURCE_DIR}/rdma_common.cpp ${TEST_DIR}/test_large_multi.cpp)
