cmake_minimum_required(VERSION 3.8)
project(paxos)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}")

set(CMAKE_CXX_STANDARD 17)

set(SOURCE_FILES main.cpp)
add_executable(paxos ${SOURCE_FILES})

find_package(rpclib REQUIRED)
target_include_directories(paxos PUBLIC ${RPCLIB_INCLUDE_DIR})
target_link_libraries(paxos PUBLIC ${RPCLIB_LIBS})
if(UNIX AND NOT APPLE)
    target_link_libraries(paxos PUBLIC pthread)
endif()

find_package(TBB REQUIRED)
target_include_directories(paxos PUBLIC ${TBB_INCLUDE_DIR})
target_link_libraries(paxos PUBLIC tbb ${TBB_LIBRARY})
