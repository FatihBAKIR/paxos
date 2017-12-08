cmake_minimum_required(VERSION 3.8)
project(paxos)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}")

set(CMAKE_CXX_STANDARD 17)

set(SOURCE_FILES src/main.cpp include/paxos/remote_end.hpp include/paxos/paxos.hpp include/paxos/local_end.hpp src/local_end.cpp src/paxos.cpp src/remote_end.cpp)
add_executable(paxos ${SOURCE_FILES})

find_package(rpclib REQUIRED)
target_include_directories(paxos PUBLIC ${RPCLIB_INCLUDE_DIR})
target_link_libraries(paxos PUBLIC ${RPCLIB_LIBS})
if(UNIX AND NOT APPLE)
    target_link_libraries(paxos PUBLIC pthread)
endif()

target_include_directories(paxos PUBLIC "include")

target_link_libraries(paxos PUBLIC -static-libstdc++ -static-libgcc)

add_executable(client src/client.cpp)

target_include_directories(client PUBLIC ${RPCLIB_INCLUDE_DIR})
target_link_libraries(client PUBLIC ${RPCLIB_LIBS})
if(UNIX AND NOT APPLE)
    target_link_libraries(client PUBLIC pthread)
endif()

target_link_libraries(client PUBLIC -static-libstdc++ -static-libgcc)