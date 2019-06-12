cmake_minimum_required(VERSION 2.8)
project(Dbx1000)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "-g ${CMAKE_CXX_FLAGS}")
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR})

add_definitions(-D NOGRAPHITE=1)
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR} ./benchmarks/ ./concurrency_control/ ./storage/ ./system/)

find_package(Threads)

file(GLOB_RECURSE SOURCE_FILES benchmarks/*.cpp concurrency_control/*.cpp storage/*.cpp system/*.cpp config.cpp)

add_executable(rundb ${SOURCE_FILES})
target_link_libraries(rundb ${CMAKE_THREAD_LIBS_INIT})
