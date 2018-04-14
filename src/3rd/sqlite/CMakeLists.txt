cmake_minimum_required(VERSION 2.8)

set(LIB_SQLITE sqlite)


SET(LIBRARY_OUTPUT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/lib)

file(GLOB SQLITE_HEADERS *.h)
file(GLOB SQLITE_SOURCES *.c)

include_directories(./)

#generate static library file
add_library(${LIB_SQLITE} STATIC ${SQLITE_HEADERS} ${SQLITE_SOURCES}) 