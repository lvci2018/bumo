
cmake_minimum_required(VERSION 2.8)

set(LIB_SOCI_SQLITE3 soci_sqlite3)

SET(LIBRARY_OUTPUT_PATH ${SOCI_PATH}/lib)

file(GLOB SOCI_SQLITE3_HEADERS *.h)
file(GLOB SOCI_SQLITE3_SOURCES *.cpp)

include_directories(
    ./
    ../../core
    ../../../../sqlite
    )

#generate static library file
add_library(${LIB_SOCI_SQLITE3} STATIC ${SOCI_SQLITE3_HEADERS} ${SOCI_SQLITE3_SOURCES}) 