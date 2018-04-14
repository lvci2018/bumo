
cmake_minimum_required(VERSION 2.8)

set(LIB_SOCI soci)

SET(LIBRARY_OUTPUT_PATH ${SOCI_PATH}/lib)


file(GLOB SOCI_CORE_HEADERS *.h)
file(GLOB SOCI_CORE_SOURCES *.cpp)

include_directories(./)

#generate static library file
add_library(${LIB_SOCI} STATIC ${SOCI_CORE_HEADERS} ${SOCI_CORE_SOURCES}) 