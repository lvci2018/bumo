cmake_minimum_required(VERSION 2.8)

project(sqlite)

set(LIB_SQLITE sqlite)

#aux_source_directory(${BUMO_SRC_DIR}/3rd/sqlite/ SQLITE_SRC)

file(GLOB SQLITE_HEADERS *.h)
file(GLOB SQLITE_SOURCES *.c)
SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

#generate static library file
#add_library(${LIB_SQLITE} ${SQLITE_SRC})
add_library(${LIB_SQLITE} ${SQLITE_HEADERS} ${SQLITE_SOURCES})

# target_compile_options(${LIB_SQLITE} 
# 	PUBLIC -DASIO_STANDALONE 
# 	PUBLIC -fPIC
# )