

set(LIB_SOCI_SQLITE3 soci_sqlite3)

file(GLOB SOCI_SQLITE3_HEADERS *.h)
file(GLOB SOCI_SQLITE3_SOURCES *.cpp)

SET(LIBRARY_OUTPUT_PATH ${SOCI_ROOT}/lib)

#aux_source_directory(${BUMO_SRC_DIR}/3rd/soci/src/backends/sqlite3 SOCI_SQLITE_SRC)
message(STATUS "--------->SQLITE_DIR=" ${SQLITE_DIR})

include_directories(
    ./
    ${SOCI_ROOT}/src/core
    ${SQLITE_DIR}
    )

link_directories(
    ${SOCI_ROOT}/lib
    ${SQLITE_DIR}/lib
    )

#generate static library file
#add_library(${LIB_SOCI_SQLITE3} ${SOCI_SQLITE_SRC})
add_library(${LIB_SOCI_SQLITE3} ${SOCI_SQLITE3_HEADERS} ${SOCI_SQLITE3_SOURCES})

# target_compile_options(${LIB_SOCI_SQLITE3} 
# 	PUBLIC -DASIO_STANDALONE 
# 	PUBLIC -fPIC
# )

link_libraries(${LIB_SOCI_SQLITE3} sqlite soci)