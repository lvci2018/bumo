

set(LIB_SOCI_POSTGRESQL soci_postgresql)

file(GLOB SOCI_POSTGRESQL_HEADERS *.h)
file(GLOB SOCI_POSTGRESQL_SOURCES *.cpp)

SET(LIBRARY_OUTPUT_PATH ${SOCI_ROOT}/lib)

#aux_source_directory(${BUMO_SRC_DIR}/3rd/soci/src/backends/sqlite3 SOCI_SQLITE_SRC)

message(STATUS "--------->POSTGRESQL_DIR=" ${POSTGRESQL_DIR})

include_directories(
    ./
    ${SOCI_ROOT}/src/core
    ${POSTGRESQL_DIR}/src/include
    ${POSTGRESQL_DIR}/src/interfaces/libpq
    )

link_directories(
    ${SOCI_ROOT}/lib
    ${POSTGRESQL_DIR}/src/interfaces/libpq
    )

#generate static library file
#add_library(${LIB_SOCI_POSTGRESQL} ${SOCI_SQLITE_SRC})
add_library(${LIB_SOCI_POSTGRESQL} ${SOCI_POSTGRESQL_HEADERS} ${SOCI_POSTGRESQL_SOURCES})

# target_compile_options(${LIB_SOCI_POSTGRESQL} 
# 	PUBLIC -DASIO_STANDALONE 
# 	PUBLIC -fPIC
# )

link_libraries(${LIB_SOCI_POSTGRESQL} ${POSTGRESQL_DIR}/src/interfaces/libpq/libpq.a soci)
