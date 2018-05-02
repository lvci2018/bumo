

set(LIB_SOCI_POSTGRESQL soci_postgresql)

file(GLOB SOCI_POSTGRESQL_HEADERS *.h)
file(GLOB SOCI_POSTGRESQL_SOURCES *.cpp)

SET(LIBRARY_OUTPUT_PATH ${SOCI_ROOT}/lib)

#aux_source_directory(${BUMO_SRC_DIR}/3rd/soci/src/backends/sqlite3 SOCI_SQLITE_SRC)

message(STATUS "POSTGRESQL_INCLUDE_DIR=" ${POSTGRESQL_INCLUDE_DIR})
message(STATUS "POSTGRESQL_LIBRARIES=" ${POSTGRESQL_LIBRARIES})

include_directories(
    ./
    ../../core
    ${POSTGRESQL_INCLUDE_DIR}
    )

link_directories(
    ${SOCI_ROOT}/lib
    ${POSTGRESQL_LIBRARIES}
    )

#generate static library file
#add_library(${LIB_SOCI_POSTGRESQL} ${SOCI_SQLITE_SRC})
add_library(${LIB_SOCI_POSTGRESQL} ${SOCI_POSTGRESQL_HEADERS} ${SOCI_POSTGRESQL_SOURCES})

# target_compile_options(${LIB_SOCI_POSTGRESQL} 
# 	PUBLIC -DASIO_STANDALONE 
# 	PUBLIC -fPIC
# )

link_libraries(${LIB_SOCI_POSTGRESQL} pq soci)