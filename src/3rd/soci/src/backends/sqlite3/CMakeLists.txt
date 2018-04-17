

set(LIB_SOCI_SQLITE3 soci_sqlite3)

#file(GLOB SOCI_SQLITE3_HEADERS *.h)
#file(GLOB SOCI_SQLITE3_SOURCES *.cpp)

aux_source_directory(${BUMO_SRC_DIR}/3rd/soci/src/backends/sqlite3 SOCI_SQLITE_SRC)


#generate static library file
add_library(${LIB_SOCI_SQLITE3} ${SOCI_SQLITE_SRC})

target_compile_options(${LIB_SOCI_SQLITE3} 
	PUBLIC -DASIO_STANDALONE 
	PUBLIC -fPIC
)

target_link_libraries(${LIB_SOCI_SQLITE3} sqlite soci)