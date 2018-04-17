
set(LIB_SQLITE sqlite)

aux_source_directory(${BUMO_SRC_DIR}/3rd/sqlite/ SQLITE_SRC)

#file(GLOB SQLITE_HEADERS *.h)
#file(GLOB SQLITE_SOURCES *.c)

#generate static library file
add_library(${LIB_SQLITE} ${SQLITE_SRC})

target_compile_options(${LIB_SQLITE} 
	PUBLIC -DASIO_STANDALONE 
	PUBLIC -fPIC
)