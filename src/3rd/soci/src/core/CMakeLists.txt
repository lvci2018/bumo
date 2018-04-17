

set(LIB_SOCI soci)

aux_source_directory(${BUMO_SRC_DIR}/3rd/soci/src/core SOCI_SRC)

#file(GLOB SOCI_CORE_HEADERS *.h)
#file(GLOB SOCI_CORE_SOURCES *.cpp)

#generate static library file
add_library(${LIB_SOCI} ${SOCI_SRC}) 

target_compile_options(${LIB_SOCI} 
	PUBLIC -DASIO_STANDALONE 
	PUBLIC -fPIC
)