#
# Copyright (C) 2021  Christian Berger
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# 

cmake_minimum_required(VERSION 3.2)

project(cluon-cabinet)

################################################################################
# Defining the relevant version of libcluon.
set(CLUON_COMPLETE cluon-complete-v0.0.140.hpp)

################################################################################
# Set the search path for .cmake files.
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}" ${CMAKE_MODULE_PATH})

################################################################################
# This project requires C++14 or newer.
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
IF(NOT(APPLE))
  # Build a static binary.
  set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")
ENDIF()

if("${CMAKE_SYSTEM_NAME}" STREQUAL "Darwin")
  set(CMAKE_CXX_FLAGS "-D_DARWIN_C_SOURCE=1 ")
endif()

# Add further warning levels.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} \
	-D_XOPEN_SOURCE=700 \
	-D_FORTIFY_SOURCE=2 \
	-O2 \
	-fstack-protector \
	-fomit-frame-pointer \
	-pipe \
	-Weffc++ \
	-Wall -Wextra -Wshadow -Wdeprecated \
	-Wdiv-by-zero -Wfloat-equal -Wfloat-conversion -Wsign-compare -Wpointer-arith \
	-Wuninitialized -Wunreachable-code \
	-Wunused -Wunused-function -Wunused-label -Wunused-parameter \
	-Wunused-value -Wunused-variable -Wunused-result \
	-Wmissing-field-initializers -Wmissing-format-attribute -Wmissing-include-dirs -Wmissing-noreturn")

SET(lmdb_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/lmdb/libraries/liblmdb")
INCLUDE_DIRECTORIES (${lmdb_SOURCE_DIR})
SET (lmdb-sources
  ${lmdb_SOURCE_DIR}/mdb.c
  ${lmdb_SOURCE_DIR}/midl.c
)
# Set compile flags for lmdb.
IF(UNIX)
	set(LMDB_COMPILE_FLAGS "-W -Wall -Wno-unused-parameter -Wbad-function-cast -Wuninitialized -O2")
	SET_SOURCE_FILES_PROPERTIES(${lmdb_SOURCE_DIR}/mdb.c PROPERTIES COMPILE_FLAGS "${LMDB_COMPILE_FLAGS}")
	SET_SOURCE_FILES_PROPERTIES(${lmdb_SOURCE_DIR}/midl.c PROPERTIES COMPILE_FLAGS "${LMDB_COMPILE_FLAGS}")
ENDIF()

SET(xxhash_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/xxhash")
INCLUDE_DIRECTORIES (${xxhash_SOURCE_DIR})
SET (xxhash-sources
  ${xxhash_SOURCE_DIR}/xxhash.c
)
# Set compile flags for xxhash.
IF(UNIX)
	set(XXHASH_COMPILE_FLAGS "-Wall -Wextra -Wconversion -Wcast-qual -Wcast-align -Wshadow \
														-Wstrict-aliasing=1 -Wswitch-enum -Wdeclaration-after-statement \
														-Wstrict-prototypes -Wundef -Wpointer-arith -Wformat-security \
														-Wvla -Wformat=2 -Winit-self -Wfloat-equal -Wwrite-strings \
														-Wredundant-decls -Wstrict-overflow=2")
	SET_SOURCE_FILES_PROPERTIES(${xxhash_SOURCE_DIR}/xxhash.c PROPERTIES COMPILE_FLAGS "${XXHASH_COMPILE_FLAGS}")
ENDIF()


# Threads are necessary for linking the resulting binaries as UDPReceiver is running in parallel.
# assume built-in pthreads on MacOS
set(THREADS_PREFER_PTHREAD_FLAG ON)
IF(APPLE)
    set(CMAKE_THREAD_LIBS_INIT "-lpthread")
    set(CMAKE_HAVE_THREADS_LIBRARY 1)
    set(CMAKE_USE_WIN32_THREADS_INIT 0)
    set(CMAKE_USE_PTHREADS_INIT 1)
ELSE()
		find_package(Threads REQUIRED)
		set(LIBRARIES ${LIBRARIES} Threads::Threads)
ENDIF()

if(UNIX)
  if(NOT "${CMAKE_SYSTEM_NAME}" STREQUAL "Darwin")
    find_package(LibRT REQUIRED)
    set(LIBRARIES ${LIBRARIES} ${LIBRT_LIBRARIES})
    include_directories(SYSTEM ${LIBRT_INCLUDE_DIR})
  endif()
endif()

################################################################################
# Create symbolic link to cluon-complete.hpp.
add_custom_command(OUTPUT ${CMAKE_BINARY_DIR}/cluon-complete.hpp
	WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
	COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/src/${CLUON_COMPLETE} ${CMAKE_BINARY_DIR}/cluon-complete.hpp
	DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/${CLUON_COMPLETE})

# Add current build directory as include directory as it contains generated files.
include_directories(SYSTEM ${CMAKE_BINARY_DIR})

################################################################################
# Create executables.
add_executable(rec2cabinet ${CMAKE_CURRENT_SOURCE_DIR}/src/rec2cabinet.cpp ${CMAKE_BINARY_DIR}/cluon-complete.hpp ${lmdb-sources} ${xxhash-sources})
target_link_libraries(rec2cabinet ${LIBRARIES})

add_executable(cabinet2rec ${CMAKE_CURRENT_SOURCE_DIR}/src/cabinet2rec.cpp ${CMAKE_BINARY_DIR}/cluon-complete.hpp ${lmdb-sources} ${xxhash-sources})
target_link_libraries(cabinet2rec ${LIBRARIES})

add_executable(cabinet-ls ${CMAKE_CURRENT_SOURCE_DIR}/src/cabinet-ls.cpp ${CMAKE_BINARY_DIR}/cluon-complete.hpp ${lmdb-sources} ${xxhash-sources})
target_link_libraries(cabinet-ls ${LIBRARIES})

################################################################################
install(TARGETS rec2cabinet DESTINATION bin COMPONENT ${PROJECT_NAME})
install(TARGETS cabinet2rec DESTINATION bin COMPONENT ${PROJECT_NAME})
install(TARGETS cabinet-ls DESTINATION bin COMPONENT ${PROJECT_NAME})
