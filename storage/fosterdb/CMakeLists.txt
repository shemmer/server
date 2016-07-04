# Copyright (c) 2006, 2010, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

INCLUDE_DIRECTORIES (
        ${CMAKE_CURRENT_SOURCE_DIR}/zero/config
        ${CMAKE_CURRENT_SOURCE_DIR}/zero/src/common
        ${CMAKE_CURRENT_SOURCE_DIR}/zero/src/cmd
        ${CMAKE_CURRENT_SOURCE_DIR}/zero/src/sm
        ${CMAKE_CURRENT_SOURCE_DIR}/generated
        ${CMAKE_CURRENT_SOURCE_DIR}
)
INCLUDE_DIRECTORIES (
        ${CMAKE_CURRENT_BINARY_DIR}/zero/config
        ${CMAKE_CURRENT_BINARY_DIR}/zero/src/common
        ${CMAKE_CURRENT_BINARY_DIR}/zero/src/sm
        ${CMAKE_CURRENT_BINARY_DIR}/zero/src/cmd
        ${CMAKE_CURRENT_BINARY_DIR}/zero
        ${CMAKE_CURRENT_BINARY_DIR}
)

#find_package(FlatBuffers REQUIRED)


find_package(CapnProto REQUIRED)
include_directories(${CAPNP_INCLUDE_DIRS})
add_definitions(${CAPNP_DEFINITIONS})
capnp_generate_cpp(CAPNP_SRCS CAPNP_HDRS /generated/catentries.capnp)

ADD_DEFINITIONS("-std=c++11 -fexceptions ")

add_subdirectory(zero)

SET(FOSTER_SOURCES ha_foster.cpp
        fstr_wrk_thr_t.cpp
        )

MYSQL_ADD_PLUGIN(foster ${FOSTER_SOURCES}
        STORAGE_ENGINE
        MODULE_ONLY
        COMPONENT
        foster)

target_link_libraries(foster sm)
target_link_libraries(foster ${CAPNP_LIBRARIES_LITE})
