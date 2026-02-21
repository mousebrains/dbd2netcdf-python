cmake_minimum_required(VERSION 3.17)
project(xarray_dbd_cpp LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler warning flags
if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(pybind11 2.11...4 CONFIG REQUIRED)

pybind11_add_module(_dbd_cpp
    csrc/dbd_python.cpp
    csrc/ColumnData.C
    csrc/Header.C
    csrc/Sensor.C
    csrc/Sensors.C
    csrc/SensorsMap.C
    csrc/KnownBytes.C
    csrc/Decompress.C
    csrc/Data.C
    csrc/lz4.c
)

target_include_directories(_dbd_cpp PRIVATE csrc)

# Suppress warnings for vendored lz4.c
if(MSVC)
    set_source_files_properties(csrc/lz4.c PROPERTIES COMPILE_FLAGS /w)
else()
    set_source_files_properties(csrc/lz4.c PROPERTIES COMPILE_FLAGS -w)
endif()

# Platform-specific: Windows needs winsock2 for ntohs/ntohl
if(WIN32)
    target_link_libraries(_dbd_cpp PRIVATE ws2_32)
endif()

install(TARGETS _dbd_cpp LIBRARY DESTINATION xarray_dbd)
