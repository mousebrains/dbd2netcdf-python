cmake_minimum_required(VERSION 3.17)
project(xarray_dbd_cpp LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler warning flags
if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic
                        -Wshadow -Wconversion -Wsign-conversion -Wnon-virtual-dtor)
endif()

# Optional clang-tidy integration
option(ENABLE_CLANG_TIDY "Enable clang-tidy static analysis" OFF)
if(ENABLE_CLANG_TIDY)
    find_program(CLANG_TIDY_EXE NAMES clang-tidy)
    if(CLANG_TIDY_EXE)
        set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_EXE}")
    else()
        message(WARNING "clang-tidy requested but not found")
    endif()
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

# std::filesystem needs explicit linking on GCC < 9
include(CheckCXXSourceCompiles)
set(CMAKE_REQUIRED_FLAGS "-std=c++17")
check_cxx_source_compiles("
    #include <filesystem>
    int main() { std::filesystem::path p(\"/tmp\"); return p.empty(); }
" STDFS_NO_LIB)
if(NOT STDFS_NO_LIB)
    set(CMAKE_REQUIRED_LIBRARIES stdc++fs)
    check_cxx_source_compiles("
        #include <filesystem>
        int main() { std::filesystem::path p(\"/tmp\"); return p.empty(); }
    " STDFS_NEEDS_STDC_FS)
    unset(CMAKE_REQUIRED_LIBRARIES)
    if(STDFS_NEEDS_STDC_FS)
        target_link_libraries(_dbd_cpp PRIVATE stdc++fs)
    endif()
endif()

# Platform-specific: Windows needs winsock2 for ntohs/ntohl
if(WIN32)
    target_link_libraries(_dbd_cpp PRIVATE ws2_32)
endif()

install(TARGETS _dbd_cpp LIBRARY DESTINATION xarray_dbd)
