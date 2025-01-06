# ======================================================================================
# ======================================================================================
cmake_minimum_required(VERSION 3.16)

project(kaplsm
    VERSION 1.0
    DESCRIPTION "Compaction experiments in the kapacity-lsm design space"
    LANGUAGES CXX C
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ======================================================================================
# HEADER options
# ======================================================================================
message(STATUS "${CMAKE_MAKE_PROGRAM}")

add_subdirectory(extern)

# ======================================================================================
# HEADER kaplsm
# ======================================================================================
add_library(kaplsm_lib OBJECT
    ${CMAKE_SOURCE_DIR}/src/kaplsm/kap_compaction.hpp
    ${CMAKE_SOURCE_DIR}/src/kaplsm/kap_options.hpp
)

target_compile_features(kaplsm_lib PRIVATE
    cxx_std_17
)

target_compile_options(kaplsm_lib PRIVATE
    "-Werror"
    "-Wall"
)

target_include_directories(kaplsm_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/extern/rocksdb/include
)

target_link_libraries(kaplsm_lib PUBLIC
    rocksdb
)

# ======================================================================================
# HEADER executables
# ======================================================================================
add_executable(build_db ${CMAKE_SOURCE_DIR}/src/build_db.cpp)
target_link_libraries(build_db PUBLIC kaplsm_lib CLI11::CLI11 spdlog)

add_executable(gen_keys ${CMAKE_SOURCE_DIR}/src/gen_keys.cpp)
target_link_libraries(gen_keys PUBLIC kaplsm_lib CLI11::CLI11 spdlog)
