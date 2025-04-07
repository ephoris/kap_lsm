# ======================================================================================
# ======================================================================================
cmake_minimum_required(VERSION 3.16)

project(kaplsm
    VERSION 1.0
    DESCRIPTION "Compaction experiments in the kapacity-lsm design space"
    LANGUAGES CXX C
)

set(CMAKE_BUILD_TYPE "Release")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ======================================================================================
# HEADER options
# ======================================================================================
message(STATUS "${CMAKE_MAKE_PROGRAM}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-rtti -Werror -Wall")

add_subdirectory(extern)

# ======================================================================================
# HEADER kaplsm
# ======================================================================================
add_library(kaplsm_lib OBJECT
    ${CMAKE_SOURCE_DIR}/src/kaplsm/kap_compactor.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/keygen.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/utils.cpp
)

target_compile_features(kaplsm_lib PRIVATE
    cxx_std_17
)

target_include_directories(kaplsm_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/extern/rocksdb/include
    ${CMAKE_SOURCE_DIR}/extern
    ${CMAKE_SOURCE_DIR}/src/kaplsm
    ${CMAKE_SOURCE_DIR}/src/uitls
)

target_link_libraries(kaplsm_lib PUBLIC
    rocksdb
    spdlog
    CLI11::CLI11 
)

# ======================================================================================
# HEADER executables
# ======================================================================================
add_executable(build_db ${CMAKE_SOURCE_DIR}/src/build_db.cpp)
target_link_libraries(build_db PUBLIC kaplsm_lib)

add_executable(run_db ${CMAKE_SOURCE_DIR}/src/run_db.cpp)
target_link_libraries(run_db PUBLIC kaplsm_lib)

add_executable(gen_keys ${CMAKE_SOURCE_DIR}/src/gen_keys.cpp)
target_link_libraries(gen_keys PUBLIC kaplsm_lib)
