include(FetchContent)

find_package(Git QUIET)

if(GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
    execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
                    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                    RESULT_VARIABLE GIT_SUBMOD_RESULT)
    if(NOT GIT_SUBMOD_RESULT EQUAL "0")
        message(FATAL_ERROR "git submodule update --init failed with ${GIT_SUBMOD_RESULT}, please checkout submodules")
    endif()
endif()

set(WITH_TOOLS OFF CACHE BOOL "Disable RocksDB tools")
set(WITH_TESTS OFF CACHE BOOL "Disable RocksDB tests")
set(WITH_SNAPPY ON CACHE BOOL "Enable RocksDB with snappy")
set(WITH_ZSTD ON CACHE BOOL "Enable RocksDB with zstd")
set(WITH_ZLIB ON CACHE BOOL "Enable RocksDB with zlib")
set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "Build static version of RocksDB")
add_subdirectory(rocksdb)

FetchContent_Declare(
    cli11
    QUIET
    GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
    GIT_TAG v2.3.2
)
FetchContent_MakeAvailable(cli11)


FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.x
)
FetchContent_MakeAvailable(spdlog)
# FetchContent_GetProperties(spdlog)
# if (NOT spdlog_POPULATED)
#     FetchContent_Populate(spdlog)
#     add_subdirectory(${spdlog_SOURCE_DIR} ${spdlog_BINARY_DIR})
# endif()

