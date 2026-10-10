cmake_minimum_required(VERSION 3.24)
project(mq C)

# Builds the rxmq RXPA plugin (rx_rxmq.rxplugin) against an existing cREXX
# source + build tree and the system librabbitmq (librabbitmq-dev).
#
#   cmake -S . -B build -DCREXX_SOURCE_DIR=/path/to/crexx \
#                       -DCREXX_BUILD_DIR=/path/to/crexx-build
#   cmake --build build
#
# The plugin lands in build/bin/rx_rxmq.rxplugin.  The REXX programs in
# rabbitmq-crexx-client/ and rabbitmq-crexx-server/ import it as "rxmq".

set(CREXX_SOURCE_DIR "" CACHE PATH "cREXX source tree (contains rxpa/crexxpa.h)")
set(CREXX_BUILD_DIR  "" CACHE PATH "cREXX build tree (contains bin/ and generated/)")

if(NOT EXISTS "${CREXX_SOURCE_DIR}/rxpa/crexxpa.h")
    message(FATAL_ERROR "Set -DCREXX_SOURCE_DIR to a cREXX source tree (rxpa/crexxpa.h not found)")
endif()

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_EXTENSIONS ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(RABBITMQ REQUIRED IMPORTED_TARGET librabbitmq)

add_library(_rxmq MODULE rxmq/rxmq.c)
target_include_directories(_rxmq PRIVATE
        "${CREXX_SOURCE_DIR}/rxpa"
        "${CREXX_BUILD_DIR}/generated")
target_compile_definitions(_rxmq PRIVATE BUILD_DLL PLUGIN_ID=rx_rxmq)
target_link_libraries(_rxmq PRIVATE PkgConfig::RABBITMQ)
set_target_properties(_rxmq PROPERTIES
        PREFIX "rx"
        SUFFIX ".rxplugin"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
