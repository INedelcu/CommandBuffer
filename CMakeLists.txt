cmake_minimum_required(VERSION 3.20)
project(CommandBuffer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

# Header-only library: include/cb/SpscRingBuffer.h and include/cb/CommandBuffer.h
add_library(CommandBuffer INTERFACE)
target_include_directories(CommandBuffer INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(CommandBuffer INTERFACE Threads::Threads)

if(MSVC)
    set(CB_WARNINGS /W4 /permissive- /Zc:__cplusplus)
else()
    set(CB_WARNINGS -Wall -Wextra -Wpedantic)
endif()

add_executable(CommandBufferTests tests/Tests.cpp)
target_link_libraries(CommandBufferTests PRIVATE CommandBuffer)
target_compile_options(CommandBufferTests PRIVATE ${CB_WARNINGS})

add_executable(CommandBufferBenchmark bench/Benchmark.cpp)
target_link_libraries(CommandBufferBenchmark PRIVATE CommandBuffer)
target_compile_options(CommandBufferBenchmark PRIVATE ${CB_WARNINGS})

add_executable(RenderThreadExample examples/RenderThreadExample.cpp)
target_link_libraries(RenderThreadExample PRIVATE CommandBuffer)
target_compile_options(RenderThreadExample PRIVATE ${CB_WARNINGS})

# Shows the store-buffer reordering that the sleep handshake guards against.
add_executable(StoreBufferingLitmus examples/StoreBufferingLitmus.cpp)
target_link_libraries(StoreBufferingLitmus PRIVATE Threads::Threads)
target_compile_options(StoreBufferingLitmus PRIVATE ${CB_WARNINGS})

enable_testing()
add_test(NAME CommandBufferTests COMMAND CommandBufferTests)
add_test(NAME RenderThreadExample COMMAND RenderThreadExample)
add_test(NAME StoreBufferingLitmus COMMAND StoreBufferingLitmus)
