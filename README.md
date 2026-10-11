# CommandBuffer

A lock-free command buffer for two threads: one thread records commands, the other
executes them in order. A command is a command type followed by a plain-data payload,
and the executing thread decodes it with a `switch`. Built on a single-producer /
single-consumer ring buffer for variable-sized data, following Kaspar Daugaard's
articles on SPSC ring buffers.

**Read [docs/index.html](docs/index.html)** for the step by step explanation (open it in a
browser, or on the repository's GitHub Pages site). It walks through every design decision,
has an interactive ring buffer simulator, and shows the measurements.

```cpp
enum RenderCommand : cb::CommandType
{
    kCmd_SetClearColor = cb::kCmd_FirstUser, // payload: Color
    kCmd_Present,                            // payload: int frame
};

class RenderCommandBuffer : public cb::CommandBuffer
{
public:
    using CommandBuffer::CommandBuffer;

    // Game thread: write the command type, then its payload.
    void SetClearColor(const Color& color) { WriteCommand(kCmd_SetClearColor, color); }
    void Present(int frame) { WriteCommand(kCmd_Present, frame); }

    // Render thread: wait for each command, read its type and payload, execute it.
    void ExecuteUntilStopped(Renderer& renderer)
    {
        cb::CommandType type;
        while (ReadCommand(type))
        {
            switch (type)
            {
                case kCmd_SetClearColor:
                {
                    renderer.SetClearColor(ReadPayload<Color>());
                    break;
                }

                case kCmd_Present:
                {
                    renderer.Present(ReadPayload<int>());
                    break;
                }
            }
            FinishCommand();
        }
    }
};

RenderCommandBuffer commands(256 * 1024);
std::thread renderThread(&RenderCommandBuffer::ExecuteUntilStopped, &commands, std::ref(renderer));

commands.SetClearColor(Color{0.5f, 0.2f, 0.3f});
commands.Present(frame);
cb::CommandBuffer::Ticket frameEnd = commands.Submit();
// ...
commands.WaitUntilExecuted(frameEnd);
commands.SubmitStop();
renderThread.join();
```

## Layout

| Path | What it is |
| --- | --- |
| `include/cb/SpscRingBuffer.h` | The SPSC byte ring buffer: variable-sized, aligned allocations, cache-line separated state, spin-then-sleep waiting |
| `include/cb/CommandBuffer.h` | Base class for command buffers: command types with plain-data payloads and arrays, batches, tickets, stop |
| `tests/Tests.cpp` | 23 tests: single-threaded layout checks, two-thread stress tests, CPU-time checks that waiting threads sleep, command round trips |
| `bench/Benchmark.cpp` | Throughput, round-trip latency and false-sharing measurements |
| `examples/RenderThreadExample.cpp` | A game loop with a render thread and frame pacing |
| `examples/StoreBufferingLitmus.cpp` | Shows the store-buffer reordering that `seq_cst` prevents: counts stale-load outcomes for relaxed, release/acquire and seq_cst |
| `docs/index.html` | The guide, served by GitHub Pages from the `docs` folder |

## Build and test

Needs a C++20 compiler and CMake 3.20+. On Windows with Visual Studio 2022 or later:

```bat
build.cmd
```

This builds Debug and Release with Ninja and runs the tests. `build.cmd bench` also runs
the benchmark. Or open the folder in Visual Studio (it reads `CMakePresets.json`), or from
a developer prompt:

```bat
cmake --preset release
cmake --build --preset release
ctest --preset release
build\release\CommandBufferBenchmark.exe
```

`CommandBufferTests [filter [timeoutSeconds]]` runs only the tests whose name contains
`filter`. A watchdog aborts any test that runs longer than the timeout, because a hang
means a lost wake-up or a deadlock.

## References

- Kaspar Daugaard, [Writing a Fast and Versatile SPSC Ring Buffer](https://www.daugaard.org/blog/writing-a-fast-and-versatile-spsc-ring-buffer/) (2018)
- Kaspar Daugaard, [Writing a Fast and Versatile SPSC Ring Buffer: Performance Results](https://www.daugaard.org/blog/writing-a-fast-and-versatile-spsc-ring-buffer--performance-results/) (2018)
- Kaspar Daugaard, [How To Make Your SPSC Ring Buffer Do Nothing, More Efficiently](https://www.daugaard.org/blog/how-to-make-your-spsc-ring-buffer-do-nothing-more-efficiently/) (2018)
