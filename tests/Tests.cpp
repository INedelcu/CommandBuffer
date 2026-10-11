// Tests for SpscRingBuffer and CommandBuffer.
//
// No framework: every test is a function, CHECK() records failures, and a watchdog
// aborts the run if a test hangs (a hang here almost always means a lost wake-up or a
// producer/consumer deadlock). Exit code 0 means everything passed.

#define CB_RING_BUFFER_TESTING
#include "cb/CommandBuffer.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#if defined(_WIN32)
#   define WIN32_LEAN_AND_MEAN
#   define NOMINMAX
#   include <windows.h>
#else
#   include <time.h>
#endif

#if defined(_MSC_VER)
#   pragma warning(disable : 4324) // MixedPayload is padded by its alignas on purpose
#endif

using namespace std::chrono_literals;
using cb::CommandBuffer;
using cb::SpscRingBuffer;

// ---------------------------------------------------------------------------- Mini framework

static std::atomic<int> g_Failures{0};

#define CHECK(cond)                                                                            \
    do                                                                                         \
    {                                                                                          \
        if (!(cond))                                                                           \
        {                                                                                      \
            std::fprintf(stderr, "    CHECK failed: %s  (%s:%d)\n", #cond, __FILE__, __LINE__); \
            ++g_Failures;                                                                      \
        }                                                                                      \
    } while (0)

// Aborts the process if the current test runs longer than the timeout.
class Watchdog
{
public:
    explicit Watchdog(std::chrono::seconds timeout) : m_Timeout(timeout), m_Thread(&Watchdog::Run, this) {}

    ~Watchdog()
    {
        {
            std::lock_guard lock(m_Mutex);
            m_Quit = true;
        }
        m_Changed.notify_one();
        m_Thread.join();
    }

    void StartTest(const char* name)
    {
        std::lock_guard lock(m_Mutex);
        m_TestName = name;
        m_Deadline = std::chrono::steady_clock::now() + m_Timeout;
        ++m_Generation;
        m_Changed.notify_one();
    }

private:
    void Run()
    {
        std::unique_lock lock(m_Mutex);
        while (!m_Quit)
        {
            if (m_TestName == nullptr)
            {
                m_Changed.wait(lock);
                continue;
            }
            // Wait until the next test starts, or the deadline of this one passes.
            std::uint64_t generation = m_Generation;
            while (!m_Quit && m_Generation == generation)
            {
                if (m_Changed.wait_until(lock, m_Deadline) == std::cv_status::timeout && !m_Quit && m_Generation == generation)
                {
                    std::fprintf(stderr, "\nTIMEOUT in %s: the test hung (lost wake-up or deadlock?)\n", m_TestName);
                    std::fflush(stderr);
                    std::_Exit(2);
                }
            }
        }
    }

    std::chrono::seconds m_Timeout;
    std::mutex m_Mutex;
    std::condition_variable m_Changed;
    const char* m_TestName = nullptr;
    std::chrono::steady_clock::time_point m_Deadline{};
    std::uint64_t m_Generation = 0;
    bool m_Quit = false;
    std::thread m_Thread;
};

// CPU time consumed by the calling thread, to tell sleeping from spinning.
static std::chrono::microseconds ThreadCpuTime()
{
#if defined(_WIN32)
    FILETIME creation, exit, kernel, user;
    GetThreadTimes(GetCurrentThread(), &creation, &exit, &kernel, &user);
    std::uint64_t kernel100ns = (std::uint64_t(kernel.dwHighDateTime) << 32) | kernel.dwLowDateTime;
    std::uint64_t user100ns = (std::uint64_t(user.dwHighDateTime) << 32) | user.dwLowDateTime;
    return std::chrono::microseconds((kernel100ns + user100ns) / 10);
#else
    timespec ts;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return std::chrono::microseconds(std::int64_t(ts.tv_sec) * 1000000 + ts.tv_nsec / 1000);
#endif
}

static bool IsAligned(const void* p, std::size_t alignment)
{
    return reinterpret_cast<std::uintptr_t>(p) % alignment == 0;
}

static long long Milliseconds(std::chrono::steady_clock::duration d)
{
    return static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(d).count());
}

// ---------------------------------------------------------------------------- Test commands

// The command types of TestCommandBuffer.
enum TestCommand : cb::CommandType
{
    kCmd_CheckSequence = cb::kCmd_FirstUser, // payload: std::uint32_t, the expected sequence number
    kCmd_AppendResult,                       // payload: int
    kCmd_SumArray,                           // payload: SumArrayPayload, then `count` ints
    kCmd_Mixed,                              // payload: MixedPayload (odd size, 32-byte alignment)
    kCmd_Sleep,                              // payload: int milliseconds
    kCmd_SampleCpuTime,                      // no payload
    kCmd_Increment,                          // no payload
    kCmd_FrameDone,                          // no payload
};

struct SumArrayPayload
{
    std::uint32_t count;
    long long expectedSum;
};

struct alignas(32) MixedPayload
{
    char tag;
    double value;
    std::uint16_t check;
};

static std::uint16_t MixedCheck(char tag, double value)
{
    return static_cast<std::uint16_t>(tag * 31 + static_cast<int>(value * 8));
}

// Everything the test commands touch. Only the consumer thread writes it while commands
// run; the producer reads it after WaitUntilExecuted() or after joining the consumer.
struct TestState
{
    std::thread::id consumerThread;
    bool wrongThread = false;
    std::uint32_t nextSequence = 0;
    bool outOfOrder = false;
    std::vector<int> results;
    int mismatches = 0;
    std::chrono::microseconds cpuTime{};
    int counter = 0;
    std::atomic<int> framesDone{0}; // also read by the producer while commands run
};

class TestCommandBuffer : public CommandBuffer
{
public:
    using CommandBuffer::CommandBuffer;

    // ------------------------------------------------------------- Producer thread

    void CheckSequence(std::uint32_t sequence) { WriteCommand(kCmd_CheckSequence, sequence); }
    void AppendResult(int value) { WriteCommand(kCmd_AppendResult, value); }
    void Mixed(const MixedPayload& payload) { WriteCommand(kCmd_Mixed, payload); }
    void Sleep(int milliseconds) { WriteCommand(kCmd_Sleep, milliseconds); }
    void SampleCpuTime() { WriteCommand(kCmd_SampleCpuTime); }
    void Increment() { WriteCommand(kCmd_Increment); }
    void FrameDone() { WriteCommand(kCmd_FrameDone); }

    void SumArray(const int* values, std::uint32_t count, long long expectedSum)
    {
        WriteCommand(kCmd_SumArray, SumArrayPayload{count, expectedSum});
        WriteArray(values, count);
    }

    // ------------------------------------------------------------- Consumer thread

    void ExecuteUntilStopped(TestState& state)
    {
        cb::CommandType type;
        while (ReadCommand(type))
        {
            switch (type)
            {
                case kCmd_CheckSequence:
                {
                    std::uint32_t sequence = ReadPayload<std::uint32_t>();
                    state.wrongThread |= std::this_thread::get_id() != state.consumerThread;
                    state.outOfOrder |= sequence != state.nextSequence;
                    ++state.nextSequence;
                    break;
                }

                case kCmd_AppendResult:
                {
                    state.results.push_back(ReadPayload<int>());
                    break;
                }

                case kCmd_SumArray:
                {
                    const SumArrayPayload& payload = ReadPayload<SumArrayPayload>();
                    const int* values = ReadArray<int>(payload.count);
                    long long sum = 0;
                    for (std::uint32_t i = 0; i < payload.count; ++i)
                        sum += values[i];
                    state.mismatches += sum != payload.expectedSum;
                    break;
                }

                case kCmd_Mixed:
                {
                    const MixedPayload& payload = ReadPayload<MixedPayload>();
                    bool ok = IsAligned(&payload, alignof(MixedPayload)) && MixedCheck(payload.tag, payload.value) == payload.check;
                    state.mismatches += !ok;
                    break;
                }

                case kCmd_Sleep:
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(ReadPayload<int>()));
                    break;
                }

                case kCmd_SampleCpuTime:
                {
                    state.cpuTime = ThreadCpuTime();
                    break;
                }

                case kCmd_Increment:
                {
                    ++state.counter;
                    break;
                }

                case kCmd_FrameDone:
                {
                    state.framesDone.fetch_add(1);
                    break;
                }

                default:
                {
                    CHECK(type == kCmd_CheckSequence); // unknown command type: always fails
                    break;
                }
            }
            FinishCommand();
        }
    }
};

// Starts the consumer thread of a TestCommandBuffer.
static std::thread StartConsumer(TestCommandBuffer& commands, TestState& state)
{
    std::thread consumer(&TestCommandBuffer::ExecuteUntilStopped, &commands, std::ref(state));
    state.consumerThread = consumer.get_id(); // published to the consumer by the first Submit()
    return consumer;
}

// ---------------------------------------------------------------------------- Ring buffer, one thread

// Values written are read back unchanged, in order.
static void RingBuffer_ValuesRoundTrip()
{
    struct Pod
    {
        char c;
        std::int64_t x;
    };

    SpscRingBuffer ring(1024);
    ring.Write<int>(42);
    ring.Write<double>(3.5);
    ring.Write(Pod{'a', -7});
    const int values[] = {1, 2, 3, 4, 5};
    ring.WriteArray(values, 5);
    ring.FinishWrite();

    CHECK(ring.Read<int>() == 42);
    CHECK(ring.Read<double>() == 3.5);
    const Pod& pod = ring.Read<Pod>();
    CHECK(pod.c == 'a' && pod.x == -7);
    const int* array = ring.ReadArray<int>(5);
    for (int i = 0; i < 5; ++i)
        CHECK(array[i] == i + 1);
    ring.FinishRead();
}

// Every allocation honours its alignment, and the consumer gets the very same addresses.
static void RingBuffer_Alignment()
{
    SpscRingBuffer ring(1024);
    void* w1 = ring.PrepareWrite(1, 1);   // [0, 1)
    void* w2 = ring.PrepareWrite(32, 32); // padded to [32, 64)
    void* w3 = ring.PrepareWrite(3, 2);   // [64, 67)
    void* w4 = ring.PrepareWrite(8, 64);  // padded to [128, 136)
    CHECK(IsAligned(w1, 64)); // the storage itself is cache-line aligned
    CHECK(static_cast<char*>(w2) - static_cast<char*>(w1) == 32);
    CHECK(static_cast<char*>(w3) - static_cast<char*>(w1) == 64);
    CHECK(static_cast<char*>(w4) - static_cast<char*>(w1) == 128);
    CHECK(ring.WritePosition() == 136);
    ring.FinishWrite();

    CHECK(ring.PrepareRead(1, 1) == w1);
    CHECK(ring.PrepareRead(32, 32) == w2);
    CHECK(ring.PrepareRead(3, 2) == w3);
    CHECK(ring.PrepareRead(8, 64) == w4);
    ring.FinishRead();
}

// A block that does not fit before the end of the buffer starts at offset 0 of the next
// lap, leaving a gap, and the consumer skips the same gap.
static void RingBuffer_WrapAroundLeavesGap()
{
    SpscRingBuffer ring(256);
    char* a = static_cast<char*>(ring.PrepareWrite(100, 4)); // [0, 100)
    char* b = static_cast<char*>(ring.PrepareWrite(100, 4)); // [100, 200)
    std::memset(a, 'A', 100);
    std::memset(b, 'B', 100);
    ring.FinishWrite();

    CHECK(ring.PrepareRead(100, 4) == a);
    ring.FinishRead(); // releases [0, 100)

    // 100 bytes do not fit in [200, 256): wraps to [0, 100) of the second lap.
    char* c = static_cast<char*>(ring.PrepareWrite(100, 4));
    CHECK(c == a);
    CHECK(ring.WritePosition() == 256 + 100);
    std::memset(c, 'C', 100);
    ring.FinishWrite();

    const char* rb = static_cast<const char*>(ring.PrepareRead(100, 4));
    const char* rc = static_cast<const char*>(ring.PrepareRead(100, 4)); // the consumer wraps too
    CHECK(rb == b && rb[0] == 'B' && rb[99] == 'B');
    CHECK(rc == a && rc[0] == 'C' && rc[99] == 'C');
    ring.FinishRead();
}

// A full buffer and an empty one are told apart without wasting a byte: stream positions
// are never reduced modulo the capacity.
static void RingBuffer_FillsCompletely()
{
    SpscRingBuffer ring(256);
    for (int lap = 0; lap < 3; ++lap)
    {
        for (int i = 0; i < 4; ++i)
            std::memset(ring.PrepareWrite(64, 64), lap * 4 + i, 64);
        ring.FinishWrite(); // the producer is now exactly one buffer ahead
        CHECK(ring.WritePosition() == std::size_t(lap + 1) * 256);

        for (int i = 0; i < 4; ++i)
            CHECK(static_cast<const unsigned char*>(ring.PrepareRead(64, 64))[63] == lap * 4 + i);
        ring.FinishRead();
    }
}

// Objects with constructors and destructors live in the buffer; the consumer destroys them.
static void RingBuffer_EmplaceObjects()
{
    SpscRingBuffer ring(1024);
    ring.Emplace<std::string>("a string long enough to need a heap allocation");
    ring.Emplace<std::vector<int>>(std::vector<int>{1, 2, 3});
    ring.Emplace<std::unique_ptr<int>>(std::make_unique<int>(7));
    ring.FinishWrite();

    std::string& s = ring.Read<std::string>();
    CHECK(s == "a string long enough to need a heap allocation");
    std::destroy_at(&s);
    std::vector<int>& v = ring.Read<std::vector<int>>();
    CHECK(v.size() == 3 && v[2] == 3);
    std::destroy_at(&v);
    std::unique_ptr<int>& p = ring.Read<std::unique_ptr<int>>();
    CHECK(*p == 7);
    std::destroy_at(&p);
    ring.FinishRead();
}

// ---------------------------------------------------------------------------- Ring buffer, two threads

struct RecordHeader
{
    std::uint32_t sequence;
    std::uint16_t size;
    std::uint8_t alignmentLog2;
};

static std::uint8_t PayloadByte(std::uint32_t sequence, std::size_t i)
{
    return static_cast<std::uint8_t>(sequence * 131 + i * 7);
}

struct StreamTest
{
    SpscRingBuffer* ring;
    std::uint32_t recordCount;
};

// Producer thread of StreamRandomRecords().
static void StreamProducer(StreamTest* test)
{
    SpscRingBuffer& ring = *test->ring;
    std::mt19937 rng(1234);
    for (std::uint32_t seq = 0; seq < test->recordCount; ++seq)
    {
        RecordHeader header{seq, static_cast<std::uint16_t>(rng() % 201), static_cast<std::uint8_t>(rng() % 7)};
        ring.Write(header);
        auto* payload = static_cast<std::uint8_t*>(ring.PrepareWrite(header.size, std::size_t(1) << header.alignmentLog2));
        for (std::size_t i = 0; i < header.size; ++i)
            payload[i] = PayloadByte(seq, i);
        if (rng() % 8 == 0)
            ring.FinishWrite();
    }
    ring.FinishWrite();
}

// Two threads stream records of random size (0..200 bytes) and alignment (1..64), with
// random publish and release points. The consumer checks every byte. A small capacity
// forces constant wrap-around and waiting on both sides.
//
// The consumer releases every 1..maxHeldRecords records. While it waits for more data it
// may hold that many records (up to ~270 bytes each) plus the current header, so this must
// stay well below the capacity, or the producer could never get the space it waits for.
// maxHeldRecords == 0 means: never FinishRead(), only FinishReadLazy() after each record.
static void StreamRandomRecords(std::size_t capacity, std::uint32_t spinCount, std::uint32_t recordCount,
                                std::uint32_t maxHeldRecords = 4, std::size_t startPosition = 0)
{
    SpscRingBuffer ring(capacity, spinCount);
    ring.TestOnlySetStreamPosition(startPosition);
    StreamTest test{&ring, recordCount};
    std::thread producer(StreamProducer, &test);

    std::mt19937 rng(5678);
    std::uint32_t held = 0;
    for (std::uint32_t seq = 0; seq < recordCount; ++seq)
    {
        const RecordHeader header = ring.Read<RecordHeader>();
        if (header.sequence != seq)
        {
            CHECK(header.sequence == seq);
            std::_Exit(3); // the stream is out of sync; everything after this would be noise
        }
        std::size_t alignment = std::size_t(1) << header.alignmentLog2;
        auto* payload = static_cast<const std::uint8_t*>(ring.PrepareRead(header.size, alignment));
        bool ok = IsAligned(payload, alignment);
        for (std::size_t i = 0; i < header.size; ++i)
            ok &= payload[i] == PayloadByte(seq, i);
        CHECK(ok);
        if (maxHeldRecords == 0)
        {
            ring.FinishReadLazy();
        }
        else if (++held >= 1 + rng() % maxHeldRecords)
        {
            ring.FinishRead();
            held = 0;
        }
    }
    ring.FinishRead();
    producer.join();
    CHECK(ring.WritePosition() - startPosition > recordCount * sizeof(RecordHeader));
}

static void RingBuffer_TwoThreads_SmallBuffer() { StreamRandomRecords(4096, SpscRingBuffer::kDefaultSpinCount, 2'000'000); }
static void RingBuffer_TwoThreads_LargeBuffer() { StreamRandomRecords(1 << 20, SpscRingBuffer::kDefaultSpinCount, 2'000'000); }
static void RingBuffer_TwoThreads_NoSpinning() { StreamRandomRecords(4096, 0, 300'000); } // every wait sleeps on a semaphore
static void RingBuffer_TwoThreads_TinyBuffer() { StreamRandomRecords(768, 16, 500'000, 1); } // records up to ~1/3 of the capacity
static void RingBuffer_TwoThreads_PositionOverflow() { StreamRandomRecords(4096, SpscRingBuffer::kDefaultSpinCount, 500'000, 4, SIZE_MAX - 50'000); }
static void RingBuffer_TwoThreads_LazyRelease() { StreamRandomRecords(4096, SpscRingBuffer::kDefaultSpinCount, 2'000'000, 0); }
static void RingBuffer_TwoThreads_LazyReleaseNoSpinning() { StreamRandomRecords(768, 0, 300'000, 0); }

// Consumer thread of RingBuffer_LazyReleaseBeforeConsumerWaits().
static void LazyConsumer(SpscRingBuffer* ring, int itemCount)
{
    for (int i = 0; i < itemCount; ++i)
    {
        CHECK(ring->Read<int>() == i);
        ring->FinishReadLazy(); // 4 bytes: far below the lazy threshold
    }
    ring->FinishRead(); // the last item: no wait follows that would release it
}

// A consumer that only ever finishes lazily must still release before it waits for more
// data: here the producer waits for every single item to be consumed before it writes the
// next one, so each lazily finished item has to be released by the consumer's wait.
static void RingBuffer_LazyReleaseBeforeConsumerWaits()
{
    constexpr int kItems = 20'000;
    SpscRingBuffer ring(4096);
    std::thread consumer(LazyConsumer, &ring, kItems);
    for (int i = 0; i < kItems; ++i)
    {
        ring.Write<int>(i);
        ring.WaitUntilConsumed(ring.WritePosition());
    }
    consumer.join();
}

// ---------------------------------------------------------------------------- Waiting without spinning

// A consumer waiting on an empty buffer sleeps instead of burning a core.
static void RingBuffer_EmptyBufferConsumerSleeps()
{
    TestCommandBuffer commands(4096);
    TestState state;
    std::thread consumer = StartConsumer(commands, state);

    std::this_thread::sleep_for(300ms); // the consumer has nothing to do all this time
    commands.SampleCpuTime();
    commands.SubmitStop();
    consumer.join();

    std::printf("    consumer CPU time while waiting 300 ms: %lld us\n", static_cast<long long>(state.cpuTime.count()));
    CHECK(state.cpuTime < 50ms);
}

// A producer waiting on a full buffer sleeps instead of burning a core.
static void RingBuffer_FullBufferProducerSleeps()
{
    TestCommandBuffer commands(4096);
    TestState state;
    std::thread consumer = StartConsumer(commands, state);

    commands.Sleep(300); // stall the consumer
    commands.Submit();
    std::chrono::microseconds cpuBefore = ThreadCpuTime();
    std::chrono::steady_clock::time_point timeBefore = std::chrono::steady_clock::now();
    for (std::uint32_t i = 0; i < 2000; ++i) // 16 KB of commands into a 4 KB buffer: must wait for the stall
        commands.CheckSequence(i);
    std::chrono::microseconds cpuUsed = ThreadCpuTime() - cpuBefore;
    std::chrono::steady_clock::duration waited = std::chrono::steady_clock::now() - timeBefore;
    commands.SubmitStop();
    consumer.join();

    std::printf("    producer waited %lld ms using %lld us of CPU\n", Milliseconds(waited), static_cast<long long>(cpuUsed.count()));
    CHECK(waited > 200ms);
    CHECK(cpuUsed < 50ms);
    CHECK(state.nextSequence == 2000 && !state.outOfOrder);
}

// ---------------------------------------------------------------------------- Command buffer

// Commands run on the consumer thread, exactly once each, in the order they were recorded.
static void CommandBuffer_ExecutesInOrder()
{
    constexpr std::uint32_t kCount = 2'000'000;
    TestCommandBuffer commands(64 * 1024);
    TestState state;
    std::thread consumer = StartConsumer(commands, state);

    for (std::uint32_t i = 0; i < kCount; ++i)
    {
        commands.CheckSequence(i);
        if (i % 64 == 63)
            commands.Submit();
    }
    commands.SubmitStop();
    consumer.join();

    CHECK(state.nextSequence == kCount);
    CHECK(!state.outOfOrder);
    CHECK(!state.wrongThread);
}

// Commands with different payload sizes and alignments, interleaved at random in a small
// buffer, all arrive intact: each case reads exactly what its producer function wrote.
static void CommandBuffer_PayloadsRoundTrip()
{
    TestCommandBuffer commands(1024);
    TestState state;
    std::thread consumer = StartConsumer(commands, state);

    std::mt19937 rng(7);
    std::vector<int> expectedResults;
    std::uint32_t sequence = 0;
    int values[7];
    for (int i = 0; i < 200'000; ++i)
    {
        switch (rng() % 5)
        {
            case 0:
            {
                commands.CheckSequence(sequence++);
                break;
            }
            case 1:
            {
                expectedResults.push_back(static_cast<int>(rng()));
                commands.AppendResult(expectedResults.back());
                break;
            }
            case 2:
            {
                char tag = static_cast<char>('a' + rng() % 26);
                double value = (rng() % 1000) / 8.0;
                commands.Mixed(MixedPayload{tag, value, MixedCheck(tag, value)});
                break;
            }
            case 3:
            {
                std::uint32_t count = rng() % 8;
                long long sum = 0;
                for (std::uint32_t j = 0; j < count; ++j)
                    sum += values[j] = static_cast<int>(rng() % 1000);
                commands.SumArray(values, count, sum);
                break;
            }
            default:
            {
                commands.Increment();
                break;
            }
        }
        if (rng() % 16 == 0)
            commands.Submit();
    }
    commands.SubmitStop();
    consumer.join();

    CHECK(state.mismatches == 0);
    CHECK(state.nextSequence == sequence && !state.outOfOrder);
    CHECK(state.results == expectedResults);
}

// After WaitUntilExecuted() the producer sees everything the commands wrote, with no
// atomics in the test: the ring buffer's release/acquire pair orders it.
static void CommandBuffer_WaitUntilExecutedSynchronizes()
{
    TestCommandBuffer commands(4096);
    TestState state;
    std::thread consumer = StartConsumer(commands, state);

    bool ok = true;
    for (int i = 0; i < 20'000; ++i)
    {
        commands.AppendResult(i * i);
        CommandBuffer::Ticket ticket = commands.Submit();
        commands.WaitUntilExecuted(ticket);
        ok &= state.results.size() == std::size_t(i) + 1 && state.results.back() == i * i;
    }
    CHECK(ok);

    commands.SubmitStop();
    consumer.join();
}

// A ticket is reached when its batch has run, even if the consumer has already moved on to
// a slow command from a later batch. (The consumer releases lazily within a batch, but
// eagerly at the end of each batch.)
static void CommandBuffer_TicketDoesNotWaitForLaterCommands()
{
    TestCommandBuffer commands(4096);
    TestState state;
    std::thread consumer = StartConsumer(commands, state);

    commands.Increment();
    CommandBuffer::Ticket first = commands.Submit();
    commands.Sleep(400);
    commands.Submit();
    std::this_thread::sleep_for(100ms); // the consumer is now inside the slow command

    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
    commands.WaitUntilExecuted(first);
    std::chrono::steady_clock::duration waited = std::chrono::steady_clock::now() - start;
    std::printf("    waited %lld us for a batch that had already run\n",
                static_cast<long long>(std::chrono::duration_cast<std::chrono::microseconds>(waited).count()));
    CHECK(waited < 50ms);

    commands.SubmitStop();
    consumer.join();
}

// Strict ping-pong with spinning disabled: every round trip puts the consumer to sleep on
// an empty buffer and the producer to sleep in WaitUntilExecuted(), so each one exercises
// the sleep/wake-up handshake on both sides. A lost wake-up hangs this test.
static void CommandBuffer_PingPongAlwaysSleeping()
{
    TestCommandBuffer commands(4096, 0);
    TestState state;
    std::thread consumer = StartConsumer(commands, state);

    bool ok = true;
    for (int i = 0; i < 50'000; ++i)
    {
        commands.Increment();
        commands.SubmitAndWait();
        ok &= state.counter == i + 1;
    }
    CHECK(ok);

    commands.SubmitStop();
    consumer.join();
}

// Variable-length arrays travel in the command buffer right after their payload.
static void CommandBuffer_ArrayPayloads()
{
    TestCommandBuffer commands(16 * 1024);
    TestState state;
    std::thread consumer = StartConsumer(commands, state);

    std::mt19937 rng(42);
    std::vector<int> values;
    for (int i = 0; i < 100'000; ++i)
    {
        values.resize(rng() % 300); // 0..299 ints, up to 1.2 KB
        long long expected = 0;
        for (int& v : values)
            expected += v = static_cast<int>(rng() % 1000);
        commands.SumArray(values.data(), static_cast<std::uint32_t>(values.size()), expected);
        if (i % 16 == 0)
            commands.Submit();
    }
    commands.SubmitAndWait();
    CHECK(state.mismatches == 0);

    commands.SubmitStop();
    consumer.join();
}

// A consumer that is much slower than the producer: the producer keeps blocking on a full
// buffer and nothing is lost or reordered.
static void CommandBuffer_SlowConsumer()
{
    TestCommandBuffer commands(1024);
    TestState state;
    std::thread consumer = StartConsumer(commands, state);

    for (int i = 0; i < 3000; ++i)
    {
        commands.AppendResult(i);
        if (i % 100 == 0)
            commands.Sleep(1);
    }
    commands.SubmitStop();
    consumer.join();

    bool inOrder = state.results.size() == 3000;
    for (std::size_t i = 0; inOrder && i < state.results.size(); ++i)
        inOrder = state.results[i] == static_cast<int>(i);
    CHECK(inOrder);
}

// The render-thread pattern: the game thread records one frame of commands per iteration
// and never has more than kMaxFramesInFlight frames (including the one it is recording)
// that the render thread has not finished.
static void CommandBuffer_RenderLoopWithFramePacing()
{
    constexpr int kFrames = 500;
    constexpr int kMaxFramesInFlight = 2;
    TestCommandBuffer commands(64 * 1024);
    TestState state;
    std::thread renderThread = StartConsumer(commands, state);

    std::vector<CommandBuffer::Ticket> frameEnd(kFrames);
    int maxAhead = 0;
    for (int frame = 0; frame < kFrames; ++frame)
    {
        if (frame >= kMaxFramesInFlight)
            commands.WaitUntilExecuted(frameEnd[frame - kMaxFramesInFlight]);
        maxAhead = std::max(maxAhead, frame + 1 - state.framesDone.load()); // unfinished frames, this one included

        for (int draw = 0; draw < 200; ++draw)
            commands.Increment();
        commands.FrameDone();
        frameEnd[frame] = commands.Submit();
    }
    commands.SubmitStop();
    renderThread.join();

    CHECK(state.framesDone == kFrames);
    CHECK(state.counter == kFrames * 200);
    CHECK(maxAhead <= kMaxFramesInFlight);
}

// ---------------------------------------------------------------------------- main

struct Test
{
    const char* name;
    void (*run)();
};

static const Test kTests[] = {
    {"RingBuffer_ValuesRoundTrip", RingBuffer_ValuesRoundTrip},
    {"RingBuffer_Alignment", RingBuffer_Alignment},
    {"RingBuffer_WrapAroundLeavesGap", RingBuffer_WrapAroundLeavesGap},
    {"RingBuffer_FillsCompletely", RingBuffer_FillsCompletely},
    {"RingBuffer_EmplaceObjects", RingBuffer_EmplaceObjects},
    {"RingBuffer_TwoThreads_SmallBuffer", RingBuffer_TwoThreads_SmallBuffer},
    {"RingBuffer_TwoThreads_LargeBuffer", RingBuffer_TwoThreads_LargeBuffer},
    {"RingBuffer_TwoThreads_NoSpinning", RingBuffer_TwoThreads_NoSpinning},
    {"RingBuffer_TwoThreads_TinyBuffer", RingBuffer_TwoThreads_TinyBuffer},
    {"RingBuffer_TwoThreads_PositionOverflow", RingBuffer_TwoThreads_PositionOverflow},
    {"RingBuffer_TwoThreads_LazyRelease", RingBuffer_TwoThreads_LazyRelease},
    {"RingBuffer_TwoThreads_LazyReleaseNoSpinning", RingBuffer_TwoThreads_LazyReleaseNoSpinning},
    {"RingBuffer_LazyReleaseBeforeConsumerWaits", RingBuffer_LazyReleaseBeforeConsumerWaits},
    {"RingBuffer_EmptyBufferConsumerSleeps", RingBuffer_EmptyBufferConsumerSleeps},
    {"RingBuffer_FullBufferProducerSleeps", RingBuffer_FullBufferProducerSleeps},
    {"CommandBuffer_ExecutesInOrder", CommandBuffer_ExecutesInOrder},
    {"CommandBuffer_PayloadsRoundTrip", CommandBuffer_PayloadsRoundTrip},
    {"CommandBuffer_WaitUntilExecutedSynchronizes", CommandBuffer_WaitUntilExecutedSynchronizes},
    {"CommandBuffer_TicketDoesNotWaitForLaterCommands", CommandBuffer_TicketDoesNotWaitForLaterCommands},
    {"CommandBuffer_PingPongAlwaysSleeping", CommandBuffer_PingPongAlwaysSleeping},
    {"CommandBuffer_ArrayPayloads", CommandBuffer_ArrayPayloads},
    {"CommandBuffer_SlowConsumer", CommandBuffer_SlowConsumer},
    {"CommandBuffer_RenderLoopWithFramePacing", CommandBuffer_RenderLoopWithFramePacing},
};

// Usage: CommandBufferTests [filter [timeoutSeconds]]
// Runs the tests whose name contains `filter`; a test running longer than the timeout
// (default 60 s) aborts the run.
int main(int argc, char** argv)
{
    const char* filter = argc > 1 ? argv[1] : "";
    const std::chrono::seconds timeout(argc > 2 ? std::atoi(argv[2]) : 60);

    Watchdog watchdog(timeout);
    int failedTests = 0;
    int ranTests = 0;
    for (const Test& test : kTests)
    {
        if (std::strstr(test.name, filter) == nullptr)
            continue;
        ++ranTests;
        std::printf("[ RUN  ] %s\n", test.name);
        std::fflush(stdout);
        watchdog.StartTest(test.name);
        int failuresBefore = g_Failures;
        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        test.run();
        long long ms = Milliseconds(std::chrono::steady_clock::now() - start);
        bool passed = g_Failures == failuresBefore;
        failedTests += !passed;
        std::printf("[ %s ] %s (%lld ms)\n", passed ? " OK " : "FAIL", test.name, ms);
        std::fflush(stdout);
    }

    std::printf("\n%d of %d tests passed\n", ranTests - failedTests, ranTests);
    return failedTests == 0 && ranTests > 0 ? 0 : 1;
}
