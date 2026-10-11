// Benchmarks for SpscRingBuffer and CommandBuffer. Build in Release.
// Every number is the median of kRuns runs.
//
//   1. Raw throughput: 4-byte ints through a 1 MB ring buffer, publishing and releasing
//      every 1, 16 or 256 items. This is the benchmark from Kaspar Daugaard's "Performance
//      Results" post, scaled down from 1 billion to 100 million ints.
//   2. Command throughput: small commands (a type and an 8-byte payload) through
//      CommandBuffer vs. the textbook std::mutex + std::condition_variable + std::deque queue.
//   3. Round-trip latency: record one command and wait for it to run, spinning vs. sleeping.
//   4. False sharing: two threads incrementing their own counters, in one cache line vs. two.

#include "cb/CommandBuffer.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#if defined(_MSC_VER)
#   pragma warning(disable : 4324) // padding due to alignas is intended in BenchFalseSharing
#endif

using Clock = std::chrono::steady_clock;

constexpr int kRuns = 5;

static double SecondsSince(Clock::time_point start)
{
    return std::chrono::duration<double>(Clock::now() - start).count();
}

static double Median(std::vector<double> seconds)
{
    std::sort(seconds.begin(), seconds.end());
    return seconds[seconds.size() / 2];
}

static void ReportThroughput(const char* name, std::uint64_t items, double seconds)
{
    std::printf("  %-58s %8.1f M/s %8.2f ns each\n", name, items / seconds / 1e6, seconds * 1e9 / items);
    std::fflush(stdout);
}

static void Verify(bool ok, const char* what)
{
    if (!ok)
    {
        std::printf("  !! wrong result in %s\n", what);
        std::exit(1);
    }
}

// ---------------------------------------------------------------------------- 1. Raw throughput

enum class Release { Eager, Lazy };

struct IntStream
{
    cb::SpscRingBuffer* ring;
    std::uint64_t count;
    std::uint64_t batch;
    Release release;
    std::uint64_t sum;
};

// Consumer thread of RunInts().
static void ConsumeInts(IntStream* stream)
{
    cb::SpscRingBuffer& ring = *stream->ring;
    std::uint64_t sum = 0;
    if (stream->release == Release::Lazy)
    {
        for (std::uint64_t i = 0; i < stream->count; ++i)
        {
            sum += ring.Read<std::uint32_t>();
            ring.FinishReadLazy();
        }
    }
    else
    {
        for (std::uint64_t i = 0; i < stream->count; i += stream->batch)
        {
            std::uint64_t n = std::min(stream->batch, stream->count - i);
            for (std::uint64_t j = 0; j < n; ++j)
                sum += ring.Read<std::uint32_t>();
            ring.FinishRead();
        }
    }
    stream->sum = sum;
}

static double RunInts(std::uint64_t count, std::uint64_t batch, Release release, std::uint32_t spinCount)
{
    cb::SpscRingBuffer ring(1 << 20, spinCount);
    IntStream stream{&ring, count, batch, release, 0};
    Clock::time_point start = Clock::now();
    std::thread consumer(ConsumeInts, &stream);
    for (std::uint64_t i = 0; i < count; i += batch)
    {
        std::uint64_t n = std::min(batch, count - i);
        for (std::uint64_t j = 0; j < n; ++j)
            ring.Write<std::uint32_t>(static_cast<std::uint32_t>(i + j));
        ring.FinishWrite();
    }
    consumer.join();
    double seconds = SecondsSince(start);
    Verify(stream.sum == count * (count - 1) / 2, "RunInts");
    return seconds;
}

static void BenchInts(const char* name, std::uint64_t batch, Release release = Release::Eager,
                      std::uint32_t spinCount = cb::SpscRingBuffer::kDefaultSpinCount)
{
    constexpr std::uint64_t kCount = 100'000'000;
    std::vector<double> seconds;
    for (int run = 0; run < kRuns; ++run)
        seconds.push_back(RunInts(kCount, batch, release, spinCount));
    ReportThroughput(name, kCount, Median(seconds));
}

// ---------------------------------------------------------------------------- 2. Command throughput

constexpr std::uint64_t kCommands = 20'000'000;

// The benchmark's own command type, used by both queues below. The stop command is built
// into the library: cb::kCmd_Stop, from CommandBuffer.h.
enum BenchCommand : cb::CommandType
{
    kCmd_Add = cb::kCmd_FirstUser, // payload: std::uint64_t
};

class SumCommandBuffer : public cb::CommandBuffer
{
public:
    using CommandBuffer::CommandBuffer;

    void Add(std::uint64_t value) { WriteCommand(kCmd_Add, value); }

    void ExecuteUntilStopped(std::uint64_t& sum)
    {
        cb::CommandType type;
        while (ReadCommand(type))
        {
            switch (type)
            {
                case kCmd_Add:
                {
                    sum += ReadPayload<std::uint64_t>();
                    break;
                }
            }
            FinishCommand();
        }
    }
};

static double RunCommandBuffer(std::uint64_t submitEvery)
{
    SumCommandBuffer commands(1 << 20);
    std::uint64_t sum = 0; // consumer only until join
    Clock::time_point start = Clock::now();
    std::thread consumer(&SumCommandBuffer::ExecuteUntilStopped, &commands, std::ref(sum));
    for (std::uint64_t i = 0; i < kCommands; ++i)
    {
        commands.Add(i);
        if ((i + 1) % submitEvery == 0)
            commands.Submit();
    }
    commands.SubmitStop();
    consumer.join();
    double seconds = SecondsSince(start);
    Verify(sum == kCommands * (kCommands - 1) / 2, "RunCommandBuffer");
    return seconds;
}

static void BenchCommandBuffer(const char* name, std::uint64_t submitEvery)
{
    std::vector<double> seconds;
    for (int run = 0; run < kRuns; ++run)
        seconds.push_back(RunCommandBuffer(submitEvery));
    ReportThroughput(name, kCommands, Median(seconds));
}

// The textbook queue: every command is a fixed-size struct, pushed and popped under a mutex.
struct QueuedCommand
{
    cb::CommandType type; // kCmd_Add, or the built-in cb::kCmd_Stop
    std::uint64_t value;
};

struct MutexQueue
{
    std::mutex mutex;
    std::condition_variable notEmpty;
    std::deque<QueuedCommand> queue;
    std::uint64_t sum = 0;

    void Push(const QueuedCommand& command)
    {
        {
            std::lock_guard lock(mutex);
            queue.push_back(command);
        }
        notEmpty.notify_one();
    }

    QueuedCommand Pop()
    {
        std::unique_lock lock(mutex);
        while (queue.empty())
            notEmpty.wait(lock);
        QueuedCommand command = queue.front();
        queue.pop_front();
        return command;
    }

    void ExecuteUntilStopped()
    {
        for (;;)
        {
            QueuedCommand command = Pop();
            switch (command.type)
            {
                case kCmd_Add:
                {
                    sum += command.value;
                    break;
                }
                case cb::kCmd_Stop:
                {
                    return;
                }
            }
        }
    }
};

static double RunMutexQueue()
{
    MutexQueue queue;
    Clock::time_point start = Clock::now();
    std::thread consumer(&MutexQueue::ExecuteUntilStopped, &queue);
    for (std::uint64_t i = 0; i < kCommands; ++i)
        queue.Push(QueuedCommand{kCmd_Add, i});
    queue.Push(QueuedCommand{cb::kCmd_Stop, 0});
    consumer.join();
    double seconds = SecondsSince(start);
    Verify(queue.sum == kCommands * (kCommands - 1) / 2, "RunMutexQueue");
    return seconds;
}

static void BenchMutexQueue(const char* name)
{
    std::vector<double> seconds;
    for (int run = 0; run < kRuns; ++run)
        seconds.push_back(RunMutexQueue());
    ReportThroughput(name, kCommands, Median(seconds));
}

// ---------------------------------------------------------------------------- 3. Round-trip latency

static double RunRoundTrip(int iterations, std::uint32_t spinCount)
{
    SumCommandBuffer commands(4096, spinCount);
    std::uint64_t sum = 0;
    std::thread consumer(&SumCommandBuffer::ExecuteUntilStopped, &commands, std::ref(sum));
    Clock::time_point start = Clock::now();
    for (int i = 0; i < iterations; ++i)
    {
        commands.Add(1);
        commands.SubmitAndWait();
    }
    double seconds = SecondsSince(start);
    commands.SubmitStop();
    consumer.join();
    Verify(sum == static_cast<std::uint64_t>(iterations), "RunRoundTrip");
    return seconds;
}

static void BenchRoundTrip(const char* name, int iterations, std::uint32_t spinCount)
{
    std::vector<double> seconds;
    for (int run = 0; run < kRuns; ++run)
        seconds.push_back(RunRoundTrip(iterations, spinCount));
    std::printf("  %-58s %8.2f us per round trip\n", name, Median(seconds) * 1e6 / iterations);
    std::fflush(stdout);
}

// ---------------------------------------------------------------------------- 4. False sharing

constexpr std::uint64_t kIncrements = 100'000'000;

template <std::size_t kSeparation>
struct alignas(kSeparation) Counter
{
    std::atomic<std::uint64_t> value{0};
};

template <std::size_t kSeparation>
static void IncrementCounter(Counter<kSeparation>* counter)
{
    for (std::uint64_t i = 0; i < kIncrements; ++i)
        counter->value.store(counter->value.load(std::memory_order_relaxed) + 1, std::memory_order_release);
}

template <std::size_t kSeparation>
static double RunFalseSharing()
{
    alignas(64) Counter<kSeparation> counters[2]; // starts on a cache line, so "same line" really is one line
    Clock::time_point start = Clock::now();
    std::thread a(IncrementCounter<kSeparation>, &counters[0]);
    std::thread b(IncrementCounter<kSeparation>, &counters[1]);
    a.join();
    b.join();
    double seconds = SecondsSince(start);
    Verify(counters[0].value == kIncrements && counters[1].value == kIncrements, "RunFalseSharing");
    return seconds;
}

template <std::size_t kSeparation>
static void BenchFalseSharing(const char* name)
{
    std::vector<double> seconds;
    for (int run = 0; run < kRuns; ++run)
        seconds.push_back(RunFalseSharing<kSeparation>());
    ReportThroughput(name, kIncrements, Median(seconds));
}

int main()
{
    std::printf("Raw throughput: 100M x 4-byte ints, 1 MB ring buffer (median of %d runs)\n", kRuns);
    BenchInts("publish + release every item", 1);
    BenchInts("publish + release every 16 items", 16);
    BenchInts("publish + release every 256 items", 256);
    BenchInts("publish every 16, FinishReadLazy() every item", 16, Release::Lazy);
    BenchInts("publish + release every 16, never spin (sleep at once)", 16, Release::Eager, 0);

    std::printf("\nCommand throughput: 20M commands, a type and an 8-byte payload each\n");
    BenchCommandBuffer("CommandBuffer, Submit() after every command", 1);
    BenchCommandBuffer("CommandBuffer, Submit() every 64 commands", 64);
    BenchCommandBuffer("CommandBuffer, Submit() every 1024 commands", 1024);
    BenchMutexQueue("mutex + condition_variable + deque<QueuedCommand>");

    std::printf("\nRound trip: record one command, then SubmitAndWait()\n");
    BenchRoundTrip("spin, then sleep (default)", 200'000, cb::SpscRingBuffer::kDefaultSpinCount);
    BenchRoundTrip("sleep at once (spinCount = 0)", 50'000, 0);

    std::printf("\nFalse sharing: two threads, each incrementing its own counter\n");
    BenchFalseSharing<8>("counters in the same cache line");
    BenchFalseSharing<64>("counters 64 bytes apart");
    return 0;
}
