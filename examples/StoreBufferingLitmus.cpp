// Store buffering litmus test.
//
// Two threads each store 1 to their own variable, then load the other thread's variable:
//
//     thread A:  x = 1;  r1 = y;          thread B:  y = 1;  r2 = x;
//
// If every store became visible to the other core before the next load ran, at least one
// of r1 and r2 would be 1. The outcome r1 == 0 && r2 == 0 means that both loads ran ahead
// of the stores: each store was still waiting in its core's store buffer. x86 and ARM both
// allow that for relaxed, release and acquire operations; memory_order_seq_cst forbids it.
//
// This is the pattern behind the sleep handshake in SpscRingBuffer (step 9 of the guide):
// "publish my position, then check whether you are asleep" against "say I am asleep, then
// check your position". If both checks read the old value, both threads sleep forever.
//
// The program runs the test with three choices of memory order and counts the trials that
// ended with r1 == 0 && r2 == 0. It exits with 1 if seq_cst ever shows that outcome.

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <thread>

#if defined(_M_X64) || defined(_M_IX86) || defined(__x86_64__) || defined(__i386__)
#   include <immintrin.h>
#   define LITMUS_PAUSE() _mm_pause()
#else
#   define LITMUS_PAUSE() std::this_thread::yield()
#endif

#if defined(_MSC_VER)
#   pragma warning(disable : 4324) // padding due to alignas is intended
#endif

constexpr int kTrials = 1'000'000;

template <std::memory_order kStoreOrder, std::memory_order kLoadOrder>
class StoreBufferingTest
{
public:
    // Runs kTrials trials and returns how many ended with r1 == 0 && r2 == 0.
    int Run()
    {
        std::thread a(&StoreBufferingTest::RunThreadA, this);
        std::thread b(&StoreBufferingTest::RunThreadB, this);
        int bothZero = 0;
        for (int trial = 1; trial <= kTrials; ++trial)
        {
            m_X.store(0, std::memory_order_relaxed);
            m_Y.store(0, std::memory_order_relaxed);
            m_Finished.store(0, std::memory_order_relaxed);
            m_Trial.store(trial, std::memory_order_release); // go
            while (m_Finished.load(std::memory_order_acquire) != 2)
                LITMUS_PAUSE();
            bothZero += m_R1 == 0 && m_R2 == 0;
        }
        a.join();
        b.join();
        return bothZero;
    }

private:
    void RunThreadA() { RunThread(m_X, m_Y, m_R1, 12345); }
    void RunThreadB() { RunThread(m_Y, m_X, m_R2, 67890); }

    void RunThread(std::atomic<int>& mine, std::atomic<int>& other, int& result, std::uint32_t seed)
    {
        std::uint32_t random = seed;
        for (int trial = 1; trial <= kTrials; ++trial)
        {
            while (m_Trial.load(std::memory_order_acquire) != trial)
                LITMUS_PAUSE();
            // A short random delay, so the two threads' stores and loads overlap in many
            // different ways from trial to trial.
            random = random * 1664525u + 1013904223u;
            for (std::uint32_t i = random >> 29; i != 0; --i)
                LITMUS_PAUSE();

            mine.store(1, kStoreOrder);
            result = other.load(kLoadOrder);

            m_Finished.fetch_add(1, std::memory_order_release);
        }
    }

    // Each variable on its own cache line, as in the ring buffer.
    alignas(64) std::atomic<int> m_X{0};
    alignas(64) std::atomic<int> m_Y{0};
    alignas(64) std::atomic<int> m_Trial{0};
    alignas(64) std::atomic<int> m_Finished{0};
    alignas(64) int m_R1 = 0;
    alignas(64) int m_R2 = 0;
};

static void Report(const char* orders, int bothZero)
{
    std::printf("  %-34s %7d of %d trials ended with r1 == 0 && r2 == 0\n", orders, bothZero, kTrials);
    std::fflush(stdout);
}

int main()
{
    std::printf("Store buffering: x = 1; r1 = y;  ||  y = 1; r2 = x;\n");

    StoreBufferingTest<std::memory_order_relaxed, std::memory_order_relaxed> relaxed;
    Report("relaxed store, relaxed load", relaxed.Run());

    StoreBufferingTest<std::memory_order_release, std::memory_order_acquire> releaseAcquire;
    Report("release store, acquire load", releaseAcquire.Run());

    StoreBufferingTest<std::memory_order_seq_cst, std::memory_order_seq_cst> seqCst;
    int seqCstBothZero = seqCst.Run();
    Report("seq_cst store, seq_cst load", seqCstBothZero);

    if (seqCstBothZero != 0)
    {
        std::printf("seq_cst must never produce r1 == 0 && r2 == 0\n");
        return 1;
    }
    return 0;
}
