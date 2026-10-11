// SpscRingBuffer.h
//
// A single-producer / single-consumer (SPSC) ring buffer for variable-sized data.
//
// The design follows Kaspar Daugaard's articles:
//   * "Writing a Fast and Versatile SPSC Ring Buffer" (2018)
//   * "How To Make Your SPSC Ring Buffer Do Nothing, More Efficiently" (2018)
// docs/index.html explains every piece step by step.
//
// Contract:
//   * Exactly one thread (the producer) calls the "Producer" functions and exactly one
//     thread (the consumer) calls the "Consumer" functions.
//   * The consumer reads with exactly the same sizes and alignments, in the same order,
//     as the producer wrote them. Both sides decide on their own where to wrap around,
//     from those sizes, so the decisions only agree if the sizes agree.
//   * Written data is invisible to the consumer until the producer calls FinishWrite().
//     (The producer also publishes automatically before it blocks.)
//   * Data the consumer has read stays valid until it calls FinishRead() (or until a
//     FinishReadLazy() release is published). The producer never overwrites bytes that
//     have not been released.
//   * The consumer must not wait for new data while it holds more than about half the
//     capacity that it has read but not finished, or both sides can end up waiting on each
//     other. See WriteSlowPath().
#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <semaphore>
#include <type_traits>
#include <utility>

#if defined(_MSC_VER)
#   define CB_FORCE_INLINE __forceinline
#   define CB_NOINLINE __declspec(noinline)
#else
#   define CB_FORCE_INLINE inline __attribute__((always_inline))
#   define CB_NOINLINE __attribute__((noinline))
#endif

// Tell the core we are busy-waiting: lets a hyper-thread sibling run and saves power.
#if defined(_M_X64) || defined(_M_IX86) || defined(__x86_64__) || defined(__i386__)
#   include <immintrin.h>
#   define CB_CPU_RELAX() _mm_pause()
#elif defined(_M_ARM64) || defined(_M_ARM)
#   include <intrin.h>
#   define CB_CPU_RELAX() __yield()
#elif defined(__aarch64__) || defined(__arm__)
#   define CB_CPU_RELAX() __asm__ __volatile__("yield")
#else
#   include <thread>
#   define CB_CPU_RELAX() std::this_thread::yield()
#endif

#if defined(_MSC_VER)
#   pragma warning(push)
#   pragma warning(disable : 4324) // "structure was padded due to alignment specifier": that is the point
#endif

namespace cb {

// 64 bytes on current x86-64 and most ARM cores (Apple M-series: 128).
inline constexpr std::size_t kCacheLineSize = 64;

class SpscRingBuffer
{
public:
    // How many times a blocked thread polls the other side before it sleeps on a semaphore.
    static constexpr std::uint32_t kDefaultSpinCount = 256;

    // `capacity` is in bytes. It does not have to be a power of two: positions are never
    // masked, see LocalState::base. Storage is aligned to kCacheLineSize, which is also
    // the largest alignment an allocation may ask for.
    explicit SpscRingBuffer(std::size_t capacity, std::uint32_t spinCount = kDefaultSpinCount)
        : m_Storage(static_cast<char*>(::operator new(capacity, std::align_val_t{kCacheLineSize})))
        , m_SpinCount(spinCount)
    {
        assert(capacity > 0);
        m_Writer.buffer = m_Reader.buffer = m_Storage;
        m_Writer.size = m_Reader.size = capacity;
        m_Writer.end = capacity; // empty buffer: the producer may fill all of it...
        m_Reader.end = 0;        // ...and the consumer has nothing to read yet
        m_Reader.lazyLimit = std::max<std::size_t>(capacity / 16, 1);
    }

    ~SpscRingBuffer()
    {
        ::operator delete(m_Storage, std::align_val_t{kCacheLineSize});
    }

    SpscRingBuffer(const SpscRingBuffer&) = delete;
    SpscRingBuffer& operator=(const SpscRingBuffer&) = delete;

    std::size_t Capacity() const { return m_Writer.size; }

    // ------------------------------------------------------------------ Producer

    // Reserve `size` bytes aligned to `alignment` (a power of two, at most kCacheLineSize).
    // Blocks while the consumer has not released enough space.
    CB_FORCE_INLINE void* PrepareWrite(std::size_t size, std::size_t alignment)
    {
        std::size_t pos = AlignUp(m_Writer.pos, alignment);
        std::size_t end = pos + size;
        if (end > m_Writer.end) [[unlikely]]
            WriteSlowPath(pos, end);
        m_Writer.pos = end;
        return m_Writer.buffer + pos;
    }

    // Make everything written so far visible to the consumer.
    CB_FORCE_INLINE void FinishWrite()
    {
        Publish(m_WriterShared, m_Writer.base + m_Writer.pos, m_ReaderWakeup);
    }

    // Copy a trivially copyable value into the buffer.
    template <class T>
    CB_FORCE_INLINE void Write(const T& value)
    {
        static_assert(std::is_trivially_copyable_v<T>, "Write() copies bytes; use Emplace() for other types");
        std::memcpy(PrepareWrite(sizeof(T), alignof(T)), &value, sizeof(T));
    }

    // Copy an array of trivially copyable values into the buffer.
    template <class T>
    CB_FORCE_INLINE void WriteArray(const T* values, std::size_t count)
    {
        static_assert(std::is_trivially_copyable_v<T>, "WriteArray() copies bytes");
        void* dest = PrepareWrite(sizeof(T) * count, alignof(T)); // aligns even when count == 0, like ReadArray()
        if (count != 0)
            std::memcpy(dest, values, sizeof(T) * count);
    }

    // Construct an object in place. The consumer gets it back with Read<T>() and must
    // destroy it (std::destroy_at) unless T is trivially destructible.
    template <class T, class... Args>
    CB_FORCE_INLINE T& Emplace(Args&&... args)
    {
        return *::new (PrepareWrite(sizeof(T), alignof(T))) T(std::forward<Args>(args)...);
    }

    // Stream position right after the last byte written (published or not).
    std::size_t WritePosition() const { return m_Writer.base + m_Writer.pos; }

    // Publish, then block until the consumer has released everything before `position`.
    // Everything the consumer did before that FinishRead() is visible to the producer afterwards.
    CB_NOINLINE void WaitUntilConsumed(std::size_t position)
    {
        FinishWrite();
        if (Distance(position, m_ReaderShared.pos.load(std::memory_order_acquire)) < 0)
            WaitForPeer(m_ReaderShared, m_WriterWakeup, position);
    }

    // ------------------------------------------------------------------ Consumer

    // Get the next `size` bytes, with the same size and alignment the producer used.
    // Blocks until the producer has published them.
    CB_FORCE_INLINE void* PrepareRead(std::size_t size, std::size_t alignment)
    {
        std::size_t pos = AlignUp(m_Reader.pos, alignment);
        std::size_t end = pos + size;
        if (end > m_Reader.end) [[unlikely]]
            ReadSlowPath(pos, end);
        m_Reader.pos = end;
        return m_Reader.buffer + pos;
    }

    // Give everything read so far back to the producer. References returned by
    // Read() / ReadArray() / PrepareRead() must not be used after this.
    CB_FORCE_INLINE void FinishRead()
    {
        m_Reader.finished = m_Reader.published = m_Reader.base + m_Reader.pos;
        Publish(m_ReaderShared, m_Reader.published, m_WriterWakeup);
    }

    // Like FinishRead(), but the release may be postponed: it is published once 1/16 of the
    // capacity is pending, when the producer is asleep waiting for the consumer, and at the
    // latest before the consumer waits for data. For a consumer finishing many small reads
    // this avoids most of the cache-line traffic of calling FinishRead() each time.
    CB_FORCE_INLINE void FinishReadLazy()
    {
        m_Reader.finished = m_Reader.base + m_Reader.pos;
        // The flag is only a hint, so relaxed is enough: if it is stale, the release still
        // happens at a later call or before the consumer waits.
        if (Distance(m_Reader.published, m_Reader.finished) >= static_cast<std::ptrdiff_t>(m_Reader.lazyLimit) ||
            m_ReaderShared.peerWaiting.load(std::memory_order_relaxed)) [[unlikely]]
            FinishRead();
    }

    // Access the next object in place, without copying it out of the buffer.
    template <class T>
    CB_FORCE_INLINE T& Read()
    {
        return *std::launder(static_cast<T*>(PrepareRead(sizeof(T), alignof(T))));
    }

    template <class T>
    CB_FORCE_INLINE const T* ReadArray(std::size_t count)
    {
        static_assert(std::is_trivially_copyable_v<T>, "ReadArray() matches WriteArray()");
        return static_cast<const T*>(PrepareRead(sizeof(T) * count, alignof(T)));
    }

#ifdef CB_RING_BUFFER_TESTING
    // Start the stream at an arbitrary position, e.g. just below SIZE_MAX to check that
    // overflowing positions are harmless. Only valid before any data has been written.
    void TestOnlySetStreamPosition(std::size_t position)
    {
        m_Writer.base = m_Reader.base = m_Reader.finished = m_Reader.published = position;
        m_WriterShared.pos = m_ReaderShared.pos = position;
    }
#endif

private:
    // State only one thread ever touches. It stays in that core's cache.
    struct alignas(kCacheLineSize) LocalState
    {
        char*       buffer = nullptr;
        std::size_t pos  = 0; // offset of the next byte to write or read, in [0, size]
        std::size_t end  = 0; // [pos, end) is known to be usable without looking at the other thread
        std::size_t base = 0; // stream position of buffer[0] in the current lap (grows by `size` per lap)
        std::size_t size = 0; // capacity; duplicated in both states so neither side reads the other's line
        // Consumer only, for FinishReadLazy():
        std::size_t finished  = 0; // stream position the consumer is done with
        std::size_t published = 0; // stream position last released to the producer
        std::size_t lazyLimit = 0; // pending bytes that make FinishReadLazy() release
    };

    // State one thread writes and the other reads: one cache line per writer.
    struct alignas(kCacheLineSize) SharedState
    {
        std::atomic<std::size_t> pos{0};      // stream position published by the owning thread
        std::atomic<bool> peerWaiting{false}; // the other thread sleeps until `pos` changes
    };

    static CB_FORCE_INLINE std::size_t AlignUp(std::size_t pos, std::size_t alignment)
    {
        assert(alignment != 0 && (alignment & (alignment - 1)) == 0 && alignment <= kCacheLineSize);
        return (pos + alignment - 1) & ~(alignment - 1);
    }

    // `to - from` as a signed number. Stream positions only move forward and the two sides
    // stay within two laps of each other, so this is right even after size_t wraps to 0.
    static CB_FORCE_INLINE std::ptrdiff_t Distance(std::size_t from, std::size_t to)
    {
        return static_cast<std::ptrdiff_t>(to - from);
    }

    CB_NOINLINE void WriteSlowPath(std::size_t& pos, std::size_t& end)
    {
        assert(end - pos <= m_Writer.size && "a single allocation must fit in the ring buffer");
        std::size_t base = m_Writer.base;
        if (end > m_Writer.size)
        {
            // The block does not fit before the physical end of the buffer. Leave the rest of
            // this lap as a gap and start the block at offset 0 of the next lap. The consumer
            // makes the same decision on its own, because it reads the same sizes.
            end -= pos;
            pos = 0;
            base += m_Writer.size;
        }

        // The producer may run at most one full buffer ahead of what the consumer released,
        // so the consumer must have released everything up to `required`.
        std::size_t required = base + end - m_Writer.size;
        std::size_t readerPos = m_ReaderShared.pos.load(std::memory_order_acquire);
        if (Distance(required, readerPos) < 0)
        {
            // Never wait while holding unpublished data: the consumer may need exactly that
            // data before it can release the space we are waiting for.
            FinishWrite();
            readerPos = WaitForPeer(m_ReaderShared, m_WriterWakeup, required);
        }
        m_Writer.base = base;
        m_Writer.end = std::min(readerPos + m_Writer.size - base, m_Writer.size);
    }

    CB_NOINLINE void ReadSlowPath(std::size_t& pos, std::size_t& end)
    {
        assert(end - pos <= m_Reader.size && "a single allocation must fit in the ring buffer");
        std::size_t base = m_Reader.base;
        if (end > m_Reader.size)
        {
            // Same rule as WriteSlowPath(): the producer skipped the rest of this lap.
            end -= pos;
            pos = 0;
            base += m_Reader.size;
        }

        // The producer must have published everything up to `required`.
        std::size_t required = base + end;
        std::size_t writerPos = m_WriterShared.pos.load(std::memory_order_acquire);
        if (Distance(required, writerPos) < 0)
        {
            // Never wait while holding bytes we are done with (FinishReadLazy()): the producer
            // may be waiting for exactly that space.
            if (m_Reader.published != m_Reader.finished)
            {
                m_Reader.published = m_Reader.finished;
                Publish(m_ReaderShared, m_Reader.published, m_WriterWakeup);
            }
            writerPos = WaitForPeer(m_WriterShared, m_ReaderWakeup, required);
        }
        m_Reader.base = base;
        m_Reader.end = std::min(writerPos - base, m_Reader.size);
    }

    // Store our new position, then wake the other thread if it asked to be woken.
    static CB_FORCE_INLINE void Publish(SharedState& mine, std::size_t pos, std::binary_semaphore& peerWakeup)
    {
        // seq_cst rather than release: with the seq_cst operations in WaitForPeer() this
        // guarantees that a peer going to sleep either sees this position or that we see
        // its peerWaiting flag. The plain load keeps the common case (nobody asleep) cheap.
        mine.pos.store(pos, std::memory_order_seq_cst);
        if (mine.peerWaiting.load(std::memory_order_seq_cst)) [[unlikely]]
            WakePeer(mine, peerWakeup);
    }

    static CB_NOINLINE void WakePeer(SharedState& mine, std::binary_semaphore& peerWakeup)
    {
        // Exactly one of us and the peer gets to reset the flag. If the peer reset it first,
        // it withdrew its request and is not going to sleep.
        if (mine.peerWaiting.exchange(false, std::memory_order_seq_cst))
            peerWakeup.release();
    }

    // Block until the peer's published position reaches `required` and return that position.
    // Spin first (cheap if the peer is running on another core), then sleep on `wakeup`,
    // which the peer's Publish() releases.
    CB_NOINLINE std::size_t WaitForPeer(SharedState& peer, std::binary_semaphore& wakeup, std::size_t required)
    {
        for (std::uint32_t i = 0; i < m_SpinCount; ++i)
        {
            CB_CPU_RELAX();
            std::size_t pos = peer.pos.load(std::memory_order_acquire);
            if (Distance(required, pos) >= 0)
                return pos;
        }
        for (;;)
        {
            std::size_t pos = peer.pos.load(std::memory_order_acquire);
            if (Distance(required, pos) >= 0)
                return pos;

            // Ask to be woken, then look again: the peer may have published after our load
            // but before it could see the request.
            peer.peerWaiting.store(true, std::memory_order_seq_cst);
            if (peer.pos.load(std::memory_order_seq_cst) != pos)
            {
                // It did publish. Withdraw the request and re-check, unless the peer already
                // took the request, in which case its release() is on the way.
                if (peer.peerWaiting.exchange(false, std::memory_order_seq_cst))
                    continue;
            }
            wakeup.acquire();
        }
    }

    // Read-only after construction.
    char*               m_Storage;
    const std::uint32_t m_SpinCount;

    LocalState  m_Writer;       // producer only
    LocalState  m_Reader;       // consumer only
    SharedState m_WriterShared; // written by the producer, read by the consumer
    SharedState m_ReaderShared; // written by the consumer, read by the producer

    // Only touched when a thread goes to sleep or wakes the other one up.
    alignas(kCacheLineSize) std::binary_semaphore m_ReaderWakeup{0}; // the consumer sleeps on it
    alignas(kCacheLineSize) std::binary_semaphore m_WriterWakeup{0}; // the producer sleeps on it
};

} // namespace cb

#if defined(_MSC_VER)
#   pragma warning(pop)
#endif
