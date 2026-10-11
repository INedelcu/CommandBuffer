// CommandBuffer.h
//
// Commands recorded on one thread (the producer) and executed, in order, on another
// thread (the consumer). Typical use: the game thread records rendering commands and a
// render thread executes them.
//
// A command is a command type followed by a plain-data payload, stored in an SpscRingBuffer:
//
//     [ type | payload ]                  WriteCommand(type, payload)
//     [ type | payload | T[count] ]       WriteCommand(type, payload), then WriteArray(values, count)
//     [ type ]                            WriteCommand(type), Submit() (end of batch), SubmitStop()
//
// CommandBuffer itself only knows the two built-in types. An application derives a class
// that defines its own command types, one producer function per command, and an
// ExecuteUntilStopped() that reads each type and switches on it:
//
//     enum RenderCommand : cb::CommandType { kCmd_SetClearColor = cb::kCmd_FirstUser, ... };
//
//     void SetClearColor(const Color& color) { WriteCommand(kCmd_SetClearColor, color); }
//
//     void ExecuteUntilStopped(Renderer& renderer)
//     {
//         cb::CommandType type;
//         while (ReadCommand(type))
//         {
//             switch (type)
//             {
//                 case kCmd_SetClearColor:
//                 {
//                     renderer.SetClearColor(ReadPayload<Color>());
//                     break;
//                 }
//                 ...
//             }
//             FinishCommand();
//         }
//     }
//
// The consumer must read exactly what the producer wrote for each command type: the same
// payload type, then the same arrays, in the same order. Giving each command one payload
// struct, written and read as a whole, makes that hard to get wrong.
//
// The consumer gives buffer space back lazily after each command (FinishReadLazy()) and
// eagerly at the end of each submitted batch, so WaitUntilExecuted() returns as soon as
// the batch has run, while long batches cost little synchronization.
#pragma once

#include "SpscRingBuffer.h"

namespace cb {

using CommandType = std::uint32_t;

// Command types every CommandBuffer understands. Applications number theirs from kCmd_FirstUser.
enum : CommandType
{
    kCmd_EndOfBatch = 0, // written by Submit()
    kCmd_Stop       = 1, // written by SubmitStop()
    kCmd_FirstUser  = 2,
};

class CommandBuffer
{
public:
    // A stream position. Every command recorded before the Submit() that returned the
    // ticket has finished executing once WaitUntilExecuted(ticket) returns.
    using Ticket = std::size_t;

    explicit CommandBuffer(std::size_t capacity, std::uint32_t spinCount = SpscRingBuffer::kDefaultSpinCount)
        : m_Ring(capacity, spinCount)
    {
    }

    // ------------------------------------------------------------- Producer thread

    // End the current batch and make everything recorded so far visible to the consumer.
    // Submit once per batch (a frame, say), not after every command: each Submit()
    // synchronizes both threads.
    Ticket Submit() noexcept
    {
        m_Ring.Write<CommandType>(kCmd_EndOfBatch);
        m_Ring.FinishWrite();
        return m_Ring.WritePosition();
    }

    // Block until every command submitted before `ticket` has run. Side effects of those
    // commands are visible to the producer afterwards.
    void WaitUntilExecuted(Ticket ticket) { m_Ring.WaitUntilConsumed(ticket); }

    // Submit, then wait until the consumer has executed everything.
    void SubmitAndWait() { WaitUntilExecuted(Submit()); }

    // Record the stop command and submit it: the consumer's ReadCommand() returns false
    // when it gets there.
    Ticket SubmitStop() noexcept
    {
        m_Ring.Write<CommandType>(kCmd_Stop);
        m_Ring.FinishWrite();
        return m_Ring.WritePosition();
    }

protected:
    // ------------------------------------------------------------- Producer thread
    // Used by the derived class's command functions. A command is not visible to the
    // consumer until the next Submit(), or earlier if the producer has to wait for space.

    // Write a command that has no payload.
    void WriteCommand(CommandType type) noexcept
    {
        assert(type >= kCmd_FirstUser && "kCmd_EndOfBatch and kCmd_Stop are written by Submit() and SubmitStop()");
        m_CommandBytes = 0;
        CheckCommandSize(sizeof(CommandType));
        m_Ring.Write<CommandType>(type);
    }

    // Write a command and its payload. The payload is copied as bytes, so it must be plain
    // data: pass handles, indices or pointers for anything that owns memory.
    template <class Payload>
    void WriteCommand(CommandType type, const Payload& payload) noexcept
    {
        static_assert(std::is_trivially_copyable_v<Payload>, "a payload is copied as bytes");
        WriteCommand(type);
        CheckCommandSize(sizeof(Payload) + alignof(Payload));
        m_Ring.Write<Payload>(payload);
    }

    // Append an array to the command written last, e.g. vertex data. The consumer reads
    // it with ReadArray() after the payload; the count usually travels in the payload.
    template <class T>
    void WriteArray(const T* values, std::size_t count) noexcept
    {
        CheckCommandSize(sizeof(T) * count + alignof(T));
        m_Ring.WriteArray<T>(values, count);
    }

    // ------------------------------------------------------------- Consumer thread
    // Used by the derived class's ExecuteUntilStopped().

    // Wait until a command is ready and return its type. Returns false when the producer
    // has called SubmitStop(). The end-of-batch markers written by Submit() are handled here.
    bool ReadCommand(CommandType& type) noexcept
    {
        for (;;)
        {
            type = m_Ring.Read<CommandType>();
            if (type == kCmd_EndOfBatch)
            {
                // The end of a submitted batch: release right away, the producer may be
                // waiting for it in WaitUntilExecuted().
                m_Ring.FinishRead();
                continue;
            }
            if (type == kCmd_Stop)
            {
                m_Ring.FinishRead();
                return false;
            }
            return true;
        }
    }

    // The payload of the command just read, in place in the buffer. Valid until FinishCommand().
    template <class Payload>
    const Payload& ReadPayload() noexcept
    {
        static_assert(std::is_trivially_copyable_v<Payload>, "a payload is copied as bytes");
        return m_Ring.Read<Payload>();
    }

    // An array written with WriteArray(), in place in the buffer. Valid until FinishCommand().
    template <class T>
    const T* ReadArray(std::size_t count) noexcept
    {
        return m_Ring.ReadArray<T>(count);
    }

    // Done with the current command. Releasing lazily keeps the consumer from writing to a
    // cache line the producer reads after every single command; the ring buffer still
    // releases before the consumer waits, so nothing can deadlock.
    void FinishCommand() noexcept { m_Ring.FinishReadLazy(); }

private:
    void CheckCommandSize([[maybe_unused]] std::size_t bytes) noexcept
    {
        // While the consumer decodes a command it holds the command's first parts and may wait
        // for the rest. A command, plus a wrap-around gap that is smaller than the command,
        // must therefore fit in the buffer, or both threads would wait on each other forever.
        m_CommandBytes += bytes;
        assert(m_CommandBytes <= m_Ring.Capacity() / 2 && "command too large for this CommandBuffer");
    }

    SpscRingBuffer m_Ring;
    std::size_t m_CommandBytes = 0; // producer only: bytes of the command being written
};

} // namespace cb
