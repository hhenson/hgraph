#ifndef HGRAPH_RUNTIME_DISTRIBUTED_TRANSPORT_H
#define HGRAPH_RUNTIME_DISTRIBUTED_TRANSPORT_H

// A connected pair of endpoints carrying framed messages (RFC 0037).
//
// The v1 barrier is synchronous -- the caller dispatches, then waits -- so
// this is blocking read/write over a pre-connected pair and nothing more. No
// event loop, no thread, no dependency: the whole transport is "move these
// bytes", because the framing and the messages already exist above it.
//
// Both platforms are a pair of one-way byte streams. POSIX gets them from
// ``socketpair``, Windows from two anonymous pipes; the only difference is how
// the pair is created, since blocking reads and writes are uniform afterwards.
// A future network transport is a different implementation of the same two
// calls, not a change to anything above them.

#include <hgraph/hgraph_export.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace hgraph::distributed
{
    /**
     * One end of a connected pair.
     *
     * The handles are owned: closed exactly once, on destruction or move-from.
     * A half-closed endpoint is a normal outcome rather than an error -- the
     * far side exiting is how a run ends -- so ``receive`` reports it by
     * returning false instead of throwing.
     */
    class HGRAPH_CLASS_EXPORT PipeEndpoint
    {
      public:
        PipeEndpoint() noexcept = default;
        PipeEndpoint(const PipeEndpoint &)            = delete;
        PipeEndpoint &operator=(const PipeEndpoint &) = delete;
        PipeEndpoint(PipeEndpoint &&other) noexcept;
        PipeEndpoint &operator=(PipeEndpoint &&other) noexcept;
        ~PipeEndpoint();

        [[nodiscard]] bool open() const noexcept;

        /**
         * Take ownership of handles this process already has.
         *
         * How a worker process picks up the end its parent left it: the
         * numeric handle survives ``exec``/``CreateProcess`` unchanged, so the
         * parent passes it in ``argv`` and the child adopts it here. On POSIX
         * the pair is one bidirectional descriptor and both arguments are the
         * same number.
         */
        static PipeEndpoint adopt(std::int64_t read_handle, std::int64_t write_handle) noexcept;

        [[nodiscard]] std::int64_t native_read_handle() const noexcept;
        [[nodiscard]] std::int64_t native_write_handle() const noexcept;

        /**
         * Allow (or refuse) this end to pass to a child process.
         *
         * The spawner marks the CHILD's end and leaves its own unmarked, so
         * that only the intended end crosses. Windows needs this because an
         * anonymous pipe handle passes ``CreateProcess`` only when it is
         * marked inheritable and the call asks for inheritance; POSIX needs
         * the mirror image, clearing ``FD_CLOEXEC`` on the descriptor that is
         * meant to survive ``exec``.
         */
        void set_inheritable(bool inheritable) const;

        /** Send one message, length-prefixed. Throws if the write fails. */
        void send(std::string_view payload);

        /**
         * Receive one whole message.
         *
         * Blocks until a complete frame has arrived, which a single read will
         * not generally deliver. Returns false when the far end closed --
         * cleanly, with nothing partial buffered -- and throws when it closed
         * mid-message, because that is a truncated cycle rather than an end.
         */
        [[nodiscard]] bool receive(std::string &payload);

        void close() noexcept;

      private:
        friend HGRAPH_EXPORT void connected_pipe_pair(PipeEndpoint &first, PipeEndpoint &second);

#ifdef _WIN32
        void *read_handle_{nullptr};
        void *write_handle_{nullptr};
#else
        int read_fd_{-1};
        int write_fd_{-1};
#endif
        /** Bytes read but not yet consumed: a read may straddle two frames. */
        std::string buffer_{};
    };

    /**
     * Create a connected pair.
     *
     * Both endpoints are open on return; handing one to a child process and
     * closing the local copy of it is the caller's job, and is what makes the
     * far side's exit observable as a clean close.
     */
    HGRAPH_EXPORT void connected_pipe_pair(PipeEndpoint &first, PipeEndpoint &second);
}  // namespace hgraph::distributed

#endif  // HGRAPH_RUNTIME_DISTRIBUTED_TRANSPORT_H
