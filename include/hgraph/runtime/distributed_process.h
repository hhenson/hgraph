#ifndef HGRAPH_RUNTIME_DISTRIBUTED_PROCESS_H
#define HGRAPH_RUNTIME_DISTRIBUTED_PROCESS_H

// Starting a worker process and holding its channel (RFC 0037).
//
// The one piece of RFC 0037 that is genuinely about operating systems. It is
// deliberately the LAST layer: everything below it -- the codec, the messages,
// the framing, the child host, the partition -- was built and tested with no
// process anywhere, so a fault here cannot be mistaken for a modelling fault,
// and a different transport (a socket, RFC 0034's NATS) replaces this file
// alone.
//
// The worker program is, by default, THIS program. That is not a convenience:
// RFC 0037's bootstrap rule is that a worker cannot be sent its graph and must
// instead link the same code, so re-launching the same executable is the
// shortest path to that guarantee. A separate worker binary is supported for
// deployments that want a smaller image, and carries the same obligation.

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/distributed_transport.h>
#include <hgraph/util/date_time.h>

#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace hgraph::distributed
{
    /** The absolute path of the running executable. */
    [[nodiscard]] HGRAPH_EXPORT std::string current_executable_path();

    /**
     * A running worker, and the channel to it.
     *
     * Owns both. Destruction closes the channel -- which is how a worker is
     * asked to finish, since its serve loop ends on a clean close -- and then
     * waits for the process, so a pool going out of scope leaves no orphans.
     */
    class HGRAPH_CLASS_EXPORT WorkerProcess
    {
      public:
        WorkerProcess() noexcept = default;
        WorkerProcess(const WorkerProcess &)            = delete;
        WorkerProcess &operator=(const WorkerProcess &) = delete;
        WorkerProcess(WorkerProcess &&other) noexcept;
        WorkerProcess &operator=(WorkerProcess &&other) noexcept;
        ~WorkerProcess();

        [[nodiscard]] bool running() const noexcept { return pid_ != 0; }

        /** The connected end this process keeps. */
        [[nodiscard]] PipeEndpoint &channel() noexcept { return channel_; }

        /** The worker's process id, for a diagnostic that has to name it. */
        [[nodiscard]] long long pid() const noexcept { return pid_; }

        /**
         * Close the channel and wait for the worker, returning its exit code.
         *
         * Idempotent, and the destructor's whole body. A worker that does not
         * end on the close is killed rather than waited for indefinitely: a
         * hung worker must not hang the caller (RFC 0037, failure handling).
         */
        int wait_for_exit();
        /** Nonblocking exit check. Returns nullopt while running; otherwise
         * closes the channel, reaps the process and returns its exit code.
         * Returns zero when no process is owned. Use from the process owner
         * thread only, just like wait_for_exit and terminate.
         */
        [[nodiscard]] std::optional<int> try_wait_for_exit();
        /** Close and terminate a failed or timed-out worker, then reap it. */
        void terminate() noexcept;

      private:
        friend HGRAPH_EXPORT WorkerProcess spawn_worker(std::string_view program,
                                                        std::string_view recipe_key,
                                                        DateTime start_time, DateTime end_time,
                                                        std::span<const std::string> arguments);

        PipeEndpoint channel_{};
        long long    pid_{0};
#ifdef _WIN32
        void *process_handle_{nullptr};
#endif
    };

    /**
     * Launch one worker and connect to it.
     *
     * ``program`` empty means this executable. ``recipe_key`` names the child
     * the worker builds, and must be registered in the worker program --
     * which, when it is this program, means registered here too.
     */
    [[nodiscard]] HGRAPH_EXPORT WorkerProcess spawn_worker(std::string_view program,
                                                           std::string_view recipe_key,
                                                           DateTime start_time, DateTime end_time,
                                                           std::span<const std::string> arguments = {});
}  // namespace hgraph::distributed

#endif  // HGRAPH_RUNTIME_DISTRIBUTED_PROCESS_H
