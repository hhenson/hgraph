#ifndef HGRAPH_WEB_TESTING_H2_DIAGNOSTICS_H
#define HGRAPH_WEB_TESTING_H2_DIAGNOSTICS_H

#include <hgraph/web/export.h>

#include <cstdint>

namespace hgraph::web::testing
{
    /**
     * Server-side counters for the HTTP/2 read loop, for FAILURE DIAGNOSIS
     * only -- nothing reads them on a passing run.
     *
     * The h2 loopback flake (issue #849) has been diagnosed blind three times.
     * The client-side counters added since then established what the server
     * does NOT do: on both Linux and macOS, byte for byte, it sends its
     * SETTINGS and SETTINGS ACK (36 bytes, 2 frames) and then nothing, while
     * the client sits with an empty socket and an empty OpenSSL buffer. That
     * rules out a missed client wakeup and narrows it to the server's read
     * loop, which has exactly three ways to stop:
     *
     *   read_stalled     > 0   the backpressure path in read_next() was taken
     *   receive_rejected > 0   engine_.receive() reported a fatal protocol error
     *   reads_armed == reads_completed + 1
     *                          a read is outstanding that never completed --
     *                          the client's bytes never reached the server
     *   reads_armed == reads_completed
     *                          the loop was simply not re-armed
     *
     * Those four are mutually exclusive, so one snapshot separates them.
     *
     * The counters are process-wide and monotonic. A test that needs a clean
     * baseline calls ``h2_read_loop_reset`` first; they are plain relaxed
     * atomics, so reading them races with a live connection and the snapshot
     * is indicative rather than a consistent instant.
     */
    struct H2ReadLoopSnapshot
    {
        std::uint64_t reads_armed{};       ///< async_read_some issued
        std::uint64_t reads_completed{};   ///< its handler ran without an error
        std::uint64_t read_errors{};       ///< its handler ran WITH an error
        std::uint64_t bytes_received{};    ///< bytes handed to the h2 engine
        std::uint64_t receive_rejected{};  ///< engine_.receive() returned false
        std::uint64_t read_stalled{};      ///< backpressure path taken
        std::uint64_t writes_armed{};      ///< async_write issued
        std::uint64_t writes_completed{};  ///< its handler ran without an error
        std::uint64_t write_errors{};      ///< its handler ran WITH an error
        std::uint64_t bytes_written{};     ///< bytes handed to async_write
    };

    [[nodiscard]] HGRAPH_WEB_EXPORT H2ReadLoopSnapshot h2_read_loop_snapshot() noexcept;
    HGRAPH_WEB_EXPORT void h2_read_loop_reset() noexcept;
}  // namespace hgraph::web::testing

#endif  // HGRAPH_WEB_TESTING_H2_DIAGNOSTICS_H
