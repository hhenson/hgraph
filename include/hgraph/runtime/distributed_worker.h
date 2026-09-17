#ifndef HGRAPH_RUNTIME_DISTRIBUTED_WORKER_H
#define HGRAPH_RUNTIME_DISTRIBUTED_WORKER_H

// The worker side of a distributed run (RFC 0037).
//
// A worker cannot be SENT its graph. RFC 0022 is explicit that a manifest does
// not serialize executable code, and a child graph is code: nodes are function
// pointers into this image. So the model RFC 0037 settles on is "the worker
// runs the same program": it links the same translation units, wires the same
// child through the same code path, and what crosses the process boundary is
// only a NAME saying which child to build.
//
// That name is what a ``WorkerRecipe`` is registered under. Registration has to
// happen in a translation unit linked into BOTH programs -- in the usual case
// one program, launched twice -- which is exactly what makes the two sides
// agree by construction rather than by protocol.
//
// Nothing here knows how a process is started; ``distributed_process.h`` does
// that. Splitting them keeps the serving loop testable over any connected
// pair, including one whose far end is a thread.

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/distributed_protocol.h>
#include <hgraph/runtime/distributed_transport.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/util/date_time.h>

#include <string>
#include <string_view>
#include <vector>

namespace hgraph::distributed
{
    /**
     * How a worker process rebuilds the child its caller wired.
     *
     * Two function pointers rather than a captured closure: a recipe is
     * program-lifetime state reached from a fresh process, where nothing the
     * caller captured exists.
     */
    struct HGRAPH_CLASS_EXPORT WorkerRecipe
    {
        /** Builds the child graph -- the same one the caller validated. */
        GraphBuilder (*build)(){nullptr};
        /** The boundary, in the order both sides index it by. */
        BoundarySlots (*boundary)(){nullptr};

        [[nodiscard]] bool valid() const noexcept
        {
            return build != nullptr && boundary != nullptr;
        }
    };

    /**
     * Register one recipe under ``key``.
     *
     * Registering the same key twice with the same recipe is accepted (a
     * header-driven registration may run from more than one translation unit);
     * registering a DIFFERENT recipe under a live key is refused, because the
     * two sides would then disagree about what the name means.
     */
    HGRAPH_EXPORT void register_worker_recipe(std::string key, WorkerRecipe recipe);

    /** The recipe registered under ``key``, or ``nullptr``. */
    [[nodiscard]] HGRAPH_EXPORT const WorkerRecipe *worker_recipe(std::string_view key);

    /** Every registered key, sorted -- for a diagnostic that can say what IS available. */
    [[nodiscard]] HGRAPH_EXPORT std::vector<std::string> registered_worker_recipes();

    /**
     * Serve cycles from ``channel`` until the caller closes it.
     *
     * One request in, one reply out, with the child driven by the request's
     * evaluation time. A clean close between messages is how a run ends, so it
     * returns rather than throws; anything else is a truncated cycle and
     * propagates.
     */
    HGRAPH_EXPORT void serve_worker(PipeEndpoint &channel, const WorkerRecipe &recipe,
                                    DateTime start_time, DateTime end_time);

    /** Serve an embedding frontend's already wired native child. */
    HGRAPH_EXPORT void serve_worker(PipeEndpoint &channel, GraphBuilder child, const BoundarySlots &slots,
                                    DateTime start_time, DateTime end_time,
                                    GraphExecutorPhaseRunner phase_runner = {});

    /** The flags a spawned worker is launched with (also its argv contract). */
    inline constexpr std::string_view worker_recipe_flag{"--hgraph-worker-recipe="};
    inline constexpr std::string_view worker_read_flag{"--hgraph-worker-read="};
    inline constexpr std::string_view worker_write_flag{"--hgraph-worker-write="};
    inline constexpr std::string_view worker_start_flag{"--hgraph-worker-start="};
    inline constexpr std::string_view worker_end_flag{"--hgraph-worker-end="};

    /**
     * Serve, if this process was launched as a worker.
     *
     * The entry point a host program calls FIRST in ``main``, returning true
     * when it has served a run and the program should exit. A program that
     * never spawns workers sees false and carries on, which is why this is
     * cheap enough to call unconditionally.
     *
     * Recipes must already be registered when this is called: a worker that
     * cannot find its recipe fails loudly rather than serving an empty graph.
     */
    [[nodiscard]] HGRAPH_EXPORT bool run_worker_if_requested(int argc, char **argv);
}  // namespace hgraph::distributed

#endif  // HGRAPH_RUNTIME_DISTRIBUTED_WORKER_H
