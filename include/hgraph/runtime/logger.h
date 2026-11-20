#ifndef HGRAPH_RUNTIME_LOGGER_H
#define HGRAPH_RUNTIME_LOGGER_H

#include <hgraph/runtime/logger_view.h>
#include <hgraph/types/static_node.h>  // selectors exposed by the original node-authoring header
#include <spdlog/spdlog.h>

namespace hgraph
{
    namespace log
    {
        /** The process hgraph logger (created on demand: a colour stdout logger named "hgraph"). */
        [[nodiscard]] HGRAPH_EXPORT spdlog::logger &logger();

        /** Shared ownership of the process logger for seeding a run logger. */
        [[nodiscard]] HGRAPH_EXPORT std::shared_ptr<spdlog::logger> shared_logger();

        /**
         * Replace the process default used by executors whose builder does
         * not provide a logger. Passing ``nullptr`` restores the default.
         */
        HGRAPH_EXPORT void set_logger(std::shared_ptr<spdlog::logger> logger);

        /**
         * Drop the process logger so the next ``logger()`` call rebuilds it
         * (including its sinks). Needed wherever the log destination's file
         * descriptor is redirected after the logger was first created:
         * spdlog's Windows stdout sinks cache the raw OS handle at
         * construction, so a redirect (e.g. pytest's per-test fd capture)
         * is only honoured by a freshly built sink.
         */
        HGRAPH_EXPORT void reset_logger();
    }  // namespace log

}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_LOGGER_H
