#ifndef HGRAPH_TYPES_TIME_SERIES_TS_OUTPUT_FORWARDING_H
#define HGRAPH_TYPES_TIME_SERIES_TS_OUTPUT_FORWARDING_H

#include <hgraph/types/time_series/ts_output/base_view.h>

#include <stdexcept>

namespace hgraph
{
    /**
     * Resolve an output through every currently bound forwarding endpoint.
     *
     * An unbound forwarding endpoint is returned as the terminal result so
     * callers can distinguish "no source yet" from an ordinary invalid output.
     * Keeping this walk here gives nested-graph wiring, reference adaptation,
     * and operators one cycle-safe definition of forwarding resolution.
     */
    [[nodiscard]] inline TSOutputView resolve_forwarding_source(TSOutputView source)
    {
        const DateTime evaluation_time = source.evaluation_time();
        const auto next_forwarding_target = [evaluation_time](const TSOutputHandle &handle) {
            if (!handle.bound()) { return TSOutputHandle{}; }
            const TSOutputView view = handle.view(evaluation_time);
            if (!view.forwarding()) { return TSOutputHandle{}; }
            const TSOutputHandle target = view.forwarding_target();
            return target.bound() ? target : TSOutputHandle{};
        };

        TSOutputHandle tortoise = source.handle();
        TSOutputHandle hare = source.handle();
        while (source.bound() && source.forwarding())
        {
            TSOutputHandle target = source.forwarding_target();
            if (!target.bound()) { break; }
            source = target.view(evaluation_time);

            tortoise = next_forwarding_target(tortoise);
            hare = next_forwarding_target(hare);
            if (hare.bound()) { hare = next_forwarding_target(hare); }
            if (tortoise.bound() && hare.bound() && tortoise.same_as(hare))
            {
                throw std::logic_error("Time-series forwarding output cycle");
            }
        }
        return source;
    }
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TIME_SERIES_TS_OUTPUT_FORWARDING_H
