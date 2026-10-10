#ifndef HGRAPH_TYPES_TIME_SERIES_TS_INPUT_RESOLVED_OBSERVATION_H
#define HGRAPH_TYPES_TIME_SERIES_TS_INPUT_RESOLVED_OBSERVATION_H

namespace hgraph
{
    struct TSDataTracking;
    struct ValueOps;

    namespace detail
    {
        /**
         * Resolved facts of an active-trie node's ``observed`` handle (RFC
         * 0008 stage 6), recomputed at every write of the handle and cleared
         * with it; see ``TSInputTargetActiveNode::resolved``. ``tracking``
         * points at the observed output's tracking record; ``native_value``
         * and ``value_ops`` describe its value memory when the output is a
         * direct native atomic (otherwise null); ``walk_free`` says no
         * target-link hop and no dynamic container lies between the observed
         * data and its root, so the per-read liveness walk has a known
         * answer.
         */
        struct ResolvedObservation
        {
            const void           *native_value{nullptr};
            const TSDataTracking *tracking{nullptr};
            const ValueOps       *value_ops{nullptr};
            bool                  walk_free{false};

            [[nodiscard]] bool native() const noexcept { return native_value != nullptr && walk_free; }
        };
    }  // namespace detail
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TIME_SERIES_TS_INPUT_RESOLVED_OBSERVATION_H
