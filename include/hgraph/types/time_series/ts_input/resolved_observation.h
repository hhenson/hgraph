#ifndef HGRAPH_TYPES_TIME_SERIES_TS_INPUT_RESOLVED_OBSERVATION_H
#define HGRAPH_TYPES_TIME_SERIES_TS_INPUT_RESOLVED_OBSERVATION_H

#include <cstdint>

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

            [[nodiscard]] const ValueOps *value_ops() const noexcept
            {
                return reinterpret_cast<const ValueOps *>(ops_word_ & ~walk_free_bit);
            }
            [[nodiscard]] bool walk_free() const noexcept { return (ops_word_ & walk_free_bit) != 0; }
            void set_value_ops(const ValueOps *ops) noexcept
            {
                ops_word_ = reinterpret_cast<std::uintptr_t>(ops) | (ops_word_ & walk_free_bit);
            }
            void set_walk_free(bool value) noexcept
            {
                ops_word_ = value ? (ops_word_ | walk_free_bit) : (ops_word_ & ~walk_free_bit);
            }
            [[nodiscard]] bool native() const noexcept { return native_value != nullptr && walk_free(); }

          private:
            static constexpr std::uintptr_t walk_free_bit = 1;
            // ``value_ops`` with ``walk_free`` in its low bit: ValueOps is
            // pointer-aligned (asserted where it is complete), so the bit is
            // free, and an active-trie node carries 24 bytes here, not 32.
            std::uintptr_t ops_word_{0};
        };
    }  // namespace detail
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TIME_SERIES_TS_INPUT_RESOLVED_OBSERVATION_H
