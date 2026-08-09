#ifndef HGRAPH_LIB_TESTING_RECORD_REPLAY_H
#define HGRAPH_LIB_TESTING_RECORD_REPLAY_H

#include <hgraph/lib/std/operators/impl/record_replay_memory_impl.h>  // in-memory record/replay backends
#include <hgraph/lib/testing/record_replay_buffer.h>                  // cycle-aligned buffer-format helpers
#include <hgraph/runtime/global_state.h>
#include <hgraph/types/value/value.h>

#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::testing
{
    /**
     * The in-memory graph testing toolkit's seed/read API over the cycle-aligned
     * buffer (``record_replay_buffer.h``): seed a replay source's buffer and read
     * a record sink's back. This is the harness umbrella: it also pulls in the
     * record/replay OPERATOR backends (``stdlib::dense_record_impl`` /
     * ``stdlib::replay_impl``, ``record_replay_memory_impl.h``) that tests wire.
     *
     * The buffer is a value-layer **mutable** ``List`` stored in ``GlobalState``
     * under a string key and **cycle-aligned**: index ``i`` is evaluation time
     * ``MIN_ST + i*MIN_TD``; a hole means "no tick that cycle". See
     * ``docs/source/user_guide/testing_graphs_cpp.rst``.
     */

    // -----------------------------------------------------------------
    // Delta buffer seed/read API (canonical delta Values <-> cycle-aligned buffer)
    //
    // A cycle's entry is the canonical per-tick delta ``Value`` (``TS<T>`` -> the
    // scalar; ``TSS``/``TSL`` -> the bundle/map; see ``static_node.h``), or empty for
    // "no tick". These are schema-agnostic — they move ``Value``s, not typed wrappers.
    // -----------------------------------------------------------------

    /** Seed a replay buffer from a sequence of canonical delta ``Value``s (nullopt = no tick). */
    inline void set_replay_deltas(const GlobalStateView &gs, std::string_view key,
                                  const std::vector<std::optional<Value>> &deltas)
    {
        Value buffer   = make_buffer();
        auto  mutation = buffer.as_list().begin_mutation();
        for (const auto &delta : deltas)
        {
            if (delta.has_value()) { mutation.push_back(make_any(delta->view()).view()); }
            else { mutation.push_back(empty_any().view()); }
        }
        gs.set(key, buffer);
    }

    /** Read a SPARSE recording back as (cycle, delta) pairs. The list is
        written in evaluation order, so it is naturally time-sorted. */
    [[nodiscard]] inline std::vector<std::pair<std::size_t, Value>> get_recorded_sparse(const GlobalStateView &gs,
                                                                                        std::string_view key)
    {
        std::vector<std::pair<std::size_t, Value>> result;
        const ValueView                            buffer = gs.get(key);
        if (!buffer.valid()) { return result; }
        const auto list = buffer.as_list();
        result.reserve(list.size());
        for (std::size_t i = 0; i < list.size(); ++i)
        {
            const auto entry = list.at(i).as_indexed_view();
            result.emplace_back(cycle_offset(entry.at(0).checked_as<DateTime>()), Value{entry.at(1)});
        }
        return result;
    }

    /** Read a DENSE recording back as a sequence of canonical delta
        ``Value``s (owning copies; nullopt = no tick). */
    [[nodiscard]] inline std::vector<std::optional<Value>> get_recorded_deltas(const GlobalStateView &gs,
                                                                               std::string_view key)
    {
        std::vector<std::optional<Value>> result;
        const ValueView                   buffer = gs.get(key);
        if (!buffer.valid()) { return result; }
        const auto list = buffer.as_list();
        if (list.size() > max_dense_cycles)
        {
            throw std::logic_error(
                "get_recorded_deltas: the recording spans too many cycles to read densely - "
                "read it with get_recorded_sparse");
        }
        result.reserve(list.size());
        for (std::size_t i = 0; i < list.size(); ++i)
        {
            result.emplace_back(dense_entry_delta(list, i));
        }
        return result;
    }

    /** Scalar convenience: seed a ``TS<T>`` buffer from plain C++ values (nullopt = no tick). */
    template <typename T>
    void set_replay_values(const GlobalStateView &gs, std::string_view key, const std::vector<std::optional<T>> &values)
    {
        std::vector<std::optional<Value>> deltas;
        deltas.reserve(values.size());
        for (const auto &value : values)
        {
            if (value.has_value()) { deltas.emplace_back(Value{*value}); }
            else { deltas.emplace_back(std::nullopt); }
        }
        set_replay_deltas(gs, key, deltas);
    }

    /** Scalar convenience: read a recorded ``TS<T>`` buffer back as plain C++ values. */
    template <typename T>
    [[nodiscard]] std::vector<std::optional<T>> get_recorded_values(const GlobalStateView &gs, std::string_view key)
    {
        std::vector<std::optional<T>> out;
        for (const auto &delta : get_recorded_deltas(gs, key))
        {
            if (delta.has_value()) { out.push_back(delta->view().template checked_as<T>()); }
            else { out.push_back(std::nullopt); }
        }
        return out;
    }
}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_RECORD_REPLAY_H
