#ifndef HGRAPH_LIB_TESTING_RECORD_REPLAY_BUFFER_H
#define HGRAPH_LIB_TESTING_RECORD_REPLAY_BUFFER_H

#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/mutable_container_ops.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/date_time.h>

#include <array>
#include <cstddef>
#include <optional>
#include <string_view>
#include <utility>

namespace hgraph::testing
{
    /**
     * The cycle-aligned record/replay BUFFER FORMAT layer: the value-layer helpers
     * that build and read the buffers both the in-memory record/replay backends
     * (``lib/std/operators/impl/record_replay_memory_impl.h``) and the harness
     * seed/read API (``record_replay.h``) share. Split out so those two — an
     * operator impl and the testing harness — depend on a common base rather than
     * on each other.
     *
     * The buffer is a value-layer ``List`` stored in ``GlobalState`` under a string
     * key and **cycle-aligned**: index ``i`` is evaluation time
     * ``MIN_ST + i*MIN_TD``; a hole means "no tick that cycle". See
     * ``docs/source/user_guide/testing_graphs_cpp.rst``.
     */

    [[nodiscard]] inline ValueTypeRef recording_binding_for(const ValueTypeMetaData *schema)
    {
        if (const auto *snapshot = active_type_realization(); snapshot != nullptr)
        {
            return value_type_for_active_realization(schema);
        }
        return ValuePlanFactory::instance().type_for(schema);
    }

    // -----------------------------------------------------------------
    // Buffer helpers (value-layer <-> ordinary C++ vectors)
    // -----------------------------------------------------------------

    /** An empty ``Any`` value (the "no tick this cycle" marker). */
    [[nodiscard]] inline Value empty_any()
    {
        const auto binding = ValuePlanFactory::instance().type_for(TypeRegistry::instance().any());
        return Value{binding};  // unset == empty == None
    }

    /** An ``Any`` boxing a copy of the value behind ``inner`` (type-erased). */
    [[nodiscard]] inline Value make_any(const ValueView &inner)
    {
        const auto binding = ValuePlanFactory::instance().type_for(TypeRegistry::instance().any());
        Value       any{binding};
        any.as_any().begin_mutation().set(inner);
        return any;
    }

    /** An ``Any`` boxing a copy of ``inner``. */
    [[nodiscard]] inline Value make_any(const Value &inner) { return make_any(inner.view()); }

    /** A fresh, empty cycle-aligned buffer (a mutable ``List<Any>``) — the
        SEEDED replay layout (set_replay does not know one element schema). */
    [[nodiscard]] inline Value make_buffer()
    {
        auto       &registry = TypeRegistry::instance();
        const auto *schema   = registry.mutable_list(registry.any());
        const auto binding  = ValuePlanFactory::instance().type_for(schema);
        return Value{binding};
    }

    /** A fresh, empty TYPED dense recording buffer: ``List<delta_schema>``
        with holes as UNSET elements (element validity). */
    [[nodiscard]] inline Value make_dense_buffer(ValueTypeRef delta_binding)
    {
        return Value{mutable_list_type(delta_binding)};
    }

    [[nodiscard]] inline Value make_dense_buffer(const ValueTypeMetaData *delta_schema)
    {
        return make_dense_buffer(recording_binding_for(delta_schema));
    }

    /** The delta at ``index`` of a dense buffer, either layout: the seeded
        ``List<Any>`` (empty box = no tick) or the typed recorded list
        (UNSET element = no tick). nullopt = no tick. */
    [[nodiscard]] inline std::optional<Value> dense_entry_delta(const ListView &list, std::size_t index)
    {
        const auto element = list.at(index);
        if (!element.has_value()) { return std::nullopt; }   // typed hole
        if (element.schema()->value_kind() == ValueTypeKind::Any)
        {
            const auto boxed = element.as_any();
            if (!boxed.has_value()) { return std::nullopt; }   // legacy empty box
            return Value{boxed.get()};
        }
        return Value{element};
    }

    /** The cycle index for ``now`` (offset from ``MIN_ST`` in ``MIN_TD`` steps). */
    [[nodiscard]] inline std::size_t cycle_offset(DateTime now) noexcept
    {
        return static_cast<std::size_t>((now - MIN_ST) / MIN_TD);
    }

    /** Densification guard: a recording (or read-back) spanning more cycles
        than this must go through the sparse path. */
    inline constexpr std::size_t max_dense_cycles = 1'000'000;

    /** SPARSE recordings are TYPED: the recorded schema is known when the
        node runs, so the buffer is ``List<Tuple<datetime, delta_schema>>``
        - no per-entry ``Any`` boxing. */
    [[nodiscard]] inline const ValueTypeMetaData *sparse_entry_meta(const ValueTypeMetaData *delta_schema)
    {
        auto &registry = TypeRegistry::instance();
        return registry.tuple({registry.value_type("datetime"), delta_schema});
    }

    [[nodiscard]] inline ValueTypeRef sparse_entry_binding(ValueTypeRef delta_binding)
    {
        auto       &factory = ValuePlanFactory::instance();
        const auto *schema  = sparse_entry_meta(delta_binding.schema());
        const auto canonical_delta = factory.type_for(delta_binding.schema());
        if (delta_binding == canonical_delta) { return recording_binding_for(schema); }
        const std::array fields{
            factory.type_for(TypeRegistry::instance().value_type("datetime")),
            delta_binding,
        };
        return factory.realized_composite_type_for(schema, fields);
    }

    /** A fresh, empty sparse buffer for the given delta schema. */
    [[nodiscard]] inline Value make_sparse_buffer(ValueTypeRef delta_binding)
    {
        return Value{mutable_list_type(sparse_entry_binding(delta_binding))};
    }

    [[nodiscard]] inline Value make_sparse_buffer(const ValueTypeMetaData *delta_schema)
    {
        return make_sparse_buffer(recording_binding_for(delta_schema));
    }

    /** Build a (time, delta) sparse-buffer entry. */
    [[nodiscard]] inline Value make_sparse_entry(ValueTypeRef delta_binding, DateTime when, Value delta)
    {
        BundleBuilder entry{sparse_entry_binding(delta_binding)};
        entry.set(0, Value{when});
        entry.set(1, std::move(delta));
        return entry.build();
    }

    [[nodiscard]] inline Value make_sparse_entry(const ValueTypeMetaData *delta_schema, DateTime when, Value delta)
    {
        (void)delta_schema;
        const auto binding = delta.binding();
        return make_sparse_entry(binding, when, std::move(delta));
    }
}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_RECORD_REPLAY_BUFFER_H
