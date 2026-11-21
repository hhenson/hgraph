#ifndef HGRAPH_LIB_TESTING_RECORD_REPLAY_BUFFER_H
#define HGRAPH_LIB_TESTING_RECORD_REPLAY_BUFFER_H

#include <hgraph/runtime/global_state.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_record_registry.h>
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

    /** Structural outputs can publish without a child or membership change. */
    [[nodiscard]] inline bool preserves_empty_collection_ticks(const TSValueTypeMetaData &schema) noexcept {
        return schema.kind == TSTypeKind::TSS || schema.kind == TSTypeKind::TSD ||
               schema.kind == TSTypeKind::TSL || schema.kind == TSTypeKind::TSB;
    }

    enum class DenseBufferLayout { Seeded, Typed, ObservedSeed };

    // Private representation identity preserves the format through owning
    // Value and GlobalState copies without changing the canonical List schema.
    [[nodiscard]] inline ValueTypeRef dense_buffer_binding(ValueTypeRef binding, DenseBufferLayout layout) {
        const char *label = "testing.dense.seeded";
        if (layout == DenseBufferLayout::Typed) { label = "testing.dense.typed"; }
        else if (layout == DenseBufferLayout::ObservedSeed) { label = "testing.dense.observed_seed"; }
        const auto &record = TypeRecordRegistry::instance().intern(TypeRecordDefinition{
            .key = TypeRecordKey{.schema = &binding.schema()->header,
                                 .role   = TypeRole::Instance,
                                 .plan   = binding.plan(),
                                 .ops    = binding.ops(),
                                 .debug  = binding.record()->debug,
                                 .implementation_label = label},
            .ops_abi_version = binding.record()->ops_abi_version,
            .capabilities    = binding.capabilities(),
        });
        return ValueTypeRef::checked(AnyPtr::typed_null(record));
    }
    [[nodiscard]] inline DenseBufferLayout dense_buffer_layout(ValueTypeRef binding) noexcept {
        if (binding) {
            const auto label = binding.record()->implementation_name();
            if (label == "testing.dense.typed") { return DenseBufferLayout::Typed; }
            if (label == "testing.dense.observed_seed") { return DenseBufferLayout::ObservedSeed; }
        }
        return DenseBufferLayout::Seeded;
    }
    [[nodiscard]] inline ValueTypeRef dense_recording_binding(ValueTypeRef delta_binding) {
        return dense_buffer_binding(mutable_list_type(delta_binding), DenseBufferLayout::Typed);
    }

    /** Seeded edits and captured events both use a boxed list so every
        entry retains its concrete binding. Only captured events carry tick provenance. */
    [[nodiscard]] inline Value make_buffer(bool observed = false)
    {
        auto       &registry = TypeRegistry::instance();
        const auto *schema   = registry.mutable_list(registry.any());
        const auto binding  = ValuePlanFactory::instance().type_for(schema);
        return Value{dense_buffer_binding(binding, observed ? DenseBufferLayout::ObservedSeed : DenseBufferLayout::Seeded)};
    }

    /** A fresh, empty TYPED dense recording buffer: ``List<delta_schema>``
        with holes as UNSET elements (element validity). */
    [[nodiscard]] inline Value make_dense_buffer(ValueTypeRef delta_binding)
    {
        return Value{dense_recording_binding(delta_binding)};
    }

    [[nodiscard]] inline Value make_dense_buffer(const ValueTypeMetaData *delta_schema)
    {
        return make_dense_buffer(recording_binding_for(delta_schema));
    }

    /** Cold convenience read at ``index`` of a dense buffer, either layout.
        Native operators cache ``dense_entry_reader`` during preparation.
        The seeded
        ``List<Any>`` (empty box = no tick) or the typed recorded list
        (UNSET element = no tick). nullopt = no tick. */
    [[nodiscard]] inline std::optional<Value> dense_entry_delta(const ListView &list, std::size_t index,
                                                                DenseBufferLayout layout = DenseBufferLayout::Seeded) {
        const auto element = list.at(index);
        if (!element.has_value()) { return std::nullopt; }   // typed hole
        if (layout == DenseBufferLayout::Typed) { return Value{element}; }
        // Only the schema-free seeded layout uses List<Any> as an envelope.
        // A typed recording may itself have an Any-kind delta schema (JSON is
        // the important nominal example); unboxing that would erase its type.
        if (list.element_schema() == TypeRegistry::instance().any())
        {
            const auto boxed = element.as_any();
            if (!boxed.has_value()) { return std::nullopt; }   // legacy empty box
            return Value{boxed.get()};
        }
        return Value{element};
    }

    using DenseEntryReader = std::optional<Value> (*)(const ListView &, std::size_t);
    [[nodiscard]] inline std::optional<Value> typed_dense_entry(const ListView &list, std::size_t index) {
        const auto element = list.at(index);
        return element.has_value() ? std::optional<Value>{Value{element}} : std::nullopt;
    }
    [[nodiscard]] inline std::optional<Value> seeded_dense_entry(const ListView &list, std::size_t index) {
        const auto element = list.at(index);
        if (!element.has_value()) { return std::nullopt; }
        const auto boxed = element.as_any();
        return boxed.has_value() ? std::optional<Value>{Value{boxed.get()}} : std::nullopt;
    }
    // Resolve the legacy canonical-Any envelope distinction once, at the
    // preparation/read boundary. The selected callbacks perform no lookup.
    [[nodiscard]] inline DenseEntryReader dense_entry_reader(ValueTypeRef binding, DenseBufferLayout layout) {
        if (layout == DenseBufferLayout::Typed) { return &typed_dense_entry; }
        if (!binding || binding.schema()->element_type == TypeRegistry::instance().any()) { return &seeded_dense_entry; }
        return &typed_dense_entry;  // ordinary legacy typed lists, including JSON
    }
    [[nodiscard]] inline DenseEntryReader dense_entry_reader(ValueTypeRef binding) {
        return dense_entry_reader(binding, dense_buffer_layout(binding));
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

    /** Timed dictionary rows may additionally identify live keys with invalid scalar children.
        Their ordinary delta retains the standard value/removal schema. */
    inline Value sparse_dictionary_entry(ValueTypeRef type, const ValueView &when, const ValueView &delta,
                                         const Value &invalid_children = {}) {
        BundleBuilder entry{type};
        entry.set(0, when);
        entry.set(1, delta);
        if (invalid_children.has_value()) { entry.set(2, invalid_children.view()); }
        return entry.build();
    }

    inline void write_sparse_entry(GlobalStateView state, const std::string &key, DateTime when,
                                   Value delta, const Value &invalid_children = {},
                                   std::optional<std::size_t> index = {}) {
        const auto buffer = state.get(key);
        const auto rows = buffer.as_list();
        const auto write = [&](const Value &entry) {
            auto mutation = rows.begin_mutation();
            if (index) { mutation.set(*index, entry.view()); }
            else { mutation.push_back(entry.view()); }
        };
        if (rows.element_schema()->field_count == 3) {
            const auto *ops = checked_value_ops<IndexedValueOps>(buffer.binding(), "sparse recording");
            const auto type = ops->element_binding(ops->context, buffer.data(), 0);
            write(sparse_dictionary_entry(type, Value{when}.view(), delta.view(), invalid_children));
            return;
        }
        if (!invalid_children.has_value()) {
            const auto type = delta.binding();
            write(make_sparse_entry(type, when, std::move(delta)));
            return;
        }
        // Only recordings with a retained invalid child need the additional key set.
        // Convert any historical rows before publishing the replacement in GlobalState.
        const Value time{when};
        const auto *schema = TypeRegistry::instance().tuple({time.schema(), delta.schema(), invalid_children.schema()});
        const auto type = ValuePlanFactory::instance().realized_composite_type_for(
            schema, std::array{time.binding(), delta.binding(), invalid_children.binding()});
        const auto entry = sparse_dictionary_entry(type, time.view(), delta.view(), invalid_children);
        Value promoted{mutable_list_type(type)};
        auto append = promoted.as_list().begin_mutation();
        for (std::size_t i = 0; i < rows.size(); ++i) {
            const auto old = rows.at(i).as_indexed_view();
            if (index == i) { append.push_back(entry.view()); }
            else { append.push_back(sparse_dictionary_entry(type, old.at(0), old.at(1)).view()); }
        }
        if (!index) { append.push_back(entry.view()); }
        state.set(key, std::move(promoted));
    }

}  // namespace hgraph::testing

#endif  // HGRAPH_LIB_TESTING_RECORD_REPLAY_BUFFER_H
