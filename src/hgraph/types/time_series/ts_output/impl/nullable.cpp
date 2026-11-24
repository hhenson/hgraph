#include "../nullable.h"

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/ts_data/base_view.h>
#include <hgraph/types/time_series/ts_data/impl/current_state_ops.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/compact_container_ops.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace hgraph::detail
{
    namespace
    {
        // Immutable representation selected at binding time. Both the TS and
        // its nullable value borrow the original TSData address. This also
        // works for a source whose value is itself an erased projection.
        struct BorrowedNullableType
        {
            TSRoleTypeRef source;
            MemoryUtils::StoragePlan plan{};
            ValueTypeRef owning_value;
            IndexedValueOps value_ops{};
            ValueTypeRef value_type;
            TSDataLayout layout{};
            TSDataOps ops{};
            TSRoleTypeRef type;

            static const BorrowedNullableType &self(const void *context)
            {
                return *static_cast<const BorrowedNullableType *>(context);
            }
            TSDataView source_view(const void *memory) const { return TSDataView{source, memory}; }
            ValueView payload(const void *memory) const { return source_view(memory).value(); }

            BorrowedNullableType(TSRoleTypeRef source_type, const TSValueTypeMetaData &requested,
                                 ValueTypeRef owning)
                : source{source_type}, owning_value{owning}
            {
                // Borrowed memory has no construction, destruction or mutation
                // hooks. Capturing it selects the ordinary owning Optional.
                plan.layout = source.checked_plan().layout;
                value_ops.kind = ValueOpsKind::Indexed;
                value_ops.context = this;
                value_ops.size = [](const void *, const void *) noexcept { return std::size_t{1}; };
                value_ops.element_at = [](const void *context, const void *memory, std::size_t index) -> const void * {
                    return index == 0 ? self(context).payload(memory).data() : nullptr;
                };
                value_ops.element_binding = [](const void *context, const void *memory, std::size_t) noexcept {
                    return self(context).payload(memory).binding();
                };
                value_ops.make_range = [](const void *context, const void *memory) {
                    return Range<ValueView>{.context = context, .memory = memory, .limit = 1,
                        .predicate = nullptr,
                        .projector = [](const void *state, const void *data, std::size_t) {
                            return self(state).payload(data);
                        }};
                };
                value_ops.hash_impl = [](const void *context, const void *memory) {
                    return container_ops_detail::combine_hash(0, self(context).payload(memory).hash());
                };
                value_ops.equals_impl = [](const void *context, const void *lhs, const void *rhs) {
                    return self(context).payload(lhs).equals(self(context).payload(rhs));
                };
                value_ops.compare_impl = [](const void *context, const void *lhs, const void *rhs) noexcept {
                    return fallback_on_exception(std::partial_ordering::unordered, [&] {
                        return self(context).payload(lhs).compare(self(context).payload(rhs));
                    });
                };
                value_ops.to_string_impl = [](const void *context, const void *memory) {
                    return self(context).payload(memory).to_string();
                };
                value_ops.to_python_impl = [](const void *context, const void *memory) {
                    const auto value = self(context).payload(memory);
                    const auto &table = value.binding().ops_ref();
                    if (table.to_python_impl == nullptr) { throw std::logic_error("nullable payload has no Python conversion"); }
                    return table.to_python_impl(table.context, value.data());
                };
                value_ops.copy_construct_view_impl = [](const void *context, const ValueTypeRef &binding,
                                                         void *dst, const void *memory) {
                    binding.default_construct_at(dst);
                    auto cleanup = make_scope_exit([&]() noexcept { binding.destroy_at(dst); });
                    binding.ops_ref().copy_assign_from(binding, dst, self(context).value_type, memory);
                    cleanup.release();
                };
                value_ops.copy_assign_view_impl = [](const void *context, const ValueTypeRef &binding,
                                                      void *dst, const void *memory) {
                    binding.ops_ref().copy_assign_from(binding, dst, self(context).value_type, memory);
                };
                value_ops.owning_type_impl = [](const void *context, ValueTypeRef) {
                    return self(context).owning_value;
                };
                value_type = intern_value_type(*requested.value_schema, plan, value_ops);
                register_value_owning_type(value_type, owning_value);

                layout.value_binding = value_type;
                layout.delta_binding = value_type;
                layout.canonical_delta_binding = owning_value;
                layout.tracking_offset = source.ops_ref().layout_impl(source.ops_ref().context)->tracking_offset;
                ops.context = this;
                ops.current_state_ops = &ts_current_state_detail::current_state_ops_for(TSTypeKind::TS);
                ops.layout_impl = [](const void *context) { return &self(context).layout; };
                ops.tracking_impl = [](const void *context, const void *memory) {
                    return &self(context).source_view(memory).tracking();
                };
                // Subscriptions share the producer's observer set. Mutation of
                // values remains forbidden by allows_mutation=false.
                ops.mutable_tracking_impl = [](const void *context, void *memory) {
                    const auto &source_ops = self(context).source.ops_ref();
                    return source_ops.mutable_tracking_impl(source_ops.context, memory);
                };
                ops.has_current_value_impl = [](const void *context, const void *memory) {
                    return self(context).source_view(memory).has_current_value();
                };
                ops.all_valid_impl = [](const void *context, const void *memory) {
                    return self(context).source_view(memory).all_valid();
                };
                ops.value_view_impl = [](const void *context, const void *memory) {
                    const auto &state = self(context);
                    return ValueView{state.value_type,
                        state.source_view(memory).has_current_value() ? memory : nullptr};
                };
                ops.delta_view_impl = [](const void *context, const void *memory, DateTime time) {
                    const auto &state = self(context);
                    const auto source_view = state.source_view(memory);
                    return ValueView{state.value_type,
                        source_view.has_current_value() && source_view.modified(time) ? memory : nullptr};
                };
                ops.capture_delta_impl = &ts_data_detail::capture_delta_ts;
                ops.empty_delta_impl = &ts_data_detail::empty_delta_atomic;
                ops.to_python_impl = [](const void *context, const void *memory) {
                    const auto &source_ops = self(context).source.ops_ref();
                    return source_ops.to_python_impl(source_ops.context, memory);
                };
                ops.delta_to_python_impl = [](const void *context, const void *memory, DateTime time) {
                    const auto &source_ops = self(context).source.ops_ref();
                    return source_ops.delta_to_python_impl(source_ops.context, memory, time);
                };
                ops.value_as_delta_to_python_impl = [](const void *context, const void *memory) {
                    const auto &source_ops = self(context).source.ops_ref();
                    return source_ops.value_as_delta_to_python_impl != nullptr
                        ? source_ops.value_as_delta_to_python_impl(source_ops.context, memory)
                        : source_ops.to_python_impl(source_ops.context, memory);
                };
                type = intern_ts_type(requested, TypeRole::Output, plan, ops, "ts.borrowed-nullable",
                                      TS_DATA_OPS_ABI_VERSION, source);
            }
        };
    }

    namespace
    {
        struct BorrowedNullableTypes
        {
            std::mutex mutex;
            std::unordered_map<const TypeRecord *,
                std::unordered_map<const TypeRecord *, std::unique_ptr<BorrowedNullableType>>> entries;
        };
        BorrowedNullableTypes &borrowed_nullable_types()
        {
            static BorrowedNullableTypes cache;
            return cache;
        }
    }

    void clear_borrowed_nullable_types() noexcept
    {
        auto &cache = borrowed_nullable_types();
        std::lock_guard lock(cache.mutex);
        cache.entries.clear();
    }

    TSRoleTypeRef borrowed_nullable_type_for(TSRoleTypeRef source, const TSValueTypeMetaData &requested)
    {
        if (!source || source.schema()->kind != TSTypeKind::TS || requested.kind != TSTypeKind::TS ||
            !requested.value_schema->is_optional() ||
            requested.value_schema->element_type != source.schema()->value_schema)
        {
            throw std::invalid_argument("borrowed nullable binding requires TS[T] -> TS[Optional[T]]");
        }
        // Keep each realization's owning capture type; reads never consult
        // the active realization or this cache.
        const auto owning = ts_data_detail::canonical_value_binding_for(requested.value_schema);
        auto &cache = borrowed_nullable_types();
        std::lock_guard lock(cache.mutex);
        auto &entry = cache.entries[source.record()][owning.record()];
        if (!entry) { entry = std::make_unique<BorrowedNullableType>(source, requested, owning); }
        return entry->type;
    }
}
