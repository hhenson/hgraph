#include <hgraph/types/time_series/ts_data/proxy.h>
#include <hgraph/types/time_series/ts_data/empty_delta_fields.h>

#include "ownership.h"

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <nanobind/nanobind.h>
#endif

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/utils/value_slot_store.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

#include <fmt/format.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] std::size_t combine_hash(std::size_t seed, std::size_t value) noexcept
        {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            return seed;
        }

        enum class TSDProxySetSurface
        {
            Live,
            Added,
            Removed,
        };

        enum class TSDProxyMapSurface
        {
            Live,
            Added,
            Removed,
            Modified,
        };

        [[nodiscard]] TSDProxy &proxy_storage(void *memory)
        {
            if (memory != nullptr) { return *static_cast<TSDProxy *>(memory); }
            throw std::logic_error("TSDProxy requires live storage");
        }

        [[nodiscard]] const TSDProxy &proxy_storage(const void *memory)
        {
            if (memory != nullptr) { return *static_cast<const TSDProxy *>(memory); }
            throw std::logic_error("TSDProxy requires live storage");
        }

        [[nodiscard]] bool pending_announced(DateTime stamp) noexcept
        {
            return stamp == MIN_DT;
        }

        [[nodiscard]] bool pending_owed(DateTime stamp) noexcept
        {
            return stamp == MAX_DT;
        }

        [[nodiscard]] bool built(DateTime stamp) noexcept
        {
            if (pending_announced(stamp) || pending_owed(stamp)) { return false; }
            return stamp >= MIN_ST && stamp <= MAX_ET;
        }

        void require_concrete_build_time(DateTime modified_time)
        {
            if (!built(modified_time))
            {
                throw std::invalid_argument("TSDProxy build time must be in [MIN_ST, MAX_ET]");
            }
        }

        struct TSDProxyContextKey
        {
            const TSValueTypeMetaData *schema{nullptr};
            TSRoleTypeRef           element_type{};
            TypeRole                   role{TypeRole::Invalid};

            [[nodiscard]] bool operator==(const TSDProxyContextKey &) const noexcept = default;
        };

        struct TSDProxyContextKeyHash
        {
            [[nodiscard]] std::size_t operator()(const TSDProxyContextKey &key) const noexcept
            {
                std::size_t seed = std::hash<const void *>{}(key.schema);
                const auto h = std::hash<const TypeRecord *>{}(key.element_type.record());
                seed ^= h + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
                return combine_hash(seed, static_cast<std::size_t>(key.role));
            }
        };

        struct TSDProxyContext
        {
            const TSValueTypeMetaData      *schema{nullptr};
            const MemoryUtils::StoragePlan *plan{nullptr};
            TSDDataLayout                   layout{};
            TSDDataOps                      dict_ops{};
            TSSDataOps                      key_set_ts_ops{};
            SetValueOps                     key_set_value_ops{};
            SetValueOps                     added_set_value_ops{};
            SetValueOps                     removed_set_value_ops{};
            MapValueOps                     value_map_ops{};
            MapValueOps                     modified_map_ops{};
            IndexedValueOps                 delta_bundle_ops{};
            TSRoleTypeRef                 element_type{};
            TSRoleTypeRef                 key_set_type{};
            TypeRole                         role{TypeRole::Invalid};
            ValueTypeRef key_set_value_binding{nullptr};
            ValueTypeRef added_set_binding{nullptr};
            ValueTypeRef removed_set_binding{nullptr};
            ValueTypeRef modified_map_binding{nullptr};
            // The projected delta's third field: user-authored strict removals
            // never originate from a live TSD, so the projection exposes the
            // interned always-empty set (NON-OWNING: cleared before the type
            // records at reset — see empty_delta_fields.h).
            const Value *empty_strict_set{nullptr};

            TSDProxyContext(const TSValueTypeMetaData &schema_, TSRoleTypeRef element_type_, TypeRole role_)
                : schema(&schema_),
                  plan(&MemoryUtils::plan_for<TSDProxy>()),
                  element_type(ts_data_plan_factory_detail::tsd_value_projection_type(element_type_, role_)),
                  role(role_)
            {
                if (schema->kind != TSTypeKind::TSD)
                {
                    throw std::logic_error("TSDProxy context requires a TSD schema");
                }
                if (schema->key_type() == nullptr || schema->element_ts() == nullptr)
                {
                    throw std::logic_error("TSDProxy schema is incomplete");
                }
                if (element_type.schema() != schema->element_ts())
                {
                    throw std::logic_error("TSDProxy element binding does not match the TSD element schema");
                }

                const auto &element_ops    = element_type.ops_ref();
                const auto *element_layout = element_ops.layout_impl(element_ops.context);
                if (element_layout == nullptr)
                {
                    throw std::logic_error("TSDProxy element layout is not resolved");
                }

                layout.key_binding           = ValuePlanFactory::instance().type_for(schema->key_type());
                layout.element_type          = element_type;
                layout.element_layout        = element_layout;
                layout.element_value_binding = element_layout->value_binding;
                layout.element_delta_binding = element_layout->delta_binding;
                layout.tracking_offset       = 0;
                if (layout.key_binding == nullptr || layout.element_value_binding == nullptr ||
                    layout.element_delta_binding == nullptr)
                {
                    throw std::logic_error("TSDProxy value bindings are not resolved");
                }

                configure_value_ops();
                configure_ts_ops();
                bind_surfaces();
            }

          private:
            void configure_value_ops()
            {
                key_set_value_ops     = set_value_ops_for<TSDProxySetSurface::Live>();
                added_set_value_ops   = set_value_ops_for<TSDProxySetSurface::Added>();
                removed_set_value_ops = set_value_ops_for<TSDProxySetSurface::Removed>();
                value_map_ops         = map_value_ops_for<TSDProxyMapSurface::Live>();
                modified_map_ops      = map_value_ops_for<TSDProxyMapSurface::Modified>();
                delta_bundle_ops      = IndexedValueOps{
                    {ValueOpsKind::Indexed, this, false, &delta_hash, &delta_equals, &delta_compare,
                     &delta_to_string},
                    &delta_size,
                    &delta_element_at,
                    &delta_element_binding,
                    &delta_range,
                    nullptr,
                };
                delta_bundle_ops.owning_type_impl      = &canonical_value_binding;
                delta_bundle_ops.copy_construct_view_impl = &delta_copy_construct_view;
                delta_bundle_ops.copy_assign_view_impl    = &delta_copy_assign_view;
            }

            void configure_ts_ops()
            {
                key_set_ts_ops = TSSDataOps{};
                TSDataOps &set_base = key_set_ts_ops;
                set_base = TSDataOps{
                    .context                   = this,
                    .kind                      = TSTypeKind::TSS,
                    .allows_mutation           = false,
                    .layout_impl               = &ts_layout,
                    .tracking_impl             = &key_set_tracking,
                    .mutable_tracking_impl     = &mutable_key_set_tracking,
                    .has_current_value_impl    = &key_set_has_current_value,
                    .all_valid_impl            = &key_set_has_current_value,
                    .value_memory_impl         = &value_memory,
                    .mutable_value_memory_impl = &mutable_value_memory,
                    .delta_memory_impl         = &delta_memory,
                    .mutable_delta_memory_impl = &mutable_delta_memory,
                    .record_child_modified_impl = &record_child_modified,
                    .empty_delta_impl          = &ts_data_detail::empty_delta_tss,
                    .capture_delta_impl        = &ts_data_detail::capture_delta_tss,
                    .delta_has_effect_impl     = &ts_data_detail::delta_has_effect_tss,
                    .apply_delta_impl          = &ts_data_detail::apply_delta_tss,
                };
                key_set_ts_ops.size_impl                      = &set_size<TSDProxySetSurface::Live>;
                key_set_ts_ops.slot_capacity_impl             = &slot_capacity;
                key_set_ts_ops.slot_occupied_impl             = &slot_occupied;
                key_set_ts_ops.slot_live_impl                 = &slot_live;
                key_set_ts_ops.slot_added_impl                = &slot_added;
                key_set_ts_ops.slot_removed_impl              = &slot_removed;
                key_set_ts_ops.next_added_slot_impl           = &next_added_slot;
                key_set_ts_ops.next_removed_slot_impl         = &next_removed_slot;
                key_set_ts_ops.key_at_slot_impl               = &key_at_slot;
                key_set_ts_ops.contains_impl                  = &set_contains;
                key_set_ts_ops.find_slot_impl                 = &find_slot;
                key_set_ts_ops.make_values_range_impl         = &set_range<TSDProxySetSurface::Live>;
                key_set_ts_ops.make_added_values_range_impl   = &set_range<TSDProxySetSurface::Added>;
                key_set_ts_ops.make_removed_values_range_impl = &set_range<TSDProxySetSurface::Removed>;
                key_set_ts_ops.subscribe_slot_observer_impl   = &subscribe_slot_observer;
                key_set_ts_ops.unsubscribe_slot_observer_impl = &unsubscribe_slot_observer;

                dict_ops = TSDDataOps{};
                TSSDataOps &dict_set = dict_ops;
                dict_set = key_set_ts_ops;
                TSDataOps &dict_base = dict_ops;
                dict_base.kind = TSTypeKind::TSD;
                dict_base.context = this;
                dict_base.tracking_impl = &tracking;
                dict_base.mutable_tracking_impl = &mutable_tracking;
                dict_base.has_current_value_impl = &has_current_value;
                dict_base.all_valid_impl = &all_valid;
                dict_base.ownership_ops = &ownership_ops();
                dict_base.empty_delta_impl = &ts_data_detail::empty_delta_tsd;
                dict_base.capture_delta_impl = &ts_data_detail::capture_delta_tsd;
                dict_base.delta_has_effect_impl = &ts_data_detail::delta_has_effect_tsd;
                dict_base.apply_delta_impl = &ts_data_detail::apply_delta_tsd;
                dict_ops.child_at_slot_impl = &tsd_child_at_slot;
                dict_ops.slot_modified_impl = &slot_modified;
                dict_ops.next_modified_slot_impl = &next_modified_slot;
                dict_ops.make_ts_values_range_impl = &ts_value_range<TSDProxyMapSurface::Live>;
                dict_ops.make_valid_keys_range_impl = &set_range<TSDProxySetSurface::Live>;
                dict_ops.make_valid_ts_values_range_impl = &ts_value_range<TSDProxyMapSurface::Live>;
                dict_ops.make_modified_keys_range_impl = &map_key_range<TSDProxyMapSurface::Modified>;
                dict_ops.make_modified_ts_values_range_impl = &ts_value_range<TSDProxyMapSurface::Modified>;
                dict_ops.make_added_ts_values_range_impl = &ts_value_range<TSDProxyMapSurface::Added>;
                dict_ops.make_removed_ts_values_range_impl = &ts_value_range<TSDProxyMapSurface::Removed>;
                dict_ops.make_ts_kv_range_impl = &ts_kv_range<TSDProxyMapSurface::Live>;
                dict_ops.make_valid_ts_kv_range_impl = &ts_kv_range<TSDProxyMapSurface::Live>;
                dict_ops.make_modified_ts_kv_range_impl = &ts_kv_range<TSDProxyMapSurface::Modified>;
                dict_ops.make_added_ts_kv_range_impl = &ts_kv_range<TSDProxyMapSurface::Added>;
                dict_ops.make_removed_ts_kv_range_impl = &ts_kv_range<TSDProxyMapSurface::Removed>;
            }

            void bind_surfaces()
            {
                const auto *set_schema        = TypeRegistry::instance().set(schema->key_type());
                const auto *key_set_ts_schema = TypeRegistry::instance().tss(schema->key_type());
                if (schema->value_schema == nullptr || schema->delta_value_schema == nullptr ||
                    set_schema == nullptr || key_set_ts_schema == nullptr)
                {
                    throw std::logic_error("TSDProxy schemas are not populated");
                }

                layout.value_binding = intern_value_type(*schema->value_schema, *plan, value_map_ops);

                if (schema->delta_value_schema->value_kind() != ValueTypeKind::Bundle ||
                    schema->delta_value_schema->field_count != 3)
                {
                    throw std::logic_error(
                        "TSDProxy delta schema must be Bundle{removed, modified, removed_strict}");
                }
                removed_set_binding = intern_value_type(*schema->delta_value_schema->fields[0].type,
                                                                *plan,
                                                                removed_set_value_ops);
                modified_map_binding = intern_value_type(*schema->delta_value_schema->fields[1].type,
                                                                 *plan,
                                                                 modified_map_ops);
                empty_strict_set = ts_data_detail::interned_empty_set(schema->key_type());
                added_set_binding = intern_value_type(*set_schema, *plan, added_set_value_ops);
                layout.delta_binding = intern_value_type(*schema->delta_value_schema, *plan, delta_bundle_ops);

                key_set_value_binding = intern_value_type(*set_schema, *plan, key_set_value_ops);
                const auto label = role == TypeRole::Data
                                       ? std::string_view{"ts.tsd.key-set.data"}
                                       : std::string_view{"ts.tsd.key-set.output"};
                key_set_type = TSRoleTypeRef{intern_ts_type(
                    *key_set_ts_schema, role, *plan, key_set_ts_ops, label)};
                layout.key_set_type = key_set_type;
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] SetValueOps set_value_ops_for()
            {
                SetValueOps ops{
                    {{ValueOpsKind::Set, this, false, &set_hash<Surface>, &set_equals<Surface>,
                      &set_compare<Surface>, &set_to_string<Surface>
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &set_surface_to_python<Surface>,
                      nullptr
#endif
                     },
                     &set_size<Surface>,
                     &set_element_at<Surface>,
                     &set_element_binding,
                     &set_range<Surface>,
                     nullptr},
                    &set_contains_raw<Surface>,
                };
                ops.owning_type_impl      = &canonical_value_binding;
                ops.copy_construct_view_impl = &set_copy_construct_view<Surface>;
                ops.copy_assign_view_impl    = &set_copy_assign_view<Surface>;
                return ops;
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] MapValueOps map_value_ops_for()
            {
                MapValueOps ops{
                    {{ValueOpsKind::Map, this, false, &map_hash<Surface>, &map_equals<Surface>,
                      &map_compare<Surface>, &map_to_string<Surface>
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                      ,
                      &map_surface_to_python<Surface>,
                      nullptr
#endif
                     },
                     &map_size<Surface>,
                     &map_key_at_index<Surface>,
                     &set_element_binding,
                     &map_key_range<Surface>,
                     nullptr},
                    &map_contains_raw<Surface>,
                    &map_value_at<Surface>,
                    &map_value_at_index<Surface>,
                    &map_value_binding<Surface>,
                    &map_key_range<Surface>,
                    &map_value_range<Surface>,
                    &map_kv_range<Surface>,
                    &map_key_set,
                };
                ops.owning_type_impl      = &canonical_value_binding;
                ops.copy_construct_view_impl = &map_copy_construct_view<Surface>;
                ops.copy_assign_view_impl    = &map_copy_assign_view<Surface>;
                return ops;
            }

            [[nodiscard]] static const TSDProxyContext *ctx(const void *context) noexcept
            {
                return static_cast<const TSDProxyContext *>(context);
            }

            [[nodiscard]] static ValueTypeRef
            canonical_value_binding(const void *, ValueTypeRef view_binding)
            {
                const auto binding = ValuePlanFactory::instance().type_for(view_binding.schema());
                if (binding == nullptr)
                {
                    throw std::logic_error("TSDProxy value surface has no canonical owning binding");
                }
                return binding;
            }

            static void delta_copy_construct_view(const void *context,
                                                  const ValueTypeRef &binding,
                                                  void *dst,
                                                  const void *memory)
            {
                const auto &plan = binding.checked_plan();
                plan.default_construct(dst);
                auto rollback = make_scope_exit([&]() noexcept { plan.destroy(dst); });
                delta_copy_assign_view(context, binding, dst, memory);
                rollback.release();
            }

            static void delta_copy_assign_view(const void *context,
                                               const ValueTypeRef &binding,
                                               void *dst,
                                               const void *memory)
            {
                if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Bundle ||
                    binding.schema()->field_count != 3)
                {
                    throw std::logic_error(
                        "TSDProxy delta copy requires canonical Bundle{removed, modified, removed_strict}");
                }

                const auto &plan = binding.checked_plan();
                if (!plan.is_composite() || plan.component_count() < 3)
                {
                    throw std::logic_error("TSDProxy delta copy requires a three-field structured plan");
                }

                auto removed  = Value{ValueView{delta_element_binding(context, memory, 0),
                                                delta_element_at(context, memory, 0)}};
                auto modified = Value{ValueView{delta_element_binding(context, memory, 1),
                                                delta_element_at(context, memory, 1)}};
                auto removed_strict = Value{ValueView{delta_element_binding(context, memory, 2),
                                                      delta_element_at(context, memory, 2)}};

                BundleBuilder builder{binding};
                builder.set(0, removed.view());
                builder.set(1, modified.view());
                builder.set(2, removed_strict.view());
                Value bundle = builder.build();
                plan.copy_assign(dst, bundle.view().data());
            }

            template <TSDProxySetSurface Surface>
            static void set_copy_construct_view(const void *context,
                                                const ValueTypeRef &binding,
                                                void *dst,
                                                const void *memory)
            {
                auto storage = build_set_storage<Surface>(context, binding, memory);
                std::construct_at(static_cast<SetStorage *>(dst), std::move(storage));
            }

            template <TSDProxySetSurface Surface>
            static void set_copy_assign_view(const void *context,
                                             const ValueTypeRef &binding,
                                             void *dst,
                                             const void *memory)
            {
                *static_cast<SetStorage *>(dst) = build_set_storage<Surface>(context, binding, memory);
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static SetStorage build_set_storage(const void *context,
                                                              const ValueTypeRef &binding,
                                                              const void *memory)
            {
                const auto *state = ctx(context);
                if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Set)
                {
                    throw std::logic_error("TSDProxy set copy requires a canonical set binding");
                }
                const auto key_binding = ValuePlanFactory::instance().type_for(binding.schema()->element_type);
                if (key_binding == nullptr || key_binding != state->layout.key_binding)
                {
                    throw std::logic_error("TSDProxy set copy key binding is not resolved");
                }

                SetBuilder builder{key_binding};
                for (const auto key : set_range<Surface>(context, memory))
                {
                    builder.insert_copy(key.data());
                }
                return builder.build_storage();
            }

            template <TSDProxyMapSurface Surface>
            static void map_copy_construct_view(const void *context,
                                                const ValueTypeRef &binding,
                                                void *dst,
                                                const void *memory)
            {
                auto storage = build_map_storage<Surface>(context, binding, memory);
                std::construct_at(static_cast<MapStorage *>(dst), std::move(storage));
            }

            template <TSDProxyMapSurface Surface>
            static void map_copy_assign_view(const void *context,
                                             const ValueTypeRef &binding,
                                             void *dst,
                                             const void *memory)
            {
                *static_cast<MapStorage *>(dst) = build_map_storage<Surface>(context, binding, memory);
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static MapStorage build_map_storage(const void *context,
                                                              const ValueTypeRef &binding,
                                                              const void *memory)
            {
                if (binding.schema() == nullptr || binding.schema()->value_kind() != ValueTypeKind::Map)
                {
                    throw std::logic_error("TSDProxy map copy requires a canonical map binding");
                }

                const auto key_binding = ValuePlanFactory::instance().type_for(binding.schema()->key_type);
                const auto value_binding = ValuePlanFactory::instance().type_for(binding.schema()->element_type);
                if (key_binding == nullptr || value_binding == nullptr)
                {
                    throw std::logic_error("TSDProxy map copy bindings are not resolved");
                }

                MapBuilder builder{key_binding, value_binding};
                for (const auto [key, value] : map_kv_range<Surface>(context, memory))
                {
                    if (!value.has_value())
                    {
                        builder.set_item_unset(key.data());
                        continue;
                    }
                    Value owned_value{value};
                    if (owned_value.binding() != value_binding)
                    {
                        throw std::logic_error("TSDProxy map copy materialized the wrong value binding");
                    }
                    builder.set_item_copy(key.data(), owned_value.view().data());
                }
                return builder.build_storage();
            }

          public:
            [[nodiscard]] static const detail::TSDataOwnershipOps &ownership_ops() noexcept
            {
                static const detail::TSDataOwnershipOps ops{
                    .child_count = [](const void *, const void *memory) noexcept {
                        if (memory == nullptr) return std::size_t{0};
                        const auto &proxy = proxy_storage(memory);
                        std::size_t count = 1;
                        for (std::size_t slot = 0; slot < proxy.child_capacity(); ++slot)
                            count += proxy.has_child(slot) ? 1U : 0U;
                        return count;
                    },
                    .child_at = [](const void *context, void *memory, std::size_t index) noexcept {
                        if (context == nullptr || memory == nullptr) return detail::TSDataOwnedChild{};
                        const auto *state = static_cast<const TSDProxyContext *>(context);
                        if (index == 0)
                            return detail::TSDataOwnedChild{
                                .type = state->key_set_type,
                                .data = memory,
                                .attach_parent = false,
                            };
                        auto &proxy = proxy_storage(memory);
                        std::size_t seen = 1;
                        for (std::size_t slot = 0; slot < proxy.child_capacity(); ++slot)
                        {
                            if (!proxy.has_child(slot)) { continue; }
                            if (seen++ == index)
                                return detail::TSDataOwnedChild{
                                    .type = proxy.element_type(),
                                    .data = proxy.owned_child_memory(slot),
                                    .parent_child_id = slot,
                                };
                        }
                        return detail::TSDataOwnedChild{};
                    },
                    .stop = [](const void *, void *memory) noexcept {
                        if (memory != nullptr) proxy_storage(memory).stop();
                    },
                };
                return ops;
            }

            [[nodiscard]] static const TSDataLayout *ts_layout(const void *context) noexcept
            {
                return &ctx(context)->layout;
            }

            [[nodiscard]] static const TSDataTracking *tracking(const void *, const void *memory) noexcept
            {
                return &proxy_storage(memory).tracking();
            }

            [[nodiscard]] static TSDataTracking *mutable_tracking(const void *, void *memory) noexcept
            {
                return &proxy_storage(memory).tracking();
            }

            [[nodiscard]] static const TSDataTracking *key_set_tracking(const void *,
                                                                         const void *memory) noexcept
            {
                return &proxy_storage(memory).key_set_tracking();
            }

            [[nodiscard]] static TSDataTracking *mutable_key_set_tracking(const void *, void *memory) noexcept
            {
                return &proxy_storage(memory).key_set_tracking();
            }

            [[nodiscard]] static bool has_current_value(const void *, const void *memory) noexcept
            {
                const auto &proxy = proxy_storage(memory);
                return proxy.source_available() && proxy.tracking().last_modified_time != MIN_DT;
            }

            [[nodiscard]] static bool key_set_has_current_value(const void *, const void *memory) noexcept
            {
                const auto &proxy = proxy_storage(memory);
                return proxy.source_available() && proxy.key_set_tracking().last_modified_time != MIN_DT;
            }

            [[nodiscard]] static bool all_valid(const void *context, const void *memory) noexcept
            {
                return fallback_on_exception(false, [&] {
                    if (!has_current_value(context, memory)) { return false; }
                    const auto *state = ctx(context);
                    const auto &ops   = state->element_type.ops_ref();
                    const auto &store = proxy_storage(memory);
                    auto        dict  = store.source_dict();
                    for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                    {
                        if (!dict.slot_live(slot)) { continue; }
                        if (!store.has_child(slot)) { return false; }
                        if (!ops.all_valid_impl(ops.context, store.child_at_slot(slot))) { return false; }
                    }
                    return true;
                });
            }

            [[nodiscard]] static const void *value_memory(const void *, const void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static void *mutable_value_memory(const void *, void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static const void *delta_memory(const void *, const void *memory) noexcept
            {
                return memory;
            }

            [[nodiscard]] static void *mutable_delta_memory(const void *, void *memory) noexcept
            {
                return memory;
            }

            static void record_child_modified(const void *, void *memory, std::size_t child_id,
                                              DateTime modified_time)
            {
                proxy_storage(memory).record_child_modified(child_id, modified_time);
            }

            static void subscribe_slot_observer(const void *, void *memory, SlotObserver *observer)
            {
                proxy_storage(memory).subscribe_slot_observer(observer);
            }

            static void unsubscribe_slot_observer(const void *, void *memory, SlotObserver *observer)
            {
                proxy_storage(memory).unsubscribe_slot_observer(observer);
            }

            [[nodiscard]] static TSDDataView source_dict(const void *memory)
            {
                return proxy_storage(memory).source_dict();
            }

            [[nodiscard]] static bool source_available(const void *memory) noexcept
            {
                return memory != nullptr && proxy_storage(memory).source_available();
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static bool slot_in_set_surface(const void *, const void *memory, std::size_t slot)
            {
                if (!source_available(memory)) { return false; }
                auto dict = source_dict(memory);
                if constexpr (Surface == TSDProxySetSurface::Live) { return dict.slot_live(slot); }
                else if constexpr (Surface == TSDProxySetSurface::Added) { return dict.slot_added(slot); }
                else { return dict.slot_removed(slot); }
            }

            [[nodiscard]] static bool slot_modified(const void *context, const void *memory, std::size_t slot)
            {
                const auto &store = proxy_storage(memory);
                if (!source_available(memory) || !store.has_child(slot) || !source_dict(memory).slot_live(slot))
                    return false;
                if (store.child_updated(slot)) { return true; }
                const auto *state          = ctx(context);
                const auto &ops            = state->element_type.ops_ref();
                const auto *child_tracking = ops.tracking_impl(ops.context, store.child_at_slot(slot));
                return child_tracking != nullptr &&
                       child_tracking->last_modified_time == store.tracking().last_modified_time;
            }

            [[nodiscard]] static std::size_t next_modified_slot(const void *context,
                                                                const void *memory,
                                                                std::size_t previous)
            {
                if (!source_available(memory)) { return TS_DATA_NO_CHILD_ID; }
                const std::size_t capacity = source_dict(memory).slot_capacity();
                for (std::size_t slot = previous == TS_DATA_NO_CHILD_ID ? 0 : previous + 1;
                     slot < capacity; ++slot)
                {
                    if (slot_modified(context, memory, slot)) { return slot; }
                }
                return TS_DATA_NO_CHILD_ID;
            }

            [[nodiscard]] static const void *tsd_child_at_slot(const void *, const void *memory, std::size_t slot)
            {
                return proxy_storage(memory).child_at_slot(slot);
            }

            [[nodiscard]] static bool slot_occupied(const void *, const void *memory, std::size_t slot)
            {
                return source_available(memory) && source_dict(memory).slot_occupied(slot);
            }

            [[nodiscard]] static bool slot_live(const void *, const void *memory, std::size_t slot)
            {
                return source_available(memory) && source_dict(memory).slot_live(slot);
            }

            [[nodiscard]] static bool slot_added(const void *, const void *memory, std::size_t slot)
            {
                return source_available(memory) && source_dict(memory).slot_added(slot);
            }

            [[nodiscard]] static bool slot_removed(const void *, const void *memory, std::size_t slot)
            {
                return source_available(memory) && source_dict(memory).slot_removed(slot);
            }

            [[nodiscard]] static std::size_t next_added_slot(const void *, const void *memory,
                                                             std::size_t previous)
            {
                return source_available(memory) ? source_dict(memory).next_added_slot(previous)
                                                : TS_DATA_NO_CHILD_ID;
            }

            [[nodiscard]] static std::size_t next_removed_slot(const void *, const void *memory,
                                                               std::size_t previous)
            {
                return source_available(memory) ? source_dict(memory).next_removed_slot(previous)
                                                : TS_DATA_NO_CHILD_ID;
            }

            [[nodiscard]] static std::size_t slot_capacity(const void *, const void *memory)
            {
                return source_available(memory) ? source_dict(memory).slot_capacity() : 0;
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static std::size_t set_size(const void *context, const void *memory) noexcept
            {
                return fallback_on_exception<std::size_t>(0, [&] {
                    if constexpr (Surface == TSDProxySetSurface::Live) { return source_dict(memory).size(); }
                    else
                    {
                        std::size_t count = 0;
                        auto        dict  = source_dict(memory);
                        for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                        {
                            if (slot_in_set_surface<Surface>(context, memory, slot)) { ++count; }
                        }
                        return count;
                    }
                });
            }

            [[nodiscard]] static const void *key_at_slot(const void *, const void *memory, std::size_t slot)
            {
                if (!source_available(memory))
                    throw std::logic_error("TSDProxy source is unavailable");
                return source_dict(memory).key_at_slot(slot).data();
            }

            [[nodiscard]] static bool set_contains(const void *context, const void *memory, const ValueView &key)
            {
                if (key.binding() != ctx(context)->layout.key_binding) { return false; }
                return source_available(memory) && source_dict(memory).contains(key);
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static bool set_contains_raw(const void *context, const void *memory, const void *key)
            {
                if (!source_available(memory)) { return false; }
                const auto slot = source_dict(memory).find_slot(ValueView{ctx(context)->layout.key_binding, key});
                return slot != TS_DATA_NO_CHILD_ID && slot_in_set_surface<Surface>(context, memory, slot);
            }

            [[nodiscard]] static std::size_t find_slot(const void *context, const void *memory, const ValueView &key)
            {
                if (key.binding() != ctx(context)->layout.key_binding) { return TS_DATA_NO_CHILD_ID; }
                return source_available(memory) ? source_dict(memory).find_slot(key) : TS_DATA_NO_CHILD_ID;
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static const void *set_element_at(const void *context, const void *memory, std::size_t index)
            {
                auto        dict = source_dict(memory);
                std::size_t seen = 0;
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!slot_in_set_surface<Surface>(context, memory, slot)) { continue; }
                    if (seen++ == index) { return dict.key_at_slot(slot).data(); }
                }
                throw std::out_of_range("TSDProxy set element index out of range");
            }

            [[nodiscard]] static ValueTypeRef set_element_binding(const void *context,
                                                                             const void *,
                                                                             std::size_t) noexcept
            {
                return ctx(context)->layout.key_binding;
            }

            [[nodiscard]] static ValueView key_projector(const void *context, const void *memory, std::size_t slot)
            {
                return ValueView{ctx(context)->layout.key_binding, key_at_slot(context, memory, slot)};
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static Range<ValueView> set_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = slot_capacity(context, memory),
                    .predicate = &slot_in_set_surface<Surface>,
                    .projector = &key_projector,
                };
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static std::size_t set_hash(const void *context, const void *memory)
            {
                const auto &key_ops = ctx(context)->layout.key_binding.ops_ref();
                std::size_t result = 0;
                for (const auto key : set_range<Surface>(context, memory))
                {
                    result ^= key_ops.hash(key.data());
                }
                return result;
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static bool set_equals(const void *context, const void *lhs, const void *rhs) noexcept
            {
                if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
                return fallback_on_exception(false, [&] {
                    if (set_size<Surface>(context, lhs) != set_size<Surface>(context, rhs)) { return false; }
                    for (const auto key : set_range<Surface>(context, lhs))
                    {
                        if (!set_contains_raw<Surface>(context, rhs, key.data())) { return false; }
                    }
                    return true;
                });
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static std::partial_ordering set_compare(const void *context, const void *lhs,
                                                                    const void *rhs) noexcept
            {
                if (const auto order = value_ops_detail::null_order(static_cast<const void *>(lhs),
                                                                     static_cast<const void *>(rhs)))
                {
                    return *order;
                }
                const auto lhs_size = set_size<Surface>(context, lhs);
                const auto rhs_size = set_size<Surface>(context, rhs);
                if (lhs_size < rhs_size) { return std::partial_ordering::less; }
                if (lhs_size > rhs_size) { return std::partial_ordering::greater; }
                return set_equals<Surface>(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                              : std::partial_ordering::unordered;
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static std::string set_to_string(const void *context, const void *memory)
            {
                const auto &key_ops = ctx(context)->layout.key_binding.ops_ref();
                fmt::memory_buffer out;
                fmt::format_to(std::back_inserter(out), "{{");
                bool first = true;
                for (const auto key : set_range<Surface>(context, memory))
                {
                    if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                    first = false;
                    fmt::format_to(std::back_inserter(out), "{}", key_ops.to_string(key.data()));
                }
                fmt::format_to(std::back_inserter(out), "}}");
                return fmt::to_string(out);
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static bool map_slot_in_surface(const void *context, const void *memory, std::size_t slot)
            {
                if (!source_available(memory)) { return false; }
                if constexpr (Surface == TSDProxyMapSurface::Live)
                {
                    return source_dict(memory).slot_live(slot);
                }
                else if constexpr (Surface == TSDProxyMapSurface::Added)
                {
                    return source_dict(memory).slot_added(slot);
                }
                else if constexpr (Surface == TSDProxyMapSurface::Removed)
                {
                    return source_dict(memory).slot_removed(slot);
                }
                else { return slot_modified(context, memory, slot); }
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static std::size_t map_size(const void *context, const void *memory) noexcept
            {
                return fallback_on_exception<std::size_t>(0, [&] {
                    if constexpr (Surface == TSDProxyMapSurface::Live) { return source_dict(memory).size(); }
                    else
                    {
                        std::size_t count = 0;
                        auto        dict  = source_dict(memory);
                        for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                        {
                            if (map_slot_in_surface<Surface>(context, memory, slot)) { ++count; }
                        }
                        return count;
                    }
                });
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static const void *map_key_at_index(const void *context,
                                                              const void *memory,
                                                              std::size_t index)
            {
                auto        dict = source_dict(memory);
                std::size_t seen = 0;
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!map_slot_in_surface<Surface>(context, memory, slot)) { continue; }
                    if (seen++ == index) { return dict.key_at_slot(slot).data(); }
                }
                throw std::out_of_range("TSDProxy map key index out of range");
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static bool map_contains_raw(const void *context, const void *memory, const void *key)
            {
                if (!source_available(memory)) { return false; }
                const auto slot = source_dict(memory).find_slot(ValueView{ctx(context)->layout.key_binding, key});
                return slot != TS_DATA_NO_CHILD_ID && map_slot_in_surface<Surface>(context, memory, slot);
            }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            /** Surface read-back for python (type-erasure rule: conversion
                binds to the ops). Children are read through their TS views -
                link-endpoint children resolve to their targets. */
            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static nanobind::object map_surface_to_python(const void *context, const void *memory)
            {
                namespace nb = nanobind;
                const auto &proxy = proxy_storage(memory);
                auto        dict  = source_dict(memory);
                nb::dict    result;
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!map_slot_in_surface<Surface>(context, memory, slot) || !proxy.has_child(slot)) { continue; }
                    const auto value_type = map_value_binding<Surface>(context, memory);
                    const auto *value_memory = map_value_at_slot<Surface>(context, memory, slot);
                    auto key = dict.key_at_slot(slot);
                    const auto py_key = key.binding().ops_ref().to_python(key.data());
                    result[py_key] = value_memory != nullptr ? value_type.ops_ref().to_python(value_memory)
                                                              : nb::none();
                }
                return result;
            }

            template <TSDProxySetSurface Surface>
            [[nodiscard]] static nanobind::object set_surface_to_python(const void *context, const void *memory)
            {
                namespace nb = nanobind;
                auto     dict = source_dict(memory);
                nb::list items;
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!slot_in_set_surface<Surface>(context, memory, slot)) { continue; }
                    auto key = dict.key_at_slot(slot);
                    items.append(key.binding().ops_ref().to_python(key.data()));
                }
                return nb::steal(PyFrozenSet_New(items.ptr()));
            }
#endif

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static const void *map_value_at_slot(const void *context,
                                                               const void *memory,
                                                               std::size_t slot)
            {
                const auto *state = ctx(context);
                const auto &child_ops = state->element_type.ops_ref();
                const auto *child_memory = proxy_storage(memory).child_at_slot(slot);
                if constexpr (Surface == TSDProxyMapSurface::Live)
                {
                    return child_ops.value_memory_impl(child_ops.context, child_memory);
                }
                else
                {
                    return child_ops.delta_memory_impl(child_ops.context, child_memory);
                }
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static const void *map_value_at(const void *context, const void *memory, const void *key)
            {
                if (!source_available(memory)) { return nullptr; }
                const auto slot = source_dict(memory).find_slot(ValueView{ctx(context)->layout.key_binding, key});
                if (slot == TS_DATA_NO_CHILD_ID || !map_slot_in_surface<Surface>(context, memory, slot))
                {
                    return nullptr;
                }
                return map_value_at_slot<Surface>(context, memory, slot);
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static const void *map_value_at_index(const void *context,
                                                                const void *memory,
                                                                std::size_t index)
            {
                auto        dict = source_dict(memory);
                std::size_t seen = 0;
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!map_slot_in_surface<Surface>(context, memory, slot)) { continue; }
                    if (seen++ == index) { return map_value_at_slot<Surface>(context, memory, slot); }
                }
                throw std::out_of_range("TSDProxy map value index out of range");
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static ValueTypeRef map_value_binding(const void *context, const void *) noexcept
            {
                if constexpr (Surface == TSDProxyMapSurface::Live)
                {
                    return ctx(context)->layout.element_value_binding;
                }
                else
                {
                    return ctx(context)->layout.element_delta_binding;
                }
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static ValueView map_value_projector(const void *context,
                                                               const void *memory,
                                                               std::size_t slot)
            {
                return ValueView{map_value_binding<Surface>(context, memory),
                                 map_value_at_slot<Surface>(context, memory, slot)};
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static std::pair<ValueView, ValueView> map_kv_projector(const void *context,
                                                                                  const void *memory,
                                                                                  std::size_t slot)
            {
                return {key_projector(context, memory, slot),
                        map_value_projector<Surface>(context, memory, slot)};
            }

            [[nodiscard]] static TSDataView ts_value_projector(const void *context,
                                                               const void *memory,
                                                               std::size_t slot)
            {
                return TSDataView{ctx(context)->layout.element_type, proxy_storage(memory).child_at_slot(slot)};
            }

            [[nodiscard]] static std::pair<ValueView, TSDataView> ts_kv_projector(const void *context,
                                                                                  const void *memory,
                                                                                  std::size_t slot)
            {
                return {key_projector(context, memory, slot), ts_value_projector(context, memory, slot)};
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static Range<ValueView> map_key_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = slot_capacity(context, memory),
                    .predicate = &map_slot_in_surface<Surface>,
                    .projector = &key_projector,
                };
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static Range<ValueView> map_value_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = slot_capacity(context, memory),
                    .predicate = &map_slot_in_surface<Surface>,
                    .projector = &map_value_projector<Surface>,
                };
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static KeyValueRange<ValueView, ValueView> map_kv_range(const void *context,
                                                                                  const void *memory)
            {
                return KeyValueRange<ValueView, ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = slot_capacity(context, memory),
                    .predicate = &map_slot_in_surface<Surface>,
                    .projector = &map_kv_projector<Surface>,
                };
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static std::size_t map_hash(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                const auto &key_ops = state->layout.key_binding.ops_ref();
                const auto value_binding = map_value_binding<Surface>(context, memory);
                const auto &value_ops = value_binding.ops_ref();
                std::size_t result = 0;
                for (const auto [key, value] : map_kv_range<Surface>(context, memory))
                {
                    const auto value_hash = value.has_value() ? value_ops.hash(value.data())
                                                              : 0x9e3779b97f4a7c15ULL;
                    result ^= combine_hash(key_ops.hash(key.data()), value_hash);
                }
                return result;
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static bool map_equals(const void *context, const void *lhs, const void *rhs) noexcept
            {
                if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
                return fallback_on_exception(false, [&] {
                    if (map_size<Surface>(context, lhs) != map_size<Surface>(context, rhs)) { return false; }
                    const auto value_binding = map_value_binding<Surface>(context, lhs);
                    const auto &value_ops = value_binding.ops_ref();
                    for (const auto [key, value] : map_kv_range<Surface>(context, lhs))
                    {
                        if (!map_contains_raw<Surface>(context, rhs, key.data())) { return false; }
                        const auto *rhs_value = map_value_at<Surface>(context, rhs, key.data());
                        if (value.has_value() != (rhs_value != nullptr)) { return false; }
                        if (value.has_value() && !value_ops.equals(value.data(), rhs_value)) { return false; }
                    }
                    return true;
                });
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static std::partial_ordering map_compare(const void *context, const void *lhs,
                                                                    const void *rhs) noexcept
            {
                if (const auto order = value_ops_detail::null_order(static_cast<const void *>(lhs),
                                                                     static_cast<const void *>(rhs)))
                {
                    return *order;
                }
                return map_equals<Surface>(context, lhs, rhs) ? std::partial_ordering::equivalent
                                                              : std::partial_ordering::unordered;
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static std::string map_to_string(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                const auto &key_ops = state->layout.key_binding.ops_ref();
                const auto value_binding = map_value_binding<Surface>(context, memory);
                const auto &value_ops = value_binding.ops_ref();
                fmt::memory_buffer out;
                fmt::format_to(std::back_inserter(out), "{{");
                bool first = true;
                for (const auto [key, value] : map_kv_range<Surface>(context, memory))
                {
                    if (!first) { fmt::format_to(std::back_inserter(out), ", "); }
                    first = false;
                    fmt::format_to(std::back_inserter(out), "{}: {}",
                                   key_ops.to_string(key.data()),
                                   value.has_value() ? value_ops.to_string(value.data()) : std::string{"None"});
                }
                fmt::format_to(std::back_inserter(out), "}}");
                return fmt::to_string(out);
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static Range<TSDataView> ts_value_range(const void *context, const void *memory)
            {
                return Range<TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = slot_capacity(context, memory),
                    .predicate = &map_slot_in_surface<Surface>,
                    .projector = &ts_value_projector,
                };
            }

            template <TSDProxyMapSurface Surface>
            [[nodiscard]] static KeyValueRange<ValueView, TSDataView> ts_kv_range(const void *context,
                                                                                  const void *memory)
            {
                return KeyValueRange<ValueView, TSDataView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = slot_capacity(context, memory),
                    .predicate = &map_slot_in_surface<Surface>,
                    .projector = &ts_kv_projector,
                };
            }

            [[nodiscard]] static SetView map_key_set(const void *context, ValueTypeRef, const void *memory)
            {
                return ValueView{ctx(context)->key_set_value_binding, memory}.as_set();
            }

            [[nodiscard]] static std::size_t delta_size(const void *, const void *) noexcept
            {
                return 3;
            }

            [[nodiscard]] static const void *delta_element_at(const void *context, const void *memory,
                                                              std::size_t index)
            {
                if (index == 0 || index == 1) { return memory; }
                if (index == 2) { return ctx(context)->empty_strict_set->view().data(); }
                throw std::out_of_range("TSDProxy delta element index out of range");
            }

            [[nodiscard]] static ValueTypeRef delta_element_binding(const void *context,
                                                                               const void *,
                                                                               std::size_t index) noexcept
            {
                if (index == 0) { return ctx(context)->removed_set_binding; }
                if (index == 1) { return ctx(context)->modified_map_binding; }
                if (index == 2) { return ctx(context)->empty_strict_set->binding(); }
                return nullptr;
            }

            [[nodiscard]] static Range<ValueView> delta_range(const void *context, const void *memory)
            {
                return Range<ValueView>{
                    .context   = context,
                    .memory    = memory,
                    .limit     = 3,
                    .predicate = nullptr,
                    .projector = &delta_projector,
                };
            }

            [[nodiscard]] static ValueView delta_projector(const void *context,
                                                           const void *memory,
                                                           std::size_t index)
            {
                return ValueView{delta_element_binding(context, memory, index),
                                 delta_element_at(context, memory, index)};
            }

            [[nodiscard]] static std::size_t delta_hash(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                return combine_hash(combine_hash(state->removed_set_binding.ops_ref().hash(memory),
                                                 state->modified_map_binding.ops_ref().hash(memory)),
                                    state->empty_strict_set->binding().ops_ref().hash(
                                        state->empty_strict_set->view().data()));
            }

            [[nodiscard]] static bool delta_equals(const void *context, const void *lhs, const void *rhs) noexcept
            {
                if (lhs == nullptr || rhs == nullptr) { return lhs == rhs; }
                return fallback_on_exception(false, [&] {
                    const auto *state = ctx(context);
                    return state->removed_set_binding.ops_ref().equals(lhs, rhs) &&
                           state->modified_map_binding.ops_ref().equals(lhs, rhs);
                });
            }

            [[nodiscard]] static std::partial_ordering delta_compare(const void *context, const void *lhs,
                                                                      const void *rhs) noexcept
            {
                if (const auto order = value_ops_detail::null_order(static_cast<const void *>(lhs),
                                                                     static_cast<const void *>(rhs)))
                {
                    return *order;
                }
                return fallback_on_exception(std::partial_ordering::unordered, [&] {
                    const auto *state = ctx(context);
                    const auto removed_order = state->removed_set_binding.ops_ref().compare(lhs, rhs);
                    if (removed_order != 0) { return removed_order; }
                    return state->modified_map_binding.ops_ref().compare(lhs, rhs);
                });
            }

            [[nodiscard]] static std::string delta_to_string(const void *context, const void *memory)
            {
                const auto *state = ctx(context);
                return fmt::format("{{removed: {}, modified: {}, removed_strict: {}}}",
                                   state->removed_set_binding.ops_ref().to_string(memory),
                                   state->modified_map_binding.ops_ref().to_string(memory),
                                   state->empty_strict_set->binding().ops_ref().to_string(
                                       state->empty_strict_set->view().data()));
            }
        };

        [[nodiscard]] std::unordered_map<TSDProxyContextKey,
                                         std::unique_ptr<TSDProxyContext>,
                                         TSDProxyContextKeyHash> &
        tsd_proxy_contexts() noexcept
        {
            static std::unordered_map<TSDProxyContextKey,
                                      std::unique_ptr<TSDProxyContext>,
                                      TSDProxyContextKeyHash> contexts;
            return contexts;
        }

        [[nodiscard]] std::recursive_mutex &tsd_proxy_context_mutex() noexcept
        {
            static std::recursive_mutex mutex;
            return mutex;
        }

        [[nodiscard]] const TSDProxyContext &tsd_proxy_context_for(const TSValueTypeMetaData &schema,
                                                                   TSRoleTypeRef element_type,
                                                                   TypeRole role)
        {
            std::lock_guard<std::recursive_mutex> lock(tsd_proxy_context_mutex());
            auto &contexts = tsd_proxy_contexts();
            const TSDProxyContextKey key{&schema, element_type, role};
            if (const auto it = contexts.find(key); it != contexts.end()) { return *it->second; }

            auto context = std::make_unique<TSDProxyContext>(schema, element_type, role);
            const auto *result = context.get();
            contexts.emplace(key, std::move(context));
            return *result;
        }
    }  // namespace

    TSDProxySlotSync::TSDProxySlotSync(TSDProxy &owner) noexcept
        : owner_(&owner)
    {
    }

    TSDProxySlotSync::~TSDProxySlotSync() = default;

    void TSDProxySlotSync::on_capacity(std::size_t old_capacity, std::size_t new_capacity)
    {
        owner_->on_slot_capacity(old_capacity, new_capacity);
    }

    void TSDProxySlotSync::on_insert(std::size_t slot)
    {
        owner_->on_slot_inserted(slot);
    }

    void TSDProxySlotSync::on_remove(std::size_t slot)
    {
        owner_->on_slot_removed(slot);
    }

    void TSDProxySlotSync::on_erase(std::size_t slot)
    {
        owner_->on_slot_erased(slot);
    }

    void TSDProxySlotSync::on_clear()
    {
        owner_->on_slots_cleared();
    }

    void TSDProxySlotSync::notify(DateTime modified_time)
    {
        owner_->on_source_modified(modified_time);
    }

    void TSDProxySlotSync::source_invalidated(const TSDataTracking *source) noexcept
    {
        owner_->on_source_invalidated(source);
    }

    TSDProxy::TSDProxy() noexcept
        : source_sync_(*this)
    {
    }

    TSDProxy::~TSDProxy()
    {
        unsubscribe_source(false);
    }

    void TSDProxy::bind(TSRoleTypeRef     self_type,
                        TSRoleTypeRef     element_type,
                        const TSDDataView   &source,
                        const TSDProxyValueOps *value_ops,
                        const void          *builder_context,
                        DateTime        modified_time,
                        TSDProxyChildRefresh child_refresh)
    {
        if (child_refresh_ == TSDProxyChildRefresh::Invalidating)
        {
            throw std::logic_error("TSDProxy cannot bind while source invalidation is in progress");
        }
        if (child_refresh == TSDProxyChildRefresh::Invalidating)
        {
            throw std::invalid_argument("TSDProxy Invalidating refresh policy is internal");
        }
        require_concrete_build_time(modified_time);
        if (source.schema() == nullptr || source.schema()->kind != TSTypeKind::TSD)
        {
            throw std::invalid_argument("TSDProxy requires a TSD source view");
        }
        if (value_ops == nullptr || value_ops->build == nullptr)
        {
            throw std::invalid_argument("TSDProxy requires value ops with a build function");
        }
        if ((child_refresh == TSDProxyChildRefresh::StructureOnly) !=
            (value_ops->source_identity_matches == nullptr))
        {
            throw std::invalid_argument("TSDProxy refresh policy and identity matcher do not agree");
        }

        child_refresh_ = child_refresh;
        const auto &element_plan = element_type.checked_plan();
        const bool reconfigure =
            self_type_ != self_type ||
            element_type_ != element_type ||
            source_storage_.storage_type() != source.base().storage_type() ||
            source_storage_.data() != source.base().data() ||
            value_ops_ != value_ops ||
            value_builder_context_ != builder_context;

        if (reconfigure)
        {
            unsubscribe_source();
            if (element_type_ && has_constructed_children()) on_slots_cleared();
            else values_.destroy_all();
            self_type_             = self_type;
            element_type_          = element_type;
            source_storage_        = TSDDataStorageRef{source.base().storage_ref(), TSTypeKind::TSD};
            value_ops_             = value_ops;
            value_builder_context_ = builder_context;
            values_.bind_plan(element_plan);
        }
        else
        {
            source_storage_ = TSDDataStorageRef{source.base().storage_ref(), TSTypeKind::TSD};
        }

        // The initial sync only FORCES a modified mark when the source has
        // actually ticked - binding over a never-ticked source must not
        // fabricate an empty first tick for the alternative's consumers.
        sync_from_source(modified_time, source_view().has_current_value());
        subscribe_source();
    }

    void TSDProxy::on_slot_capacity(std::size_t old_capacity, std::size_t new_capacity)
    {
        values_.reserve_to(new_capacity);
        if (built_times_.size() < new_capacity) { built_times_.resize(new_capacity, MIN_DT); }
        slot_observers_.notify_capacity(old_capacity, new_capacity);
    }

    void TSDProxy::on_slot_inserted(std::size_t slot)
    {
        structure_pending_ = true;
        if (has_child(slot))
        {
            const auto state = slot < built_times_.size() ? built_times_[slot] : MIN_DT;
            if (!pending_owed(state))
            {
                throw std::logic_error("TSDProxy retained insertion requires insert-owed state");
            }
            const auto modified_time = current_lifecycle_time(slot);
            static_cast<void>(retry_pending_child_at_slot(slot, modified_time));
            mark_modified(modified_time);
            return;
        }

        construct_child_at_slot(slot);
        if (slot < built_times_.size()) { built_times_[slot] = MIN_DT; }
        slot_observers_.notify_insert(slot);
    }

    void TSDProxy::on_slot_removed(std::size_t slot)
    {
        structure_pending_ = true;
        if (has_child(slot))
        {
            detail::stop_owned_ts_data_tree(TSDataView{element_type_, values_.value_memory(slot)});
            if (slot >= built_times_.size()) { built_times_.resize(slot + 1, MIN_DT); }
            built_times_[slot] = MAX_DT;
        }
        else if (slot < built_times_.size()) { built_times_[slot] = MIN_DT; }
        slot_observers_.notify_remove(slot);
    }

    void TSDProxy::on_slot_erased(std::size_t slot)
    {
        if (values_.has_slot(slot))
            detail::invalidate_owned_ts_data_tree(TSDataView{element_type_, values_.value_memory(slot)});
        if (slot < built_times_.size()) { built_times_[slot] = MIN_DT; }
        slot_observers_.notify_erase(slot);
        values_.destroy_at(slot);
    }

    void TSDProxy::on_slots_cleared()
    {
        for (std::size_t slot = 0; slot < values_.slot_capacity(); ++slot)
        {
            if (values_.has_slot(slot))
                detail::invalidate_owned_ts_data_tree(TSDataView{element_type_, values_.value_memory(slot)});
        }
        built_times_.assign(built_times_.size(), MIN_DT);
        slot_observers_.notify_clear();
        values_.destroy_all();
    }

    void TSDProxy::on_source_modified(DateTime modified_time)
    {
        if (!source_storage_.valid() || value_ops_ == nullptr) { return; }
        require_concrete_build_time(modified_time);

        auto dict = source_dict();
        bool touched = false;
        for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
        {
            const bool live = dict.slot_live(slot);
            if (dict.slot_removed(slot))
            {
                touched = true;
                continue;
            }
            if (!live) { continue; }

            if (!has_child(slot))
            {
                construct_child_at_slot(slot);
                if (slot >= built_times_.size()) { built_times_.resize(slot + 1, MIN_DT); }
                built_times_[slot] = MAX_DT;
                static_cast<void>(retry_pending_child_at_slot(slot, modified_time));
                touched = true;
                continue;
            }

            const auto state = slot < built_times_.size() ? built_times_[slot] : MIN_DT;
            if (pending_announced(state) || pending_owed(state))
            {
                static_cast<void>(retry_pending_child_at_slot(slot, modified_time));
                touched = true;
                continue;
            }
            if (!built(state)) { throw std::logic_error("TSDProxy child has an invalid build stamp"); }
            if (dict.slot_added(slot)) { touched = true; }

            // OnChildTick proxies (from-REF): a LIVE slot whose source child
            // ticked re-runs the builder so links rebind on a retarget. The
            // proxy only marks itself modified when the child actually
            // recorded at this time, so same-reference re-publication stays
            // silent. StructureOnly proxies (to-REF) never rebuild on value
            // ticks - the materialised identity did not change.
            if (child_refresh_ == TSDProxyChildRefresh::OnChildTick)
            {
                refresh_stale_child(slot);
                auto child = TSDataView{element_type_, values_.value_memory(slot)};
                if (child.tracking().last_modified_time == modified_time) { touched = true; }
            }
        }

        // With no live slots, a source-root notification cannot have come
        // from a child value. It is therefore an explicit sample of the empty
        // collection and must remain visible through the proxy. Non-empty
        // sources retain the identity-sensitive suppression above.
        if (touched || dict.size() == 0) { mark_modified(modified_time); }
        if (structure_pending_)
        {
            mark_structure_modified(modified_time);
            structure_pending_ = false;
        }
    }

    TSDataView TSDProxy::source_view() const noexcept
    {
        return TSDataView{source_storage_.storage_ref()};
    }

    TSDDataView TSDProxy::source_dict() const
    {
        if (!source_storage_.valid()) { throw std::logic_error("TSDProxy is not bound to a source"); }
        auto source = source_view();
        return source.as_dict();
    }

    TSDataView TSDProxy::source_child_at_slot(std::size_t slot) const
    {
        return source_dict().at_slot(slot);
    }

    TSDataTracking &TSDProxy::tracking() noexcept
    {
        return tracking_;
    }

    const TSDataTracking &TSDProxy::tracking() const noexcept
    {
        return tracking_;
    }

    TSDataTracking &TSDProxy::key_set_tracking() noexcept
    {
        return key_set_tracking_;
    }

    const TSDataTracking &TSDProxy::key_set_tracking() const noexcept
    {
        return key_set_tracking_;
    }

    bool TSDProxy::has_child(std::size_t slot) const noexcept
    {
        return values_.has_slot(slot);
    }

    bool TSDProxy::child_updated(std::size_t slot) const noexcept
    {
        // TIME-ANCHORED: updated bits belong to the window they were marked
        // in (updated_window_). A reader in a LATER window (the proxy has
        // recorded since) must not see them - the lazy roll clears them on
        // the next record, but reads can arrive first (iteration order).
        return values_.slot_updated(slot) && updated_window_ == tracking_.last_modified_time;
    }

    const void *TSDProxy::child_at_slot(std::size_t slot) const
    {
        if (!has_child(slot)) { throw std::out_of_range("TSDProxy child slot is not constructed"); }
        refresh_stale_child(slot);
        return values_.value_memory(slot);
    }

    void *TSDProxy::child_at_slot(std::size_t slot)
    {
        if (!has_child(slot)) { throw std::out_of_range("TSDProxy child slot is not constructed"); }
        refresh_stale_child(slot);
        return values_.value_memory(slot);
    }

    void TSDProxy::subscribe_source()
    {
        if (subscribed_ || !source_storage_.valid()) { return; }
        source_dict().key_set().subscribe_slot_observer(&source_sync_);
        auto rollback_slot_observer = make_scope_exit<true>([&] {
            source_dict().key_set().unsubscribe_slot_observer(&source_sync_);
        });
        source_view().subscribe(&source_sync_);
        rollback_slot_observer.release();
        subscribed_ = true;
    }

    void TSDProxy::unsubscribe_source(bool strict) noexcept
    {
        if (!source_storage_.valid())
        {
            subscribed_ = false;
            return;
        }
        auto source = source_view();
        if (subscribed_)
        {
            FirstExceptionRecorder cleanup_errors;
            if (strict || (source.valid() && source.tracking().observers.contains(&source_sync_)))
                cleanup_errors.capture([&] { source.unsubscribe(&source_sync_); });
            cleanup_errors.capture([&] {
                source.as_dict().key_set().unsubscribe_slot_observer(&source_sync_);
            });
        }
        subscribed_ = false;
        source_storage_ = {};
    }

    void TSDProxy::on_source_invalidated(const TSDataTracking *source) noexcept
    {
        if (!subscribed_ || source == nullptr || !source_storage_.valid()) { return; }
        auto current = source_view();
        if (!current.valid() || &current.tracking() != source) { return; }

        const auto prior_refresh = child_refresh_;
        child_refresh_ = TSDProxyChildRefresh::Invalidating;
        auto restore_refresh = make_scope_exit([&]() noexcept { child_refresh_ = prior_refresh; });
        subscribed_ = false;
        static_cast<void>(fallback_on_exception(false, [&] {
            current.as_dict().key_set().unsubscribe_slot_observer(&source_sync_);
            return true;
        }));
        source_storage_ = {};

        for (std::size_t slot = 0; slot < values_.slot_capacity(); ++slot)
        {
            if (!values_.has_slot(slot)) { continue; }
            detail::stop_owned_ts_data_tree(TSDataView{element_type_, values_.value_memory(slot)});
        }
        for (std::size_t slot = 0; slot < values_.slot_capacity(); ++slot)
        {
            if (!values_.has_slot(slot)) { continue; }
            detail::invalidate_owned_ts_data_tree(TSDataView{element_type_, values_.value_memory(slot)});
        }
        tracking_.observers.invalidate(&tracking_);
        key_set_tracking_.observers.invalidate(&key_set_tracking_);
        static_cast<void>(fallback_on_exception(false, [&] {
            slot_observers_.notify_clear();
            return true;
        }));
        values_.destroy_all();
        std::ranges::fill(built_times_, MIN_DT);
        updated_window_ = MIN_DT;
        structure_pending_ = false;
    }

    void TSDProxy::sync_from_source(DateTime modified_time, bool force_modified)
    {
        if (!element_type_ || !source_storage_.valid() || value_ops_ == nullptr)
        {
            return;
        }

        auto dict = source_dict();
        values_.reserve_to(dict.slot_capacity());
        if (built_times_.size() < dict.slot_capacity()) { built_times_.resize(dict.slot_capacity(), MIN_DT); }

        bool changed = force_modified;
        bool structure_changed = dict.key_set().base().has_current_value() &&
                                 key_set_tracking_.last_modified_time == MIN_DT;
        const auto limit = std::max(dict.slot_capacity(), values_.slot_capacity());
        for (std::size_t slot = 0; slot < limit; ++slot)
        {
            if (slot < dict.slot_capacity() && dict.slot_live(slot))
            {
                if (!has_child(slot))
                {
                    construct_child_at_slot(slot);
                    structure_changed = true;
                    if (slot >= built_times_.size()) { built_times_.resize(slot + 1, MIN_DT); }
                    built_times_[slot] = MAX_DT;
                }

                const auto state = slot < built_times_.size() ? built_times_[slot] : MIN_DT;
                if (pending_announced(state) || pending_owed(state))
                {
                    static_cast<void>(retry_pending_child_at_slot(slot, modified_time));
                    changed = true;
                }
                else if (!built(state)) { throw std::logic_error("TSDProxy child has an invalid build stamp"); }
                continue;
            }

            // Pending removal retains constructed, stopped storage at its
            // stable address. The source's physical erase callback owns its
            // eventual invalidation and destruction.
            if (slot < dict.slot_capacity() && dict.slot_occupied(slot)) { continue; }

            if (has_child(slot))
            {
                detail::invalidate_owned_ts_data_tree(
                    TSDataView{element_type_, values_.value_memory(slot)});
                if (slot < built_times_.size()) { built_times_[slot] = MIN_DT; }
                slot_observers_.notify_erase(slot);
                values_.destroy_at(slot);
                changed = true;
                structure_changed = true;
            }
        }

        if (changed) { mark_modified(modified_time); }
        if (structure_changed) { mark_structure_modified(modified_time); }
        structure_pending_ = false;
    }

    void TSDProxy::construct_child_at_slot(std::size_t slot)
    {
        if (!self_type_ || !element_type_)
        {
            throw std::logic_error("TSDProxy is not initialised");
        }

        if (values_.has_slot(slot)) { return; }

        values_.construct_at(slot);
        auto target = TSDataView{element_type_, values_.value_memory(slot)};
        target.mutable_tracking().parent = TSParentLink{self_type_, this, slot};
        detail::attach_owned_ts_data_parents(target.borrowed_ref());
    }

    bool TSDProxy::retry_pending_child_at_slot(std::size_t slot, DateTime modified_time)
    {
        if (!has_child(slot) || value_ops_ == nullptr || !source_available())
        {
            throw std::logic_error("TSDProxy pending build requires a live child and source");
        }
        auto dict = source_dict();
        if (!dict.slot_live(slot)) { throw std::logic_error("TSDProxy cannot build a non-live source slot"); }

        const auto state = slot < built_times_.size() ? built_times_[slot] : MIN_DT;
        const bool owes_insert = pending_owed(state);
        if (!pending_announced(state) && !owes_insert)
        {
            if (built(state)) { return false; }
            throw std::logic_error("TSDProxy child has an invalid pending build stamp");
        }

        require_concrete_build_time(modified_time);
        refresh_child_at_slot(slot, modified_time);
        if (owes_insert) { slot_observers_.notify_insert(slot); }
        return true;
    }

    DateTime TSDProxy::current_lifecycle_time(std::size_t slot) const
    {
        if (built(tracking_.last_modified_time)) { return tracking_.last_modified_time; }
        if (source_available())
        {
            auto dict = source_dict();
            if (dict.slot_live(slot))
            {
                const auto source_child_time = dict.at_slot(slot).tracking().last_modified_time;
                if (built(source_child_time)) { return source_child_time; }
            }
            const auto source_time = source_view().tracking().last_modified_time;
            if (built(source_time)) { return source_time; }
        }
        throw std::logic_error("TSDProxy pending build requires a concrete lifecycle time");
    }

    void TSDProxy::refresh_child_at_slot(std::size_t slot, DateTime modified_time)
    {
        if (!element_type_ || value_ops_ == nullptr)
        {
            throw std::logic_error("TSDProxy is not initialised");
        }
        auto target = TSDataView{element_type_, values_.value_memory(slot)};
        value_ops_->build(*this, slot, target, source_child_at_slot(slot), modified_time,
                          value_builder_context_);
        stamp_built(slot, modified_time);
    }

    void TSDProxy::stamp_built(std::size_t slot, DateTime modified_time)
    {
        require_concrete_build_time(modified_time);
        // Capacity is managed by the slot-observer callbacks (aligned with
        // ``values_`` and the source keyset); sync_from_source pre-reserves
        // for the initial bind, so this only defends against a stale call.
        if (built_times_.size() <= slot) { built_times_.resize(slot + 1, MIN_DT); }
        built_times_[slot] = modified_time;
    }

    bool TSDProxy::has_constructed_children() const noexcept
    {
        for (std::size_t slot = 0; slot < values_.slot_capacity(); ++slot)
        {
            if (values_.has_slot(slot)) { return true; }
        }
        return false;
    }

    bool TSDProxy::source_identities_match() const
    {
        if (!source_available() || value_ops_ == nullptr) { return false; }
        if (value_ops_->source_identity_matches == nullptr)
            return child_refresh_ == TSDProxyChildRefresh::StructureOnly;

        auto dict = source_dict();
        for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
        {
            if (!dict.slot_live(slot)) { continue; }
            if (!has_child(slot)) { return false; }
            const auto state = slot < built_times_.size() ? built_times_[slot] : MIN_DT;
            if (!built(state)) { return false; }
            const auto target = TSDataView{element_type_, values_.value_memory(slot)};
            if (!value_ops_->source_identity_matches(*this, slot, target, dict.at_slot(slot),
                                                     value_builder_context_))
                return false;
        }
        return true;
    }

    void TSDProxy::refresh_stale_child(std::size_t slot) const
    {
        if (!has_child(slot) || value_ops_ == nullptr || !source_storage_.valid()) { return; }
        auto dict = source_dict();
        if (!dict.slot_live(slot)) { return; }
        auto source_child = dict.at_slot(slot);
        if (!source_child.valid()) { return; }
        auto *self = const_cast<TSDProxy *>(this);
        auto built_time = slot < built_times_.size() ? built_times_[slot] : MIN_DT;
        if (pending_announced(built_time) || pending_owed(built_time))
        {
            static_cast<void>(self->retry_pending_child_at_slot(slot, current_lifecycle_time(slot)));
            built_time = built_times_[slot];
        }
        if (!built(built_time)) { throw std::logic_error("TSDProxy child has an invalid build stamp"); }

        const auto source_time = source_child.tracking().last_modified_time;
        if (pending_owed(source_time))
        {
            throw std::logic_error("TSDProxy source child has an invalid modified time");
        }
        if (pending_announced(source_time)) { return; }
        if (!built(source_time)) { throw std::logic_error("TSDProxy source child time is out of range"); }
        if (source_time < built_time) { return; }
        if (source_time > built_time)
        {
            self->refresh_child_at_slot(slot, source_time);
            return;
        }

        if (child_refresh_ == TSDProxyChildRefresh::StructureOnly) { return; }
        if (child_refresh_ != TSDProxyChildRefresh::OnChildTick ||
            value_ops_->source_identity_matches == nullptr)
            throw std::logic_error("TSDProxy identity reconciliation requires a matcher");

        const auto target = TSDataView{element_type_, values_.value_memory(slot)};
        if (value_ops_->source_identity_matches(*this, slot, target, source_child,
                                                value_builder_context_))
            return;

        self->refresh_child_at_slot(slot, source_time);
    }

    void TSDProxy::mark_modified(DateTime modified_time)
    {
        if (tracking_.record_modified(modified_time)) { tracking_.parent.notify_child_modified(modified_time); }
    }

    void TSDProxy::mark_structure_modified(DateTime modified_time)
    {
        static_cast<void>(key_set_tracking_.record_modified(modified_time));
    }

    void TSDProxy::record_child_modified(std::size_t slot, DateTime modified_time)
    {
        if (!has_child(slot) || !source_available()) { return; }
        // LAZY delta-window roll: updated bits describe the CURRENT window
        // only - a record at a NEWER time clears the previous window's bits
        // (they otherwise over-report the Modified surface forever). Older
        // times (replayed source history on a fresh bind) join the current
        // window instead of rebasing it.
        if (modified_time != MIN_DT && modified_time > updated_window_)
        {
            for (std::size_t index = 0; index < built_times_.size(); ++index)
            {
                if (values_.has_slot(index)) { values_.clear_updated(index); }
            }
            updated_window_ = modified_time;
        }
        auto dict = source_dict();
        if (dict.slot_live(slot)) { values_.mark_updated(slot); }
    }

    void TSDProxy::subscribe_slot_observer(SlotObserver *observer)
    {
        slot_observers_.add(observer);
    }

    void TSDProxy::unsubscribe_slot_observer(SlotObserver *observer)
    {
        slot_observers_.remove(observer);
    }

    void TSDProxy::stop() noexcept
    {
        unsubscribe_source(false);
    }

    TSDataTypeRef tsd_proxy_data_type_for(const TSValueTypeMetaData &schema,
                                          TSRoleTypeRef element_type)
    {
        const auto &context = tsd_proxy_context_for(schema, element_type, TypeRole::Data);
        return TSDataTypeRef::checked(intern_ts_type(
            schema, TypeRole::Data, *context.plan, context.dict_ops, "ts.tsd.proxy.data"));
    }

    TSOutputTypeRef tsd_proxy_output_type_for(const TSValueTypeMetaData &schema,
                                              TSRoleTypeRef element_type)
    {
        const auto &context = tsd_proxy_context_for(schema, element_type, TypeRole::Output);
        return TSOutputTypeRef::checked(intern_ts_type(
            schema, TypeRole::Output, *context.plan, context.dict_ops, "ts.tsd.proxy.output"));
    }

    void clear_tsd_proxy_contexts() noexcept
    {
        std::lock_guard<std::recursive_mutex> lock(tsd_proxy_context_mutex());
        tsd_proxy_contexts().clear();
    }

    void bind_tsd_proxy(const TSDataView       &proxy,
                        const TSDDataView      &source,
                        const TSDProxyValueOps *value_ops,
                        const void             *builder_context,
                        DateTime modified_time,
                        TSDProxyChildRefresh child_refresh)
    {
        if (!proxy.valid()) { throw std::invalid_argument("bind_tsd_proxy requires a live proxy view"); }
        if (proxy.schema() == nullptr || proxy.schema()->kind != TSTypeKind::TSD)
        {
            throw std::invalid_argument("bind_tsd_proxy requires a TSD proxy schema");
        }
        if (proxy.storage_type().plan() != &MemoryUtils::plan_for<TSDProxy>())
        {
            throw std::invalid_argument("bind_tsd_proxy requires storage backed by TSDProxy");
        }
        if (source.schema() == nullptr || source.schema()->kind != TSTypeKind::TSD ||
            source.schema()->key_type() != proxy.schema()->key_type())
        {
            throw std::invalid_argument("bind_tsd_proxy requires a source TSD with the same key schema");
        }

        const auto &dict   = proxy.as_dict();
        const auto &layout = dict.layout();
        if (!layout.element_type)
        {
            throw std::logic_error("bind_tsd_proxy requires an element binding");
        }

        auto &storage = proxy_storage(const_cast<void *>(proxy.data()));
        storage.bind(proxy.storage_type(), layout.element_type, source, value_ops,
                     builder_context, modified_time, child_refresh);
    }
}  // namespace hgraph
