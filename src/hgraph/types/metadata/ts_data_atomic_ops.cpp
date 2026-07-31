#include <hgraph/types/metadata/ts_data_plan_factory_detail.h>

#include <hgraph/types/utils/intern_table.h>
#include <hgraph/types/value/value.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <hgraph/python/bridge_state.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#endif

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace hgraph::ts_data_plan_factory_detail
{
    struct AtomicTSDataOpsEntry
    {
        TSDataLayout layout{};
        TSDataOps    ops{};
        ValueStorageVariant storage{ValueStorageVariant::Native};
        std::size_t python_value_offset{ValueStorageSelection::no_offset};

        AtomicTSDataOpsEntry(TSTypeKind kind, const ValueTypeRef &value_binding,
                             const ValueTypeRef &delta_binding,
                             std::size_t value_offset, std::size_t tracking_offset,
                             ValueStorageVariant value_storage,
                             std::size_t python_offset)
            : storage(value_storage),
              python_value_offset(python_offset)
        {
            layout = TSDataLayout{
                .value_binding   = value_binding,
                .delta_binding   = delta_binding,
                .value_offset    = value_offset,
                .tracking_offset = tracking_offset,
            };

            ops = TSDataOps{
                .context                   = &layout,
                .kind                      = kind,
                .allows_mutation           = true,
                .layout_impl               = &atomic_layout,
                .tracking_impl             = &atomic_tracking,
                .mutable_tracking_impl     = &atomic_mutable_tracking,
                .has_current_value_impl    = &atomic_has_current_value,
                .all_valid_impl            = &atomic_has_current_value,
                .value_memory_impl         = &atomic_value_memory,
                .mutable_value_memory_impl = &atomic_mutable_value_memory,
                .delta_memory_impl         = &atomic_delta_memory,
                .mutable_delta_memory_impl = &atomic_mutable_delta_memory,
                .copy_value_from_impl =
                    value_storage ==
                            ValueStorageVariant::NativeWithPythonCache
                        ? &atomic_copy_value_from<true>
                        : &atomic_copy_value_from<false>,
                .move_value_from_impl =
                    value_storage ==
                            ValueStorageVariant::NativeWithPythonCache
                        ? &atomic_move_value_from<true>
                        : &atomic_move_value_from<false>,
                // REF data is whole-value like TS: the delta IS the carried
                // reference value (the opaque-reference ruling - map_ over
                // REF-returning functions forwards elements through these).
                .empty_delta_impl          = &ts_data_detail::empty_delta_atomic,
                .capture_delta_impl        = kind == TSTypeKind::SIGNAL ? &ts_data_detail::capture_delta_signal
                                                                        : &ts_data_detail::capture_delta_ts,
                .delta_has_effect_impl     = &ts_data_detail::delta_has_effect_atomic,
                .apply_delta_impl          = &ts_data_detail::apply_delta_atomic,
#if HGRAPH_ENABLE_PYTHON_USER_NODES
                .from_python_impl =
                    value_storage == ValueStorageVariant::PythonOnly
                        ? &atomic_from_python<
                              ValueStorageVariant::PythonOnly>
                    : value_storage ==
                              ValueStorageVariant::NativeWithPythonCache
                        ? &atomic_from_python<
                              ValueStorageVariant::NativeWithPythonCache>
                        : &atomic_from_python<ValueStorageVariant::Native>,
                .to_python_impl =
                    value_storage == ValueStorageVariant::PythonOnly
                        ? &atomic_to_python<ValueStorageVariant::PythonOnly>
                    : value_storage ==
                              ValueStorageVariant::NativeWithPythonCache
                        ? &atomic_to_python<
                              ValueStorageVariant::NativeWithPythonCache>
                        : &atomic_to_python<ValueStorageVariant::Native>,
                .delta_to_python_impl =
                    value_storage == ValueStorageVariant::PythonOnly
                        ? &atomic_delta_to_python<
                              ValueStorageVariant::PythonOnly>
                    : value_storage ==
                              ValueStorageVariant::NativeWithPythonCache
                        ? &atomic_delta_to_python<
                              ValueStorageVariant::NativeWithPythonCache>
                        : &atomic_delta_to_python<
                              ValueStorageVariant::Native>,
#endif
            };
        }

        AtomicTSDataOpsEntry(const AtomicTSDataOpsEntry &other) : layout(other.layout), ops(other.ops)
        {
            storage = other.storage;
            python_value_offset = other.python_value_offset;
            ops.context = &layout;
        }

        AtomicTSDataOpsEntry(AtomicTSDataOpsEntry &&other) noexcept : layout(other.layout), ops(other.ops)
        {
            storage = other.storage;
            python_value_offset = other.python_value_offset;
            ops.context = &layout;
        }

        [[nodiscard]] static const TSDataLayout *atomic_layout(const void *context) noexcept
        {
            return static_cast<const TSDataLayout *>(context);
        }

        [[nodiscard]] static const AtomicTSDataOpsEntry &entry(
            const void *context) noexcept
        {
            // ``layout`` is the first member of this standard-layout cache
            // entry, so its address is pointer-interconvertible with the
            // enclosing entry. Native operations continue to receive the
            // exact TSDataLayout context used before Python-aware storage.
            return *reinterpret_cast<const AtomicTSDataOpsEntry *>(context);
        }

        [[nodiscard]] static const void *advance(const void *memory, std::size_t offset) noexcept
        {
            return static_cast<const std::byte *>(memory) + offset;
        }

        [[nodiscard]] static void *advance(void *memory, std::size_t offset) noexcept
        {
            return static_cast<std::byte *>(memory) + offset;
        }

        [[nodiscard]] static const TSDataTracking *atomic_tracking(const void *context, const void *memory) noexcept
        {
            const auto *layout = atomic_layout(context);
            return MemoryUtils::cast<TSDataTracking>(advance(memory, layout->tracking_offset));
        }

        [[nodiscard]] static TSDataTracking *atomic_mutable_tracking(const void *context, void *memory) noexcept
        {
            const auto *layout = atomic_layout(context);
            return MemoryUtils::cast<TSDataTracking>(advance(memory, layout->tracking_offset));
        }

        [[nodiscard]] static bool atomic_has_current_value(const void *context, const void *memory) noexcept
        {
            return atomic_tracking(context, memory)->last_modified_time != MIN_DT;
        }

        [[nodiscard]] static const void *atomic_value_memory(const void *context, const void *memory) noexcept
        {
            const auto *layout = atomic_layout(context);
            return advance(memory, layout->value_offset);
        }

        [[nodiscard]] static void *atomic_mutable_value_memory(const void *context, void *memory) noexcept
        {
            const auto *layout = atomic_layout(context);
            return advance(memory, layout->value_offset);
        }

        [[nodiscard]] static const void *atomic_delta_memory(const void *context, const void *memory) noexcept
        {
            return atomic_value_memory(context, memory);
        }

        [[nodiscard]] static void *atomic_mutable_delta_memory(const void *context, void *memory) noexcept
        {
            return atomic_mutable_value_memory(context, memory);
        }

        /** Same binding, or distinct schema identities over ONE layout
            (variadic tuple vs list: the PLAN is the layout contract; the ops
            may be per-binding variants, e.g. tuple-shaped python read-back). */
        [[nodiscard]] static bool atomic_value_binding_compatible(ValueTypeRef source,
                                                                  ValueTypeRef bound) noexcept
        {
            if (source == bound) { return true; }
            if (source == nullptr || bound == nullptr) { return false; }
            if (bound.ops_ref().accepts_source(bound, source)) { return true; }
            return source.plan() == bound.plan();
        }

        template <bool InvalidatePythonCache>
        [[nodiscard]] static bool atomic_copy_value_from(
            const void *context, void *memory, const ValueView &source,
            DateTime modified_time)
        {
            if (memory == nullptr)
            {
                throw std::logic_error("TSData atomic copy requires live TSData memory");
            }
            if (!source.has_value())
            {
                throw std::invalid_argument("TSData atomic copy requires a live source value");
            }
            if (modified_time == MIN_DT)
            {
                throw std::invalid_argument("TSData atomic copy requires a concrete evaluation time");
            }

            const auto *layout = atomic_layout(context);
            if (!atomic_value_binding_compatible(source.binding(), layout->value_binding))
            {
                throw std::invalid_argument(
                    "TSData atomic copy cannot assign source '" +
                    std::string{source.schema() != nullptr ? source.schema()->name() : "<unbound>"} +
                    "' to bound value '" +
                    std::string{layout->value_binding.schema() != nullptr
                                    ? layout->value_binding.schema()->name()
                                    : "<unbound>"} + "'");
            }

            const auto *tracking       = atomic_tracking(context, memory);
            const bool  first_for_time = tracking->last_modified_time != modified_time;

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            if constexpr (InvalidatePythonCache)
            {
                invalidate_python_value(context, memory);
            }
#endif
            layout->value_binding.ops_ref().copy_assign_from(
                layout->value_binding,
                atomic_mutable_value_memory(context, memory),
                source.binding(),
                source.data());
            return first_for_time;
        }

        template <bool InvalidatePythonCache>
        [[nodiscard]] static bool atomic_move_value_from(
            const void *context, void *memory, ValueView source,
            DateTime modified_time)
        {
            if (memory == nullptr)
            {
                throw std::logic_error("TSData atomic move requires live TSData memory");
            }
            if (!source.has_value())
            {
                throw std::invalid_argument("TSData atomic move requires a live source value");
            }
            if (!source.writable_payload())
            {
                throw std::invalid_argument("TSData atomic move requires writable source storage");
            }
            if (modified_time == MIN_DT)
            {
                throw std::invalid_argument("TSData atomic move requires a concrete evaluation time");
            }

            const auto *layout = atomic_layout(context);
            if (!atomic_value_binding_compatible(source.binding(), layout->value_binding))
            {
                throw std::invalid_argument("TSData atomic move requires the bound value schema and plan");
            }

            const auto *tracking       = atomic_tracking(context, memory);
            const bool  first_for_time = tracking->last_modified_time != modified_time;

#if HGRAPH_ENABLE_PYTHON_USER_NODES
            if constexpr (InvalidatePythonCache)
            {
                invalidate_python_value(context, memory);
            }
#endif
            layout->value_binding.ops_ref().move_assign_from(
                layout->value_binding,
                atomic_mutable_value_memory(context, memory),
                source.binding(),
                const_cast<void *>(source.data()));
            return first_for_time;
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        [[nodiscard]] static python_bridge::PythonValueHolder *
        python_value(const void *context, void *memory) noexcept
        {
            const auto &self = entry(context);
            if (self.python_value_offset == ValueStorageSelection::no_offset)
            {
                return nullptr;
            }
            return MemoryUtils::cast<python_bridge::PythonValueHolder>(
                advance(memory, self.python_value_offset));
        }

        [[nodiscard]] static const python_bridge::PythonValueHolder *
        python_value(const void *context, const void *memory) noexcept
        {
            return python_value(context, const_cast<void *>(memory));
        }

        static void invalidate_python_value(const void *context,
                                            void *memory) noexcept
        {
            const auto &self = entry(context);
            if (self.storage != ValueStorageVariant::NativeWithPythonCache)
            {
                return;
            }
            if (auto *cached = python_value(context, memory);
                cached != nullptr)
            {
                cached->clear();
            }
        }

        template <ValueStorageVariant Storage>
        [[nodiscard]] static bool atomic_from_python(
            const void *context, void *memory, nb::handle source,
            DateTime modified_time)
        {
            if (memory == nullptr)
            {
                throw std::logic_error("TSData atomic from_python requires live TSData memory");
            }
            if (source.is_none())
            {
                throw std::invalid_argument("TSData atomic from_python requires a non-None source");
            }
            if (modified_time == MIN_DT)
            {
                throw std::invalid_argument("TSData atomic from_python requires a concrete evaluation time");
            }

            const auto *layout = atomic_layout(context);
            const auto *tracking = atomic_tracking(context, memory);
            const bool  first_for_time = tracking->last_modified_time != modified_time;
            if constexpr (Storage == ValueStorageVariant::PythonOnly)
            {
                layout->value_binding.ops_ref().from_python(
                    layout->value_binding,
                    atomic_mutable_value_memory(context, memory), source);
                return first_for_time;
            }
            if constexpr (Storage ==
                          ValueStorageVariant::NativeWithPythonCache)
            {
                nb::object retained =
                    ValuePlanFactory::instance().prepare_python_storage_value(
                        layout->value_binding.schema(), source);
                layout->value_binding.ops_ref().from_python(
                    layout->value_binding,
                    atomic_mutable_value_memory(context, memory), source);
                python_value(context, memory)->set(retained);
            }
            else
            {
                layout->value_binding.ops_ref().from_python(
                    layout->value_binding,
                    atomic_mutable_value_memory(context, memory), source);
            }
            return first_for_time;
        }

        template <ValueStorageVariant Storage>
        [[nodiscard]] static nb::object atomic_to_python(
            const void *context, const void *memory)
        {
            const auto *layout = atomic_layout(context);
            if constexpr (Storage !=
                          ValueStorageVariant::NativeWithPythonCache)
            {
                return layout->value_binding.ops_ref().to_python(
                    atomic_value_memory(context, memory));
            }
            else
            {
                if (const auto *retained = python_value(context, memory);
                    retained != nullptr && retained->has_value())
                {
                    return retained->get();
                }
                nb::object converted =
                    layout->value_binding.ops_ref().to_python(
                        atomic_value_memory(context, memory));
                if (auto *cached =
                        python_value(context, const_cast<void *>(memory));
                    cached != nullptr)
                {
                    cached->set(converted);
                }
                return converted;
            }
        }

        template <ValueStorageVariant Storage>
        [[nodiscard]] static nb::object atomic_delta_to_python(
            const void *context, const void *memory,
            DateTime evaluation_time)
        {
            if (atomic_tracking(context, memory)->last_modified_time != evaluation_time) { return nb::none(); }
            if constexpr (Storage == ValueStorageVariant::Native)
            {
                const auto *layout = atomic_layout(context);
                return layout->delta_binding.ops_ref().to_python(
                    atomic_delta_memory(context, memory));
            }
            else
            {
                return atomic_to_python<Storage>(context, memory);
            }
        }
#endif
    };

    static_assert(std::is_standard_layout_v<AtomicTSDataOpsEntry>);
    static_assert(offsetof(AtomicTSDataOpsEntry, layout) == 0);

    struct AtomicTSDataOpsKey
    {
        ValueTypeRef value_binding{nullptr};
        ValueTypeRef delta_binding{nullptr};
        const MemoryUtils::StoragePlan *plan{nullptr};
        TSTypeKind                      kind{TSTypeKind::TS};
        std::size_t                     value_offset{0};
        std::size_t                     tracking_offset{0};
        ValueStorageVariant             storage{ValueStorageVariant::Native};
        std::size_t python_value_offset{ValueStorageSelection::no_offset};

        [[nodiscard]] bool operator==(const AtomicTSDataOpsKey &) const noexcept = default;
    };

    struct AtomicTSDataOpsKeyHash
    {
        [[nodiscard]] std::size_t operator()(const AtomicTSDataOpsKey &key) const noexcept
        {
            std::size_t seed = std::hash<ValueTypeRef>{}(key.value_binding);
            seed ^= std::hash<ValueTypeRef>{}(key.delta_binding) + 0x9e3779b97f4a7c15ULL + (seed << 6U) +
                    (seed >> 2U);
            seed ^= std::hash<const MemoryUtils::StoragePlan *>{}(key.plan) + 0x9e3779b97f4a7c15ULL + (seed << 6U) +
                    (seed >> 2U);
            seed ^= std::hash<std::uint8_t>{}(static_cast<std::uint8_t>(key.kind)) + 0x9e3779b97f4a7c15ULL +
                    (seed << 6U) + (seed >> 2U);
            seed ^= std::hash<std::size_t>{}(key.value_offset) + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            seed ^= std::hash<std::size_t>{}(key.tracking_offset) + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            seed ^= std::hash<std::uint8_t>{}(static_cast<std::uint8_t>(key.storage)) +
                    0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            seed ^= std::hash<std::size_t>{}(key.python_value_offset) +
                    0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
            return seed;
        }
    };

    [[nodiscard]] InternTable<AtomicTSDataOpsKey, AtomicTSDataOpsEntry, AtomicTSDataOpsKeyHash> &
    atomic_ts_data_ops_cache() noexcept
    {
        static InternTable<AtomicTSDataOpsKey, AtomicTSDataOpsEntry, AtomicTSDataOpsKeyHash> cache;
        return cache;
    }

    [[nodiscard]] const TSDataOps &atomic_ts_data_ops(TSTypeKind                     kind,
                                                      const ValueTypeRef         &value_binding,
                                                      const ValueTypeRef         &delta_binding,
                                                      const MemoryUtils::StoragePlan &plan, std::size_t value_offset,
                                                      std::size_t tracking_offset,
                                                      ValueStorageVariant storage,
                                                      std::size_t python_value_offset)
    {
        return atomic_ts_data_ops_cache()
            .emplace(AtomicTSDataOpsKey{value_binding, delta_binding, &plan, kind,
                                       value_offset, tracking_offset, storage,
                                       python_value_offset},
                     kind, value_binding, delta_binding, value_offset,
                     tracking_offset, storage, python_value_offset)
            .ops;
    }

    void clear_atomic_ts_data_ops() noexcept
    {
        atomic_ts_data_ops_cache().clear();
    }
} // namespace hgraph::ts_data_plan_factory_detail
