#include <hgraph/types/value/any_ops.h>

#include <hgraph/config.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value.h>

#include <hgraph/types/python_ops.h>

#include <compare>
#include <cstddef>
#include <string>

namespace hgraph
{
    namespace
    {
        // The Any storage is an embedded owning Value; the ops below interpret
        // `memory` as that Value and delegate to it, with an explicit empty state.

        std::size_t any_hash(const void *, const void *memory)
        {
            const Value &value = *static_cast<const Value *>(memory);
            return value.has_value() ? value.hash() : std::size_t{0};
        }

        bool any_equals(const void *, const void *lhs, const void *rhs) noexcept
        {
            const Value &a = *static_cast<const Value *>(lhs);
            const Value &b = *static_cast<const Value *>(rhs);
            if (a.has_value() != b.has_value()) { return false; }
            if (!a.has_value()) { return true; }
            return a.equals(b);
        }

        std::partial_ordering any_compare(const void *, const void *lhs, const void *rhs) noexcept
        {
            const Value &a = *static_cast<const Value *>(lhs);
            const Value &b = *static_cast<const Value *>(rhs);
            if (a.has_value() != b.has_value())
            {
                return a.has_value() ? std::partial_ordering::greater : std::partial_ordering::less;
            }
            if (!a.has_value()) { return std::partial_ordering::equivalent; }
            return a.compare(b);
        }

        std::string any_to_string(const void *, const void *memory)
        {
            const Value &value = *static_cast<const Value *>(memory);
            return value.has_value() ? value.to_string() : std::string{"None"};
        }

        DynamicStorageMetrics any_dynamic_storage_metrics(
            const void *, const void *memory) noexcept
        {
            return static_cast<const Value *>(memory)->dynamic_storage_metrics();
        }

        bool any_accepts_source(const void *, ValueTypeRef binding,
                                ValueTypeRef source) noexcept
        {
            return binding && source;
        }

        bool any_storage_accepts_source(const void *, ValueTypeRef binding,
                                        ValueTypeRef source) noexcept
        {
            return binding && source && binding.plan() == source.plan();
        }

        void any_copy_assign_from(const void *, ValueTypeRef, void *dst,
                                  ValueTypeRef source, const void *src)
        {
            Value &target = *static_cast<Value *>(dst);
            if (source.schema()->value_kind() == ValueTypeKind::Any)
            {
                // Any-like storage already contains a Value. Copy that value
                // directly rather than wrapping an Any inside another Any.
                target = *static_cast<const Value *>(src);
                return;
            }
            target = Value{ValueView{source, src}};
        }

        void any_move_assign_from(const void *, ValueTypeRef, void *dst,
                                  ValueTypeRef source, void *src)
        {
            Value &target = *static_cast<Value *>(dst);
            if (source.schema()->value_kind() == ValueTypeKind::Any)
            {
                target = std::move(*static_cast<Value *>(src));
                return;
            }

            // Materialise the source's natural owning representation, then
            // move into it through that representation's erased assignment
            // contract. This handles graph-local views as well as atomics.
            Value boxed{source};
            auto  boxed_view = boxed.begin_mutation();
            boxed.binding().ops_ref().move_assign_from(
                boxed.binding(), boxed_view.mutable_data(), source, src);
            target = std::move(boxed);
        }


        const ValueOps &constrained_any_ops() noexcept
        {
            static const ValueOps ops = [] {
                ValueOps result = any_ops();
                result.accepts_source_impl = &any_storage_accepts_source;
                return result;
            }();
            return ops;
        }

        const ValueOps &json_any_ops() noexcept
        {
            static const ValueOps ops = [] {
                ValueOps result = constrained_any_ops();
                result.to_python_impl   = &python_ops_detail::forwarder<&PythonOps::Any::json_to_python>::call;
                result.from_python_impl = &python_ops_detail::forwarder<&PythonOps::Any::json_from_python>::call;
                return result;
            }();
            return ops;
        }
    }  // namespace

    const ValueOps &any_ops() noexcept
    {
        static const ValueOps ops{
            .kind = ValueOpsKind::Base,
            .context = nullptr,
            .allows_mutation = true,
            .hash_impl = &any_hash,
            .equals_impl = &any_equals,
            .compare_impl = &any_compare,
            .to_string_impl = &any_to_string,
            // Python conversion resolves through the registered provider
            // (RFC 0035); the bridge unit holds the Any conversions.
            .to_python_impl = &python_ops_detail::forwarder<&PythonOps::Any::to_python>::call,
            .from_python_impl = &python_ops_detail::forwarder<&PythonOps::Any::from_python>::call,
            .to_python_buffer_impl = nullptr,
            .accepts_source_impl = &any_accepts_source,
            .copy_assign_from_impl = &any_copy_assign_from,
            .move_assign_from_impl = &any_move_assign_from,
            .dynamic_storage_metrics_impl = &any_dynamic_storage_metrics,
        };
        return ops;
    }

    ValueTypeRef any_type()
    {
        const ValueTypeMetaData *meta = TypeRegistry::instance().any();
        return intern_value_type(*meta, MemoryUtils::plan_for<Value>(), any_ops());
    }

    ValueTypeRef any_type(const ValueTypeMetaData &meta)
    {
        auto &registry = TypeRegistry::instance();
        const ValueOps &ops = &meta == registry.json() ? json_any_ops() :
                              &meta == registry.any() ? any_ops() : constrained_any_ops();
        return intern_value_type(meta, MemoryUtils::plan_for<Value>(), ops);
    }
}  // namespace hgraph
