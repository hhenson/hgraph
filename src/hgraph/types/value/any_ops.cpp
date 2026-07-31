#include <hgraph/types/value/any_ops.h>

#include <hgraph/config.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/utils/memory_utils.h>
#include <hgraph/types/value/value.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES
#include <nanobind/nanobind.h>
#include <hgraph/python/bridge_state.h>
namespace hgraph { namespace nb = nanobind; }
#endif

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

        std::string any_format_string(const void *, const void *memory)
        {
            const Value &value = *static_cast<const Value *>(memory);
            return value.has_value() ? value.format_string() : std::string{"None"};
        }

        DynamicStorageMetrics any_dynamic_storage_metrics(
            const void *, const void *memory) noexcept
        {
            return static_cast<const Value *>(memory)->dynamic_storage_metrics();
        }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
        nb::object any_to_python(const void *, const void *memory)
        {
            const Value &value = *static_cast<const Value *>(memory);
            if (!value.has_value()) { return nb::none(); }
            // Type-erased delegation: the BOXED value's own binding converts.
            return value.view().binding().ops_ref().to_python(value.view().data());
        }

        void any_from_python(const void *, const ValueTypeRef &, void *memory, nb::handle source)
        {
            Value &boxed = *static_cast<Value *>(memory);
            if (source.is_none())
            {
                boxed = Value{};
                return;
            }
            // Schema-free INFERENCE lives in the module (a dispatch on
            // python types); it installs this hook at import.
            const auto slot = python_bridge::py_infer_value_slot();
            if (slot == nullptr)
            {
                throw std::logic_error("Any::from_python requires the python module's inference hook");
            }
            Value inferred = reinterpret_cast<Value (*)(nb::handle)>(slot)(source);
            // Schema-free Python inference may itself choose the public Any
            // type (opaque objects and heterogeneous containers). The storage
            // here is already the EMBEDDED value of an Any box, so retain its
            // concrete content rather than creating Any<Any<T>>.
            if (inferred.view().is_any())
            {
                const ValueView contained = inferred.as_any().get();
                boxed = contained.valid() ? Value{contained} : Value{};
            }
            else { boxed = std::move(inferred); }
        }
#endif
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
#if HGRAPH_ENABLE_PYTHON_USER_NODES
            .to_python_impl = &any_to_python,
            .from_python_impl = &any_from_python,
            .to_python_buffer_impl = nullptr,
#endif
            .format_string_impl = &any_format_string,
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
        return intern_value_type(meta, MemoryUtils::plan_for<Value>(), any_ops());
    }
}  // namespace hgraph
