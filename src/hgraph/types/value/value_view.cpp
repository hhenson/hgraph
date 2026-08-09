#include <hgraph/types/value/specialized_views.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <fmt/format.h>
#include <stdexcept>
#include <string_view>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] const char *schema_name(const ValueTypeMetaData *schema) noexcept
        {
            if (schema == nullptr || schema->header.label == nullptr) { return "<unnamed>"; }
            return schema->header.label;
        }

        [[nodiscard]] const ValueTypeMetaData *structural_schema(const ValueTypeMetaData *schema) noexcept
        {
            if (schema != nullptr && schema->try_value_kind() == ValueTypeKind::Bundle &&
                schema->wrapped_un_named != nullptr)
            {
                return schema->wrapped_un_named;
            }
            return schema;
        }

        [[nodiscard]] bool value_schema_equivalent(const ValueTypeMetaData *lhs,
                                                   const ValueTypeMetaData *rhs) noexcept
        {
            if (lhs == rhs) { return true; }
            if (lhs == nullptr || rhs == nullptr) { return false; }
            // Owned[T] borrows Bundle-shaped indexed operations, but is an
            // indirection schema rather than T's structural twin.
            if (lhs->is_owned() || rhs->is_owned()) { return false; }

            // A named bundle is structurally compatible with its anonymous
            // twin, but two different named bundles retain nominal identity.
            if (lhs->try_value_kind() == ValueTypeKind::Bundle &&
                rhs->try_value_kind() == ValueTypeKind::Bundle)
            {
                if (lhs->is_named_bundle() && rhs->is_named_bundle()) { return false; }
                if (lhs->is_named_bundle()) { lhs = lhs->wrapped_un_named; }
                if (rhs->is_named_bundle()) { rhs = rhs->wrapped_un_named; }
                if (lhs == rhs) { return true; }
            }

            const auto lhs_kind = lhs->try_value_kind();
            const auto rhs_kind = rhs->try_value_kind();
            if (!lhs_kind.has_value() || lhs_kind != rhs_kind) { return false; }

            switch (*lhs_kind)
            {
                case ValueTypeKind::Atomic:
                    return false;

                case ValueTypeKind::Tuple:
                case ValueTypeKind::Bundle:
                    if (lhs->field_count != rhs->field_count) { return false; }
                    for (std::size_t index = 0; index < lhs->field_count; ++index)
                    {
                        const std::string_view lhs_name = lhs->fields[index].name != nullptr
                                                              ? std::string_view{lhs->fields[index].name}
                                                              : std::string_view{};
                        const std::string_view rhs_name = rhs->fields[index].name != nullptr
                                                              ? std::string_view{rhs->fields[index].name}
                                                              : std::string_view{};
                        if (lhs_name != rhs_name) { return false; }
                        if (!value_schema_equivalent(lhs->fields[index].type, rhs->fields[index].type))
                        {
                            return false;
                        }
                    }
                    return true;

                case ValueTypeKind::List:
                case ValueTypeKind::Set:
                case ValueTypeKind::CyclicBuffer:
                case ValueTypeKind::Queue:
                    return value_schema_equivalent(lhs->element_type, rhs->element_type);

                case ValueTypeKind::Map:
                    return value_schema_equivalent(lhs->key_type, rhs->key_type) &&
                           value_schema_equivalent(lhs->element_type, rhs->element_type);

                case ValueTypeKind::Any:
                    // Unconstrained and singleton: any two Any schemas are equivalent.
                    return true;
            }

            return false;
        }

        [[nodiscard]] bool ordered_indexed_kind(ValueTypeKind kind) noexcept
        {
            return kind == ValueTypeKind::Tuple || kind == ValueTypeKind::Bundle ||
                   kind == ValueTypeKind::List || kind == ValueTypeKind::CyclicBuffer ||
                   kind == ValueTypeKind::Queue;
        }

        [[nodiscard]] bool semantic_indexed_equals(const ValueView &lhs, const ValueView &rhs)
        {
            const auto a    = lhs.as_indexed_view();
            const auto b    = rhs.as_indexed_view();
            const auto size = a.size();
            if (size != b.size()) { return false; }
            for (std::size_t index = 0; index < size; ++index)
            {
                if (!a.at(index).equals(b.at(index))) { return false; }
            }
            return true;
        }

        [[nodiscard]] std::partial_ordering semantic_indexed_compare(const ValueView &lhs, const ValueView &rhs)
        {
            const auto a    = lhs.as_indexed_view();
            const auto b    = rhs.as_indexed_view();
            const auto size = std::min(a.size(), b.size());
            for (std::size_t index = 0; index < size; ++index)
            {
                const auto child_order = a.at(index).compare(b.at(index));
                if (child_order != 0) { return child_order; }
            }
            if (a.size() < b.size()) { return std::partial_ordering::less; }
            if (a.size() > b.size()) { return std::partial_ordering::greater; }
            return std::partial_ordering::equivalent;
        }

        [[nodiscard]] ValueTypeRef first_indexed_element_binding(const ValueView &view)
        {
            const auto indexed = view.as_indexed_view();
            return indexed.size() == 0 ? nullptr : indexed.at(0).binding();
        }

        [[nodiscard]] bool semantic_set_equals(const ValueView &lhs, const ValueView &rhs)
        {
            const auto a = lhs.as_set();
            const auto b = rhs.as_set();
            if (a.size() != b.size()) { return false; }
            if (a.size() == 0) { return true; }
            if (first_indexed_element_binding(lhs) != first_indexed_element_binding(rhs)) { return false; }

            for (const auto key : a)
            {
                if (!b.contains(key)) { return false; }
            }
            return true;
        }

        [[nodiscard]] std::partial_ordering semantic_set_compare(const ValueView &lhs, const ValueView &rhs)
        {
            const auto a = lhs.as_set();
            const auto b = rhs.as_set();
            if (a.size() < b.size()) { return std::partial_ordering::less; }
            if (a.size() > b.size()) { return std::partial_ordering::greater; }
            return semantic_set_equals(lhs, rhs) ? std::partial_ordering::equivalent
                                                 : std::partial_ordering::unordered;
        }

        [[nodiscard]] bool semantic_map_equals(const ValueView &lhs, const ValueView &rhs)
        {
            const auto a = lhs.as_map();
            const auto b = rhs.as_map();
            if (a.size() != b.size()) { return false; }
            if (a.size() == 0) { return true; }
            if (first_indexed_element_binding(lhs) != first_indexed_element_binding(rhs)) { return false; }

            for (const auto entry : a)
            {
                const auto &key   = entry.first;
                const auto &value = entry.second;
                if (!b.contains(key)) { return false; }
                if (!value.equals(b.at(key))) { return false; }
            }
            return true;
        }

        [[nodiscard]] bool semantic_equals(const ValueView &lhs, const ValueView &rhs)
        {
            const auto *schema = structural_schema(lhs.schema());
            if (schema == nullptr) { return false; }

            if (ordered_indexed_kind(schema->value_kind())) { return semantic_indexed_equals(lhs, rhs); }
            if (schema->value_kind() == ValueTypeKind::Set) { return semantic_set_equals(lhs, rhs); }
            if (schema->value_kind() == ValueTypeKind::Map) { return semantic_map_equals(lhs, rhs); }
            return false;
        }

        [[nodiscard]] std::partial_ordering semantic_compare(const ValueView &lhs, const ValueView &rhs)
        {
            const auto *schema = structural_schema(lhs.schema());
            if (schema == nullptr) { return std::partial_ordering::unordered; }

            if (ordered_indexed_kind(schema->value_kind())) { return semantic_indexed_compare(lhs, rhs); }

            if (schema->value_kind() == ValueTypeKind::Set) { return semantic_set_compare(lhs, rhs); }

            if (schema->value_kind() == ValueTypeKind::Map)
            {
                return semantic_equals(lhs, rhs) ? std::partial_ordering::equivalent
                                                 : std::partial_ordering::unordered;
            }

            return std::partial_ordering::unordered;
        }
    }  // namespace

    Value ValueView::clone() const
    {
        if (!binding()) { throw std::logic_error("ValueView::clone requires a bound view"); }
        return Value{*this};
    }

    void ValueView::copy_from(const ValueView &other)
    {
        if (!has_value() || !other.has_value())
        {
            throw std::runtime_error("ValueView::copy_from requires non-empty views");
        }
        if (!mutable_payload()) { throw std::logic_error("ValueView::copy_from requires a mutable destination"); }
        if (data() == other.data()) { return; }
        if (schema() != other.schema())
        {
            throw std::invalid_argument(fmt::format("ValueView::copy_from requires matching schemas: {} != {}",
                                                    schema_name(schema()),
                                                    schema_name(other.schema())));
        }
        const auto bound       = binding();
        const auto other_bound = other.binding();
        if (!bound || !other_bound)
        {
            throw std::invalid_argument("ValueView::copy_from requires matching storage plans");
        }

        const auto &other_ops = other_bound.ops_ref();
        const auto copy_type = other_ops.owning_type(other_bound);
        if (bound.plan() != copy_type.plan())
        {
            throw std::invalid_argument("ValueView::copy_from requires matching storage plans");
        }

        other_ops.copy_assign_view(bound, mutable_data(), other.data());
    }

    bool ValueView::try_copy_from(const ValueView &other)
    {
        if (!has_value() || !other.has_value()) { return false; }
        if (!mutable_payload()) { return false; }
        if (data() == other.data()) { return true; }
        if (schema() != other.schema()) { return false; }
        const auto bound       = binding();
        const auto other_bound = other.binding();
        if (!bound || !other_bound)
        {
            return false;
        }

        const auto &other_ops = other_bound.ops_ref();
        const auto copy_type = other_ops.owning_type(other_bound);
        if (bound.plan() != copy_type.plan()) { return false; }
        other_ops.copy_assign_view(bound, mutable_data(), other.data());
        return true;
    }

    bool ValueView::equals(const ValueView &other) const
    {
        const auto bound       = binding();
        const auto other_bound = other.binding();
        if (!bound || !other_bound)
        {
            return !bound && !other_bound && data() == other.data();
        }
        if (data() == nullptr || other.data() == nullptr)
        {
            if (data() != other.data()) { return false; }
            return bound == other_bound || value_schema_equivalent(schema(), other.schema());
        }

        if (bound == other_bound) { return bound.ops_ref().equals(data(), other.data()); }

        auto lhs_concrete = concrete();
        auto rhs_concrete = other.concrete();
        if (lhs_concrete.binding() != bound || rhs_concrete.binding() != other_bound)
        {
            return lhs_concrete.equals(rhs_concrete);
        }
        if (!value_schema_equivalent(schema(), other.schema())) { return false; }
        return semantic_equals(*this, other);
    }

    std::partial_ordering ValueView::compare(const ValueView &other) const noexcept
    {
        const auto bound       = binding();
        const auto other_bound = other.binding();
        if (const auto order = value_ops_detail::null_order(bound, other_bound)) { return *order; }
        if (data() == nullptr || other.data() == nullptr)
        {
            if (data() != other.data()) { return data() == nullptr ? std::partial_ordering::less
                                                                  : std::partial_ordering::greater; }
            if (bound == other_bound || value_schema_equivalent(schema(), other.schema()))
            {
                return std::partial_ordering::equivalent;
            }
            return std::partial_ordering::unordered;
        }

        return fallback_on_exception(std::partial_ordering::unordered, [&]() {
            if (bound == other_bound) { return bound.ops_ref().compare(data(), other.data()); }
            if (!value_schema_equivalent(schema(), other.schema())) { return std::partial_ordering::unordered; }
            return semantic_compare(*this, other);
        });
    }

#if HGRAPH_ENABLE_PYTHON_USER_NODES
    nb::object ValueView::to_python() const
    {
        if (!valid()) { throw std::runtime_error("ValueView::to_python requires a non-empty view"); }
        return binding().ops_ref().to_python(data());
    }

    void ValueView::from_python(nb::handle source)
    {
        if (!valid()) { throw std::runtime_error("ValueView::from_python requires a non-empty view"); }
        if (source.is_none())
        {
            throw std::invalid_argument("ValueView::from_python cannot reset a view from None");
        }

        const auto bound = binding();
        bound.ops_ref().from_python(bound, mutable_data(), source);
    }

    nb::object Value::to_python() const
    {
        if (!has_value()) { return nb::none(); }
        return view().to_python();
    }

    void Value::from_python(nb::handle source)
    {
        if (source.is_none())
        {
            reset();
            return;
        }
        if (!binding())
        {
            throw std::logic_error("Value::from_python requires a schema-bound Value");
        }

        const auto bound = binding();
        Value replacement{bound};
        bound.ops_ref().from_python(bound, const_cast<void *>(replacement.view().data()), source);
        *this = std::move(replacement);
    }
#endif

    // -- AnyView / MutableAnyView ----------------------------------------------
    // Defined here (not inline) because they reach into the embedded owning
    // ``Value`` that backs an ``Any`` value, and ``Value`` is complete here.

    bool AnyView::has_value() const noexcept
    {
        return static_cast<const Value *>(data())->has_value();
    }

    ValueView AnyView::get() const
    {
        if (writable_payload())
        {
            // The Any box is in writable storage, so the value it contains is too:
            // hand back a writable view. The contained value's own ops still gate
            // ``begin_mutation`` (an immutable value stays immutable; a mutable one
            // — e.g. a mutable List/Map — can be mutated in place).
            return const_cast<Value *>(static_cast<const Value *>(data()))->view();
        }
        return static_cast<const Value *>(data())->view();
    }

    const ValueTypeMetaData *AnyView::value_schema() const noexcept
    {
        return static_cast<const Value *>(data())->schema();
    }

    MutableAnyView AnyView::begin_mutation() const
    {
        return MutableAnyView{ValueView::begin_mutation()};
    }

    void MutableAnyView::set(const ValueView &value) const
    {
        *static_cast<Value *>(mutable_data()) = Value{value};
    }

    void MutableAnyView::set(const Value &value) const
    {
        *static_cast<Value *>(mutable_data()) = value;
    }

    void MutableAnyView::set(Value &&value) const
    {
        *static_cast<Value *>(mutable_data()) = std::move(value);
    }

    void MutableAnyView::clear() const
    {
        static_cast<Value *>(mutable_data())->reset();
    }
}  // namespace hgraph
