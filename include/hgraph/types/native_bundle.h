#ifndef HGRAPH_TYPES_NATIVE_BUNDLE_H
#define HGRAPH_TYPES_NATIVE_BUNDLE_H

#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value_builder.h>

namespace hgraph
{
    template <NativeBundleValue T>
    Value::Value(const T &value)
    {
        using Fields = static_schema_detail::native_bundle_fields<T>;
        BundleBuilder builder{ValuePlanFactory::instance().type_for(scalar_descriptor<T>::value_meta())};
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            (builder.set(I, Value{value.*std::get<I>(Fields::members)}), ...);
        }(typename Fields::indices{});
        *this = builder.build();
    }

    template <NativeBundleValue T>
    void ValueView::set(const T &value)
    {
        if (schema() != scalar_descriptor<T>::value_meta()) {
            throw std::invalid_argument("set requires the mapped nominal bundle");
        }
        void *destination = mutable_data();
        const Value source{value};
        binding().ops_ref().copy_assign_from(binding(), destination, source.binding(), source.view().data());
    }

    template <NativeBundleValue T>
    T ValueView::checked_as() const
    {
        if (!valid() || schema() != scalar_descriptor<T>::value_meta()) {
            throw std::logic_error("checked_as<T> requires a live value of the mapped nominal bundle");
        }
        using Fields = static_schema_detail::native_bundle_fields<T>;
        const auto bundle = as_bundle();
        T value{};
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            ((value.*std::get<I>(Fields::members) =
                  bundle.at(I).template checked_as<typename Fields::template member_type<I>>()), ...);
        }(typename Fields::indices{});
        return value;
    }
}

#endif
