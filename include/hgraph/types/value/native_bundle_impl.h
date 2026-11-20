#ifndef HGRAPH_TYPES_VALUE_NATIVE_BUNDLE_IMPL_H
#define HGRAPH_TYPES_VALUE_NATIVE_BUNDLE_IMPL_H

#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/bundle_builder.h>

namespace hgraph
{
    namespace native_bundle_detail
    {
        template <typename Member, typename Schema>
        struct field_access
        {
            static void pack(BundleBuilder &builder, std::size_t index, const Member &value) { builder.set(index, Value{value}); }
            static decltype(auto) unbox(const ValueView &value) { return value.template checked_as<Member>(); }

            template <typename Input>
            static decltype(auto) read(const Input &input) { return input.value(); }

            template <typename Output>
            static void write(const Output &output, const Member &value) { output.set(value); }
        };

        template <typename Payload, typename Schema>
        struct field_access<std::optional<Payload>, Optional<Schema>>
        {
            using Member = std::optional<Payload>;

            static void pack(BundleBuilder &builder, std::size_t index, const Member &value)
            {
                Value boxed{builder.field_binding(index)};
                if (value) { boxed.view().as_optional().begin_mutation().emplace(Value{*value}.view()); }
                builder.set(index, std::move(boxed));
            }

            static Member from_optional(const OptionalView &value)
            {
                if (!value.has_value()) { return std::nullopt; }
                return value.value().template checked_as<Payload>();
            }

            static Member unbox(const ValueView &value) { return from_optional(value.as_optional()); }

            template <typename Input>
            static Member read(const Input &input) { return from_optional(input.value()); }

            template <typename Output>
            static void write(const Output &output, const Member &value)
            {
                if (!value) { output.set(Null); }
                else if constexpr (NativeBundleValue<Payload>) { output.set(Value{*value}.view()); }
                else { output.set(*value); }
            }
        };

        template <typename Fields, std::size_t I>
        using member_access = field_access<typename Fields::template member_type<I>, typename Fields::template field<I>::schema>;
    }

    template <NativeBundleValue T>
    Value::Value(const T &value)
    {
        using Fields = static_schema_detail::native_bundle_fields<T>;
        BundleBuilder builder{ValuePlanFactory::instance().type_for(scalar_descriptor<T>::value_meta())};
        [&]<std::size_t... I>(std::index_sequence<I...>) {
            (native_bundle_detail::member_access<Fields, I>::pack(builder, I, value.*std::get<I>(Fields::members)), ...);
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
                  native_bundle_detail::member_access<Fields, I>::unbox(bundle.at(I))), ...);
        }(typename Fields::indices{});
        return value;
    }
}

#endif
