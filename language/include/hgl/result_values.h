#ifndef HGL_RESULT_VALUES_H
#define HGL_RESULT_VALUES_H

#include <hgl/ordinary_values.h>
#include <hgraph/runtime/canonical_error.h>
#include <variant>
#include <hgraph/types/graph_wiring.h>

namespace hgl::ordinary {
    [[nodiscard]] inline const hgraph::ValueTypeMetaData *result_meta(const hgraph::ValueTypeMetaData *value,
                                                                    const hgraph::ValueTypeMetaData *error) {
        return hgraph::TypeRegistry::instance().bundle("hgl.result", "result[" + std::string{value->name()} + "," + std::string{error->name()} + "]",
            {{"tag", hgraph::scalar_descriptor<hgraph::Bool>::value_meta()}, {"value", value}, {"error", error}}, {}, false, "__type__", {value, error});
    }

    class PreparedResultPlan {
      public:
        explicit PreparedResultPlan(const hgraph::ValueTypeMetaData *schema) : value_{schema} {}
        [[nodiscard]] bool is_ok(const hgraph::ValueView &result) const { return required_scalar<hgraph::Bool>(value_.index(result, 0)); }
        [[nodiscard]] hgraph::ValueView payload(const hgraph::ValueView &result, bool success) const {
            if (is_ok(result) != success) { throw std::logic_error("result payload requires the selected variant"); }
            return value_.index(result, success ? 1 : 2);
        }
        [[nodiscard]] hgraph::Value make(bool success, const hgraph::ValueView &payload) const {
            require_payload(payload, true);
            hgraph::Value tag{success};
            std::array<std::pair<std::size_t, hgraph::ValueView>, 2> fields{
                std::pair<std::size_t, hgraph::ValueView>{0, tag.view()},
                std::pair<std::size_t, hgraph::ValueView>{success ? 1U : 2U, hgraph::ValueView{payload.binding(), payload.data()}}};
            return value_.bundle(fields);
        }
      private:
        PreparedValuePlan value_;
    };

    struct ResultSuccessPredicateTag {};
    struct ResultFailurePredicateTag {};
    [[nodiscard]] inline hgraph::WiringPortRef result_predicate(hgraph::Wiring &wiring, hgraph::WiringPortRef source, bool success) {
        auto &registry = hgraph::TypeRegistry::instance();
        const auto *input = registry.un_named_tsb({{"value", source.schema}});
        const auto *output = registry.ts(hgraph::scalar_descriptor<hgraph::Bool>::value_meta());
        hgraph::NodeTypeMetaData metadata;
        metadata.display_name = success ? "is_ok" : "is_err";
        metadata.input_schema = input;
        metadata.output_schema = output;
        const std::array inputs{source};
        const auto boolean = hgraph::ValuePlanFactory::instance().type_for(output->value_schema);
        hgraph::NodeCallbacks callbacks;
        callbacks.evaluate = [success, boolean](const hgraph::NodeView &node, hgraph::DateTime time) {
            const auto value = node.input(time).indexed_child_at(0).value();
            const bool selected = value.as_bundle().at(0).checked_as<hgraph::Bool>();
            const bool flag = success ? selected : !selected;
            auto output = node.output(time);
            auto mutation = output.begin_mutation(time);
            static_cast<void>(mutation.copy_value_from(hgraph::ValueView{boolean, &flag}));
        };
        return wiring.add_node(success ? std::type_index(typeid(ResultSuccessPredicateTag)) : std::type_index(typeid(ResultFailurePredicateTag)),
            hgraph::NodeBuilder::native(std::move(metadata), std::move(callbacks)), inputs, hgraph::Value{});
    }

    // Private compiled control transfer; native providers return Result data.
    struct PropagatedResult { hgraph::Value payload; };

    template <typename T> struct NativePayload { using type = T; };
    template <typename K, typename V> struct NativePayload<hgraph::Map<K, V>> { using type = hgraph::Value; };
    template <typename T> struct NativePayload<hgraph::Set<T>> { using type = hgraph::Value; };
    template <typename T, std::int64_t N> struct NativePayload<List<T, N>> { using type = hgraph::Value; };
    template <typename... T> struct NativePayload<hgraph::Tuple<T...>> { using type = hgraph::Value; };
    template <hgraph::fixed_string Name, typename... Fields>
    struct NativePayload<hgraph::Bundle<Name, Fields...>> { using type = hgraph::Value; };
    template <hgraph::fixed_string Namespace, hgraph::fixed_string Name, bool Abstract, typename Parents, typename Arguments, typename... Fields>
    struct NativePayload<hgraph::NominalBundle<Namespace, Name, Abstract, Parents, Arguments, Fields...>> { using type = hgraph::Value; };

    template <typename T, typename E> class Result;
    template <typename T, typename E> struct NativePayload<Result<T, E>> { using type = hgraph::Value; };

    template <typename T, typename E> class Result {
      public:
        using value_type = typename NativePayload<T>::type;
        using error_type = typename NativePayload<E>::type;
        static Result ok(value_type value) { return Result{std::in_place_index<0>, std::move(value)}; }
        static Result err(error_type value) { return Result{std::in_place_index<1>, std::move(value)}; }
        [[nodiscard]] bool is_ok() const noexcept { return payload_.index() == 0; }
        [[nodiscard]] const value_type &value() const { return std::get<0>(payload_); }
        [[nodiscard]] const error_type &error() const { return std::get<1>(payload_); }
        [[nodiscard]] static Result from_value(const hgraph::ValueView &source, const PreparedValuePlan &plan) {
            const bool success = plan.result_is_ok(source);
            const auto payload = plan.result_payload(source, success);
            if (success) {
                if constexpr (std::is_same_v<value_type, hgraph::Value>) { return ok(plan.field_plan(1).retain(payload)); }
                else { return ok(required_scalar<value_type>(payload)); }
            }
            if constexpr (std::is_same_v<error_type, hgraph::Value>) { return err(plan.field_plan(2).retain(payload)); }
            else { return err(required_scalar<error_type>(payload)); }
        }
      private:
        template <std::size_t I, typename P> Result(std::in_place_index_t<I> index, P &&payload) : payload_{index, std::forward<P>(payload)} {}
        std::variant<value_type, error_type> payload_;
    };
}

namespace hgl { template <typename T, typename E> using NativeScalarResult = ordinary::Result<T, E>; }

namespace hgraph {
    template <typename T, typename E> struct scalar_descriptor<hgl::ordinary::Result<T, E>> {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return scalar_descriptor<T>::is_concrete() && scalar_descriptor<E>::is_concrete(); }
        [[nodiscard]] static const ValueTypeMetaData *value_meta() { return hgl::ordinary::result_meta(scalar_descriptor<T>::value_meta(), scalar_descriptor<E>::value_meta()); }
    };
}
#endif
