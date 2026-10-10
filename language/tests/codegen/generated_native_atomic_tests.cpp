#include <catch2/catch_test_macros.hpp>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <native-atomic-values.h>

using namespace hgraph;
using namespace hgraph::testing;
namespace native    = checks::native_atomic_values;
namespace generated = examples::native_atomic_values;

TEST_CASE("generated native atomic nodes preserve owning empty repeated and sparse publications",
          "[codegen][runtime][native-atomic]") {
    generated::register_operators();
    const native::Token empty{}, a{"a"}, b{"b"};
    CHECK_OUTPUT(eval_node<generated::leaf>(values<native::Token>(std::nullopt, empty, empty, a, std::nullopt, a, b)),
                 values<native::Token>(std::nullopt, empty, empty, a, std::nullopt, a, b));
    CHECK_OUTPUT(eval_node<generated::make>(values<Str>("", "a", std::nullopt, "b")),
                 values<native::Token>(empty, a, std::nullopt, b));
    CHECK_OUTPUT(eval_node<generated::text>(values<native::Token>(empty, a, std::nullopt, b)),
                 values<Str>("", "a", std::nullopt, "b"));
    CHECK_OUTPUT(eval_node<generated::text_leaf>(values<native::TextOnly>(native::TextOnly{}, native::TextOnly{"a"}, std::nullopt)),
                 values<Str>("", "a", std::nullopt));
    CHECK_OUTPUT(eval_node<generated::less>(values<native::Token>(a, b, a), values<native::Token>(b, a, a)),
                 values<Bool>(true, false, false));
}

TEST_CASE("generated native atomic nested values and keys retain canonical content", "[codegen][runtime][native-atomic]") {
    generated::register_operators();
    const native::Token empty{}, a{"a"}, b{"b"};
    const auto          first  = list_delta<TS<native::Token>>({a, b});
    const auto          second = list_delta<TS<native::Token>>({std::nullopt, empty});
    CHECK_OUTPUT(eval_node<generated::fixed>(values<Value>(first, std::nullopt, second)),
                 values<Value>(first, std::nullopt, second));
    const auto added = set_delta<native::Token>({a, b}, {}), removed = set_delta<native::Token>({}, {a});
    CHECK_OUTPUT(eval_node<generated::members>(values<Value>(added, std::nullopt, removed)),
                 values<Value>(added, std::nullopt, removed));
    const auto entry  = dict_delta<native::Token, TS<native::Token>>({{a, b}});
    const auto erased = dict_delta<native::Token, TS<native::Token>>({}, {a});
    CHECK_OUTPUT(eval_node<generated::mapping>(values<Value>(entry, std::nullopt, erased)),
                 values<Value>(entry, std::nullopt, erased));
}

TEST_CASE("generated native complete payloads boxes and rolling arrivals retain exact owners",
          "[codegen][runtime][native-atomic]") {
    generated::register_operators();
    const native::Token                    empty{}, a{"a"};
    const Value                            empty_value{empty}, value{a};
    const hgl::ordinary::PreparedValuePlan boxes{scalar_descriptor<hgl::ordinary::Any>::value_meta()};
    CHECK_OUTPUT(eval_node<generated::operators::boxed>(values<native::Token>(empty, a, std::nullopt, a)),
                 values<Value>(boxes.box(empty_value.view()), boxes.box(value.view()), std::nullopt, boxes.box(value.view())));
    using Payload = hgl::ordinary::List<native::Token, 2>;
    const hgl::ordinary::PreparedValuePlan lists{scalar_descriptor<Payload>::value_meta()};
    ListBuilder                            first_builder{lists.element_binding(), *lists.binding().schema()};
    first_builder.push_back(empty_value.view());
    first_builder.push_back(value.view());
    const auto  first_storage = first_builder.build_storage();
    const auto  first         = lists.list(first_storage);
    ListBuilder second_builder{lists.element_binding(), *lists.binding().schema()};
    second_builder.push_back(empty_value.view());
    second_builder.push_back(empty_value.view());
    const auto second_storage = second_builder.build_storage();
    const auto second         = lists.list(second_storage);
    CHECK_OUTPUT((eval_node<generated::operators::complete, TS<Payload>>(values<Value>(first, first, std::nullopt, second))),
                 values<Value>(first, first, std::nullopt, second));
    CHECK_OUTPUT((eval_node<generated::operators::arrival, TSW<native::Token, 2, 1>>(
                     values<Value>(empty_value, value, std::nullopt, value))),
                 values<native::Token>(empty, a, std::nullopt, a));
}

TEST_CASE("generated native scalar local retention copies an aggregate global borrow", "[codegen][runtime][native-atomic]") {
    generated::register_operators();
    const native::Token empty{}, a{"a"};
    CHECK_OUTPUT(eval_node<generated::scalar_copy>(values<native::Token>(empty, a, std::nullopt, a)),
                 values<native::Token>(empty, a, std::nullopt, a));
}
