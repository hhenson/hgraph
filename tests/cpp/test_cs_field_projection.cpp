// Native coverage for the CompoundScalar field projection tick rule.
//
// getattr_ over a TS<Bundle> suppressed the tick whenever the projected field
// repeated its previous value, so a parent update that changed only a sibling
// field published nothing downstream (parity issues #570-#604). A
// TS[CompoundScalar] is one value stream rather than a bundle of
// independently ticking fields, so the projection follows its parent.
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/standard_types.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>

#include <string_view>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    using Inner = Bundle<"tests.cs_field_projection::Inner", Field<"symbol", Str>>;
    using Outer = Bundle<"tests.cs_field_projection::Outer", Field<"symbol", Str>,
                         Field<"inner", Inner>>;

    [[nodiscard]] Value inner_value(std::string_view symbol)
    {
        BundleBuilder builder{ValuePlanFactory::instance().type_for(
            scalar_descriptor<Inner>::value_meta())};
        builder.set("symbol", Value{Str{symbol}}.view());
        return builder.build();
    }

    [[nodiscard]] Value outer_value(std::string_view symbol,
                                    std::string_view inner_symbol)
    {
        BundleBuilder builder{ValuePlanFactory::instance().type_for(
            scalar_descriptor<Outer>::value_meta())};
        builder.set("symbol", Value{Str{symbol}}.view());
        builder.set("inner", inner_value(inner_symbol).view());
        return builder.build();
    }

    struct FieldProjectionGraph
    {
        static constexpr auto name = "cs_field_projection_graph";

        static Port<TS<Inner>> compose(Wiring &w, Port<TS<Outer>> series)
        {
            return wire<stdlib::getattr_>(w, series, Str{"inner"}).as<TS<Inner>>();
        }
    };
}  // namespace

TEST_CASE("container: a CompoundScalar field projection ticks with its parent")
{
    stdlib::register_standard_operators();
    // The projected field repeats while a sibling changes: both ticks publish.
    CHECK_OUTPUT(eval_node<FieldProjectionGraph>(values<Value>(
                     outer_value("BACK", "BOM"), outer_value("BOM", "BOM"))),
                 values<Value>(inner_value("BOM"), inner_value("BOM")));
}

TEST_CASE("container: a changing CompoundScalar field still ticks each change")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<FieldProjectionGraph>(values<Value>(
                     outer_value("A", "X"), outer_value("B", "Y"))),
                 values<Value>(inner_value("X"), inner_value("Y")));
}

TEST_CASE("container: a CompoundScalar field projection is silent without a parent tick")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<FieldProjectionGraph>(values<Value>(
                     outer_value("A", "X"), std::nullopt, outer_value("A", "X"))),
                 values<Value>(inner_value("X"), std::nullopt, inner_value("X")));
}
