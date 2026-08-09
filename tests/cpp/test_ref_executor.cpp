#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

// User-visible REF ports through the full executor (closes the review gap:
// prior REF coverage was view/binding-level plus std operators). A
// user-authored node publishes ``Out<REF<TS<Int>>>`` retargeting between two
// INDEPENDENT upstream producers mid-run; an ordinary ``TS<Int>`` consumer
// follows the reference (REF-transparent input adaptation + alternative-store
// rebinds) across executor cycles.

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct RefSelector
    {
        static constexpr auto name = "ref_selector";

        static void eval(In<"pick_rhs", TS<Bool>> pick_rhs,
                         In<"lhs", TS<Int>, InputValidity::Unchecked> lhs,
                         In<"rhs", TS<Int>, InputValidity::Unchecked> rhs,
                         Out<REF<TS<Int>>> out)
        {
            if (!pick_rhs.modified()) { return; }  // retarget only on a selection change
            out.set(pick_rhs.value() ? rhs.reference() : lhs.reference());
        }
    };

    struct RefSelectGraph
    {
        [[maybe_unused]] static constexpr auto name = "ref_select_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> pick_rhs, Port<TS<Int>> lhs, Port<TS<Int>> rhs)
        {
            // The REF<TS<Int>> port is consumed as a plain TS<Int> — the
            // REF-transparent binding dereferences whatever output the
            // reference currently designates.
            return wire<RefSelector>(w, pick_rhs, lhs, rhs).as<TS<Int>>();
        }
    };

    struct InspectNarrowedReference
    {
        static constexpr auto name = "inspect_narrowed_reference";

        static void eval(In<"ref", REF<TS<Float>>> ref, Out<TS<Bool>> out)
        {
            const TimeSeriesReference value = ref.value();
            const auto *endpoint_schema = value.target_output().view(out.evaluation_time()).schema();
            out.set(value.target_schema() == schema_descriptor<TS<Float>>::ts_meta() &&
                    endpoint_schema == schema_descriptor<TS<Int>>::ts_meta());
        }
    };

    struct DowncastRefGraph
    {
        static constexpr auto name = "downcast_ref_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> source)
        {
            auto source_ref = source.template as<REF<TS<Int>>>();
            auto narrowed = wire<stdlib::downcast_ref, REF<TS<Float>>>(w, source_ref);
            return wire<InspectNarrowedReference>(w, narrowed);
        }
    };

    struct ForwardDowncastRefGraph
    {
        static constexpr auto name = "forward_downcast_ref_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> source)
        {
            auto source_ref = source.template as<REF<TS<Int>>>();
            return wire<stdlib::downcast_ref, REF<TS<Int>>>(w, source_ref).template as<TS<Int>>();
        }
    };

    struct NestedStructuralRefProbe
    {
        static constexpr auto name = "nested_structural_ref_probe";

        static void eval(In<"refs", TSL<REF<TSL<TS<Int>, 2>>, 2>> refs,
                         Out<TS<Bool>> out)
        {
            const auto first = refs[0].value();
            const auto second = refs[1].value();
            const auto *target = schema_descriptor<TSL<TS<Int>, 2>>::ts_meta();
            out.set(first.is_non_peered() && second.is_non_peered() &&
                    time_series_schema_equivalent(first.target_schema(), target) &&
                    time_series_schema_equivalent(second.target_schema(), target) &&
                    first.items().size() == 2 && second.items().size() == 2);
        }
    };

    struct NestedStructuralRefGraph
    {
        static constexpr auto name = "nested_structural_ref_graph";

        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Int>> a, Port<TS<Int>> b,
                                      Port<TS<Int>> c, Port<TS<Int>> d)
        {
            auto lhs = stdlib::to_tsl<TSL<TS<Int>, 2>>(w, a, b);
            auto rhs = stdlib::to_tsl<TSL<TS<Int>, 2>>(w, c, d);
            auto refs = stdlib::to_tsl<TSL<TSL<TS<Int>, 2>, 2>>(w, lhs, rhs);
            return wire<NestedStructuralRefProbe>(w, refs);
        }
    };
}  // namespace

TEST_CASE("REF executor: a user REF output retargets between independent producers mid-run")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<RefSelectGraph>(values<Bool>(true, none, false, none, none),
                                           values<Int>(1, 2, none, 4, none),
                                           values<Int>(10, none, 30, none, 50)),
                 // cycle 0: ref -> rhs, sees 10. cycle 1: rhs silent (lhs's 2
                 // is not observed — the ref points at rhs). cycle 2: retarget
                 // to lhs, whose current value (2) is observed on rebind.
                 // cycle 3: lhs ticks 4. cycle 4: rhs's 50 is not observed.
                 values<Int>(10, none, 2, 4, none));
}

TEST_CASE("REF executor: a reference retarget to an already-valid producer samples its current value")
{
    stdlib::register_standard_operators();

    // lhs ticks only at cycle 0; the retarget at cycle 2 must still deliver
    // that value to the consumer (rebind-time sampling, matching the Python
    // REF semantics), not wait for the producer's next tick.
    CHECK_OUTPUT(eval_node<RefSelectGraph>(values<Bool>(true, none, false, none),
                                           values<Int>(7, none, none, none),
                                           values<Int>(10, 20, none, none)),
                 values<Int>(10, 20, 7, none));
}

TEST_CASE("downcast_ref: C++ wiring preserves the endpoint and replaces only its target schema")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<DowncastRefGraph>(values<Int>(42)), values<Bool>(true));
}

TEST_CASE("downcast_ref: C++ wiring still forwards the referenced output")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<ForwardDowncastRefGraph>(values<Int>(1, 2, 3)),
                 values<Int>(1, 2, 3));
}

TEST_CASE("REF executor: nested structural sources adapt to fixed REF children")
{
    stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<NestedStructuralRefGraph>(values<Int>(1, 10), values<Int>(2, 20),
                                                      values<Int>(3, 30), values<Int>(4, 40)),
                 values<Bool>(true, none));
}
