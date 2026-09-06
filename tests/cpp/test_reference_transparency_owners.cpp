// RFC 0036 (docs/source/rfc/rfc_0036_reference_transparency_owners.rst):
// the four owners of the REF transparency rule, exercised directly.
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <stdexcept>

namespace
{
    using namespace hgraph;

    struct OwnersFixture
    {
        TypeRegistry              &registry = TypeRegistry::instance();
        const ValueTypeMetaData   *int_meta = registry.register_scalar<std::int32_t>("int32");
        const ValueTypeMetaData   *float_meta = registry.register_scalar<double>("float64");
        const TSValueTypeMetaData *ts_int   = registry.ts(int_meta);
        const TSValueTypeMetaData *ts_float = registry.ts(float_meta);
        const TSValueTypeMetaData *ref_int  = registry.ref(ts_int);
    };

    void set_int(TSOutput &output, std::int32_t value, DateTime time)
    {
        Value wrapped{value};
        auto  mutation = output.view(time).begin_mutation(time);
        REQUIRE(mutation.copy_value_from(wrapped.view()));
    }

    void set_reference(TSOutput &ref_output, const TSOutput &target, DateTime time)
    {
        Value wrapped{TimeSeriesReference{target.view(time)}};
        auto  mutation = ref_output.view(time).begin_mutation(time);
        REQUIRE(mutation.copy_value_from(wrapped.view()));
    }

    struct OwnersConstantSource
    {
        static constexpr auto name              = "rfc0036_constant_source";
        static constexpr bool schedule_on_start = true;
        static void           eval(Out<TS<Int>> out) { out.set(Int{41}); }
    };

    struct OwnersRefCopy
    {
        static constexpr auto name              = "rfc0036_ref_copy";
        static constexpr bool schedule_on_start = true;
        static void eval(In<"ref", REF<TS<Int>>> ref, Out<REF<TS<Int>>> out) { out.set(ref.value()); }
    };

    struct OwnersRefDeref
    {
        static constexpr auto name              = "rfc0036_ref_deref";
        static constexpr bool schedule_on_start = true;
        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out) { out.set(value.value()); }
    };

    /** A compose that reads what its named parameters will observe. */
    struct ObservedProbeGraph
    {
        static constexpr auto name = "rfc0036_observed_probe_graph";
        static void           compose(Wiring &w)
        {
            const auto *ts_int  = schema_descriptor<TS<Int>>::ts_meta();
            auto        source  = wire<OwnersConstantSource>(w);
            auto        ref     = wire<OwnersRefCopy>(w, source);
            REQUIRE(ref.erased().schema->kind == TSTypeKind::REF);

            // A generic parameter observes the value however it is reached.
            NamedPort<"value", TsVar<"S">> generic{Port<TsVar<"S">>{w, ref.erased()}};
            CHECK(generic.observed().schema == ts_int);
            CHECK(generic.erased().schema->kind == TSTypeKind::REF);

            // A REF-declared parameter observes the reference itself.
            NamedPort<"token", REF<TS<Int>>> token{ref};
            CHECK(token.observed().schema == ref.erased().schema);

            // A concrete value parameter given a plain source observes it as is.
            NamedPort<"plain", TS<Int>> plain{source};
            CHECK(plain.observed().schema == ts_int);
            CHECK(plain.observed().schema == plain.erased().schema);

            wire<OwnersRefDeref>(w, ref);
        }
    };
}  // namespace

TEST_CASE("RFC 0036 owners: value_element_ts answers the element with every reference followed")
{
    OwnersFixture f;
    auto         &registry = f.registry;

    CHECK(registry.value_element_ts(registry.tsd(f.int_meta, f.ref_int)) == f.ts_int);
    CHECK(registry.value_element_ts(registry.tsd(f.int_meta, f.ts_int)) == f.ts_int);
    CHECK(registry.value_element_ts(registry.tsl(f.ref_int, 2)) == f.ts_int);
    CHECK(registry.value_element_ts(registry.tsl(f.ref_int)) == f.ts_int);
    CHECK(registry.value_element_ts(registry.tsl(f.ts_int, 3)) == f.ts_int);

    // A reference to the collection is followed first; nested references are
    // followed all the way down.
    const auto *dict_of_ref = registry.tsd(f.int_meta, f.ref_int);
    CHECK(registry.value_element_ts(registry.ref(dict_of_ref)) == f.ts_int);
    CHECK(registry.value_element_ts(registry.tsd(f.int_meta, registry.ref(dict_of_ref))) ==
          registry.tsd(f.int_meta, f.ts_int));

    CHECK_THROWS_AS(registry.value_element_ts(f.ts_int), std::invalid_argument);
    CHECK_THROWS_AS(registry.value_element_ts(f.ref_int), std::invalid_argument);
    CHECK_THROWS_AS(registry.value_element_ts(nullptr), std::invalid_argument);
}

TEST_CASE("RFC 0036 owners: ref is idempotent so ref(dereference(x)) is ref(x)")
{
    OwnersFixture f;
    CHECK(f.registry.ref(f.ref_int) == f.ref_int);
    CHECK(f.registry.ref(f.registry.dereference(f.ref_int)) == f.ref_int);
}

TEST_CASE("RFC 0036 owners: time_series_value_equivalent compares through references on both sides")
{
    OwnersFixture f;
    auto         &registry = f.registry;

    CHECK(time_series_value_equivalent(f.ref_int, f.ts_int));
    CHECK(time_series_value_equivalent(f.ts_int, f.ref_int));
    CHECK(time_series_value_equivalent(f.ref_int, f.ref_int));
    CHECK(time_series_value_equivalent(f.ts_int, f.ts_int));
    CHECK_FALSE(time_series_value_equivalent(f.ref_int, f.ts_float));
    CHECK_FALSE(time_series_value_equivalent(f.ts_int, registry.ref(f.ts_float)));

    // Interior references are transparent too.
    CHECK(time_series_value_equivalent(registry.tsd(f.int_meta, f.ref_int), registry.tsd(f.int_meta, f.ts_int)));
    CHECK(time_series_value_equivalent(registry.ref(registry.tsd(f.int_meta, f.ref_int)),
                                       registry.tsd(f.int_meta, f.ts_int)));
    CHECK(time_series_value_equivalent(registry.tsl(f.ref_int, 2), registry.tsl(f.ts_int, 2)));
    CHECK(time_series_value_equivalent(registry.un_named_tsb({{"v", f.ref_int}}),
                                       registry.un_named_tsb({{"v", f.ts_int}})));
    CHECK_FALSE(time_series_value_equivalent(registry.tsl(f.ref_int, 2), registry.tsl(f.ts_int, 3)));
    CHECK_FALSE(time_series_value_equivalent(registry.tsd(f.int_meta, f.ref_int),
                                             registry.tsd(f.int_meta, f.ts_float)));

    // The equivalence the binding step applies is this one.
    CHECK(graph_wiring_detail::input_accepts_output_schema(f.ts_int, f.ref_int));
    CHECK(graph_wiring_detail::input_accepts_output_schema(f.ref_int, f.ts_int));
    CHECK_FALSE(graph_wiring_detail::input_accepts_output_schema(f.ts_float, f.ref_int));
}

TEST_CASE("RFC 0036 owners: through_reference resolves a REF output and leaves a plain one alone")
{
    OwnersFixture f;
    TSOutput      target{*f.ts_int};
    TSOutput      ref_output{*f.ref_int};

    const auto t1 = MIN_ST + TimeDelta{1};
    set_int(target, 41, t1);
    set_reference(ref_output, target, t1);

    const auto plain = target.view(t1).through_reference();
    REQUIRE(plain.schema() == f.ts_int);
    CHECK(plain.handle().same_as(target.view(t1).handle()));
    CHECK(plain.value().checked_as<std::int32_t>() == 41);

    const auto resolved = ref_output.view(t1).through_reference();
    REQUIRE(resolved.schema() == f.ts_int);
    CHECK(resolved.value().checked_as<std::int32_t>() == 41);
}

TEST_CASE("RFC 0036 owners: a target link records at bind whether its bound output can move")
{
    OwnersFixture f;
    auto         &registry = f.registry;

    const auto *root_schema = registry.tsb("Rfc0036OwnersInputRoot", {{"value", f.ts_int}, {"token", f.ref_int}});
    const auto  input_schema = TSEndpointSchema::non_peered(
        root_schema, {TSEndpointSchema::peered(f.ts_int), TSEndpointSchema::peered(f.ref_int)});

    TSOutput plain{*f.ts_int};
    TSOutput other{*f.ts_int};
    TSOutput ref_output{*f.ref_int};
    TSInput  input{TSInputBuilderFactory::checked_builder_for(*root_schema, input_schema)};

    const auto t1 = MIN_ST + TimeDelta{10};
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    set_int(plain, 1, t1);
    set_int(other, 2, t1);
    set_reference(ref_output, other, t1);

    {
        auto root   = input.view(nullptr, t1);
        auto bundle = root.as_bundle();
        auto value = bundle.field("value");
        REQUIRE(value.is_bindable());
        CHECK_FALSE(value.bound_target_is_reference());   // unbound

        value.bind_output(plain.view(t1));
        REQUIRE(value.bound());
        CHECK_FALSE(value.bound_target_is_reference());   // a plain output cannot move
        CHECK(value.value().checked_as<std::int32_t>() == 1);
    }
    {
        // A value input bound to a REF output reads through a from-REF
        // alternative, which retargets as the reference ticks.
        auto root   = input.view(nullptr, t2);
        auto bundle = root.as_bundle();
        auto value = bundle.field("value");
        value.bind_output(ref_output.view(t2));
        REQUIRE(value.bound());
        CHECK(value.bound_target_is_reference());
        CHECK(value.value().checked_as<std::int32_t>() == 2);
    }
    {
        // A rebind to a plain output clears the record.
        auto root   = input.view(nullptr, t3);
        auto bundle = root.as_bundle();
        auto value = bundle.field("value");
        value.bind_output(plain.view(t3));
        CHECK_FALSE(value.bound_target_is_reference());
        value.unbind_output();
        CHECK_FALSE(value.bound_target_is_reference());
    }
    {
        // A REF-declared input bound to a REF output observes the reference,
        // whose target can move.
        auto root   = input.view(nullptr, t1);
        auto bundle = root.as_bundle();
        auto token = bundle.field("token");
        token.bind_output(ref_output.view(t1));
        REQUIRE(token.bound());
        CHECK(token.bound_target_is_reference());
    }
    // The root is non-peered: not bindable, so never a moving target.
    const auto root = input.view(nullptr, t3);
    CHECK_FALSE(root.bound_target_is_reference());
}

TEST_CASE("RFC 0036 owners: NamedPort::observed reads what the parameter will be bound to")
{
    OwnersFixture f;
    static_cast<void>(f);
    const auto executor = testing::run_graph(build_graph<ObservedProbeGraph>());
    CHECK(executor.view().graph().node_at(2).output(MIN_ST).value().checked_as<Int>() == Int{41});
}
