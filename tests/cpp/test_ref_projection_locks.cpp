// RFC 0036 PR 2 (docs/source/rfc/rfc_0036_reference_transparency_owners.rst,
// implementation status, finding 1): the REF-declared container projections
// (getattr_ / getitem_ over REF[TSB], dereference over REF[TSB] / REF[TSL])
// observe their referenced container once in start, so a reference that
// retargets every cycle costs no type-system lock per tick.
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/operators/impl/record_replay_memory_impl.h>
#include <hgraph/lib/testing/record_replay.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/utils/counted_mutex.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    using LockBundle = TSB<"RefProjectionLockBundle", Field<"a", TS<Int>>, Field<"b", TS<Int>>>;

    std::uint64_t g_locks_before     = 0;
    std::uint64_t g_projection_locks = 0;

    /** Packs the replayed value into both fields of a bundle. */
    template <int Offset>
    struct PackBundle
    {
        static constexpr auto name = Offset == 0 ? "ref_projection_locks_pack_a" : "ref_projection_locks_pack_b";
        static void           eval(In<"value", TS<Int>> value, Out<LockBundle> out)
        {
            out.field<"a">().set(value.value() + Int{Offset});
            out.field<"b">().set(value.value() + Int{Offset + 1});
        }
    };

    /** Publishes a reference that retargets every cycle (bundle A on odd
        ticks, bundle B on even ticks) and records the lock count as the last
        node evaluated before the projection under test. */
    struct AlternatingReference
    {
        static constexpr auto name = "ref_projection_locks_alternating_ref";
        static void           eval(In<"tick", TS<Int>> tick, In<"a", REF<LockBundle>> a, In<"b", REF<LockBundle>> b,
                                   Out<REF<LockBundle>> out)
        {
            out.set(tick.value() % 2 == 1 ? a.value() : b.value());
            g_locks_before = type_system_lock_count();
        }
    };

    /** The first node evaluated after the projection: the locks taken between
        the two readings are the projection's own. */
    struct ProbeAfter
    {
        static constexpr auto name = "ref_projection_locks_probe";
        static void           eval(In<"value", TsVar<"S">> value)
        {
            static_cast<void>(value);
            g_projection_locks += type_system_lock_count() - g_locks_before;
        }
    };

    [[nodiscard]] WiringPortRef alternating_reference(Wiring &w)
    {
        auto src = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
        auto a   = wire<PackBundle<0>>(w, src);
        auto b   = wire<PackBundle<10>>(w, src);
        return wire<AlternatingReference>(w, src, a, b).erased();
    }

    struct FieldProjectionGraph
    {
        static constexpr auto name = "ref_projection_locks_field_graph";
        static void           compose(Wiring &w)
        {
            auto field = wire<stdlib::getattr_>(w, Port<void>{w, alternating_reference(w)}, Str{"a"});
            wire<ProbeAfter>(w, field);
        }
    };

    struct DereferenceGraph
    {
        static constexpr auto name = "ref_projection_locks_dereference_graph";
        static void           compose(Wiring &w)
        {
            auto materialized = wire<stdlib::dereference>(w, Port<void>{w, alternating_reference(w)});
            wire<ProbeAfter>(w, materialized);
        }
    };

    template <typename Graph>
    std::uint64_t projection_locks_over(std::size_t cycles)
    {
        std::vector<std::optional<Value>> ticks;
        for (std::size_t i = 1; i <= cycles; ++i) { ticks.emplace_back(Value{Int{static_cast<Int>(i)}}); }
        g_projection_locks = 0;
        GraphBuilder gb = build_graph<Graph>();
        set_replay_deltas(gb.global_state(), "in", ticks);
        GraphExecutorBuilder eb;
        eb.graph_builder(std::move(gb))
            .start_time(MIN_ST)
            .end_time(MIN_ST + TimeDelta{static_cast<std::int64_t>(cycles) + 1});
        GraphExecutorValue ex = eb.make_executor();
        ex.view().run();
        return g_projection_locks;
    }
}  // namespace

TEST_CASE("RFC 0036: getattr_ over a retargeting REF[TSB] takes no type-system lock per tick")
{
    static_cast<void>(TypeRegistry::instance().register_scalar<Int>("int"));
    stdlib::register_standard_operators();
    static_cast<void>(projection_locks_over<FieldProjectionGraph>(4));  // warm: publications happen once
    CHECK(projection_locks_over<FieldProjectionGraph>(4) == 0);
    CHECK(projection_locks_over<FieldProjectionGraph>(8) == 0);
}

TEST_CASE("RFC 0036: dereference over a retargeting REF[TSB] takes no type-system lock per tick")
{
    static_cast<void>(TypeRegistry::instance().register_scalar<Int>("int"));
    stdlib::register_standard_operators();
    static_cast<void>(projection_locks_over<DereferenceGraph>(4));
    CHECK(projection_locks_over<DereferenceGraph>(4) == 0);
    CHECK(projection_locks_over<DereferenceGraph>(8) == 0);
}
