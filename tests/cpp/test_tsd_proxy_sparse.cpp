#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/ts_data/proxy.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <stdexcept>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;
    // Instrument the source ops, not the proxy builder: a scan which leaves every
    // binding unchanged must still fail these tests. Static tables outlive interned types.
    template <int Tag> struct CountedDict
    {
        static inline const TSDDataOps *original{};
        static inline TSDDataOps        ops{};
        static inline std::size_t       visits{};
        static inline std::size_t       seeks{};
        static inline TSRoleTypeRef     child_type{};
        static inline std::size_t       seek_span{};
        static TSRoleTypeRef            wrap(TSRoleTypeRef type) {
            original           = &static_cast<const TSDDataOps &>(type.ops_ref());
            ops                = *original;
            ops.slot_live_impl = [](const void *c, const void *m, std::size_t s) {
                ++visits;
                return original->slot_live_impl(c, m, s);
            };
            ops.slot_occupied_impl = [](const void *c, const void *m, std::size_t s) {
                ++visits;
                return original->slot_occupied_impl(c, m, s);
            };
            ops.slot_added_impl = [](const void *c, const void *m, std::size_t s) {
                ++visits;
                return original->slot_added_impl(c, m, s);
            };
            ops.slot_removed_impl = [](const void *c, const void *m, std::size_t s) {
                ++visits;
                return original->slot_removed_impl(c, m, s);
            };
            ops.slot_modified_impl = [](const void *c, const void *m, std::size_t s) {
                ++visits;
                return original->slot_modified_impl(c, m, s);
            };
            ops.child_at_slot_impl = [](const void *c, const void *m, std::size_t s) {
                ++visits;
                return original->child_at_slot_impl(c, m, s);
            };
            ops.next_modified_slot_impl = [](const void *c, const void *m, std::size_t p, std::size_t limit) {
                ++seeks;
                const auto first = p == TS_DATA_NO_CHILD_ID ? 0 : p + 1;
                const auto end   = std::min(limit, original->slot_capacity_impl(c, m));
                if (end > first) seek_span += end - first;
                return original->next_modified_slot_impl(c, m, p, limit);
            };
            if (child_type) {
                ops.child_binding_at_slot_impl = [](const void *, const void *, std::size_t) { return child_type; };
            }
            return intern_ts_type(*type.schema(), type.role(), type.checked_plan(), ops, "test.counted-dictionary");
        }
        static void reset() { visits = seeks = seek_span = 0; }
    };

    struct FlatFixture
    {
        const TSValueTypeMetaData *ts;
        const TSValueTypeMetaData *schema;
        TSData                     source;
        TSData                     proxy;
        Value                      zero{std::int32_t{0}};
        Value                      one{std::int32_t{1}};
        static inline bool         fail_next{};
        static void build(TSDProxy &, std::size_t, const TSDataView &target, const TSDataView &input, DateTime time, const void *) {
            if (std::exchange(fail_next, false)) { throw std::runtime_error("injected refresh failure"); }
            static_cast<void>(target.begin_mutation(time).copy_value_from(input.value()));
        }
        static bool matches(const TSDProxy &, std::size_t, const TSDataView &target, const TSDataView &input, const void *) {
            return target.value() == input.value();
        }
        static inline const TSDProxyValueOps value_ops{&build, &matches};
        explicit FlatFixture(std::int32_t n)
            : ts(TypeRegistry::instance().ts(TypeRegistry::instance().register_scalar<std::int32_t>("int32"))),
              schema(TypeRegistry::instance().tsd(ts->value_type, ts)),
              source(TSDataTypeRef::checked(CountedDict<0>::wrap(TSDataPlanFactory::instance().data_type_for(schema).as_role()))),
              proxy(tsd_proxy_data_type_for(*schema, TSDataPlanFactory::instance().data_type_for(ts).as_role(),
                                            ValuePlanFactory::instance().type_for(schema->key_type()))) {
            auto data = source.view();
            {
                auto m = data.as_dict().begin_mutation(MIN_ST);
                for (std::int32_t i = 0; i < n; ++i) {
                    Value key{i};
                    m.set(key.view(), zero.view());
                }
            }
            bind_tsd_proxy(proxy.view(), data.as_dict(), &value_ops, nullptr, MIN_ST, TSDProxyChildRefresh::OnChildTick);
            CountedDict<0>::reset();
        }
    };

    template <class R> std::size_t count(const R &range) {
        std::size_t n = 0;
        for (auto it = range.begin(); it != range.end(); ++it) {
            static_cast<void>(*it);
            ++n;
        }
        return n;
    }
}  // namespace

TEST_CASE("Proxy delta surfaces visit changed slots rather than dictionary capacity", "[proxy][sparse]") {
    using namespace hgraph;
    for (const std::int32_t n : {256, 4096}) {
        CAPTURE(n);
        FlatFixture f(n);
        auto        source_data = f.source.view();
        auto        source      = source_data.as_dict();
        auto        proxy_data  = f.proxy.view();
        auto        proxy       = proxy_data.as_dict();
        Value       key{n / 2};
        const auto  t = MIN_ST + TimeDelta{1};
        REQUIRE(source.at(key.view()).begin_mutation(t).copy_value_from(f.one.view()));
        CountedDict<0>::reset();
        REQUIRE(count(proxy.modified_values(t)) == 1);
        REQUIRE(count(proxy.modified_items(t)) == 1);
        REQUIRE(count(proxy.modified_keys(t)) == 1);
        auto delta_value = proxy.delta_value(t);
        auto delta       = delta_value.as_bundle();
        REQUIRE(delta.at("modified").as_map().size() == 1);
        REQUIRE(count(delta.at("modified").as_map().items()) == 1);
        REQUIRE(delta.at("removed").as_set().size() == 0);
        REQUIRE(count(delta.at("removed").as_set().values()) == 0);
        REQUIRE(CountedDict<0>::visits < 160);
        REQUIRE(CountedDict<0>::seeks < 32);
    }
}

TEST_CASE("Proxy structural changes visit affected and pending slots only", "[proxy][sparse]") {
    using namespace hgraph;
    for (const std::int32_t n : {256, 4096}) {
        CAPTURE(n);
        FlatFixture f(n);
        auto        source_data = f.source.view();
        auto        source      = source_data.as_dict();
        auto        proxy_data  = f.proxy.view();
        auto        proxy       = proxy_data.as_dict();
        Value       added{n}, removed{n / 2};
        {
            auto m = source.begin_mutation(MIN_ST + TimeDelta{1});
            m.set(added.view(), f.one.view());
        }
        REQUIRE(CountedDict<0>::visits < 80);
        CountedDict<0>::reset();
        REQUIRE(count(proxy.added_items()) == 1);
        REQUIRE(count(proxy.key_set().added_values()) == 1);
        REQUIRE(CountedDict<0>::visits < 80);
        CountedDict<0>::reset();
        {
            auto m = source.begin_mutation(MIN_ST + TimeDelta{2});
            REQUIRE(m.erase(removed.view()));
        }
        REQUIRE(CountedDict<0>::visits < 80);
        CountedDict<0>::reset();
        REQUIRE(count(proxy.removed_items()) == 1);
        REQUIRE(count(proxy.key_set().removed_values()) == 1);
        REQUIRE(CountedDict<0>::visits < 80);
    }
}

TEST_CASE("Proxy mixed updates use bounded sparse source seeks and see later writes", "[proxy][sparse]") {
    using namespace hgraph;
    FlatFixture f(4096);
    auto        source_data = f.source.view();
    auto        source      = source_data.as_dict();
    auto        proxy_data  = f.proxy.view();
    auto        proxy       = proxy_data.as_dict();
    Value       first{std::int32_t{0}}, middle{std::int32_t{2048}}, later{std::int32_t{1024}};
    const auto  t = MIN_ST + TimeDelta{1};
    REQUIRE(source.at(first.view()).begin_mutation(t).copy_value_from(f.one.view()));
    auto      &storage     = *static_cast<TSDProxy *>(const_cast<void *>(f.proxy.view().data()));
    const auto middle_slot = source.find_slot(middle.view());
    auto       child       = TSDataView{storage.element_type(), storage.owned_child_memory(middle_slot)};
    REQUIRE(child.begin_mutation(t).copy_value_from(f.one.view()));
    CountedDict<0>::reset();
    const auto first_slot = proxy.next_modified_slot();
    REQUIRE(first_slot == source.find_slot(first.view()));
    REQUIRE(proxy.next_modified_slot(first_slot) == middle_slot);
    REQUIRE(proxy.next_modified_slot(middle_slot) == TS_DATA_NO_CHILD_ID);
    REQUIRE(CountedDict<0>::visits < 80);
    REQUIRE(CountedDict<0>::seeks < 8);
    REQUIRE(source.at(later.view()).begin_mutation(t).copy_value_from(f.one.view()));
    const auto later_slot = source.find_slot(later.view());
    CountedDict<0>::reset();
    REQUIRE(proxy.next_modified_slot(first_slot, later_slot) == TS_DATA_NO_CHILD_ID);
    REQUIRE(proxy.next_modified_slot(first_slot) == later_slot);
    REQUIRE(proxy.next_modified_slot(later_slot) == middle_slot);
    REQUIRE(proxy.next_modified_slot() == first_slot);
    REQUIRE(CountedDict<0>::visits < 80);
}

TEST_CASE("Nested reference proxies reconcile selected leaves without scanning siblings", "[proxy][sparse]") {
    using namespace hgraph;
    auto &r                    = TypeRegistry::instance();
    auto &factory              = TSDataPlanFactory::instance();
    auto  integer              = r.register_scalar<std::int32_t>("int32");
    auto  ts                   = r.ts(integer);
    auto  inner                = r.tsd(integer, r.ref(ts));
    auto  outer                = r.tsd(integer, inner);
    auto  requested            = r.tsd(integer, r.tsd(integer, ts));
    CountedDict<2>::child_type = CountedDict<1>::wrap(factory.data_type_for(inner).as_role());
    const auto source_type     = CountedDict<2>::wrap(factory.output_type_for(outer).as_role());
    TSOutput   source{TSOutputTypeRef::checked(source_type)};
    TSOutput   a{ts}, b{ts};
    Value      zero{std::int32_t{0}}, one{std::int32_t{1}};
    static_cast<void>(a.view(MIN_ST).begin_mutation(MIN_ST).copy_value_from(zero.view()));
    static_cast<void>(b.view(MIN_ST).begin_mutation(MIN_ST).copy_value_from(one.view()));
    Value ar{TimeSeriesReference{a.view(MIN_ST)}}, br{TimeSeriesReference{b.view(MIN_ST)}};
    auto  root  = source.data_view();
    auto  input = root.as_dict().begin_mutation(MIN_ST).at(zero.view());
    {
        auto m = input.as_dict().begin_mutation(MIN_ST);
        for (std::int32_t i = 0; i < 4096; ++i) {
            Value key{i};
            m.set(key.view(), ar.view());
        }
    }
    auto    handle = source.view(MIN_ST).binding_for(*requested);
    TSInput input_endpoint{TSInputBuilderFactory::checked_builder_for(*requested, TSEndpointSchema::peered(requested))};
    input_endpoint.view(nullptr, MIN_ST).bind_output(handle.view(MIN_ST));
    auto       output = handle.data_view();
    Value      key{std::int32_t{2048}};
    const auto t    = MIN_ST + TimeDelta{1};
    auto       leaf = input.as_dict().at(key.view());
    CountedDict<1>::reset();
    CountedDict<2>::reset();
    REQUIRE(leaf.begin_mutation(t).copy_value_from(br.view()));
    auto read = [&] {
        auto inner_view = output.as_dict().at(zero.view());
        return inner_view.as_dict().at(key.view()).value().checked_as<std::int32_t>();
    };
    {
        auto out_view = handle.view(t);
        auto out_dict = out_view.as_dict();
        auto in_view  = input_endpoint.view(nullptr, t);
        auto in_dict  = in_view.as_dict();
        REQUIRE(count(out_dict.modified_items()) == 1);
        REQUIRE(count(in_dict.modified_items()) == 1);
        auto inner_in   = in_dict.at(zero.view());
        auto inner_dict = inner_in.as_dict();
        REQUIRE(count(inner_dict.modified_values()) == 1);
    }
    REQUIRE(read() == 1);
    REQUIRE(CountedDict<1>::visits < 100);
    CountedDict<1>::reset();
    for (int i = 0; i < 10; ++i) REQUIRE(read() == 1);
    REQUIRE(CountedDict<1>::visits < 160);
    // A second write at the same timestamp must not be hidden by the shortcut.
    static_cast<void>(leaf.begin_mutation(t).copy_value_from(ar.view()));
    REQUIRE(read() == 0);
    {
        auto m = input.as_dict().begin_mutation(t);
        REQUIRE(m.erase(key.view()));
        m.set(key.view(), br.view());
    }
    REQUIRE(read() == 1);
}

TEST_CASE("Sparse ranges preserve candidate filtering and independent iterators", "[proxy][sparse]") {
    struct Candidates
    {
        std::vector<std::size_t> indices{3, 900, 4000};
        mutable std::size_t      visits{};
    } candidates;
    hgraph::Range<std::size_t> range{.context = &candidates,
                                     .limit   = 2000,
                                     .predicate =
                                         [](const void *c, const void *, std::size_t i) {
                                             ++static_cast<const Candidates *>(c)->visits;
                                             return i != 3;
                                         },
                                     .projector = [](const void *, const void *, std::size_t i) { return i; },
                                     .seek =
                                         [](const void *c, const void *, std::size_t first) {
                                             const auto &indices = static_cast<const Candidates *>(c)->indices;
                                             auto        it      = std::ranges::lower_bound(indices, first);
                                             return it == indices.end() ? std::size_t(-1) : *it;
                                         }};
    auto                       first = range.begin();
    auto                       copy  = first;
    REQUIRE(*first == 900);
    REQUIRE(++first == range.end());
    REQUIRE(*copy == 900);
    REQUIRE(candidates.visits == 2);
    range.limit = 0;
    REQUIRE(range.begin() == range.end());
}

TEST_CASE("Proxy failed builds retry without scanning unchanged slots", "[proxy][sparse]") {
    using namespace hgraph;
    FlatFixture f(4096);
    auto        source_data = f.source.view();
    auto        source      = source_data.as_dict();
    auto        proxy_data  = f.proxy.view();
    auto        proxy       = proxy_data.as_dict();
    Value       key{std::int32_t{2048}}, added{std::int32_t{4096}};
    FlatFixture::fail_next = true;
    REQUIRE_THROWS_AS(source.at(key.view()).begin_mutation(MIN_ST + TimeDelta{1}).copy_value_from(f.one.view()),
                      std::runtime_error);
    CountedDict<0>::reset();
    {
        auto m = source.begin_mutation(MIN_ST + TimeDelta{2});
        m.set(added.view(), f.one.view());
    }
    REQUIRE(proxy.at(key.view()).value().checked_as<std::int32_t>() == 1);
    REQUIRE(proxy.at(added.view()).value().checked_as<std::int32_t>() == 1);
    REQUIRE(CountedDict<0>::visits < 100);
}

TEST_CASE("Dense mixed proxy changes never repeatedly search a distant source bit", "[proxy][sparse]") {
    using namespace hgraph;
    constexpr std::int32_t n = 4096;
    FlatFixture            f(n);
    auto                   source_data = f.source.view();
    auto                   source      = source_data.as_dict();
    auto                   proxy_data  = f.proxy.view();
    auto                   proxy       = proxy_data.as_dict();
    auto                  &storage     = *static_cast<TSDProxy *>(const_cast<void *>(proxy_data.data()));
    const auto             t           = MIN_ST + TimeDelta{1};
    Value                  far{n - 1};
    REQUIRE(source.at(far.view()).begin_mutation(t).copy_value_from(f.one.view()));
    for (std::int32_t i = 0; i < n - 1; i += 2) {
        Value      key{i};
        const auto slot  = source.find_slot(key.view());
        auto       child = TSDataView{storage.element_type(), storage.owned_child_memory(slot)};
        REQUIRE(child.begin_mutation(t).copy_value_from(f.one.view()));
    }
    CountedDict<0>::reset();
    REQUIRE(count(proxy.modified_values(t)) == n / 2 + 1);
    REQUIRE(CountedDict<0>::seek_span <= 2 * n);
    REQUIRE(CountedDict<0>::visits < 10 * n);
}

TEST_CASE("Proxy resurrection rechecks its slot after source delta rollover", "[proxy][sparse]") {
    using namespace hgraph;
    FlatFixture f(4096);
    auto        source_data = f.source.view();
    auto        source      = source_data.as_dict();
    auto        proxy_data  = f.proxy.view();
    auto       &storage     = *static_cast<TSDProxy *>(const_cast<void *>(proxy_data.data()));
    Value       key{std::int32_t{2048}}, added{std::int32_t{4096}};
    const auto  slot = source.find_slot(key.view());
    {
        auto m = source.begin_mutation(MIN_ST + TimeDelta{1});
        REQUIRE(m.erase(key.view()));
        m.set(key.view(), f.one.view());
    }
    SECTION("before any projected read") {}
    SECTION("after a projected read followed by another same-time rewrite") {
        REQUIRE(proxy_data.as_dict().at(key.view()).value().checked_as<std::int32_t>() == 1);
    }
    Value latest{std::int32_t{2}};
    static_cast<void>(source.at(key.view()).begin_mutation(MIN_ST + TimeDelta{1}).copy_value_from(latest.view()));
    CountedDict<0>::reset();
    {
        auto m = source.begin_mutation(MIN_ST + TimeDelta{2});
        m.set(added.view(), f.one.view());
    }
    // Inspect storage without a projected read, which would reconcile lazily.
    auto child = TSDataView{storage.element_type(), storage.owned_child_memory(slot)};
    REQUIRE(child.value().checked_as<std::int32_t>() == 2);
    REQUIRE(CountedDict<0>::visits < 100);
}

TEST_CASE("Proxy resume drops rechecks for slots erased while suspended", "[proxy][sparse]") {
    using namespace hgraph;
    FlatFixture f(4096);
    auto source_data = f.source.view();
    auto source = source_data.as_dict();
    auto proxy_data = f.proxy.view();
    auto &storage = *static_cast<TSDProxy *>(const_cast<void *>(proxy_data.data()));
    {
        auto m = source.begin_mutation(MIN_ST + TimeDelta{1});
        for (std::int32_t i = 0; i < 256; ++i) {
            Value key{i};
            REQUIRE(m.erase(key.view()));
            m.set(key.view(), f.one.view());
        }
    }
    storage.suspend_source();
    {
        auto m = source.begin_mutation(MIN_ST + TimeDelta{2});
        for (std::int32_t i = 0; i < 256; ++i) {
            Value key{i};
            REQUIRE(m.erase(key.view()));
        }
    }
    // Roll the removals out while the proxy is unsubscribed.
    Value key{std::int32_t{2048}};
    source.begin_mutation(MIN_ST + TimeDelta{3}).set(key.view(), f.one.view());
    bind_tsd_proxy(proxy_data, source, &FlatFixture::value_ops, nullptr,
                   MIN_ST + TimeDelta{4}, TSDProxyChildRefresh::OnChildTick);
    CountedDict<0>::reset();
    source.begin_mutation(MIN_ST + TimeDelta{5}).set(key.view(), f.zero.view());
    REQUIRE(CountedDict<0>::visits < 100);
}
