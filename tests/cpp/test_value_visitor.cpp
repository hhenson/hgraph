#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/any_ops.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/visitor.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>

namespace
{
    struct VisitorExtensionScalar
    {
        std::int32_t value{0};
    };

    enum class VisitedValueKind
    {
        Atomic,
        Tuple,
        Bundle,
        List,
        Set,
        Map,
        CyclicBuffer,
        Queue,
    };

    [[nodiscard]] VisitedValueKind classify(const hgraph::ValueView &value)
    {
        using namespace hgraph;
        return visit(
            value, [](AtomicView) { return VisitedValueKind::Atomic; }, [](TupleView) { return VisitedValueKind::Tuple; },
            [](BundleView) { return VisitedValueKind::Bundle; }, [](ListView) { return VisitedValueKind::List; },
            [](SetView) { return VisitedValueKind::Set; }, [](MapView) { return VisitedValueKind::Map; },
            [](CyclicBufferView) { return VisitedValueKind::CyclicBuffer; }, [](QueueView) { return VisitedValueKind::Queue; });
    }
}  // namespace

TEST_CASE("ValueView visit dispatches every concrete semantic value kind")
{
    using namespace hgraph;

    static_assert(!std::is_copy_constructible_v<AtomicView>);
    static_assert(AtomicView::kind == ValueTypeKind::Atomic);
    static_assert(TupleView::kind == ValueTypeKind::Tuple);
    static_assert(BundleView::kind == ValueTypeKind::Bundle);
    static_assert(ListView::kind == ValueTypeKind::List);
    static_assert(SetView::kind == ValueTypeKind::Set);
    static_assert(MapView::kind == ValueTypeKind::Map);
    static_assert(CyclicBufferView::kind == ValueTypeKind::CyclicBuffer);
    static_assert(QueueView::kind == ValueTypeKind::Queue);
    static_assert(sizeof(AtomicView) == sizeof(ValueView));

    auto       &registry       = TypeRegistry::instance();
    auto       &factory        = ValuePlanFactory::instance();
    const auto *extension_meta = registry.register_scalar<VisitorExtensionScalar>("value_visitor_extension_scalar");
    const auto *int_meta       = registry.register_scalar<std::int32_t>("int32");
    const auto  int_binding    = factory.type_for(int_meta);

    Value atomic{VisitorExtensionScalar{7}};
    CHECK(classify(atomic.view()) == VisitedValueKind::Atomic);
    visit(
        atomic.view(),
        [](AtomicView selected)
        {
            REQUIRE(selected.holds_alternative<VisitorExtensionScalar>());
            CHECK(selected.checked_as<VisitorExtensionScalar>().value == 7);
        },
        [](ValueView) { FAIL("custom scalar did not dispatch as AtomicView"); });

    const auto tuple_binding = factory.type_for(registry.tuple({extension_meta, int_meta}));
    Value      tuple{tuple_binding};
    CHECK(classify(tuple.view()) == VisitedValueKind::Tuple);

    const auto bundle_binding =
        factory.type_for(registry.bundle("ValueVisitorBundle", {{"extension", extension_meta}, {"number", int_meta}}));
    Value bundle{bundle_binding};
    CHECK(classify(bundle.view()) == VisitedValueKind::Bundle);

    const auto fixed_list_binding = factory.type_for(registry.list(int_meta, 2));
    Value      fixed_list{fixed_list_binding};
    CHECK(classify(fixed_list.view()) == VisitedValueKind::List);

    const auto shaped_array_binding = factory.type_for(registry.array(int_meta, 2));
    Value      shaped_array{shaped_array_binding};
    REQUIRE(shaped_array.view().schema()->is_shaped_array());
    CHECK(classify(shaped_array.view()) == VisitedValueKind::List);

    SetBuilder set_builder{int_binding};
    set_builder.insert<std::int32_t>(1);
    Value set = set_builder.build();
    CHECK(classify(set.view()) == VisitedValueKind::Set);

    MapBuilder map_builder{int_binding, int_binding};
    map_builder.set_item<std::int32_t, std::int32_t>(1, 2);
    Value map = map_builder.build();
    CHECK(classify(map.view()) == VisitedValueKind::Map);

    CyclicBufferBuilder cyclic_builder{int_binding, 2};
    cyclic_builder.push_back<std::int32_t>(1);
    Value cyclic = cyclic_builder.build();
    CHECK(classify(cyclic.view()) == VisitedValueKind::CyclicBuffer);

    QueueBuilder queue_builder{int_binding, 2};
    queue_builder.push<std::int32_t>(1);
    Value queue = queue_builder.build();
    CHECK(classify(queue.view()) == VisitedValueKind::Queue);
}

TEST_CASE("value visit prefers shape handlers over the ValueView fallback")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    registry.register_scalar<VisitorExtensionScalar>("value_visitor_extension_scalar");
    registry.register_scalar<std::int32_t>("int32");

    Value      atomic{VisitorExtensionScalar{3}};
    const auto specialised = [](AtomicView) { return 1; };
    const auto fallback    = [](ValueView) { return 2; };

    CHECK(visit(atomic.view(), specialised, fallback) == 1);

    ListBuilder builder{registry.scalar_type<std::int32_t>()};
    builder.push_back<std::int32_t>(4);
    Value list = builder.build();
    CHECK(visit(list.view(), specialised, fallback) == 2);

    bool called = false;
    visit(atomic.view(), [&](AtomicView) { called = true; }, [](ValueView) {});
    CHECK(called);
}

TEST_CASE("value visit transparently unwraps populated Any boxes")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    registry.register_scalar<VisitorExtensionScalar>("value_visitor_extension_scalar");

    Value scalar{VisitorExtensionScalar{11}};
    Value inner{any_type()};
    inner.as_any().begin_mutation().set(scalar.view());
    Value outer{any_type()};
    outer.as_any().begin_mutation().set(std::move(inner));

    CHECK(classify(outer.view()) == VisitedValueKind::Atomic);

    bool       any_handler_called = false;
    const auto result             = visit(
        outer.view(), [](AtomicView selected) { return selected.checked_as<VisitorExtensionScalar>().value; },
        [&](AnyView)
        {
            any_handler_called = true;
            return std::int32_t{-1};
        },
        [](ValueView) { return std::int32_t{-2}; });
    CHECK(result == 11);
    CHECK_FALSE(any_handler_called);

    Value empty{any_type()};
    REQUIRE_THROWS_WITH(visit(empty.view(), [](ValueView) {}), "cannot visit an empty Any value");
}

TEST_CASE("value visit preserves access capability through trusted projections")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    registry.register_scalar<VisitorExtensionScalar>("value_visitor_extension_scalar");

    Value scalar{VisitorExtensionScalar{5}};
    auto  mutation = scalar.begin_mutation();
    visit(
        mutation,
        [](AtomicView selected)
        {
            REQUIRE(selected.mutable_payload());
            selected.checked_mutable_as<VisitorExtensionScalar>().value = 6;
        },
        [](ValueView) { FAIL("unexpected mutation fallback"); });
    CHECK(scalar.view().checked_as<VisitorExtensionScalar>().value == 6);

    Value boxed{any_type()};
    boxed.as_any().begin_mutation().set(scalar.view());
    auto box_mutation = boxed.begin_mutation();
    visit(
        box_mutation,
        [](AtomicView selected)
        {
            REQUIRE(selected.writable_payload());
            CHECK_FALSE(selected.mutable_payload());
            auto selected_mutation                                               = selected.begin_mutation();
            selected_mutation.checked_mutable_as<VisitorExtensionScalar>().value = 7;
        },
        [](ValueView) { FAIL("unexpected boxed mutation fallback"); });
    CHECK(boxed.as_any().get().checked_as<VisitorExtensionScalar>().value == 7);

    const Value &read_only_box = boxed;
    visit(
        read_only_box.view(),
        [](AtomicView selected)
        {
            CHECK_FALSE(selected.writable_payload());
            CHECK_FALSE(selected.mutable_payload());
        },
        [](ValueView) { FAIL("unexpected read-only fallback"); });
}

TEST_CASE("value visit rejects absent payloads and direct AtomicView validates "
          "kind")
{
    using namespace hgraph;

    auto       &registry       = TypeRegistry::instance();
    const auto *extension_meta = registry.register_scalar<VisitorExtensionScalar>("value_visitor_extension_scalar");
    const auto *int_meta       = registry.register_scalar<std::int32_t>("int32");

    ValueView empty;
    REQUIRE_THROWS_WITH(visit(empty, [](ValueView) {}), "cannot visit a value without a live payload");

    Value typed_null{*extension_meta};
    CHECK(typed_null.view().bound());
    CHECK_FALSE(typed_null.view().valid());
    REQUIRE_THROWS_WITH(visit(typed_null.view(), [](ValueView) {}), "cannot visit a value without a live payload");

    Value list{ValuePlanFactory::instance().type_for(registry.list(int_meta, 2))};
    CHECK_THROWS_AS(AtomicView{list.view()}, std::logic_error);
}

TEST_CASE("value visit result contract rejects references and borrowed lazy ranges")
{
    using namespace hgraph;

    static_assert(detail::visitor_result_safe_v<void>);
    static_assert(detail::visitor_result_safe_v<std::string>);
    static_assert(detail::visitor_result_safe_v<std::vector<Value>>);
    static_assert(!detail::visitor_result_safe_v<ValueView &>);
    static_assert(!detail::visitor_result_safe_v<const AtomicView &>);
    static_assert(!detail::visitor_result_safe_v<Range<ValueView>>);
    static_assert(!detail::visitor_result_safe_v<const Range<ValueView>>);
    static_assert(!detail::visitor_result_safe_v<KeyValueRange<ValueView, ValueView>>);
}

TEST_CASE("value visit uses declared enum and owned-bundle shapes")
{
    using namespace hgraph;

    auto       &registry    = TypeRegistry::instance();
    const auto *integer     = registry.register_scalar<std::int64_t>("int");
    const auto *enumeration = registry.enum_type("ValueVisitorEnum", {{"First", 1}, {"Second", 2}});

    Value enum_value{ValuePlanFactory::instance().type_for(enumeration)};
    CHECK(classify(enum_value.view()) == VisitedValueKind::Atomic);

    const auto *recursive =
        registry.recursive_bundle("tests.value_visitor", "RecursiveValue", {{"value", integer}, {"next", nullptr}});
    const auto *owned = recursive->fields[1].type;
    REQUIRE(owned != nullptr);
    REQUIRE(owned->is_owned());

    Value empty_owner{ValuePlanFactory::instance().type_for(owned)};
    CHECK(classify(empty_owner.view()) == VisitedValueKind::Bundle);
    CHECK(empty_owner.view().schema() == owned);
}
