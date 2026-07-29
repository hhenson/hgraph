#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/visitor.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace
{
    enum class VisitedKind
    {
        Value,
        Set,
        Dict,
        List,
        Window,
        Bundle,
        Reference,
        Signal,
    };

    struct VisitorSchemas
    {
        const hgraph::TSValueTypeMetaData *value;
        const hgraph::TSValueTypeMetaData *set;
        const hgraph::TSValueTypeMetaData *dict;
        const hgraph::TSValueTypeMetaData *list;
        const hgraph::TSValueTypeMetaData *window;
        const hgraph::TSValueTypeMetaData *bundle;
        const hgraph::TSValueTypeMetaData *reference;
        const hgraph::TSValueTypeMetaData *signal;

        [[nodiscard]] std::array<const hgraph::TSValueTypeMetaData *, 8> all() const
        {
            return {value, set, dict, list, window, bundle, reference, signal};
        }
    };

    [[nodiscard]] VisitorSchemas visitor_schemas()
    {
        auto &registry = hgraph::TypeRegistry::instance();
        const auto *integer = registry.register_scalar<std::int32_t>("int32");
        const auto *value = registry.ts(integer);
        return VisitorSchemas{
            .value = value,
            .set = registry.tss(integer),
            .dict = registry.tsd(integer, value),
            .list = registry.tsl(value, 2),
            .window = registry.tsw(integer, 3, 1),
            .bundle = registry.tsb("TSEndpointVisitorNestedBundle", {{"value", value}}),
            .reference = registry.ref(value),
            .signal = registry.signal(),
        };
    }

    [[nodiscard]] VisitedKind classify(const hgraph::TSOutputView &view)
    {
        using namespace hgraph;
        return visit(
            view, [](TSValueOutputView) { return VisitedKind::Value; }, [](TSSOutputView) { return VisitedKind::Set; },
            [](TSDOutputView) { return VisitedKind::Dict; }, [](TSLOutputView) { return VisitedKind::List; },
            [](TSWOutputView) { return VisitedKind::Window; }, [](TSBOutputView) { return VisitedKind::Bundle; },
            [](TSReferenceOutputView) { return VisitedKind::Reference; },
            [](TSSignalOutputView) { return VisitedKind::Signal; });
    }

    [[nodiscard]] VisitedKind classify(const hgraph::TSInputView &view)
    {
        using namespace hgraph;
        return visit(
            view, [](TSValueInputView) { return VisitedKind::Value; }, [](TSSInputView) { return VisitedKind::Set; },
            [](TSDInputView) { return VisitedKind::Dict; }, [](TSLInputView) { return VisitedKind::List; },
            [](TSWInputView) { return VisitedKind::Window; }, [](TSBInputView) { return VisitedKind::Bundle; },
            [](TSReferenceInputView) { return VisitedKind::Reference; },
            [](TSSignalInputView) { return VisitedKind::Signal; });
    }
}  // namespace

TEST_CASE("TSOutputView visit dispatches all semantic time-series kinds")
{
    using namespace hgraph;

    static_assert(!std::is_copy_constructible_v<TSValueOutputView>);
    static_assert(!std::is_copy_constructible_v<TSReferenceOutputView>);
    static_assert(!std::is_copy_constructible_v<TSSignalOutputView>);
    static_assert(TSValueOutputView::kind == TSTypeKind::TS);
    static_assert(TSReferenceOutputView::kind == TSTypeKind::REF);
    static_assert(TSSignalOutputView::kind == TSTypeKind::SIGNAL);

    const auto schemas = visitor_schemas();
    const std::array expected{
        VisitedKind::Value,  VisitedKind::Set,    VisitedKind::Dict,      VisitedKind::List,
        VisitedKind::Window, VisitedKind::Bundle, VisitedKind::Reference, VisitedKind::Signal,
    };

    for (std::size_t index = 0; index < expected.size(); ++index)
    {
        TSOutput output{*schemas.all()[index]};
        auto view = output.view(MIN_ST);
        CHECK(classify(view) == expected[index]);
    }
}

TEST_CASE("TSInputView visit dispatches unbound endpoints by their exposed schema")
{
    using namespace hgraph;

    static_assert(!std::is_copy_constructible_v<TSValueInputView>);
    static_assert(!std::is_copy_constructible_v<TSReferenceInputView>);
    static_assert(!std::is_copy_constructible_v<TSSignalInputView>);
    static_assert(TSValueInputView::kind == TSTypeKind::TS);
    static_assert(TSReferenceInputView::kind == TSTypeKind::REF);
    static_assert(TSSignalInputView::kind == TSTypeKind::SIGNAL);

    const auto schemas = visitor_schemas();
    const std::array names{
        std::string_view{"value"},     std::string_view{"set"},    std::string_view{"dict"},
        std::string_view{"list"},      std::string_view{"window"}, std::string_view{"bundle"},
        std::string_view{"reference"}, std::string_view{"signal"},
    };
    const std::array expected{
        VisitedKind::Value,  VisitedKind::Set,    VisitedKind::Dict,      VisitedKind::List,
        VisitedKind::Window, VisitedKind::Bundle, VisitedKind::Reference, VisitedKind::Signal,
    };

    std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
    std::vector<TSEndpointSchema> endpoints;
    for (std::size_t index = 0; index < names.size(); ++index)
    {
        fields.emplace_back(names[index], schemas.all()[index]);
        endpoints.push_back(TSEndpointSchema::peered(schemas.all()[index]));
    }

    auto &registry = TypeRegistry::instance();
    const auto *root_schema = registry.tsb("TSEndpointVisitorInputRoot", fields);
    const auto endpoint_schema = TSEndpointSchema::non_peered(root_schema, std::move(endpoints));
    TSInput input = TSInputBuilderFactory::checked_builder_for(*root_schema, endpoint_schema).make_input();

    auto root_view = input.view();
    auto bundle = root_view.as_bundle();
    for (std::size_t index = 0; index < names.size(); ++index)
    {
        auto child = bundle.field(names[index]);
        CHECK_FALSE(child.bound());
        CHECK_FALSE(child.valid());
        CHECK(classify(child) == expected[index]);
    }
}

TEST_CASE("endpoint visit prefers a specialised handler and supports a role "
          "fallback")
{
    using namespace hgraph;

    const auto schemas = visitor_schemas();
    TSOutput value_output{*schemas.value};
    TSOutput set_output{*schemas.set};
    auto value_view = value_output.view(MIN_ST);
    auto set_view = set_output.view(MIN_ST);

    const auto specialised = [](TSValueOutputView) { return 1; };
    const auto fallback = [](TSOutputView) { return 2; };

    CHECK(visit(value_view, specialised, fallback) == 1);
    CHECK(visit(set_view, specialised, fallback) == 2);

    bool called = false;
    visit(value_view, [&](TSValueOutputView) { called = true; }, [](TSOutputView) {});
    CHECK(called);
}

TEST_CASE("endpoint visit result contract rejects borrowed lazy ranges")
{
    using namespace hgraph;

    static_assert(detail::endpoint_result_safe_v<void>);
    static_assert(detail::endpoint_result_safe_v<std::vector<TSInputView>>);
    static_assert(detail::endpoint_result_safe_v<std::vector<TSOutputView>>);
    static_assert(!detail::endpoint_result_safe_v<TSInputView &>);
    static_assert(!detail::endpoint_result_safe_v<TSOutputView &>);
    static_assert(!detail::endpoint_result_safe_v<Range<TSInputView>>);
    static_assert(!detail::endpoint_result_safe_v<const Range<TSOutputView>>);
    static_assert(!detail::endpoint_result_safe_v<KeyValueRange<std::string_view, TSInputView>>);
    static_assert(!detail::endpoint_result_safe_v<const KeyValueRange<std::size_t, TSOutputView>>);
}

TEST_CASE("endpoint visit keeps references opaque and recursion explicit")
{
    using namespace hgraph;

    const auto schemas = visitor_schemas();
    TSOutput reference_output{*schemas.reference};
    auto reference_view = reference_output.view(MIN_ST);
    CHECK(classify(reference_view) == VisitedKind::Reference);

    TSOutput bundle_output{*schemas.bundle};
    auto bundle_view = bundle_output.view(MIN_ST);
    const auto child_kind = visit(
        bundle_view,
        [](TSBOutputView bundle) {
            auto child = bundle.field("value");
            return classify(child);
        },
        [](TSOutputView) { return VisitedKind::Signal; });
    CHECK(child_kind == VisitedKind::Value);
}

TEST_CASE("endpoint visit preserves output mutation and input binding capabilities")
{
    using namespace hgraph;

    const auto schemas = visitor_schemas();
    const auto time = MIN_ST + TimeDelta{1};
    TSOutput output{*schemas.value};
    auto output_view = output.view(time);
    Value forty_two{std::int32_t{42}};

    visit(
        output_view,
        [&](TSValueOutputView selected) {
            auto mutation = selected.begin_mutation(time);
            REQUIRE(mutation.copy_value_from(forty_two.view()));
        },
        [](TSOutputView) { FAIL("unexpected output visitor fallback"); });

    auto current = output.view(time);
    REQUIRE(current.valid());
    CHECK(current.value().checked_as<std::int32_t>() == 42);

    auto &registry = TypeRegistry::instance();
    const auto *root_schema = registry.tsb("TSEndpointVisitorBindingRoot", {{"value", schemas.value}});
    const auto endpoint_schema = TSEndpointSchema::non_peered(root_schema, {TSEndpointSchema::peered(schemas.value)});
    TSInput input = TSInputBuilderFactory::checked_builder_for(*root_schema, endpoint_schema).make_input();
    auto input_view = input.view();
    auto input_bundle = input_view.as_bundle();
    auto child = input_bundle.field("value");

    visit(
        child, [&](TSValueInputView selected) { selected.bind_output(current); },
        [](TSInputView) { FAIL("unexpected input visitor fallback"); });

    auto rebound_input_view = input.view(nullptr, time);
    auto rebound_bundle = rebound_input_view.as_bundle();
    auto rebound_child = rebound_bundle.field("value");
    REQUIRE(rebound_child.valid());
    CHECK(rebound_child.value().checked_as<std::int32_t>() == 42);
}

TEST_CASE("endpoint visit rejects untyped views and direct leaf construction "
          "validates kind")
{
    using namespace hgraph;

    TSInputView empty_input;
    TSOutputView empty_output;
    CHECK_THROWS_AS(visit(empty_input, [](TSInputView) {}), std::invalid_argument);
    CHECK_THROWS_AS(visit(empty_output, [](TSOutputView) {}), std::invalid_argument);

    const auto schemas = visitor_schemas();
    TSOutput set_output{*schemas.set};
    auto set_view = set_output.view(MIN_ST);
    CHECK_THROWS_AS(TSValueOutputView{set_view.borrowed_ref()}, std::invalid_argument);
}
