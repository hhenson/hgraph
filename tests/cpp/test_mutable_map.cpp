// Tests for the value-layer mutable (structurally-mutable) Map — a slot-store
// backed dynamic dict distinct from the immutable compact map. Covers the
// Mutable schema axis, distinct interning, insert/get/contains/erase/clear,
// value replacement, iteration, copy independence, and a string -> Any map
// (the GlobalState shape) holding heterogeneous values.

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value.h>

#include <string>
#include <stdexcept>

namespace
{
    using namespace hgraph;

    Value make_mutable_map(const ValueTypeMetaData *key_meta, const ValueTypeMetaData *value_meta)
    {
        const auto *schema  = TypeRegistry::instance().mutable_map(key_meta, value_meta);
        const auto binding = ValuePlanFactory::instance().type_for(schema);
        REQUIRE(binding != nullptr);
        REQUIRE(binding.ops_ref().kind == ValueOpsKind::MutableMap);
        return Value{binding};
    }

    Value make_any(const Value &inner)
    {
        const auto binding = ValuePlanFactory::instance().type_for(TypeRegistry::instance().any());
        Value       any{binding};
        any.as_any().begin_mutation().set(inner.view());
        return any;
    }

    template <typename BreakHook, typename Invoke>
    void require_missing_map_hook(Value &map, BreakHook break_hook, Invoke invoke)
    {
        MutableMapValueOps ops = *checked_value_ops<MutableMapValueOps>(map.binding(), "mutable map hook test");
        break_hook(ops);
        const ValueTypeRef binding = intern_value_type(*map.schema(), *map.binding().plan(), ops);
        auto view = ValueView{binding, const_cast<void *>(map.view().data())}.as_map().begin_mutation();
        REQUIRE_THROWS_AS(invoke(view), std::logic_error);
    }
}  // namespace

TEST_CASE("mutable map: the Mutable schema axis interns distinctly from the immutable map")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    const ValueTypeMetaData *immutable = registry.map(int_meta, int_meta);
    const ValueTypeMetaData *mutable_  = registry.mutable_map(int_meta, int_meta);

    REQUIRE(mutable_ != nullptr);
    CHECK(mutable_->value_kind() == ValueTypeKind::Map);
    CHECK(mutable_->is_mutable());
    CHECK_FALSE(immutable->is_mutable());
    CHECK(mutable_ != immutable);
    CHECK(registry.mutable_map(int_meta, int_meta) == mutable_);  // interned
}

TEST_CASE("mutable map: insert, lookup, contains, replace, and erase")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    Value map = make_mutable_map(int_meta, int_meta);
    CHECK(map.as_map().size() == 0);

    {
        auto m = map.as_map().begin_mutation();
        m.set_item(Value{std::int32_t{1}}.view(), Value{std::int32_t{100}}.view());
        m.set_item(Value{std::int32_t{2}}.view(), Value{std::int32_t{200}}.view());
        m.set_item(Value{std::int32_t{3}}.view(), Value{std::int32_t{300}}.view());
    }

    auto view = map.as_map();
    REQUIRE(view.size() == 3);
    CHECK(view.contains(Value{std::int32_t{2}}.view()));
    CHECK_FALSE(view.contains(Value{std::int32_t{9}}.view()));
    CHECK(view.at(Value{std::int32_t{1}}.view()).checked_as<std::int32_t>() == 100);
    CHECK(view.at(Value{std::int32_t{3}}.view()).checked_as<std::int32_t>() == 300);

    // Replace an existing key's value.
    map.as_map().begin_mutation().set_item(Value{std::int32_t{2}}.view(), Value{std::int32_t{222}}.view());
    CHECK(map.as_map().size() == 3);
    CHECK(map.as_map().at(Value{std::int32_t{2}}.view()).checked_as<std::int32_t>() == 222);

    // Erase.
    CHECK(map.as_map().begin_mutation().remove(Value{std::int32_t{2}}.view()));
    CHECK_FALSE(map.as_map().begin_mutation().remove(Value{std::int32_t{2}}.view()));  // already gone
    CHECK(map.as_map().size() == 2);
    CHECK_FALSE(map.as_map().contains(Value{std::int32_t{2}}.view()));
    CHECK(map.as_map().at(Value{std::int32_t{1}}.view()).checked_as<std::int32_t>() == 100);
}

TEST_CASE("mutable map: metrics distinguish the key-set projection from values", "[memory]")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *str_meta = registry.register_scalar<std::string>("str");
    Value map = make_mutable_map(str_meta, str_meta);
    const std::string key(256, 'k');
    const std::string value(384, 'v');

    map.as_map().begin_mutation().set_item(Value{key}.view(), Value{value}.view());
    const auto full = map.view().dynamic_storage_metrics();
    const auto key_set = map.as_map().key_set().dynamic_storage_metrics();
    CHECK(full.live_bytes > key_set.live_bytes);
    CHECK(full.reserved_bytes > key_set.reserved_bytes);
    CHECK(key_set.live_bytes >= key.size() + 1);
}

TEST_CASE("mutable map: iteration over keys, values and entries; clear")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    Value map = make_mutable_map(int_meta, int_meta);
    {
        auto m = map.as_map().begin_mutation();
        m.set_item(Value{std::int32_t{1}}.view(), Value{std::int32_t{10}}.view());
        m.set_item(Value{std::int32_t{2}}.view(), Value{std::int32_t{20}}.view());
        m.set_item(Value{std::int32_t{3}}.view(), Value{std::int32_t{30}}.view());
    }

    std::int32_t key_sum = 0;
    std::int32_t val_sum = 0;
    const auto map_view = map.as_map();
    const auto entries  = map_view.entries();
    for (const auto &[key, value] : entries)
    {
        key_sum += key.checked_as<std::int32_t>();
        val_sum += value.checked_as<std::int32_t>();
    }
    CHECK(key_sum == 6);    // 1 + 2 + 3
    CHECK(val_sum == 60);   // 10 + 20 + 30

    map.as_map().begin_mutation().clear();
    CHECK(map.as_map().size() == 0);
    CHECK(map.to_string() == "{}");
}

TEST_CASE("mutable map: equality and a copy is independent")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    Value a = make_mutable_map(int_meta, int_meta);
    {
        auto m = a.as_map().begin_mutation();
        m.set_item(Value{std::int32_t{1}}.view(), Value{std::int32_t{10}}.view());
        m.set_item(Value{std::int32_t{2}}.view(), Value{std::int32_t{20}}.view());
    }

    Value b = a;  // deep copy
    CHECK(a.equals(b));

    b.as_map().begin_mutation().set_item(Value{std::int32_t{3}}.view(), Value{std::int32_t{30}}.view());
    CHECK(b.as_map().size() == 3);
    CHECK(a.as_map().size() == 2);
    CHECK_FALSE(a.equals(b));
}

TEST_CASE("mutable map: string -> Any (GlobalState shape) holds heterogeneous values")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *str_meta = registry.register_scalar<std::string>("str");
    (void)registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<double>("double");
    const auto *any_meta = registry.any();

    Value dict = make_mutable_map(str_meta, any_meta);
    {
        auto m = dict.as_map().begin_mutation();
        m.set_item(Value{std::string{"count"}}.view(), make_any(Value{std::int32_t{42}}).view());
        m.set_item(Value{std::string{"ratio"}}.view(), make_any(Value{1.5}).view());
    }

    auto view = dict.as_map();
    REQUIRE(view.size() == 2);
    CHECK(view.at(Value{std::string{"count"}}.view()).as_any().get().checked_as<std::int32_t>() == 42);
    CHECK(view.at(Value{std::string{"ratio"}}.view()).as_any().get().checked_as<double>() == 1.5);

    // Replace a heterogeneous value with a different schema.
    dict.as_map().begin_mutation().set_item(Value{std::string{"count"}}.view(),
                                            make_any(Value{std::string{"many"}}).view());
    CHECK(dict.as_map().at(Value{std::string{"count"}}.view()).as_any().get().checked_as<std::string>() == "many");
}

TEST_CASE("mutable map: every missing mutation hook fails before invocation")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    Value       map      = make_mutable_map(int_meta, int_meta);
    Value       key{std::int32_t{7}};
    Value       value{std::int32_t{11}};

    SECTION("insert")
    {
        require_missing_map_hook(map, [](auto &ops) { ops.insert = nullptr; },
                                 [&](auto &view) { view.set_item(key.view(), value.view()); });
    }
    SECTION("value_or_emplace")
    {
        require_missing_map_hook(map, [](auto &ops) { ops.value_or_emplace = nullptr; },
                                 [&](auto &view) { (void)view.value(key.view()); });
    }
    SECTION("erase")
    {
        require_missing_map_hook(map, [](auto &ops) { ops.erase = nullptr; },
                                 [&](auto &view) { (void)view.remove(key.view()); });
    }
    SECTION("clear")
    {
        require_missing_map_hook(map, [](auto &ops) { ops.clear = nullptr; },
                                 [](auto &view) { view.clear(); });
    }
}
