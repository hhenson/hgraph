// Tests for the read-only specialised views (``ListView``,
// ``CyclicBufferView``, ``QueueView``, ``SetView``, ``MapView``) and
// the per-kind ``ValueOps`` (``compact_list_ops`` / ``compact_set_ops``
// / ``compact_map_ops`` / ``compact_cyclic_buffer_ops`` /
// ``compact_queue_ops``).
//
// These tests exercise both the storage/ops layer directly and the
// ``Value`` / ``ValueView`` casting surface that resolves canonical
// bindings through ``ValuePlanFactory``.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/any_ops.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/container_ops.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_view.h>

#include <compare>
#include <array>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace
{
    template <typename T>
    [[nodiscard]] const T &as_const(const void *memory) noexcept
    {
        return *static_cast<const T *>(memory);
    }
}  // namespace

TEST_CASE("ListView: size, at, iteration over a built list")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<std::string>("string");
    const auto element_binding = registry.scalar_type<std::int32_t>();
    const auto list_binding     = compact_list_type(element_binding);

    ListBuilder builder{element_binding};
    builder.push_back<std::int32_t>(10);
    builder.push_back<std::int32_t>(20);
    builder.push_back<std::int32_t>(30);
    auto storage = builder.build_storage();

    ListView view{ValueView{list_binding, &storage}};
    REQUIRE(view.valid());
    REQUIRE(view.size() == 3);
    REQUIRE_FALSE(view.empty());
    REQUIRE_FALSE(view.is_fixed());
    REQUIRE(view.element_schema() == element_binding.schema());
    REQUIRE(view.front().checked_as<std::int32_t>() == 10);
    REQUIRE(view.back().checked_as<std::int32_t>() == 30);
    REQUIRE(view.at(0).checked_as<std::int32_t>() == 10);
    REQUIRE(view[2].checked_as<std::int32_t>() == 30);

    std::int32_t sum = 0;
    for (const auto element : view) { sum += element.checked_as<std::int32_t>(); }
    REQUIRE(sum == 60);

    std::int32_t values_sum = 0;
    for (const auto element : view.values()) { values_sum += element.checked_as<std::int32_t>(); }
    REQUIRE(values_sum == 60);
}

TEST_CASE("ValueOps narrowing follows only the declared ops hierarchy")
{
    using namespace hgraph;

    ValueOps invalid{};
    ValueOps base{};
    IndexedValueOps indexed{};
    ListValueOps list{};
    MutableListValueOps mutable_list{};
    CyclicBufferValueOps cyclic_buffer{};
    QueueValueOps queue{};
    SetValueOps set{};
    MutableSetValueOps mutable_set{};
    MapValueOps map{};
    MutableMapValueOps mutable_map{};
    ValueOps unknown{};

    base.kind          = ValueOpsKind::Base;
    indexed.kind       = ValueOpsKind::Indexed;
    list.kind          = ValueOpsKind::List;
    mutable_list.kind  = ValueOpsKind::MutableList;
    cyclic_buffer.kind = ValueOpsKind::CyclicBuffer;
    queue.kind         = ValueOpsKind::Queue;
    set.kind           = ValueOpsKind::Set;
    mutable_set.kind   = ValueOpsKind::MutableSet;
    map.kind           = ValueOpsKind::Map;
    mutable_map.kind   = ValueOpsKind::MutableMap;
    unknown.kind       = static_cast<ValueOpsKind>(255);

    const std::array<const ValueOps *, 12> actual{
        &invalid, &base, &indexed, &list, &mutable_list, &cyclic_buffer,
        &queue, &set, &mutable_set, &map, &mutable_map, &unknown,
    };
    constexpr std::array<std::array<bool, 10>, 12> expected{{
        {{false, false, false, false, false, false, false, false, false, false}},
        {{true,  false, false, false, false, false, false, false, false, false}},
        {{true,  true,  false, false, false, false, false, false, false, false}},
        {{true,  true,  true,  false, false, false, false, false, false, false}},
        {{true,  true,  true,  true,  false, false, false, false, false, false}},
        {{true,  true,  false, false, true,  false, false, false, false, false}},
        {{true,  true,  false, false, false, true,  false, false, false, false}},
        {{true,  true,  false, false, false, false, true,  false, false, false}},
        {{true,  true,  false, false, false, false, true,  true,  false, false}},
        {{true,  true,  false, false, false, false, false, false, true,  false}},
        {{true,  true,  false, false, false, false, false, false, true,  true}},
        {{false, false, false, false, false, false, false, false, false, false}},
    }};

    const auto matches = [](const ValueOps *ops, std::size_t requested) {
        switch (requested)
        {
        case 0: return try_value_ops<ValueOps>(ops) != nullptr;
        case 1: return try_value_ops<IndexedValueOps>(ops) != nullptr;
        case 2: return try_value_ops<ListValueOps>(ops) != nullptr;
        case 3: return try_value_ops<MutableListValueOps>(ops) != nullptr;
        case 4: return try_value_ops<CyclicBufferValueOps>(ops) != nullptr;
        case 5: return try_value_ops<QueueValueOps>(ops) != nullptr;
        case 6: return try_value_ops<SetValueOps>(ops) != nullptr;
        case 7: return try_value_ops<MutableSetValueOps>(ops) != nullptr;
        case 8: return try_value_ops<MapValueOps>(ops) != nullptr;
        case 9: return try_value_ops<MutableMapValueOps>(ops) != nullptr;
        default: return false;
        }
    };

    for (std::size_t actual_index = 0; actual_index < actual.size(); ++actual_index)
    {
        for (std::size_t requested = 0; requested < expected[actual_index].size(); ++requested)
        {
            CAPTURE(actual_index, requested);
            REQUIRE(matches(actual[actual_index], requested) == expected[actual_index][requested]);
        }
    }

    REQUIRE(try_value_ops<ValueOps>(static_cast<const ValueOps *>(nullptr)) == nullptr);
    REQUIRE(try_value_ops<IndexedValueOps>(static_cast<ValueTypeRef>(nullptr)) == nullptr);
    REQUIRE_THROWS_WITH(checked_value_ops<MapValueOps>(&set, "consumer"),
                        "consumer: requested Map ValueOps, actual Set");
    REQUIRE_THROWS_WITH(checked_value_ops<SetValueOps>(&unknown, "consumer"),
                        "consumer: requested Set ValueOps, actual Unknown");
    REQUIRE_THROWS_WITH(checked_value_ops<IndexedValueOps>(static_cast<const ValueOps *>(nullptr), "consumer"),
                        "consumer: requested Indexed ValueOps, actual null");
}

TEST_CASE("ValueOps narrowing uses ABI kind independently of value schema")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    auto       &factory = ValuePlanFactory::instance();
    const auto *element  = registry.register_scalar<std::int32_t>("int32");
    const auto *schema   = registry.list(element, 2);
    const auto canonical = factory.type_for(schema);
    IndexedValueOps indexed = *checked_value_ops<IndexedValueOps>(canonical, "narrowing test");
    const auto binding = intern_value_type(*schema, *canonical.plan(), indexed);

    REQUIRE(try_value_ops<IndexedValueOps>(binding) == &indexed);
    REQUIRE(try_value_ops<ListValueOps>(binding) == nullptr);

    ValueOps wrong_abi = indexed;
    wrong_abi.kind = ValueOpsKind::Base;
    const auto wrong_binding = intern_value_type(*schema, *canonical.plan(), wrong_abi);
    std::int32_t storage{};
    REQUIRE_THROWS_WITH((ListView{ValueView{wrong_binding, &storage}}),
                        "IndexedValueView: requested Indexed ValueOps, actual Base");

    IndexedValueOps missing_hooks = indexed;
    missing_hooks.size = nullptr;
    const auto missing_hooks_binding = intern_value_type(*schema, *canonical.plan(), missing_hooks);
    REQUIRE_THROWS_WITH((ListView{ValueView{missing_hooks_binding, &storage}}),
                        "IndexedValueView: binding does not expose indexed ops");
}

TEST_CASE("published value-layer ops tables carry exact kinds")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *scalar   = registry.register_scalar<std::int32_t>("int32");
    const auto binding  = registry.scalar_type<std::int32_t>();
    REQUIRE(binding != nullptr);
    REQUIRE(binding.ops_ref().kind == ValueOpsKind::Base);

    const auto *enum_schema = registry.enum_type("ValueOpsKindInventory", {{"A", 1}, {"B", 2}});
    const auto enum_binding = ValuePlanFactory::instance().type_for(enum_schema);
    REQUIRE(enum_binding != nullptr);
    REQUIRE(enum_binding.ops_ref().kind == ValueOpsKind::Base);

    REQUIRE(any_ops().kind == ValueOpsKind::Base);
    REQUIRE(compact_list_ops().kind == ValueOpsKind::List);
    REQUIRE(compact_cyclic_buffer_ops().kind == ValueOpsKind::CyclicBuffer);
    REQUIRE(compact_queue_ops().kind == ValueOpsKind::Queue);
    REQUIRE(compact_set_ops().kind == ValueOpsKind::Set);
    REQUIRE(compact_map_ops().kind == ValueOpsKind::Map);

    const auto *fixed_list = registry.list(scalar, 2);
    const auto fixed_binding = ValuePlanFactory::instance().type_for(fixed_list);
    REQUIRE(fixed_binding != nullptr);
    REQUIRE(fixed_binding.ops_ref().kind == ValueOpsKind::Indexed);
}

TEST_CASE("compact_list_ops: hash / equals / compare / to_string walk elements")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto element_binding = registry.scalar_type<std::int32_t>();

    ListBuilder a{element_binding};
    a.push_back<std::int32_t>(1); a.push_back<std::int32_t>(2); a.push_back<std::int32_t>(3);
    auto storage_a = a.build_storage();

    ListBuilder b{element_binding};
    b.push_back<std::int32_t>(1); b.push_back<std::int32_t>(2); b.push_back<std::int32_t>(3);
    auto storage_b = b.build_storage();

    ListBuilder c{element_binding};
    c.push_back<std::int32_t>(1); c.push_back<std::int32_t>(2); c.push_back<std::int32_t>(4);
    auto storage_c = c.build_storage();

    const ValueOps &ops = compact_list_ops();

    REQUIRE(ops.hash(&storage_a) == ops.hash(&storage_b));
    REQUIRE(ops.equals(&storage_a, &storage_b));
    REQUIRE_FALSE(ops.equals(&storage_a, &storage_c));
    REQUIRE(std::is_lt(ops.compare(&storage_a, &storage_c)));
    REQUIRE(std::is_gt(ops.compare(&storage_c, &storage_a)));
    REQUIRE(std::is_eq(ops.compare(&storage_a, &storage_b)));
    REQUIRE(ops.to_string(&storage_a) == "[1, 2, 3]");
}

TEST_CASE("compact container compares order null storage consistently")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<std::string>("string");
    const auto int_binding = registry.scalar_type<std::int32_t>();
    const auto str_binding = registry.scalar_type<std::string>();

    auto assert_null_order = [](const ValueOps &ops, const void *storage) {
        REQUIRE(std::is_eq(ops.compare(nullptr, nullptr)));
        REQUIRE(std::is_lt(ops.compare(nullptr, storage)));
        REQUIRE(std::is_gt(ops.compare(storage, nullptr)));
    };

    ListBuilder list_builder{int_binding};
    list_builder.push_back<std::int32_t>(1);
    auto list_storage = list_builder.build_storage();
    assert_null_order(compact_list_ops(), &list_storage);

    CyclicBufferBuilder cyclic_builder{int_binding, 2};
    cyclic_builder.push_back<std::int32_t>(1);
    auto cyclic_storage = cyclic_builder.build_storage();
    assert_null_order(compact_cyclic_buffer_ops(), &cyclic_storage);

    QueueBuilder queue_builder{int_binding, 2};
    queue_builder.push<std::int32_t>(1);
    auto queue_storage = queue_builder.build_storage();
    assert_null_order(compact_queue_ops(), &queue_storage);

    SetBuilder set_builder{int_binding};
    set_builder.insert<std::int32_t>(1);
    auto set_storage = set_builder.build_storage();
    assert_null_order(compact_set_ops(), &set_storage);

    MapBuilder map_builder{str_binding, int_binding};
    map_builder.set_item<std::string, std::int32_t>(std::string{"one"}, 1);
    auto map_storage = map_builder.build_storage();
    assert_null_order(compact_map_ops(), &map_storage);
}

TEST_CASE("CyclicBufferView: head, ring iteration, empty")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto element_binding = registry.scalar_type<std::int32_t>();
    const auto binding         = compact_cyclic_buffer_type(element_binding, 3);

    CyclicBufferBuilder builder{element_binding, 3};
    builder.push_back<std::int32_t>(1);
    builder.push_back<std::int32_t>(2);
    builder.push_back<std::int32_t>(3);
    builder.push_back<std::int32_t>(4);  // rotates: oldest (1) drops out
    builder.push_back<std::int32_t>(5);
    auto storage = builder.build_storage();

    CyclicBufferView view{ValueView{binding, &storage}};
    REQUIRE(view.size() == 3);
    REQUIRE(view.capacity() == 3);
    REQUIRE(view.element_schema() == element_binding.schema());
    REQUIRE_FALSE(view.empty());
    // Read in ring order: oldest first.
    REQUIRE(view.front().checked_as<std::int32_t>() == 3);
    REQUIRE(view.back().checked_as<std::int32_t>() == 5);
    REQUIRE(view.at(0).checked_as<std::int32_t>() == 3);
    REQUIRE(view.at(1).checked_as<std::int32_t>() == 4);
    REQUIRE(view.at(2).checked_as<std::int32_t>() == 5);
}

TEST_CASE("QueueView: front, size, iteration")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto element_binding = registry.scalar_type<std::int32_t>();
    const auto binding         = compact_queue_type(element_binding, /*max_capacity=*/0);

    QueueBuilder builder{element_binding};
    builder.push<std::int32_t>(100);
    builder.push<std::int32_t>(200);
    builder.push<std::int32_t>(300);
    auto storage = builder.build_storage();

    QueueView view{ValueView{binding, &storage}};
    REQUIRE_FALSE(view.empty());
    REQUIRE(view.size() == 3);
    REQUIRE_FALSE(view.has_max_capacity());
    REQUIRE(view.element_schema() == element_binding.schema());
    REQUIRE(view.front().checked_as<std::int32_t>() == 100);
    REQUIRE(view.back().checked_as<std::int32_t>() == 300);
    REQUIRE(view.at(0).checked_as<std::int32_t>() == 100);
    REQUIRE(view.at(2).checked_as<std::int32_t>() == 300);
}

TEST_CASE("SetView: contains, size, iteration; ops are order-independent")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto element_binding = registry.scalar_type<std::int32_t>();
    const auto binding         = compact_set_type(element_binding);

    SetBuilder a{element_binding};
    a.insert<std::int32_t>(7); a.insert<std::int32_t>(11); a.insert<std::int32_t>(13);
    auto storage_a = a.build_storage();

    SetView view_a{ValueView{binding, &storage_a}};
    REQUIRE(view_a.size() == 3);
    REQUIRE(view_a.element_schema() == element_binding.schema());

    std::int32_t seven = 7;
    std::int32_t twelve = 12;
    std::string wrong_type{"7"};
    REQUIRE(view_a.contains(ValueView{element_binding, &seven}));
    REQUIRE_FALSE(view_a.contains(ValueView{element_binding, &twelve}));
    REQUIRE_FALSE(view_a.contains(ValueView{registry.scalar_type<std::string>(), &wrong_type}));

    std::int32_t sum = 0;
    for (const auto member : view_a) { sum += member.checked_as<std::int32_t>(); }
    REQUIRE(sum == (7 + 11 + 13));

    std::int32_t values_sum = 0;
    for (const auto member : view_a.values()) { values_sum += member.checked_as<std::int32_t>(); }
    REQUIRE(values_sum == (7 + 11 + 13));

    // Sets compare order-independently via the ops table.
    SetBuilder b{element_binding};
    b.insert<std::int32_t>(13); b.insert<std::int32_t>(7); b.insert<std::int32_t>(11);  // different insertion order
    auto storage_b = b.build_storage();

    const ValueOps &ops = compact_set_ops();
    REQUIRE(ops.hash(&storage_a) == ops.hash(&storage_b));
    REQUIRE(ops.equals(&storage_a, &storage_b));
    REQUIRE(std::is_eq(ops.compare(&storage_a, &storage_b)));

    SetBuilder c{element_binding};
    c.insert<std::int32_t>(7); c.insert<std::int32_t>(11); c.insert<std::int32_t>(17);
    auto storage_c = c.build_storage();
    REQUIRE_FALSE(ops.equals(&storage_a, &storage_c));
    REQUIRE(ops.compare(&storage_a, &storage_c) == std::partial_ordering::unordered);

    SetBuilder small{element_binding};
    small.insert<std::int32_t>(7); small.insert<std::int32_t>(11);
    auto storage_small = small.build_storage();
    REQUIRE(std::is_lt(ops.compare(&storage_small, &storage_a)));
    REQUIRE(std::is_gt(ops.compare(&storage_a, &storage_small)));
}

TEST_CASE("MapView: contains, at, iteration; ops are order-independent over keys")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::string>("string");
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto key_binding   = registry.scalar_type<std::string>();
    const auto value_binding = registry.scalar_type<std::int32_t>();
    const auto binding       = compact_map_type(key_binding, value_binding);

    MapBuilder a{key_binding, value_binding};
    a.set_item<std::string, std::int32_t>(std::string{"alpha"}, 1);
    a.set_item<std::string, std::int32_t>(std::string{"beta"}, 2);
    a.set_item<std::string, std::int32_t>(std::string{"gamma"}, 3);
    auto storage_a = a.build_storage();

    MapView view{ValueView{binding, &storage_a}};
    REQUIRE(view.size() == 3);
    REQUIRE(view.key_schema() == key_binding.schema());
    REQUIRE(view.value_schema() == value_binding.schema());

    const std::string alpha{"alpha"};
    const std::string beta{"beta"};
    const std::string delta{"delta"};
    std::int32_t wrong_key_type = 1;
    REQUIRE(view.contains(ValueView{key_binding, const_cast<std::string *>(&alpha)}));
    REQUIRE_FALSE(view.contains(ValueView{key_binding, const_cast<std::string *>(&delta)}));
    REQUIRE_FALSE(view.contains(ValueView{value_binding, &wrong_key_type}));

    REQUIRE(view.at(ValueView{key_binding, const_cast<std::string *>(&beta)}).checked_as<std::int32_t>() == 2);
    REQUIRE_THROWS_AS(view.at(ValueView{key_binding, const_cast<std::string *>(&delta)}),
                      std::out_of_range);
    REQUIRE_THROWS_AS(view.at(ValueView{value_binding, &wrong_key_type}), std::out_of_range);

    // Iterate; verify we see all three entries (in some order). The
    // map view yields ``std::pair<ValueView, ValueView>`` (key,
    // value) through its KeyValueRange.
    std::int32_t total = 0;
    std::int32_t count = 0;
    for (const auto entry : view)
    {
        total += entry.second.checked_as<std::int32_t>();
        ++count;
    }
    REQUIRE(count == 3);
    REQUIRE(total == (1 + 2 + 3));

    std::int32_t item_total = 0;
    for (const auto entry : view.items()) { item_total += entry.second.checked_as<std::int32_t>(); }
    REQUIRE(item_total == (1 + 2 + 3));

    // Keys-only range.
    std::int32_t key_count = 0;
    for (const auto key : view.keys())
    {
        REQUIRE(key.valid());
        ++key_count;
    }
    REQUIRE(key_count == 3);

    // Values-only range.
    std::int32_t value_total = 0;
    for (const auto val : view.values()) { value_total += val.checked_as<std::int32_t>(); }
    REQUIRE(value_total == (1 + 2 + 3));

    // Order-independent: same map built in different order has the same hash / equals.
    MapBuilder b{key_binding, value_binding};
    b.set_item<std::string, std::int32_t>(std::string{"gamma"}, 3);
    b.set_item<std::string, std::int32_t>(std::string{"alpha"}, 1);
    b.set_item<std::string, std::int32_t>(std::string{"beta"}, 2);
    auto storage_b = b.build_storage();

    const ValueOps &ops = compact_map_ops();
    REQUIRE(ops.hash(&storage_a) == ops.hash(&storage_b));
    REQUIRE(ops.equals(&storage_a, &storage_b));
    REQUIRE(std::is_eq(ops.compare(&storage_a, &storage_b)));

    MapBuilder c{key_binding, value_binding};
    c.set_item<std::string, std::int32_t>(std::string{"gamma"}, 30);
    c.set_item<std::string, std::int32_t>(std::string{"alpha"}, 1);
    c.set_item<std::string, std::int32_t>(std::string{"beta"}, 2);
    auto storage_c = c.build_storage();
    REQUIRE_FALSE(ops.equals(&storage_a, &storage_c));
    REQUIRE(ops.compare(&storage_a, &storage_c) == std::partial_ordering::unordered);
}

TEST_CASE("compact types: same inputs return the same canonical type record")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<std::string>("string");
    const auto int_binding = registry.scalar_type<std::int32_t>();
    const auto str_binding = registry.scalar_type<std::string>();

    REQUIRE(compact_list_type(int_binding) == compact_list_type(int_binding));
    REQUIRE(compact_set_type(int_binding) == compact_set_type(int_binding));
    REQUIRE(compact_map_type(str_binding, int_binding) == compact_map_type(str_binding, int_binding));
    REQUIRE(compact_cyclic_buffer_type(int_binding, 4) == compact_cyclic_buffer_type(int_binding, 4));
    REQUIRE(compact_queue_type(int_binding, 0) == compact_queue_type(int_binding, 0));

    // Different inputs produce different canonical records.
    REQUIRE(compact_list_type(int_binding) != compact_list_type(str_binding));
    REQUIRE(compact_cyclic_buffer_type(int_binding, 4) !=
            compact_cyclic_buffer_type(int_binding, 8));
}

TEST_CASE("MapView::key_set: returns a SetView wrapping the map's keys")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::string>("string");
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto key_binding   = registry.scalar_type<std::string>();
    const auto value_binding = registry.scalar_type<std::int32_t>();
    const auto binding       = compact_map_type(key_binding, value_binding);

    MapBuilder b{key_binding, value_binding};
    b.set_item<std::string, std::int32_t>(std::string{"a"}, 1);
    b.set_item<std::string, std::int32_t>(std::string{"b"}, 2);
    b.set_item<std::string, std::int32_t>(std::string{"c"}, 3);
    auto storage = b.build_storage();

    MapView map_view{ValueView{binding, &storage}};
    SetView keys = map_view.key_set();

    REQUIRE(keys.valid());
    REQUIRE(keys.size() == 3);

    const std::string a{"a"};
    const std::string b_key{"b"};
    const std::string z{"z"};
    REQUIRE(keys.contains(ValueView{key_binding, const_cast<std::string *>(&a)}));
    REQUIRE(keys.contains(ValueView{key_binding, const_cast<std::string *>(&b_key)}));
    REQUIRE_FALSE(keys.contains(ValueView{key_binding, const_cast<std::string *>(&z)}));

    std::int32_t count = 0;
    for (auto member : keys)
    {
        REQUIRE_THROWS_AS(member.checked_mutable_as<std::string>() = "mutated", std::logic_error);
        ++count;
    }
    REQUIRE(count == 3);

    MapBuilder smaller{key_binding, value_binding};
    smaller.set_item<std::string, std::int32_t>(std::string{"a"}, 1);
    smaller.set_item<std::string, std::int32_t>(std::string{"b"}, 2);
    auto smaller_storage = smaller.build_storage();
    SetView smaller_keys = MapView{ValueView{binding, &smaller_storage}}.key_set();

    REQUIRE(std::is_lt(smaller_keys.compare(keys)));
    REQUIRE(std::is_gt(keys.compare(smaller_keys)));
}

TEST_CASE("Value and ValueView expose direct specialized casts for compact containers")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<std::string>("string");
    const auto int_binding = registry.scalar_type<std::int32_t>();
    const auto str_binding = registry.scalar_type<std::string>();

    Value list_value = stdlib::make_list<std::int32_t>({4, 5});
    auto  list_view  = list_value.view();
    const auto indexed_list = list_view.as_indexed_view();
    REQUIRE(indexed_list.size() == 2);
    REQUIRE(indexed_list.at(1).checked_as<std::int32_t>() == 5);
    REQUIRE(list_view.try_as_indexed_view().has_value());
    REQUIRE(list_value.as_indexed_view().size() == 2);
    REQUIRE(list_value.as_list().size() == 2);
    REQUIRE(list_value.as_list().at(1).checked_as<std::int32_t>() == 5);
    REQUIRE(list_value.view().try_as_list().has_value());
    REQUIRE_FALSE(list_value.view().try_as_map().has_value());
    REQUIRE_FALSE(list_value.view().can_begin_mutation());
    REQUIRE_THROWS_AS(list_value.view().begin_mutation(), std::logic_error);
    REQUIRE_THROWS_AS(list_value.as_list().begin_mutation(), std::logic_error);

    Value set_value = stdlib::make_set<std::int32_t>({7, 11});
    std::int32_t seven = 7;
    REQUIRE(set_value.as_set().contains(ValueView{int_binding, &seven}));
    REQUIRE_FALSE(set_value.view().can_begin_mutation());
    REQUIRE_THROWS_AS(set_value.view().begin_mutation(), std::logic_error);

    Value map_value = stdlib::make_map<std::string, std::int32_t>({{"x", 10}});
    const std::string x{"x"};
    REQUIRE(map_value.as_map().at(ValueView{str_binding, const_cast<std::string *>(&x)}).checked_as<std::int32_t>() == 10);
    REQUIRE_FALSE(map_value.view().can_begin_mutation());
    REQUIRE_THROWS_AS(map_value.view().begin_mutation(), std::logic_error);

    Value cyclic_value = stdlib::make_cyclic_buffer<std::int32_t>(2, {1, 2, 3});
    REQUIRE(cyclic_value.as_cyclic_buffer().size() == 2);
    REQUIRE(cyclic_value.as_cyclic_buffer().at(0).checked_as<std::int32_t>() == 2);
    REQUIRE(cyclic_value.as_cyclic_buffer().full());
    REQUIRE_FALSE(cyclic_value.view().can_begin_mutation());
    REQUIRE_THROWS_AS(cyclic_value.as_cyclic_buffer().begin_mutation(), std::logic_error);

    Value queue_value = stdlib::make_queue<std::int32_t>({8, 9}, 2);
    REQUIRE(queue_value.as_queue().front().checked_as<std::int32_t>() == 8);
    REQUIRE(queue_value.as_queue().full());
    REQUIRE_FALSE(queue_value.view().can_begin_mutation());
    REQUIRE_THROWS_AS(queue_value.as_queue().begin_mutation(), std::logic_error);

    Value atomic_value{3};
    REQUIRE_FALSE(atomic_value.view().try_as_indexed_view().has_value());
    REQUIRE_THROWS_AS(atomic_value.view().as_indexed_view(), std::logic_error);
}

TEST_CASE("TupleView, BundleView and fixed ListView read structured MemoryUtils storage")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *str_meta = registry.register_scalar<std::string>("string");

    const auto *tuple_meta = registry.tuple({int_meta, str_meta});
    const auto tuple_binding = factory.type_for(tuple_meta);
    REQUIRE(tuple_binding != nullptr);
    Value tuple_value{tuple_binding};
    TupleView tuple = tuple_value.as_tuple();
    REQUIRE(tuple.size() == 2);
    auto readonly_tuple_child = tuple.at(0);
    REQUIRE_FALSE(readonly_tuple_child.mutable_payload());
    REQUIRE_THROWS_AS(readonly_tuple_child.checked_mutable_as<std::int32_t>() = 42, std::logic_error);
    auto mutable_tuple = tuple_value.as_tuple().begin_mutation();
    mutable_tuple.at(0).checked_mutable_as<std::int32_t>() = 42;
    mutable_tuple.at(1).checked_mutable_as<std::string>() = "forty-two";
    REQUIRE(tuple[0].checked_as<std::int32_t>() == 42);
    REQUIRE(tuple[1].checked_as<std::string>() == "forty-two");
    REQUIRE(tuple_value.to_string() == "(42, forty-two)");

    const auto *bundle_meta = registry.bundle("SpecializedViewBundle", {{"count", int_meta}, {"name", str_meta}});
    const auto bundle_binding = factory.type_for(bundle_meta);
    REQUIRE(bundle_binding != nullptr);
    Value bundle_value{bundle_binding};
    BundleView bundle = bundle_value.as_bundle();
    REQUIRE(bundle.size() == 2);
    REQUIRE(bundle.has_field("count"));
    REQUIRE_FALSE(bundle.has_field("missing"));
    auto readonly_bundle_child = bundle["count"];
    REQUIRE_FALSE(readonly_bundle_child.mutable_payload());
    REQUIRE_THROWS_AS(readonly_bundle_child.checked_mutable_as<std::int32_t>() = 3, std::logic_error);
    auto mutable_bundle = bundle_value.as_bundle().begin_mutation();
    mutable_bundle["count"].checked_mutable_as<std::int32_t>() = 3;
    mutable_bundle["name"].checked_mutable_as<std::string>() = "items";
    REQUIRE(bundle.field("count").checked_as<std::int32_t>() == 3);
    REQUIRE(bundle.at("count").checked_as<std::int32_t>() == 3);
    REQUIRE(bundle.at("name").checked_as<std::string>() == "items");
    REQUIRE(bundle_value.to_string() == "{count: 3, name: items}");

    const auto *fixed_list_meta = registry.list(int_meta, 3);
    const auto fixed_list_binding = factory.type_for(fixed_list_meta);
    REQUIRE(fixed_list_binding != nullptr);
    Value fixed_list_value{fixed_list_binding};
    ListView fixed_list = fixed_list_value.as_list();
    REQUIRE(fixed_list.size() == 3);
    REQUIRE(fixed_list.is_fixed());
    auto readonly_list_child = fixed_list.at(0);
    REQUIRE_FALSE(readonly_list_child.mutable_payload());
    REQUIRE_THROWS_AS(readonly_list_child.checked_mutable_as<std::int32_t>() = 1, std::logic_error);
    auto mutable_fixed_list = fixed_list_value.as_list().begin_mutation();
    mutable_fixed_list.at(0).checked_mutable_as<std::int32_t>() = 1;
    mutable_fixed_list.at(1).checked_mutable_as<std::int32_t>() = 2;
    mutable_fixed_list.at(2).checked_mutable_as<std::int32_t>() = 3;
    for (auto element : mutable_fixed_list) { element.checked_mutable_as<std::int32_t>() += 10; }
    std::int32_t sum = 0;
    for (const auto element : fixed_list) { sum += element.checked_as<std::int32_t>(); }
    REQUIRE(sum == 36);
    REQUIRE(fixed_list_value.to_string() == "[11, 12, 13]");
}

TEST_CASE("ValueView semantic fallback compares fixed and compact lists by index")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    const auto *fixed_list_meta = registry.list(int_meta, 3);
    const auto fixed_list_binding = factory.type_for(fixed_list_meta);
    REQUIRE(fixed_list_binding != nullptr);

    Value fixed{fixed_list_binding};
    auto fixed_mutation = fixed.as_list().begin_mutation();
    fixed_mutation.at(0).checked_mutable_as<std::int32_t>() = 1;
    fixed_mutation.at(1).checked_mutable_as<std::int32_t>() = 2;
    fixed_mutation.at(2).checked_mutable_as<std::int32_t>() = 3;

    Value same      = stdlib::make_list<std::int32_t>({1, 2, 3});
    Value different = stdlib::make_list<std::int32_t>({1, 2, 4});

    REQUIRE(fixed.binding() != same.binding());
    REQUIRE(fixed.view().equals(same.view()));
    REQUIRE(std::is_eq(fixed.view().compare(same.view())));
    REQUIRE_FALSE(fixed.view().equals(different.view()));
    REQUIRE(std::is_lt(fixed.view().compare(different.view())));
}

TEST_CASE("ValueView semantic fallback compares named and structural bundles by index")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *str_meta = registry.register_scalar<std::string>("string");

    const auto fields = std::vector<std::pair<std::string, const ValueTypeMetaData *>>{
        {"count", int_meta},
        {"name", str_meta},
    };
    const auto *structural_meta = registry.un_named_bundle(fields);
    const auto *named_meta      = registry.bundle("SemanticFallbackBundle", fields);
    const auto structural_binding = factory.type_for(structural_meta);
    const auto named_binding      = factory.type_for(named_meta);
    REQUIRE(structural_binding != nullptr);
    REQUIRE(named_binding != nullptr);

    Value structural{structural_binding};
    auto structural_mutation = structural.as_bundle().begin_mutation();
    structural_mutation.at("count").checked_mutable_as<std::int32_t>() = 7;
    structural_mutation.at("name").checked_mutable_as<std::string>() = "seven";

    Value named{named_binding};
    auto named_mutation = named.as_bundle().begin_mutation();
    named_mutation.at("count").checked_mutable_as<std::int32_t>() = 7;
    named_mutation.at("name").checked_mutable_as<std::string>() = "seven";

    REQUIRE(structural.binding() != named.binding());
    REQUIRE(structural.view().equals(named.view()));
    REQUIRE(std::is_eq(structural.view().compare(named.view())));

    named.as_bundle().begin_mutation().at("count").checked_mutable_as<std::int32_t>() = 8;
    REQUIRE_FALSE(structural.view().equals(named.view()));
    REQUIRE(std::is_lt(structural.view().compare(named.view())));
}

TEST_CASE("ValueView semantic fallback compares set-compatible lookup surfaces")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::string>("string");
    (void)registry.register_scalar<std::int32_t>("int32");

    Value   map_value = stdlib::make_map<std::string, std::int32_t>({{"a", 1}, {"b", 2}});
    SetView keys = map_value.as_map().key_set();

    Value set_value = stdlib::make_set<std::string>({"b", "a"});
    Value different = stdlib::make_set<std::string>({"a", "c"});

    REQUIRE(keys.binding() != set_value.binding());
    REQUIRE(keys.equals(set_value.view()));
    REQUIRE(std::is_eq(keys.compare(set_value.view())));
    REQUIRE_FALSE(keys.equals(different.view()));
    REQUIRE(keys.compare(different.view()) == std::partial_ordering::unordered);

    Value smaller = stdlib::make_set<std::string>({"a"});
    REQUIRE(std::is_lt(smaller.view().compare(keys)));
    REQUIRE(std::is_gt(keys.compare(smaller.view())));
}

TEST_CASE("ValueView semantic fallback compares maps with equivalent value layouts")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    auto       &factory  = ValuePlanFactory::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<std::string>("string");
    const auto key_binding = registry.scalar_type<std::string>();

    const auto *fixed_list_meta    = registry.list(int_meta, 3);
    const auto fixed_list_binding = factory.type_for(fixed_list_meta);
    REQUIRE(fixed_list_binding != nullptr);

    auto make_fixed_list = [&](std::int32_t a, std::int32_t b, std::int32_t c) {
        Value value{fixed_list_binding};
        auto mutation = value.as_list().begin_mutation();
        mutation.at(0).checked_mutable_as<std::int32_t>() = a;
        mutation.at(1).checked_mutable_as<std::int32_t>() = b;
        mutation.at(2).checked_mutable_as<std::int32_t>() = c;
        return value;
    };

    auto make_compact_list = [&](std::int32_t a, std::int32_t b, std::int32_t c) {
        return stdlib::make_list<std::int32_t>({a, b, c});
    };

    const std::string alpha{"alpha"};
    const std::string beta{"beta"};

    Value fixed_alpha = make_fixed_list(1, 2, 3);
    Value fixed_beta  = make_fixed_list(4, 5, 6);
    MapBuilder fixed_map_builder{key_binding, fixed_list_binding};
    fixed_map_builder.set_item_copy(&alpha, fixed_alpha.view().data());
    fixed_map_builder.set_item_copy(&beta, fixed_beta.view().data());
    Value fixed_map = fixed_map_builder.build();

    Value compact_alpha = make_compact_list(1, 2, 3);
    Value compact_beta  = make_compact_list(4, 5, 6);
    MapBuilder compact_map_builder{key_binding, compact_alpha.binding()};
    compact_map_builder.set_item_copy(&beta, compact_beta.view().data());
    compact_map_builder.set_item_copy(&alpha, compact_alpha.view().data());
    Value compact_map = compact_map_builder.build();

    Value different_beta = make_compact_list(4, 5, 7);
    MapBuilder different_map_builder{key_binding, compact_alpha.binding()};
    different_map_builder.set_item_copy(&alpha, compact_alpha.view().data());
    different_map_builder.set_item_copy(&beta, different_beta.view().data());
    Value different_map = different_map_builder.build();

    REQUIRE(fixed_map.binding() != compact_map.binding());
    REQUIRE(fixed_map.view().equals(compact_map.view()));
    REQUIRE(std::is_eq(fixed_map.view().compare(compact_map.view())));
    REQUIRE_FALSE(fixed_map.view().equals(different_map.view()));
    REQUIRE(fixed_map.view().compare(different_map.view()) == std::partial_ordering::unordered);
}
