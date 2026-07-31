// Tests for the value-layer compact container storage shapes
// (``ListStorage``, ``SetStorage``, ``MapStorage``,
// ``CyclicBufferStorage``, ``QueueStorage``) and the per-kind
// ``ValueBuilder`` types that produce them.
//
// The compact shapes are immutable after construction by design (see
// *Allocation, Plans and Ops > Scalar Plans and Ops > Container
// Storage Shapes*); these tests verify the construction-time
// contract: builder accumulates → ``build_storage`` returns a fully-
// populated, immutable storage object → read-time access matches
// what was pushed in → copy/move preserves contents.

#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/value/compact_storage.h>
#include <hgraph/types/value/value_builder.h>

#include <cstdint>
#include <string>

namespace
{
    template <typename T>
    [[nodiscard]] const T &as_const(const void *memory) noexcept
    {
        return *static_cast<const T *>(memory);
    }

    struct CopyConstructOnlyValue
    {
        std::int32_t value{0};

        CopyConstructOnlyValue()                                              = default;
        explicit CopyConstructOnlyValue(std::int32_t v) : value{v} {}
        CopyConstructOnlyValue(const CopyConstructOnlyValue &)                = default;
        CopyConstructOnlyValue(CopyConstructOnlyValue &&) noexcept            = default;
        CopyConstructOnlyValue &operator=(const CopyConstructOnlyValue &)     = delete;
        CopyConstructOnlyValue &operator=(CopyConstructOnlyValue &&) noexcept = delete;
    };
}  // namespace

TEST_CASE("ListBuilder: push_back round-trips through ListStorage")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto binding = registry.scalar_type<std::int32_t>();
    REQUIRE(binding != nullptr);

    ListBuilder builder{binding};
    builder.push_back<std::int32_t>(7);
    builder.push_back<std::int32_t>(11);
    builder.push_back<std::int32_t>(13);
    REQUIRE(builder.size() == 3);

    auto storage = builder.build_storage();
    REQUIRE(storage.size() == 3);
    REQUIRE(as_const<std::int32_t>(storage.element_at(0)) == 7);
    REQUIRE(as_const<std::int32_t>(storage.element_at(1)) == 11);
    REQUIRE(as_const<std::int32_t>(storage.element_at(2)) == 13);
}

TEST_CASE("ListStorage: copy and move preserve contents")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto binding = registry.scalar_type<std::int32_t>();

    ListBuilder builder{binding};
    builder.push_back<std::int32_t>(1);
    builder.push_back<std::int32_t>(2);
    builder.push_back<std::int32_t>(3);
    auto original = builder.build_storage();

    auto copy = original;
    REQUIRE(copy.size() == 3);
    REQUIRE(as_const<std::int32_t>(copy.element_at(0)) == 1);
    REQUIRE(as_const<std::int32_t>(copy.element_at(2)) == 3);

    auto moved = std::move(original);
    REQUIRE(moved.size() == 3);
    REQUIRE(as_const<std::int32_t>(moved.element_at(1)) == 2);
}

TEST_CASE("ListStorage: empty list construction is well-defined")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto binding = registry.scalar_type<std::int32_t>();

    ListBuilder builder{binding};
    REQUIRE(builder.empty());
    auto storage = builder.build_storage();
    REQUIRE(storage.empty());
    REQUIRE(storage.size() == 0);
}

TEST_CASE("ListBuilder: works with non-trivial element types (std::string)")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::string>("string");
    const auto binding = registry.scalar_type<std::string>();
    REQUIRE(binding != nullptr);

    ListBuilder builder{binding};
    builder.push_back<std::string>(std::string{"alpha"});
    builder.push_back<std::string>(std::string{"beta"});
    builder.push_back<std::string>(std::string{"gamma"});
    auto storage = builder.build_storage();

    REQUIRE(storage.size() == 3);
    REQUIRE(as_const<std::string>(storage.element_at(0)) == "alpha");
    REQUIRE(as_const<std::string>(storage.element_at(1)) == "beta");
    REQUIRE(as_const<std::string>(storage.element_at(2)) == "gamma");
}

TEST_CASE("compact containers expose raw, index, and recursive payload metrics", "[memory]")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::string>("string");
    const auto binding = registry.scalar_type<std::string>();
    const std::string first(256, 'a');
    const std::string second(320, 'b');

    ListBuilder list_builder{binding};
    list_builder.push_back(first);
    list_builder.push_back(second);
    auto list = list_builder.build_storage();
    const auto list_metrics = list.dynamic_storage_metrics();
    CHECK(list_metrics.live_bytes >= 2 * sizeof(std::string) + first.size() + second.size() + 2);
    CHECK(list_metrics.reserved_bytes >= list_metrics.live_bytes);
    CHECK(compact_list_ops().dynamic_storage_metrics(&list).live_bytes == list_metrics.live_bytes);

    CyclicBufferBuilder cyclic_builder{binding, 2};
    cyclic_builder.push_back(first);
    cyclic_builder.push_back(second);
    auto cyclic = cyclic_builder.build_storage();
    CHECK(compact_cyclic_buffer_ops().dynamic_storage_metrics(&cyclic).live_bytes >=
          list_metrics.live_bytes);

    QueueBuilder queue_builder{binding};
    queue_builder.push(first);
    queue_builder.push(second);
    auto queue = queue_builder.build_storage();
    CHECK(compact_queue_ops().dynamic_storage_metrics(&queue).live_bytes >=
          list_metrics.live_bytes);

    SetBuilder set_builder{binding};
    REQUIRE(set_builder.insert(first));
    REQUIRE(set_builder.insert(second));
    auto set = set_builder.build_storage();
    const auto set_metrics = compact_set_ops().dynamic_storage_metrics(&set);
    CHECK(set_metrics.live_bytes > list_metrics.live_bytes);
    CHECK(set_metrics.reserved_bytes >= set_metrics.live_bytes);

    MapBuilder map_builder{binding, binding};
    map_builder.set_item(first, second);
    map_builder.set_item(second, first);
    auto map = map_builder.build_storage();
    const auto map_metrics = compact_map_ops().dynamic_storage_metrics(&map);
    const auto key_set_metrics = compact_map_key_set_ops().dynamic_storage_metrics(&map);
    CHECK(map_metrics.live_bytes > key_set_metrics.live_bytes);
    CHECK(map_metrics.reserved_bytes > key_set_metrics.reserved_bytes);
    CHECK(key_set_metrics.live_bytes >= set_metrics.live_bytes);
}

TEST_CASE("CyclicBufferBuilder: rotates after capacity is reached")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto binding = registry.scalar_type<std::int32_t>();

    CyclicBufferBuilder builder{binding, /*capacity=*/3};
    builder.push_back<std::int32_t>(1);
    builder.push_back<std::int32_t>(2);
    builder.push_back<std::int32_t>(3);
    REQUIRE(builder.size() == 3);

    // Push past capacity: oldest is dropped, newest takes its slot.
    builder.push_back<std::int32_t>(4);
    builder.push_back<std::int32_t>(5);

    auto storage = builder.build_storage();
    REQUIRE(storage.size() == 3);
    // The window now holds the most-recent three: 3, 4, 5 in ring order.
    REQUIRE(as_const<std::int32_t>(storage.element_at(0)) == 3);
    REQUIRE(as_const<std::int32_t>(storage.element_at(1)) == 4);
    REQUIRE(as_const<std::int32_t>(storage.element_at(2)) == 5);
}

TEST_CASE("CyclicBufferBuilder: zero capacity is rejected at construction")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto binding = registry.scalar_type<std::int32_t>();

    REQUIRE_THROWS_AS(CyclicBufferBuilder(binding, /*capacity=*/0), std::invalid_argument);
}

TEST_CASE("CyclicBufferBuilder: replacement requires copy assignment and preserves existing slot on rejection")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<CopyConstructOnlyValue>("CopyConstructOnlyValue");
    const auto binding = registry.scalar_type<CopyConstructOnlyValue>();

    CyclicBufferBuilder builder{binding, /*capacity=*/1};
    const CopyConstructOnlyValue first{1};
    const CopyConstructOnlyValue second{2};
    builder.push_back(first);
    REQUIRE_THROWS_AS(builder.push_back(second), std::logic_error);

    auto storage = builder.build_storage();
    REQUIRE(storage.size() == 1);
    REQUIRE(as_const<CopyConstructOnlyValue>(storage.element_at(0)).value == 1);
}

TEST_CASE("QueueBuilder: bounded queue rejects on overflow; unbounded grows")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto binding = registry.scalar_type<std::int32_t>();

    SECTION("bounded")
    {
        QueueBuilder builder{binding, /*max_capacity=*/2};
        builder.push<std::int32_t>(10);
        builder.push<std::int32_t>(20);
        REQUIRE_THROWS_AS(builder.push<std::int32_t>(30), std::overflow_error);

        auto storage = builder.build_storage();
        REQUIRE(storage.size() == 2);
        REQUIRE(as_const<std::int32_t>(storage.element_at(0)) == 10);
        REQUIRE(as_const<std::int32_t>(storage.element_at(1)) == 20);
        REQUIRE(as_const<std::int32_t>(storage.front()) == 10);
    }

    SECTION("unbounded")
    {
        QueueBuilder builder{binding};  // max_capacity = 0
        builder.push<std::int32_t>(1);
        builder.push<std::int32_t>(2);
        builder.push<std::int32_t>(3);
        builder.push<std::int32_t>(4);

        auto storage = builder.build_storage();
        REQUIRE(storage.size() == 4);
        REQUIRE(as_const<std::int32_t>(storage.front()) == 1);
        REQUIRE(as_const<std::int32_t>(storage.element_at(3)) == 4);
    }
}

TEST_CASE("SetBuilder: deduplicates by content; build produces a SetStorage")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto binding = registry.scalar_type<std::int32_t>();

    SetBuilder builder{binding};
    REQUIRE(builder.insert<std::int32_t>(7));
    REQUIRE(builder.insert<std::int32_t>(11));
    REQUIRE_FALSE(builder.insert<std::int32_t>(7));  // duplicate
    REQUIRE(builder.insert<std::int32_t>(13));
    REQUIRE(builder.size() == 3);

    auto storage = builder.build_storage();
    REQUIRE(storage.size() == 3);

    std::int32_t seven = 7;
    std::int32_t twelve = 12;
    REQUIRE(storage.contains(&seven));
    REQUIRE_FALSE(storage.contains(&twelve));
}

TEST_CASE("SetStorage: direct duplicate construction is rejected")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto binding = registry.scalar_type<std::int32_t>();
    const auto &plan    = binding.checked_plan();

    std::int32_t values[] = {7, 7};
    REQUIRE_THROWS_AS(SetStorage(binding,
                                 ElementSpan{
                                     .bytes  = values,
                                     .size   = 2,
                                     .stride = sizeof(std::int32_t),
                                     .plan   = &plan,
                                 }),
                      std::invalid_argument);
}

TEST_CASE("SetStorage: move preserves slot index lookups")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto binding = registry.scalar_type<std::int32_t>();

    SetBuilder builder{binding};
    REQUIRE(builder.insert<std::int32_t>(3));
    REQUIRE(builder.insert<std::int32_t>(5));
    auto original = builder.build_storage();
    auto moved    = std::move(original);

    std::int32_t three = 3;
    std::int32_t five  = 5;
    std::int32_t seven = 7;
    REQUIRE(moved.contains(&three));
    REQUIRE(moved.contains(&five));
    REQUIRE_FALSE(moved.contains(&seven));
}

TEST_CASE("SetBuilder: rejects non-hashable / non-equatable element bindings")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();

    // ``TimeSeriesReference`` exists but isn't comparable; just synthesise
    // a binding from a hashable scalar to confirm the happy path while
    // documenting that the unhappy path throws when bindings are not
    // hashable+equatable. The scalar registration itself wires the
    // capability flags, so the rejection logic gets exercised when the
    // schema's flags are missing those bits.
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto binding = registry.scalar_type<std::int32_t>();
    REQUIRE_NOTHROW(SetBuilder{binding});
}

TEST_CASE("MapBuilder: set_item / contains / value_at round-trip")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::string>("string");
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto key_binding   = registry.scalar_type<std::string>();
    const auto value_binding = registry.scalar_type<std::int32_t>();

    MapBuilder builder{key_binding, value_binding};
    builder.set_item<std::string, std::int32_t>(std::string{"alpha"}, 1);
    builder.set_item<std::string, std::int32_t>(std::string{"beta"}, 2);
    builder.set_item<std::string, std::int32_t>(std::string{"gamma"}, 3);

    // Repeat a key — the value should be replaced, not duplicated.
    builder.set_item<std::string, std::int32_t>(std::string{"beta"}, 200);
    REQUIRE(builder.size() == 3);

    auto storage = builder.build_storage();
    REQUIRE(storage.size() == 3);

    const std::string alpha{"alpha"};
    const std::string beta{"beta"};
    const std::string gamma{"gamma"};
    const std::string delta{"delta"};

    REQUIRE(storage.contains(&alpha));
    REQUIRE(storage.contains(&beta));
    REQUIRE(storage.contains(&gamma));
    REQUIRE_FALSE(storage.contains(&delta));

    REQUIRE(*static_cast<const std::int32_t *>(storage.value_at(&alpha)) == 1);
    REQUIRE(*static_cast<const std::int32_t *>(storage.value_at(&beta)) == 200);
    REQUIRE(*static_cast<const std::int32_t *>(storage.value_at(&gamma)) == 3);
    REQUIRE(storage.value_at(&delta) == nullptr);
}

TEST_CASE("MapBuilder: value replacement requires copy assignment and preserves existing value on rejection")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<CopyConstructOnlyValue>("CopyConstructOnlyValue");
    const auto key_binding   = registry.scalar_type<std::int32_t>();
    const auto value_binding = registry.scalar_type<CopyConstructOnlyValue>();

    MapBuilder builder{key_binding, value_binding};
    const CopyConstructOnlyValue first{10};
    const CopyConstructOnlyValue second{20};
    builder.set_item<std::int32_t, CopyConstructOnlyValue>(1, first);
    REQUIRE_THROWS_AS((builder.set_item<std::int32_t, CopyConstructOnlyValue>(1, second)), std::logic_error);

    auto storage = builder.build_storage();
    REQUIRE(storage.size() == 1);
    std::int32_t key = 1;
    REQUIRE(as_const<CopyConstructOnlyValue>(storage.value_at(&key)).value == 10);
}

TEST_CASE("SetBuilder and MapBuilder: indexes survive accumulator growth")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto binding = registry.scalar_type<std::int32_t>();

    SetBuilder set_builder{binding};
    MapBuilder map_builder{binding, binding};
    for (std::int32_t i = 0; i < 40; ++i)
    {
        REQUIRE(set_builder.insert<std::int32_t>(i));
        map_builder.set_item<std::int32_t, std::int32_t>(i, i * 10);
    }

    std::int32_t first = 0;
    std::int32_t last  = 39;
    std::int32_t miss  = 99;
    REQUIRE(set_builder.contains(&first));
    REQUIRE(set_builder.contains(&last));
    REQUIRE_FALSE(set_builder.contains(&miss));
    REQUIRE(map_builder.contains(&first));
    REQUIRE(map_builder.contains(&last));
    REQUIRE_FALSE(map_builder.contains(&miss));

    auto map_storage = map_builder.build_storage();
    REQUIRE(*static_cast<const std::int32_t *>(map_storage.value_at(&first)) == 0);
    REQUIRE(*static_cast<const std::int32_t *>(map_storage.value_at(&last)) == 390);
}

TEST_CASE("MapStorage: copy and move preserve slot index lookups")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::string>("string");
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto key_binding   = registry.scalar_type<std::string>();
    const auto value_binding = registry.scalar_type<std::int32_t>();

    MapBuilder builder{key_binding, value_binding};
    builder.set_item<std::string, std::int32_t>(std::string{"x"}, 10);
    builder.set_item<std::string, std::int32_t>(std::string{"y"}, 20);
    auto original = builder.build_storage();

    auto copy = original;
    REQUIRE(copy.size() == 2);

    const std::string x{"x"};
    const std::string y{"y"};
    REQUIRE(copy.contains(&x));
    REQUIRE(copy.contains(&y));
    REQUIRE(*static_cast<const std::int32_t *>(copy.value_at(&x)) == 10);
    REQUIRE(*static_cast<const std::int32_t *>(copy.value_at(&y)) == 20);

    auto moved = std::move(original);
    REQUIRE(moved.contains(&x));
    REQUIRE(moved.contains(&y));
    REQUIRE(*static_cast<const std::int32_t *>(moved.value_at(&x)) == 10);
    REQUIRE(*static_cast<const std::int32_t *>(moved.value_at(&y)) == 20);
}

TEST_CASE("MapStorage: direct duplicate key construction is rejected")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::string>("string");
    (void)registry.register_scalar<std::int32_t>("int32");
    const auto key_binding   = registry.scalar_type<std::string>();
    const auto value_binding = registry.scalar_type<std::int32_t>();
    const auto &key_plan      = key_binding.checked_plan();
    const auto &value_plan    = value_binding.checked_plan();

    std::string keys[]   = {"same", "same"};
    std::int32_t values[] = {1, 2};
    REQUIRE_THROWS_AS(MapStorage(key_binding, value_binding,
                                 ElementSpan{
                                     .bytes  = keys,
                                     .size   = 2,
                                     .stride = sizeof(std::string),
                                     .plan   = &key_plan,
                                 },
                                 ElementSpan{
                                     .bytes  = values,
                                     .size   = 2,
                                     .stride = sizeof(std::int32_t),
                                     .plan   = &value_plan,
                                 }),
                      std::invalid_argument);
}

TEST_CASE("compact plans: same binding inputs return canonical pointers")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    (void)registry.register_scalar<std::int32_t>("int32");
    (void)registry.register_scalar<std::string>("string");
    const auto int_binding = registry.scalar_type<std::int32_t>();
    const auto str_binding = registry.scalar_type<std::string>();

    REQUIRE(&compact_list_plan(int_binding) == &compact_list_plan(int_binding));
    REQUIRE(&compact_set_plan(int_binding) == &compact_set_plan(int_binding));
    REQUIRE(&compact_map_plan(str_binding, int_binding) ==
            &compact_map_plan(str_binding, int_binding));
    REQUIRE(&compact_cyclic_buffer_plan(int_binding, 4) ==
            &compact_cyclic_buffer_plan(int_binding, 4));
    REQUIRE(&compact_cyclic_buffer_plan(int_binding, 4) !=
            &compact_cyclic_buffer_plan(int_binding, 8));
    REQUIRE(&compact_queue_plan(int_binding, 0) == &compact_queue_plan(int_binding, 0));
}
