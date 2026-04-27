#include <catch2/catch_test_macros.hpp>

#include <hgraph/v2/types/value/value.h>

#include <compare>
#include <new>
#include <ostream>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph::v2;

    static_assert(MemoryUtils::storage_binding<ValueTypeBinding>);

    struct AllocationProbe
    {
        static inline int allocations{0};
        static inline int deallocations{0};

        static void reset() {
            allocations   = 0;
            deallocations = 0;
        }
    };

    void *tracked_allocate(MemoryUtils::StorageLayout layout) {
        ++AllocationProbe::allocations;
        return ::operator new(layout.size == 0 ? 1 : layout.size, std::align_val_t{layout.alignment});
    }

    void tracked_deallocate(void *memory, MemoryUtils::StorageLayout layout) noexcept {
        ++AllocationProbe::deallocations;
        ::operator delete(memory, std::align_val_t{layout.alignment});
    }

    struct CopyConstructOnlyValue
    {
        int value{0};

        CopyConstructOnlyValue()                                              = default;
        CopyConstructOnlyValue(const CopyConstructOnlyValue &)                = default;
        CopyConstructOnlyValue(CopyConstructOnlyValue &&) noexcept            = default;
        CopyConstructOnlyValue &operator=(const CopyConstructOnlyValue &)     = delete;
        CopyConstructOnlyValue &operator=(CopyConstructOnlyValue &&) noexcept = delete;

        [[nodiscard]] bool operator==(const CopyConstructOnlyValue &) const noexcept = default;
    };
}  // namespace

namespace std
{
    template <> struct hash<CopyConstructOnlyValue>
    {
        [[nodiscard]] size_t operator()(const CopyConstructOnlyValue &value) const noexcept { return hash<int>{}(value.value); }
    };
}  // namespace std

std::ostream &operator<<(std::ostream &stream, const CopyConstructOnlyValue &value) {
    stream << value.value;
    return stream;
}

TEST_CASE("scalar value builders bridge v2 metadata to storage and view ops", "[v2 value]") {
    const ValueTypeMetaData *int_meta = value::scalar_type_meta<int>();
    REQUIRE(int_meta == TypeRegistry::instance().register_scalar<int>("int"));

    const ValueBuilder     &int_builder = value::scalar_value_builder<int>();
    const ValueTypeBinding &int_binding =
        ValueTypeBinding::intern(*int_meta, MemoryUtils::plan_for<int>(), scalar_value_ops<int>());

    REQUIRE(int_builder.type() == int_meta);
    REQUIRE(int_builder.plan() == &MemoryUtils::plan_for<int>());
    REQUIRE(int_builder.binding() == &int_binding);
    REQUIRE(int_binding.type_meta == int_meta);
    REQUIRE(int_binding.plan() == &MemoryUtils::plan_for<int>());
    REQUIRE(int_binding.lifecycle() == &MemoryUtils::plan_for<int>().lifecycle);
    REQUIRE(int_binding.lifecycle_context() == MemoryUtils::plan_for<int>().lifecycle_context);
    REQUIRE(int_binding.ops == &scalar_value_ops<int>());
    REQUIRE(ValueTypeBinding::find(int_meta, &MemoryUtils::plan_for<int>(), &scalar_value_ops<int>()) == &int_binding);
    REQUIRE(int_builder.ops() != nullptr);
    REQUIRE(ValueBuilder::find(int_meta) == &int_builder);

    const ValueTypeMetaData *string_meta = value::scalar_type_meta<std::string>();
    REQUIRE(ValueBuilder::find(string_meta) == &value::scalar_value_builder<std::string>());
}

TEST_CASE("scalar values collapse onto their bound storage handle", "[v2 value]") {
    using BoundStorage = MemoryUtils::StorageHandle<MemoryUtils::InlineStoragePolicy<>, ValueTypeBinding>;

    REQUIRE(sizeof(BoundStorage) == sizeof(void *) * 3);
    REQUIRE(sizeof(Value) == sizeof(BoundStorage));

    Value value(*value::scalar_type_meta<int>());
    REQUIRE(value.binding() == value::scalar_value_builder<int>().binding());
}

TEST_CASE("scalar values default construct and expose typed views", "[v2 value]") {
    Value number(*value::scalar_type_meta<int>());

    REQUIRE(number.has_value());
    REQUIRE(number.type() == value::scalar_type_meta<int>());
    REQUIRE(number.view().binding() == value::scalar_value_builder<int>().binding());
    REQUIRE(number.view().as<int>() == 0);

    number.view().set(42);
    CHECK(number.as<int>() == 42);
    CHECK(number.hash() == std::hash<int>{}(42));
    CHECK(number.to_string() == "42");
    CHECK(number == value::value_for(42));

    Value from_helper = value::value_for(7);
    REQUIRE(from_helper.as<int>() == 7);

    Value explicit_schema = value::value_for(*value::scalar_type_meta<int>(), 9);
    REQUIRE(explicit_schema.as<int>() == 9);
}

TEST_CASE("scalar values copy from views and reset to defaults", "[v2 value]") {
    Value text(std::string("alpha"));

    REQUIRE(text.as<std::string>() == "alpha");

    Value copied(text.view());
    REQUIRE(copied.as<std::string>() == "alpha");

    text.view().set(std::string("beta"));
    copied.view().copy_from(text.view());
    REQUIRE(copied.as<std::string>() == "beta");

    Value cloned = copied;
    REQUIRE(cloned.as<std::string>() == "beta");

    text.reset();
    REQUIRE(text.as<std::string>().empty());
}

TEST_CASE("scalar views enforce schema identity and can bridge raw registered scalars", "[v2 value]") {
    const ValueTypeMetaData *raw_meta     = TypeRegistry::instance().register_scalar<long double>("long double");
    const ValueTypeMetaData *bridged_meta = value::scalar_type_meta<long double>("long double");

    REQUIRE(raw_meta == bridged_meta);

    Value precise(*raw_meta);
    precise.view().set(1.25L);

    REQUIRE(precise.view().try_as<double>() == nullptr);
    REQUIRE_THROWS_AS(static_cast<void>(precise.view().checked_as<double>()), std::logic_error);

    Value text(std::string("text"));
    REQUIRE_FALSE(precise.view().equals(text.view()));
    CHECK((precise.view() <=> text.view()) == std::partial_ordering::unordered);
    REQUIRE_THROWS_AS(precise.view().copy_from(text.view()), std::logic_error);
    REQUIRE_THROWS_AS(precise.view().set(std::string("bad")), std::logic_error);
}

TEST_CASE("scalar values compare and assign through matching bindings only", "[v2 value]") {
    Value lower = value::value_for(1);
    Value upper = value::value_for(2);

    CHECK(std::is_lt(lower <=> upper));
    CHECK(std::is_gt(upper.view() <=> lower.view()));
    CHECK_FALSE(lower.equals(upper));

    Value assigned(*value::scalar_type_meta<int>());
    assigned = upper;
    REQUIRE(assigned.as<int>() == 2);

    Value unbound;
    unbound = lower;
    REQUIRE(unbound.as<int>() == 1);

    Value mismatch(*value::scalar_type_meta<double>());
    REQUIRE_THROWS_AS(mismatch = lower, std::invalid_argument);
    REQUIRE_THROWS_AS(mismatch = value::value_for(3), std::invalid_argument);

    ValueView empty_int = ValueView::invalid_for(*value::scalar_value_builder<int>().binding());
    CHECK((empty_int <=> empty_int) == std::partial_ordering::equivalent);
    CHECK((empty_int <=> lower.view()) == std::partial_ordering::unordered);
}

TEST_CASE("scalar values honor custom allocators for heap-backed payloads", "[v2 value]") {
    AllocationProbe::reset();

    const MemoryUtils::AllocatorOps allocator{
        .allocate   = &tracked_allocate,
        .deallocate = &tracked_deallocate,
    };

    {
        Value text(*value::scalar_type_meta<std::string>(), allocator);

        REQUIRE(&text.allocator() == &allocator);
        REQUIRE(text.as<std::string>().empty());
        REQUIRE(AllocationProbe::allocations == 1);

        text.view().set(std::string("owned"));
        REQUIRE(text.as<std::string>() == "owned");

        text.reset();
        REQUIRE(text.as<std::string>().empty());
        REQUIRE(&text.allocator() == &allocator);
        REQUIRE(AllocationProbe::allocations == 2);
        REQUIRE(AllocationProbe::deallocations == 1);
    }

    REQUIRE(AllocationProbe::deallocations == 2);
}

TEST_CASE("scalar views require copy-assign support for copy_from", "[v2 value]") {
    const ValueTypeMetaData *meta = value::scalar_type_meta<CopyConstructOnlyValue>("CopyConstructOnlyValue");

    Value src(*meta);
    Value dst(*meta);

    src.as<CopyConstructOnlyValue>().value = 41;
    dst.as<CopyConstructOnlyValue>().value = 3;

    REQUIRE_THROWS_AS(dst.view().copy_from(src.view()), std::logic_error);
}

TEST_CASE("tuple and bundle views expose structured child access", "[v2 value]") {
    TypeRegistry &registry = TypeRegistry::instance();

    const auto *int_meta    = value::scalar_type_meta<int>();
    const auto *string_meta = value::scalar_type_meta<std::string>();
    const auto *tuple_meta  = registry.tuple({int_meta, string_meta});

    REQUIRE(ValueBuilder::find(tuple_meta) == nullptr);

    Value tuple_value(*tuple_meta);
    auto  tuple_view = tuple_value.as_tuple();

    tuple_view.set(0, value::value_for(42).view());
    Value label(std::string("alpha"));
    tuple_view.set(1, label.view());

    REQUIRE(tuple_view.size() == 2);
    CHECK(tuple_view[0].as<int>() == 42);
    CHECK(tuple_view[1].as<std::string>() == "alpha");
    CHECK(tuple_value.to_string() == "(42, alpha)");
    REQUIRE(ValueBuilder::find(tuple_meta) != nullptr);

    const auto *bundle_meta = registry.bundle({{"count", int_meta}, {"label", string_meta}}, "Pair");
    Value       bundle_value(*bundle_meta);
    auto        bundle_view = bundle_value.as_bundle();

    bundle_view.set("count", value::value_for(3).view());
    bundle_view.set("label", value::value_for(std::string("beta")).view());

    REQUIRE(bundle_view.field_count() == 2);
    CHECK(bundle_view.has_field("count"));
    CHECK(bundle_view["count"].as<int>() == 3);
    CHECK(bundle_view["label"].as<std::string>() == "beta");
    CHECK(bundle_value.to_string() == "Pair{count: 3, label: beta}");
}

TEST_CASE("list views support fixed and dynamic schemas", "[v2 value]") {
    TypeRegistry &registry = TypeRegistry::instance();

    const auto *int_meta        = value::scalar_type_meta<int>();
    const auto *fixed_list_meta = registry.list(int_meta, 3);
    Value       fixed_list(*fixed_list_meta);
    auto        fixed_view = fixed_list.as_list();

    REQUIRE(fixed_view.is_fixed());
    REQUIRE(fixed_view.size() == 3);
    fixed_view.set(1, value::value_for(9).view());
    CHECK(fixed_view[1].as<int>() == 9);
    REQUIRE_THROWS_AS(fixed_view.push_back(value::value_for(4).view()), std::logic_error);

    const auto *dynamic_list_meta = registry.list(int_meta);
    Value       dynamic_list(*dynamic_list_meta);
    auto        dynamic_view = dynamic_list.as_list();

    REQUIRE_FALSE(dynamic_view.is_fixed());
    REQUIRE(dynamic_view.size() == 0);

    dynamic_view.push_back(value::value_for(1).view());
    dynamic_view.push_back(value::value_for(2).view());
    REQUIRE(dynamic_view.size() == 2);
    CHECK(dynamic_view.front().as<int>() == 1);
    CHECK(dynamic_view.back().as<int>() == 2);

    dynamic_view.pop_back();
    REQUIRE(dynamic_view.size() == 1);
    dynamic_view.resize(3);
    REQUIRE(dynamic_view.size() == 3);
    CHECK(dynamic_view[0].as<int>() == 1);
    CHECK(dynamic_view[1].as<int>() == 0);
    CHECK(dynamic_view[2].as<int>() == 0);
    CHECK(dynamic_list.to_string() == "[1, 0, 0]");
}

TEST_CASE("set and map views use stable keyed storage semantics", "[v2 value]") {
    TypeRegistry &registry = TypeRegistry::instance();

    const auto *int_meta    = value::scalar_type_meta<int>();
    const auto *string_meta = value::scalar_type_meta<std::string>();

    Value one   = value::value_for(1);
    Value two   = value::value_for(2);
    Value three = value::value_for(3);

    const auto *set_meta = registry.set(int_meta);
    Value       set_value(*set_meta);
    auto        set_view = set_value.as_set();

    REQUIRE(set_view.add(one.view()));
    REQUIRE(set_view.add(two.view()));
    REQUIRE_FALSE(set_view.add(two.view()));
    REQUIRE(set_view.contains(one.view()));
    REQUIRE(set_view.size() == 2);

    set_view.begin_mutation();
    REQUIRE(set_view.remove(one.view()));
    REQUIRE_FALSE(set_view.contains(one.view()));
    REQUIRE(set_view.has_pending_erase());
    REQUIRE(set_view.add(one.view()));
    set_view.end_mutation();

    REQUIRE(set_view.contains(one.view()));
    REQUIRE_FALSE(set_view.has_pending_erase());
    CHECK(set_value.to_string() == "{1, 2}");

    const auto *map_meta = registry.map(string_meta, int_meta);
    Value       map_value(*map_meta);
    auto        map_view = map_value.as_map();
    Value       alpha(std::string("alpha"));
    Value       beta(std::string("beta"));

    map_view.set(alpha.view(), one.view());
    map_view.set(beta.view(), two.view());
    REQUIRE(map_view.size() == 2);
    CHECK(map_view.at(alpha.view()).as<int>() == 1);

    map_view.set(alpha.view(), three.view());
    CHECK(map_view.at(alpha.view()).as<int>() == 3);

    map_view.begin_mutation();
    REQUIRE(map_view.remove(beta.view()));
    REQUIRE_FALSE(map_view.contains(beta.view()));
    REQUIRE(map_view.has_pending_erase());
    map_view.end_mutation();
    map_view.erase_pending();

    REQUIRE(map_view.size() == 1);
    CHECK(map_view.at(alpha.view()).as<int>() == 3);
    CHECK(map_value.to_string() == "{alpha: 3}");
}

TEST_CASE("cyclic buffer and queue views expose sequence semantics", "[v2 value]") {
    TypeRegistry &registry = TypeRegistry::instance();
    const auto   *int_meta = value::scalar_type_meta<int>();

    const auto *buffer_meta = registry.cyclic_buffer(int_meta, 3);
    Value       buffer_value(*buffer_meta);
    auto        buffer_view = buffer_value.as_cyclic_buffer();

    buffer_view.push(value::value_for(1).view());
    buffer_view.push(value::value_for(2).view());
    buffer_view.push(value::value_for(3).view());

    REQUIRE(buffer_view.full());
    CHECK(buffer_view.front().as<int>() == 1);
    CHECK(buffer_view.back().as<int>() == 3);

    buffer_view.push(value::value_for(4).view());
    REQUIRE(buffer_view.size() == 3);
    CHECK(buffer_view[0].as<int>() == 2);
    CHECK(buffer_view[1].as<int>() == 3);
    CHECK(buffer_view[2].as<int>() == 4);
    CHECK(buffer_value.to_string() == "CyclicBuffer[2, 3, 4]");

    const auto *queue_meta = registry.queue(int_meta, 2);
    Value       queue_value(*queue_meta);
    auto        queue_view = queue_value.as_queue();

    queue_view.push(value::value_for(5).view());
    queue_view.push(value::value_for(6).view());
    queue_view.push(value::value_for(7).view());

    REQUIRE(queue_view.size() == 2);
    CHECK(queue_view.front().as<int>() == 6);
    CHECK(queue_view.back().as<int>() == 7);

    queue_view.pop();
    REQUIRE(queue_view.size() == 1);
    CHECK(queue_view.front().as<int>() == 7);
    CHECK(queue_value.to_string() == "Queue[7]");
}

TEST_CASE("value views expose kind predicates and specialized conversions", "[v2 value]") {
    TypeRegistry &registry = TypeRegistry::instance();

    const auto *int_meta        = value::scalar_type_meta<int>();
    const auto *string_meta     = value::scalar_type_meta<std::string>();
    const auto *tuple_meta      = registry.tuple({int_meta, string_meta});
    const auto *bundle_meta     = registry.bundle({{"count", int_meta}}, "Counter");
    const auto *list_meta       = registry.list(int_meta);
    const auto *set_meta        = registry.set(int_meta);
    const auto *map_meta        = registry.map(string_meta, int_meta);
    const auto *buffer_meta     = registry.cyclic_buffer(int_meta, 2);
    const auto *queue_meta      = registry.queue(int_meta, 2);
    const auto *invalid_binding = value::scalar_value_builder<int>().binding();

    ValueView invalid_from_binding = ValueView::invalid_for(*invalid_binding);
    ValueView invalid_from_builder = ValueView::invalid_for(value::scalar_value_builder<int>());
    Value     scalar               = value::value_for(1);
    Value     tuple_value(*tuple_meta);
    Value     bundle_value(*bundle_meta);
    Value     list_value(*list_meta);
    Value     set_value(*set_meta);
    Value     map_value(*map_meta);
    Value     buffer_value(*buffer_meta);
    Value     queue_value(*queue_meta);

    REQUIRE_FALSE(invalid_from_binding.has_value());
    REQUIRE_FALSE(invalid_from_builder.has_value());
    REQUIRE_FALSE(invalid_from_binding.try_as_tuple().has_value());
    REQUIRE_FALSE(invalid_from_binding.try_as_list().has_value());

    CHECK(scalar.view().is_atomic());
    CHECK(tuple_value.view().is_tuple());
    CHECK(bundle_value.view().is_bundle());
    CHECK(list_value.view().is_list());
    CHECK(set_value.view().is_set());
    CHECK(map_value.view().is_map());
    CHECK(buffer_value.view().is_cyclic_buffer());
    CHECK(queue_value.view().is_queue());

    REQUIRE(tuple_value.view().try_as_tuple().has_value());
    REQUIRE(bundle_value.view().try_as_bundle().has_value());
    REQUIRE(list_value.view().try_as_list().has_value());
    REQUIRE(set_value.view().try_as_set().has_value());
    REQUIRE(map_value.view().try_as_map().has_value());
    REQUIRE(buffer_value.view().try_as_cyclic_buffer().has_value());
    REQUIRE(queue_value.view().try_as_queue().has_value());

    REQUIRE_FALSE(scalar.view().try_as_tuple().has_value());
    REQUIRE_FALSE(tuple_value.view().try_as_bundle().has_value());
    REQUIRE_FALSE(tuple_value.view().try_as_list().has_value());
    REQUIRE_FALSE(list_value.view().try_as_set().has_value());
    REQUIRE_FALSE(set_value.view().try_as_map().has_value());
    REQUIRE_FALSE(buffer_value.view().try_as_queue().has_value());

    REQUIRE_THROWS_AS(static_cast<void>(scalar.view().as_tuple()), std::logic_error);
    REQUIRE_THROWS_AS(static_cast<void>(tuple_value.view().as_bundle()), std::logic_error);
    REQUIRE_THROWS_AS(static_cast<void>(tuple_value.view().as_list()), std::logic_error);
    REQUIRE_THROWS_AS(static_cast<void>(list_value.view().as_set()), std::logic_error);
    REQUIRE_THROWS_AS(static_cast<void>(set_value.view().as_map()), std::logic_error);
    REQUIRE_THROWS_AS(static_cast<void>(buffer_value.view().as_queue()), std::logic_error);
    REQUIRE_THROWS_AS(static_cast<void>(invalid_from_builder.hash()), std::logic_error);
    REQUIRE_THROWS_AS(static_cast<void>(invalid_from_builder.to_string()), std::logic_error);
}

TEST_CASE("tuple and bundle views cover indexing iteration and field errors", "[v2 value]") {
    TypeRegistry &registry = TypeRegistry::instance();

    const auto *int_meta    = value::scalar_type_meta<int>();
    const auto *string_meta = value::scalar_type_meta<std::string>();
    const auto *tuple_meta  = registry.tuple({int_meta, int_meta, int_meta});
    const auto *bundle_meta = registry.bundle({{"count", int_meta}, {"label", string_meta}}, "Pair");

    Value tuple_value(*tuple_meta);
    auto  tuple_view = tuple_value.as_tuple();

    tuple_view.set(0, value::value_for(10).view());
    tuple_view.set(1, value::value_for(20).view());
    tuple_view.set(2, value::value_for(30).view());

    std::vector<int> tuple_values;
    for (const ValueView element : tuple_view) { tuple_values.push_back(element.as<int>()); }

    REQUIRE(tuple_values == std::vector<int>{10, 20, 30});
    CHECK(tuple_view.at(1).as<int>() == 20);
    REQUIRE_THROWS_AS(static_cast<void>(tuple_view.at(3)), std::out_of_range);

    Value bundle_value(*bundle_meta);
    auto  bundle_view = bundle_value.as_bundle();

    bundle_view.set("count", value::value_for(7).view());
    bundle_view.set("label", value::value_for(std::string("delta")).view());

    REQUIRE(bundle_view.field_count() == 2);
    REQUIRE(bundle_view.has_field("count"));
    REQUIRE_FALSE(bundle_view.has_field("missing"));
    CHECK(bundle_view.at("count").as<int>() == 7);
    CHECK(bundle_view["label"].as<std::string>() == "delta");
    REQUIRE_THROWS_AS(static_cast<void>(bundle_view.at("missing")), std::out_of_range);
    REQUIRE_THROWS_AS(static_cast<void>(bundle_view["missing"]), std::out_of_range);
    REQUIRE_THROWS_AS(bundle_view.set("missing", value::value_for(9).view()), std::out_of_range);
    REQUIRE_THROWS_AS(bundle_view.set("count", value::value_for(std::string("bad")).view()), std::logic_error);
}

TEST_CASE("list views enforce fixed and dynamic contracts including errors", "[v2 value]") {
    TypeRegistry &registry = TypeRegistry::instance();
    const auto   *int_meta = value::scalar_type_meta<int>();

    const auto *fixed_list_meta = registry.list(int_meta, 2);
    Value       fixed_list(*fixed_list_meta);
    auto        fixed_view = fixed_list.as_list();

    fixed_view.set(0, value::value_for(11).view());
    fixed_view.set(1, value::value_for(12).view());

    REQUIRE(fixed_view.element_type() == int_meta);
    REQUIRE(fixed_view.is_fixed());
    CHECK(fixed_view.front().as<int>() == 11);
    CHECK(fixed_view.back().as<int>() == 12);
    REQUIRE_THROWS_AS(fixed_view.push_back(value::value_for(13).view()), std::logic_error);
    REQUIRE_THROWS_AS(fixed_view.pop_back(), std::logic_error);
    REQUIRE_THROWS_AS(fixed_view.resize(3), std::logic_error);
    REQUIRE_THROWS_AS(fixed_view.clear(), std::logic_error);

    const auto *dynamic_list_meta = registry.list(int_meta);
    Value       dynamic_list(*dynamic_list_meta);
    auto        dynamic_view = dynamic_list.as_list();

    REQUIRE(dynamic_view.element_type() == int_meta);
    REQUIRE_FALSE(dynamic_view.is_fixed());
    REQUIRE(dynamic_view.size() == 0);
    REQUIRE_THROWS_AS(dynamic_view.pop_back(), std::out_of_range);
    REQUIRE_THROWS_AS(dynamic_view.push_back(value::value_for(std::string("bad")).view()), std::logic_error);

    dynamic_view.push_back(value::value_for(1).view());
    dynamic_view.push_back(value::value_for(2).view());
    dynamic_view.push_back(value::value_for(3).view());
    dynamic_view.set(1, value::value_for(9).view());

    std::vector<int> dynamic_values;
    for (const ValueView element : dynamic_view) { dynamic_values.push_back(element.as<int>()); }

    REQUIRE(dynamic_values == std::vector<int>{1, 9, 3});
    REQUIRE_THROWS_AS(static_cast<void>(dynamic_view.at(3)), std::out_of_range);

    dynamic_view.clear();
    REQUIRE(dynamic_view.size() == 0);
}

TEST_CASE("set views cover iteration delayed erase clear and type safety", "[v2 value]") {
    TypeRegistry &registry = TypeRegistry::instance();

    const auto *int_meta = value::scalar_type_meta<int>();
    const auto *set_meta = registry.set(int_meta);

    Value set_value(*set_meta);
    auto  set_view = set_value.as_set();

    REQUIRE(set_view.empty());
    REQUIRE(set_view.add(value::value_for(1).view()));
    REQUIRE(set_view.add(value::value_for(2).view()));
    REQUIRE(set_view.size() == 2);
    REQUIRE_FALSE(set_view.empty());

    std::vector<int> initial_values;
    for (const ValueView element : set_view) { initial_values.push_back(element.as<int>()); }
    REQUIRE(initial_values == std::vector<int>{1, 2});

    set_view.begin_mutation();
    REQUIRE(set_view.remove(value::value_for(1).view()));
    REQUIRE_FALSE(set_view.contains(value::value_for(1).view()));
    REQUIRE(set_view.has_pending_erase());
    set_view.end_mutation();

    std::vector<int> live_values;
    for (const ValueView element : set_view) { live_values.push_back(element.as<int>()); }
    REQUIRE(live_values == std::vector<int>{2});

    set_view.erase_pending();
    REQUIRE_FALSE(set_view.has_pending_erase());
    REQUIRE_FALSE(set_view.remove(value::value_for(9).view()));

    Value wrong_type(std::string("bad"));
    REQUIRE_THROWS_AS(set_view.contains(wrong_type.view()), std::logic_error);
    REQUIRE_THROWS_AS(set_view.add(wrong_type.view()), std::logic_error);
    REQUIRE_THROWS_AS(set_view.remove(wrong_type.view()), std::logic_error);

    set_view.clear();
    REQUIRE(set_view.empty());
    REQUIRE(set_view.size() == 0);
}

TEST_CASE("map views cover iteration delayed erase clear and type safety", "[v2 value]") {
    TypeRegistry &registry = TypeRegistry::instance();

    const auto *int_meta    = value::scalar_type_meta<int>();
    const auto *string_meta = value::scalar_type_meta<std::string>();
    const auto *map_meta    = registry.map(string_meta, int_meta);

    Value map_value(*map_meta);
    auto  map_view = map_value.as_map();

    Value alpha(std::string("alpha"));
    Value beta(std::string("beta"));
    Value gamma(std::string("gamma"));

    REQUIRE(map_view.empty());
    REQUIRE(map_view.key_type() == string_meta);
    REQUIRE(map_view.value_type() == int_meta);
    REQUIRE(map_view.add(alpha.view(), value::value_for(1).view()));
    REQUIRE(map_view.add(beta.view(), value::value_for(2).view()));
    REQUIRE_FALSE(map_view.add(beta.view(), value::value_for(3).view()));
    REQUIRE(map_view.size() == 2);
    REQUIRE_FALSE(map_view.empty());

    std::vector<std::string> entry_strings;
    for (const auto entry : map_view) {
        entry_strings.push_back(entry.key.as<std::string>() + "=" + std::to_string(entry.value.as<int>()));
    }
    REQUIRE(entry_strings == std::vector<std::string>{"alpha=1", "beta=2"});

    CHECK(map_view.at(alpha.view()).as<int>() == 1);
    REQUIRE_THROWS_AS(static_cast<void>(map_view.at(gamma.view())), std::out_of_range);

    map_view.begin_mutation();
    REQUIRE(map_view.remove(beta.view()));
    REQUIRE(map_view.has_pending_erase());
    REQUIRE_FALSE(map_view.contains(beta.view()));
    map_view.end_mutation();

    std::vector<std::string> live_entries;
    for (const auto entry : map_view) {
        live_entries.push_back(entry.key.as<std::string>() + "=" + std::to_string(entry.value.as<int>()));
    }
    REQUIRE(live_entries == std::vector<std::string>{"alpha=1"});

    map_view.erase_pending();
    REQUIRE_FALSE(map_view.has_pending_erase());
    REQUIRE_FALSE(map_view.remove(gamma.view()));

    Value wrong_key(1);
    Value wrong_value(std::string("bad"));
    REQUIRE_THROWS_AS(map_view.contains(wrong_key.view()), std::logic_error);
    REQUIRE_THROWS_AS(static_cast<void>(map_view.at(wrong_key.view())), std::logic_error);
    REQUIRE_THROWS_AS(map_view.set(wrong_key.view(), value::value_for(9).view()), std::logic_error);
    REQUIRE_THROWS_AS(map_view.set(alpha.view(), wrong_value.view()), std::logic_error);
    REQUIRE_THROWS_AS(map_view.add(wrong_key.view(), value::value_for(9).view()), std::logic_error);
    REQUIRE_THROWS_AS(map_view.remove(wrong_key.view()), std::logic_error);

    map_view.clear();
    REQUIRE(map_view.empty());
    REQUIRE(map_view.size() == 0);
}

TEST_CASE("cyclic buffer and queue views cover clear bounds and empty checks", "[v2 value]") {
    TypeRegistry &registry = TypeRegistry::instance();
    const auto   *int_meta = value::scalar_type_meta<int>();

    const auto *buffer_meta = registry.cyclic_buffer(int_meta, 2);
    Value       buffer_value(*buffer_meta);
    auto        buffer_view = buffer_value.as_cyclic_buffer();

    REQUIRE(buffer_view.capacity() == 2);
    REQUIRE_FALSE(buffer_view.full());
    buffer_view.push(value::value_for(1).view());
    buffer_view.push(value::value_for(2).view());
    REQUIRE(buffer_view.full());
    REQUIRE(buffer_view.size() == 2);

    std::vector<int> buffer_values;
    for (const ValueView element : buffer_view) { buffer_values.push_back(element.as<int>()); }
    REQUIRE(buffer_values == std::vector<int>{1, 2});

    buffer_view.clear();
    REQUIRE(buffer_view.size() == 0);
    REQUIRE_FALSE(buffer_view.full());
    REQUIRE_THROWS_AS(static_cast<void>(buffer_view.front()), std::out_of_range);

    const auto *queue_meta = registry.queue(int_meta, 2);
    Value       queue_value(*queue_meta);
    auto        queue_view = queue_value.as_queue();

    REQUIRE(queue_view.max_capacity() == 2);
    REQUIRE(queue_view.has_max_capacity());
    queue_view.push(value::value_for(5).view());
    queue_view.push(value::value_for(6).view());
    REQUIRE(queue_view.size() == 2);

    std::vector<int> queue_values;
    for (const ValueView element : queue_view) { queue_values.push_back(element.as<int>()); }
    REQUIRE(queue_values == std::vector<int>{5, 6});

    queue_view.clear();
    REQUIRE(queue_view.size() == 0);
    REQUIRE_THROWS_AS(queue_view.pop(), std::out_of_range);
    REQUIRE_THROWS_AS(static_cast<void>(queue_view.back()), std::out_of_range);
}

TEST_CASE("container values support copy assignment equality compare and reset", "[v2 value]") {
    TypeRegistry &registry = TypeRegistry::instance();
    const auto   *int_meta = value::scalar_type_meta<int>();

    const auto *list_meta = registry.list(int_meta);
    Value       lhs(*list_meta);
    auto        lhs_view = lhs.as_list();
    lhs_view.push_back(value::value_for(1).view());
    lhs_view.push_back(value::value_for(2).view());

    Value copied(lhs.view());
    REQUIRE(copied.equals(lhs));
    CHECK(copied.hash() == lhs.hash());
    CHECK((copied <=> lhs) == std::partial_ordering::equivalent);

    Value assigned(*list_meta);
    assigned = lhs;
    REQUIRE(assigned.equals(lhs));
    REQUIRE(assigned.to_string() == "[1, 2]");

    copied.as_list().push_back(value::value_for(3).view());
    CHECK(std::is_lt(lhs.compare(copied)));
    CHECK_FALSE(lhs.equals(copied));

    lhs.reset();
    REQUIRE(lhs.as_list().size() == 0);
    REQUIRE(lhs.to_string() == "[]");
}
