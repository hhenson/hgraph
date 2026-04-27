#include <catch2/catch_test_macros.hpp>

#include <hgraph/v2/types/value/value.h>

#include <compare>
#include <new>
#include <ostream>
#include <string>

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
