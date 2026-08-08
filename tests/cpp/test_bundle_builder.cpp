// Tests for BundleBuilder — assembling a compact (immutable) Bundle Value from
// prebuilt field Values via whole-value copy/move-assign at the field offsets.
// This is the construction path for the canonical delta bundles (e.g. the TSS
// delta Bundle{added: Set<T>, removed: Set<T>}), which have immutable container
// fields that cannot be populated through begin_mutation.

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <ranges>
#include <utility>

#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>

namespace
{
    using namespace hgraph;

    struct BundleMoveCountingScalar
    {
        inline static int copy_constructs{0};
        inline static int copy_assigns{0};

        int value{0};

        BundleMoveCountingScalar() = default;
        explicit BundleMoveCountingScalar(int value_)
            : value(value_)
        {
        }
        BundleMoveCountingScalar(const BundleMoveCountingScalar &other)
            : value(other.value)
        {
            ++copy_constructs;
        }
        BundleMoveCountingScalar(BundleMoveCountingScalar &&) noexcept = default;

        BundleMoveCountingScalar &operator=(const BundleMoveCountingScalar &other)
        {
            value = other.value;
            ++copy_assigns;
            return *this;
        }
        BundleMoveCountingScalar &operator=(BundleMoveCountingScalar &&) noexcept = default;
    };

    void reset_bundle_move_counting_scalar_counts()
    {
        BundleMoveCountingScalar::copy_constructs = 0;
        BundleMoveCountingScalar::copy_assigns    = 0;
    }

    struct ProjectedBundleStorage
    {
        std::int32_t count{0};
        Str          label{};

        bool operator==(const ProjectedBundleStorage &) const = default;
    };

    struct ProjectedBundleContext
    {
        ValueTypeRef binding{};
        ValueTypeRef count_binding{};
        ValueTypeRef label_binding{};
    };

    std::size_t projected_bundle_size(const void *, const void *) noexcept { return 2; }

    ValueTypeRef projected_bundle_element_binding(const void *context, const void *,
                                                  std::size_t index) noexcept
    {
        const auto &self = *static_cast<const ProjectedBundleContext *>(context);
        if (index == 0) { return self.count_binding; }
        if (index == 1) { return self.label_binding; }
        return {};
    }

    const void *projected_bundle_element_at(const void *, const void *memory, std::size_t index)
    {
        if (memory == nullptr) { return nullptr; }
        const auto &value = *static_cast<const ProjectedBundleStorage *>(memory);
        if (index == 0) { return std::addressof(value.count); }
        if (index == 1) { return std::addressof(value.label); }
        throw std::out_of_range("projected Bundle field index");
    }

    ValueView projected_bundle_project(const void *context, const void *memory,
                                       std::size_t index)
    {
        return ValueView{
            projected_bundle_element_binding(context, memory, index),
            projected_bundle_element_at(context, memory, index),
        };
    }

    Range<ValueView> projected_bundle_range(const void *context, const void *memory)
    {
        return Range<ValueView>{
            .context = context,
            .memory = memory,
            .limit = 2,
            .predicate = nullptr,
            .projector = &projected_bundle_project,
        };
    }

    std::size_t projected_bundle_hash(const void *, const void *memory)
    {
        const auto &value = *static_cast<const ProjectedBundleStorage *>(memory);
        return std::hash<std::int32_t>{}(value.count) ^
               (std::hash<Str>{}(value.label) << 1U);
    }

    std::partial_ordering projected_bundle_compare(
        const void *, const void *lhs, const void *rhs) noexcept
    {
        const auto &left = *static_cast<const ProjectedBundleStorage *>(lhs);
        const auto &right = *static_cast<const ProjectedBundleStorage *>(rhs);
        if (left.count < right.count) { return std::partial_ordering::less; }
        if (left.count > right.count) { return std::partial_ordering::greater; }
        if (left.label < right.label) { return std::partial_ordering::less; }
        if (left.label > right.label) { return std::partial_ordering::greater; }
        return std::partial_ordering::equivalent;
    }

    bool projected_bundle_accepts(const void *, ValueTypeRef binding,
                                  ValueTypeRef source) noexcept
    {
        const auto *target = binding.schema();
        const auto *candidate = source.schema();
        if (target == nullptr || candidate == nullptr ||
            candidate->try_value_kind() != ValueTypeKind::Bundle ||
            target->field_count != candidate->field_count)
        {
            return false;
        }
        for (std::size_t index = 0; index < target->field_count; ++index)
        {
            if (target->fields[index].type != candidate->fields[index].type)
            {
                return false;
            }
        }
        return true;
    }

    void projected_bundle_assign(const void *, ValueTypeRef, void *destination,
                                 ValueTypeRef source, const void *source_memory)
    {
        const auto concrete = source.ops_ref().concrete_type(source, source_memory);
        const auto *memory = source.ops_ref().concrete_memory(source_memory);
        const auto *indexed = checked_value_ops<IndexedValueOps>(
            concrete, "projected Bundle assignment");
        auto &result = *static_cast<ProjectedBundleStorage *>(destination);
        result.count = ValueView{
            indexed->element_binding(indexed->context, memory, 0),
            indexed->element_at(indexed->context, memory, 0),
        }.checked_as<std::int32_t>();
        result.label = ValueView{
            indexed->element_binding(indexed->context, memory, 1),
            indexed->element_at(indexed->context, memory, 1),
        }.checked_as<Str>();
    }

    // Build the canonical TSS-delta bundle: Bundle{added: Set<std::int32_t>, removed: Set<std::int32_t>}.
    Value make_delta_bundle(std::initializer_list<std::int32_t> added, std::initializer_list<std::int32_t> removed)
    {
        auto       &registry      = TypeRegistry::instance();
        const auto *int_meta       = registry.register_scalar<std::int32_t>("int32");
        const auto *set_meta        = registry.set(int_meta);
        const auto *bundle_schema   = registry.un_named_bundle({{"added", set_meta}, {"removed", set_meta}});
        const auto bundle_binding  = ValuePlanFactory::instance().type_for(bundle_schema);

        Value added_set   = stdlib::make_set<std::int32_t>(added);
        Value removed_set = stdlib::make_set<std::int32_t>(removed);

        BundleBuilder builder{bundle_binding};
        builder.set("added", added_set.view());
        builder.set("removed", removed_set.view());
        return builder.build();
    }
}  // namespace

TEST_CASE("BundleBuilder: assembles a Bundle{Set,Set} and reads its fields back")
{
    using namespace hgraph;

    Value bundle = make_delta_bundle({1, 2}, {});
    REQUIRE(bundle.has_value());

    const auto view  = bundle.view().as_bundle();
    const auto added = view.field("added").as_set();
    CHECK(added.size() == 2);
    CHECK(added.contains(Value{std::int32_t{1}}.view()));
    CHECK(added.contains(Value{std::int32_t{2}}.view()));
    CHECK(view.field("removed").as_set().size() == 0);
}

TEST_CASE("BundleBuilder: bundle fields are unset until explicitly set")
{
    using namespace hgraph;

    auto       &registry      = TypeRegistry::instance();
    const auto *int_meta       = registry.register_scalar<std::int32_t>("int32");
    const auto *str_meta       = registry.register_scalar<Str>("str");
    const auto *bundle_schema  = registry.un_named_bundle({{"count", int_meta}, {"label", str_meta}});
    const auto bundle_binding = ValuePlanFactory::instance().type_for(bundle_schema);
    REQUIRE(bundle_binding != nullptr);

    Value empty{bundle_binding};
    auto  empty_view = empty.as_bundle();
    CHECK(empty_view.size() == 2);
    CHECK_FALSE(empty_view.at("count").has_value());
    CHECK_FALSE(empty_view.at("label").has_value());
    CHECK_FALSE(empty_view.has_field("___validity"));

    BundleBuilder builder{bundle_binding};
    CHECK(builder.size() == 2);
    builder.set("count", Value{std::int32_t{42}});
    Value partial = builder.build();

    auto partial_view = partial.as_bundle();
    CHECK(partial_view.size() == 2);
    CHECK(partial_view.at("count").checked_as<std::int32_t>() == 42);
    CHECK_FALSE(partial_view.at("label").has_value());

    BundleBuilder rejecting_builder{bundle_binding};
    Value         typed_null{*str_meta};
    CHECK_THROWS_AS(rejecting_builder.set("label", typed_null.view()), std::invalid_argument);
}

TEST_CASE("BundleBuilder: moves owned field values")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *field_meta = registry.register_scalar<BundleMoveCountingScalar>("BundleMoveCountingScalar");
    const auto *bundle_schema = registry.un_named_bundle({{"field", field_meta}});
    const auto bundle_binding = ValuePlanFactory::instance().type_for(bundle_schema);
    REQUIRE(bundle_binding != nullptr);

    BundleBuilder builder{bundle_binding};
    Value         field{BundleMoveCountingScalar{13}};

    reset_bundle_move_counting_scalar_counts();
    builder.set("field", std::move(field));
    Value bundle = builder.build();

    auto        field_view = bundle.view().as_bundle().field("field");
    const auto &stored     = field_view.checked_as<BundleMoveCountingScalar>();
    REQUIRE(stored.value == 13);
    REQUIRE(BundleMoveCountingScalar::copy_constructs == 0);
    REQUIRE(BundleMoveCountingScalar::copy_assigns == 0);
}

TEST_CASE("BundleBuilder: equality is content-based and order-independent (Value::equals)")
{
    using namespace hgraph;

    // Same content, different element insertion order -> equal.
    CHECK(make_delta_bundle({1, 2}, {3}).equals(make_delta_bundle({2, 1}, {3})));

    // Different content -> not equal.
    CHECK_FALSE(make_delta_bundle({1, 2}, {3}).equals(make_delta_bundle({1, 2}, {4})));
    CHECK_FALSE(make_delta_bundle({1}, {}).equals(make_delta_bundle({1, 2}, {})));
}

TEST_CASE("BundleBuilder: the built value matches the canonical un_named_bundle schema")
{
    using namespace hgraph;
    auto       &registry     = TypeRegistry::instance();
    const auto *int_meta      = registry.register_scalar<std::int32_t>("int32");
    const auto *set_meta       = registry.set(int_meta);
    const auto *bundle_schema  = registry.un_named_bundle({{"added", set_meta}, {"removed", set_meta}});

    Value bundle = make_delta_bundle({5}, {6});
    // The built value's schema is exactly the interned canonical bundle schema, so
    // it compares (Value::equals) against a runtime-produced delta of the same shape.
    CHECK(bundle.schema() == bundle_schema);
    CHECK_FALSE(bundle.view().to_string().empty());
}

TEST_CASE("BundleBuilder: constructs a registered non-composite Bundle binding")
{
    using namespace hgraph;

    auto &registry = TypeRegistry::instance();
    auto &factory = ValuePlanFactory::instance();
    const auto *count_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *label_meta = registry.register_scalar<Str>("str");
    const auto *schema = registry.bundle(
        "tests.bundle_binding",
        "ProjectedBundle",
        {{"count", count_meta}, {"label", label_meta}});

    // The registered ops table is process-global, so its context must outlive
    // this test case just as a production extension binding's context does.
    auto *context = new ProjectedBundleContext{
        .count_binding = factory.type_for(count_meta),
        .label_binding = factory.type_for(label_meta),
    };
    IndexedValueOps ops{};
    static_cast<ValueOps &>(ops) = ops_for<ProjectedBundleStorage>();
    ops.kind = ValueOpsKind::Indexed;
    ops.context = context;
    ops.allows_mutation = false;
    ops.hash_impl = &projected_bundle_hash;
    ops.compare_impl = &projected_bundle_compare;
    ops.accepts_source_impl = &projected_bundle_accepts;
    ops.copy_assign_from_impl = &projected_bundle_assign;
    ops.move_assign_from_impl =
        [](const void *ops_context, ValueTypeRef binding, void *destination,
           ValueTypeRef source, void *source_memory) {
            projected_bundle_assign(
                ops_context, binding, destination, source, source_memory);
        };
    ops.size = &projected_bundle_size;
    ops.element_binding = &projected_bundle_element_binding;
    ops.element_at = &projected_bundle_element_at;
    ops.make_range = &projected_bundle_range;

    context->binding = intern_value_type(
        *schema, MemoryUtils::plan_for<ProjectedBundleStorage>(), ops);
    factory.register_type(context->binding);
    REQUIRE_FALSE(context->binding.checked_plan().is_composite());

    BundleBuilder builder{context->binding};
    builder.set("count", Value{std::int32_t{7}});
    builder.set("label", Value{Str{"seven"}});
    Value value = builder.build();

    REQUIRE(value.binding() == context->binding);
    const auto bundle = value.as_bundle();
    REQUIRE(bundle.size() == 2);
    CHECK(bundle.at("count").checked_as<std::int32_t>() == 7);
    CHECK(bundle.at("label").checked_as<Str>() == "seven");
    CHECK(std::ranges::distance(bundle) == 2);
}
