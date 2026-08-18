// TSCheckpointOps (RFC 0023): exact endpoint capture and quiet import.
//
// Each case follows the uninterrupted-versus-restored discipline at the TS
// layer: build an output, mutate it through the ordinary publication path,
// capture the image, quietly import into FRESH unstarted storage, and
// require the restored endpoint to be indistinguishable — values, validity,
// and ORIGINAL modification times — with no publication side effects.

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_data/checkpoint_ops.h>
#include <hgraph/types/time_series/ts_output.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

namespace
{
    using namespace hgraph;

    const ValueTypeMetaData *int_meta()
    {
        return TypeRegistry::instance().register_scalar<std::int64_t>("int");
    }

    void tick(TSOutput &output, DateTime t, Value value)
    {
        auto mutation = output.view(t).begin_mutation(t);
        REQUIRE(mutation.copy_value_from(value.view()));
    }
}  // namespace

TEST_CASE("checkpoint: TS round-trips value, validity, and original time", "[ts_checkpoint]")
{
    const auto *ts_int = TypeRegistry::instance().ts(int_meta());
    const auto t1 = MIN_ST + TimeDelta{10};
    const auto t2 = t1 + TimeDelta{5};

    TSOutput original{*ts_int};
    tick(original, t1, Value{std::int64_t{41}});
    tick(original, t2, Value{std::int64_t{42}});

    REQUIRE(checkpoint_supported(original.data_view()));
    const auto image = capture_checkpoint(original.data_view());
    CHECK(image.kind == TSTypeKind::TS);
    CHECK(image.schema == ts_int);
    CHECK(image.modified_time == t2);
    REQUIRE(image.value.has_value());
    CHECK(image.value.view().checked_as<std::int64_t>() == 42);

    TSOutput restored{*ts_int};
    TSCheckpointDiagnostics why;
    REQUIRE(validate_checkpoint(restored.data_view(), image, why));
    import_checkpoint(restored.data_view(), image, TSCheckpointRestoreGuard::begin());

    const auto view = restored.view(t2 + TimeDelta{1});
    CHECK(view.valid());
    CHECK(view.last_modified_time() == t2);  // ORIGINAL time, never re-stamped
    CHECK_FALSE(view.modified());            // importing is not a tick
    CHECK(view.value().checked_as<std::int64_t>() == 42);
}

TEST_CASE("checkpoint: a never-ticked TS restores as never-ticked", "[ts_checkpoint]")
{
    const auto *ts_int = TypeRegistry::instance().ts(int_meta());

    TSOutput original{*ts_int};
    const auto image = capture_checkpoint(original.data_view());
    CHECK(image.modified_time == MIN_DT);
    CHECK_FALSE(image.value.has_value());

    TSOutput restored{*ts_int};
    import_checkpoint(restored.data_view(), image, TSCheckpointRestoreGuard::begin());
    CHECK_FALSE(restored.view(MIN_ST).valid());
    CHECK(restored.view(MIN_ST).last_modified_time() == MIN_DT);
}

TEST_CASE("checkpoint: validation is path-addressed and schema-strict", "[ts_checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *ts_int = registry.ts(int_meta());
    const auto *ts_float = registry.ts(registry.register_scalar<double>("float"));

    TSOutput original{*ts_int};
    tick(original, MIN_ST, Value{std::int64_t{7}});
    const auto image = capture_checkpoint(original.data_view());

    TSOutput other{*ts_float};
    TSCheckpointDiagnostics why;
    CHECK_FALSE(validate_checkpoint(other.data_view(), image, why));
    CHECK_FALSE(why.reason.empty());
    CHECK_THROWS_AS(
        import_checkpoint(other.data_view(), image, TSCheckpointRestoreGuard::begin()),
        std::invalid_argument);
}

TEST_CASE("checkpoint: TSB round-trips per-field values, times, and validity", "[ts_checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *int32_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int32_meta);
    const auto *tsb = registry.tsb("CkptBundle", {{"a", ts_int}, {"b", ts_int}});

    const auto t1 = MIN_ST + TimeDelta{3};
    const auto t2 = t1 + TimeDelta{2};
    const auto t3 = t2 + TimeDelta{2};

    TSOutput original{*tsb};
    {
        auto root = original.data_view();
        auto bundle = root.as_bundle();
        auto field_a = bundle.field("a");
        auto field_b = bundle.field("b");
        { auto m = field_a.begin_mutation(t1); REQUIRE(m.copy_value_from(Value{1}.view())); }
        { auto m = field_b.begin_mutation(t2); REQUIRE(m.copy_value_from(Value{2}.view())); }
        { auto m = field_a.begin_mutation(t3); REQUIRE(m.copy_value_from(Value{3}.view())); }
    }

    const auto image = capture_checkpoint(original.data_view());
    CHECK(image.kind == TSTypeKind::TSB);
    CHECK(image.modified_time == t3);  // parent re-stamped by the last child tick
    REQUIRE(image.children.size() == 2);
    CHECK(image.children[0].modified_time == t3);
    CHECK(image.children[1].modified_time == t2);

    TSOutput restored{*tsb};
    TSCheckpointDiagnostics why;
    REQUIRE(validate_checkpoint(restored.data_view(), image, why));
    import_checkpoint(restored.data_view(), image, TSCheckpointRestoreGuard::begin());

    CHECK(restored.data_view().last_modified_time() == t3);
    auto restored_root = restored.data_view();
    auto bundle = restored_root.as_bundle();
    CHECK(bundle.field("a").last_modified_time() == t3);
    CHECK(bundle.field("b").last_modified_time() == t2);
    CHECK(bundle.field("a").ops().has_current_value_impl(
        bundle.field("a").ops().context, bundle.field("a").data()));

    // The whole-bundle value read depends on the value-layer validity words
    // the import replayed; both fields must present their restored values.
    const auto view = restored.view(t3 + TimeDelta{1});
    CHECK(view.all_valid());
    const auto value = view.value();
    CHECK(value.as_bundle().field("a").checked_as<std::int32_t>() == 3);
    CHECK(value.as_bundle().field("b").checked_as<std::int32_t>() == 2);
}

TEST_CASE("checkpoint: a partially valid TSB restores partially valid", "[ts_checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *int32_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int = registry.ts(int32_meta);
    const auto *tsb = registry.tsb("CkptPartialBundle", {{"a", ts_int}, {"b", ts_int}});

    const auto t1 = MIN_ST + TimeDelta{3};
    TSOutput original{*tsb};
    {
        auto root = original.data_view();
        auto bundle = root.as_bundle();
        auto field_a = bundle.field("a");
        auto m = field_a.begin_mutation(t1);
        REQUIRE(m.copy_value_from(Value{9}.view()));
    }

    const auto image = capture_checkpoint(original.data_view());
    TSOutput restored{*tsb};
    import_checkpoint(restored.data_view(), image, TSCheckpointRestoreGuard::begin());

    auto restored_root = restored.data_view();
    auto bundle = restored_root.as_bundle();
    CHECK(bundle.field("a").last_modified_time() == t1);
    CHECK(bundle.field("b").last_modified_time() == MIN_DT);
    CHECK(restored.view(t1 + TimeDelta{1}).valid());
    CHECK_FALSE(restored.view(t1 + TimeDelta{1}).all_valid());
}

TEST_CASE("checkpoint: fixed TSL round-trips per-index state", "[ts_checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *int32_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tsl = registry.tsl(registry.ts(int32_meta), 2);

    const auto t1 = MIN_ST + TimeDelta{1};
    const auto t2 = t1 + TimeDelta{1};

    TSOutput original{*tsl};
    {
        auto element_0 = original.data_view().indexed_child_at(0);
        auto element_1 = original.data_view().indexed_child_at(1);
        { auto m = element_0.begin_mutation(t1); REQUIRE(m.copy_value_from(Value{10}.view())); }
        { auto m = element_1.begin_mutation(t2); REQUIRE(m.copy_value_from(Value{20}.view())); }
    }

    const auto image = capture_checkpoint(original.data_view());
    REQUIRE(image.children.size() == 2);

    TSOutput restored{*tsl};
    import_checkpoint(restored.data_view(), image, TSCheckpointRestoreGuard::begin());
    CHECK(restored.data_view().indexed_child_at(0).last_modified_time() == t1);
    CHECK(restored.data_view().indexed_child_at(1).last_modified_time() == t2);
    CHECK(restored.view(t2 + TimeDelta{1}).all_valid());
}

TEST_CASE("checkpoint: dynamic TSL round-trips shape and children", "[ts_checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *int32_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tsl = registry.tsl(registry.ts(int32_meta), 0);

    const auto t1 = MIN_ST + TimeDelta{1};
    const auto t2 = t1 + TimeDelta{1};

    TSOutput original{*tsl};
    {
        auto element_0 = original.data_view().ensure_indexed_child_at(0);
        auto element_1 = original.data_view().ensure_indexed_child_at(1);
        { auto m = element_0.begin_mutation(t1); REQUIRE(m.copy_value_from(Value{10}.view())); }
        { auto m = element_1.begin_mutation(t2); REQUIRE(m.copy_value_from(Value{20}.view())); }
    }

    const auto image = capture_checkpoint(original.data_view());
    CHECK(image.kind == TSTypeKind::TSL);
    REQUIRE(image.children.size() == 2);  // the durable shape

    TSOutput restored{*tsl};
    CHECK(restored.data_view().indexed_child_count() == 0);
    import_checkpoint(restored.data_view(), image, TSCheckpointRestoreGuard::begin());

    REQUIRE(restored.data_view().indexed_child_count() == 2);
    auto restored_0 = restored.data_view().indexed_child_at(0);
    auto restored_1 = restored.data_view().indexed_child_at(1);
    CHECK(restored_0.last_modified_time() == t1);
    CHECK(restored_1.last_modified_time() == t2);
    // Imported children carry parent links exactly as grown children do.
    CHECK(restored_0.has_parent());
    CHECK(restored_1.has_parent());
    CHECK(restored.data_view().last_modified_time() == t2);
}

TEST_CASE("checkpoint: TSW round-trips entries with ORIGINAL timestamps", "[ts_checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *int32_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tsw = registry.tsw(int32_meta, 3, 1);

    const auto t1 = MIN_ST + TimeDelta{1};
    const auto t2 = t1 + TimeDelta{7};
    const auto t3 = t2 + TimeDelta{7};
    const auto t4 = t3 + TimeDelta{7};

    TSOutput original{*tsw};
    {
        auto root = original.data_view();
        auto window = root.as_window();
        { auto m = window.begin_mutation(t1); m.push(Value{1}.view()); }
        { auto m = window.begin_mutation(t2); m.push(Value{2}.view()); }
        { auto m = window.begin_mutation(t3); m.push(Value{3}.view()); }
        { auto m = window.begin_mutation(t4); m.push(Value{4}.view()); }  // evicts 1
    }

    const auto image = capture_checkpoint(original.data_view());
    CHECK(image.kind == TSTypeKind::TSW);
    CHECK(image.modified_time == t4);
    REQUIRE(image.window.size() == 3);
    CHECK(image.window[0].time == t2);
    CHECK(image.window[2].time == t4);
    CHECK(image.window[0].value.view().checked_as<std::int32_t>() == 2);
    REQUIRE(image.evicted.has_value());  // the pushed-out element survives
    CHECK(image.evicted.view().checked_as<std::int32_t>() == 1);
    CHECK(image.evicted_time == t4);

    TSOutput restored{*tsw};
    TSCheckpointDiagnostics why;
    REQUIRE(validate_checkpoint(restored.data_view(), image, why));
    import_checkpoint(restored.data_view(), image, TSCheckpointRestoreGuard::begin());

    CHECK(restored.data_view().last_modified_time() == t4);
    auto restored_root = restored.data_view();
    auto window = restored_root.as_window();
    REQUIRE(window.size() == 3);
    CHECK(window.time_at(0) == t2);  // ORIGINAL per-entry times
    CHECK(window.time_at(1) == t3);
    CHECK(window.time_at(2) == t4);
    CHECK(window.at(0).checked_as<std::int32_t>() == 2);
    CHECK(window.at(2).checked_as<std::int32_t>() == 4);
    CHECK(restored.view(t4 + TimeDelta{1}).all_valid());
}

TEST_CASE("checkpoint: duration TSW restores without pruning historical entries",
          "[ts_checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *int32_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tsw = registry.tsw_duration(int32_meta, TimeDelta{20}, TimeDelta{0});

    const auto t1 = MIN_ST + TimeDelta{1};
    const auto t2 = t1 + TimeDelta{15};

    TSOutput original{*tsw};
    {
        auto root = original.data_view();
        auto window = root.as_window();
        { auto m = window.begin_mutation(t1); m.push(Value{1}.view()); }
        { auto m = window.begin_mutation(t2); m.push(Value{2}.view()); }
    }

    const auto image = capture_checkpoint(original.data_view());
    REQUIRE(image.window.size() == 2);

    // A naive replay through push would prune t1 against t2 - 20; the quiet
    // import must retain both entries exactly.
    TSOutput restored{*tsw};
    import_checkpoint(restored.data_view(), image, TSCheckpointRestoreGuard::begin());
    auto restored_root = restored.data_view();
    auto window = restored_root.as_window();
    REQUIRE(window.size() == 2);
    CHECK(window.time_at(0) == t1);
    CHECK(window.time_at(1) == t2);
}

TEST_CASE("checkpoint: REF representations refuse conservatively", "[ts_checkpoint]")
{
    auto &registry = TypeRegistry::instance();
    const auto *ref_int = registry.ref(registry.ts(int_meta()));

    TSOutput output{*ref_int};
    CHECK_FALSE(checkpoint_supported(output.data_view()));
    CHECK_THROWS_AS(capture_checkpoint(output.data_view()), std::logic_error);
}
