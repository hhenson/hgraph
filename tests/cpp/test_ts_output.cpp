#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/utils/slot_observer.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/date_time.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include <functional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

struct TSSMoveTrackedKey
{
    std::int32_t value{0};

    static inline std::size_t copy_construct_count{0};
    static inline std::size_t move_construct_count{0};

    TSSMoveTrackedKey() = default;
    explicit TSSMoveTrackedKey(std::int32_t v) : value{v} {}

    TSSMoveTrackedKey(const TSSMoveTrackedKey &other) : value(other.value)
    {
        ++copy_construct_count;
    }

    TSSMoveTrackedKey(TSSMoveTrackedKey &&other) noexcept : value(other.value)
    {
        other.value = 0;
        ++move_construct_count;
    }

    TSSMoveTrackedKey &operator=(const TSSMoveTrackedKey &other)
    {
        value = other.value;
        return *this;
    }

    TSSMoveTrackedKey &operator=(TSSMoveTrackedKey &&other) noexcept
    {
        value = other.value;
        other.value = 0;
        return *this;
    }

    [[nodiscard]] friend bool operator==(const TSSMoveTrackedKey &lhs,
                                         const TSSMoveTrackedKey &rhs) noexcept
    {
        return lhs.value == rhs.value;
    }

    [[nodiscard]] friend bool operator<(const TSSMoveTrackedKey &lhs,
                                        const TSSMoveTrackedKey &rhs) noexcept
    {
        return lhs.value < rhs.value;
    }

    static void reset_counts() noexcept
    {
        copy_construct_count = 0;
        move_construct_count = 0;
    }
};

template <>
struct std::hash<TSSMoveTrackedKey>
{
    [[nodiscard]] std::size_t operator()(const TSSMoveTrackedKey &key) const noexcept
    {
        return std::hash<std::int32_t>{}(key.value);
    }
};

namespace
{
    template <typename Range>
    std::size_t range_count(const Range &range)
    {
        std::size_t count = 0;
        for (const auto _ : range)
        {
            (void)_;
            ++count;
        }
        return count;
    }

    struct RecordingNotifiable : hgraph::Notifiable
    {
        std::vector<hgraph::DateTime> notified{};

        void notify(hgraph::DateTime modified_time) override
        {
            notified.push_back(modified_time);
        }
    };

    struct RecordingSlotObserver final : hgraph::SlotObserver
    {
        std::vector<std::string> events{};

        void on_capacity(std::size_t old_capacity, std::size_t new_capacity) override
        {
            events.push_back("capacity:" + std::to_string(old_capacity) + "->" +
                             std::to_string(new_capacity));
        }
        void on_insert(std::size_t slot) override { events.push_back("insert:" + std::to_string(slot)); }
        void on_remove(std::size_t slot) override { events.push_back("remove:" + std::to_string(slot)); }
        void on_erase(std::size_t slot) override { events.push_back("erase:" + std::to_string(slot)); }
        void on_clear() override { events.push_back("clear"); }
    };

    struct SelfUnsubscribingNotifiable : RecordingNotifiable
    {
        hgraph::TSDataView observed{};

        void notify(hgraph::DateTime modified_time) override
        {
            RecordingNotifiable::notify(modified_time);
            observed.unsubscribe(this);
        }
    };

    struct RemovingNotifiable : RecordingNotifiable
    {
        hgraph::TSDataView observed{};
        hgraph::Notifiable *target{nullptr};

        void notify(hgraph::DateTime modified_time) override
        {
            RecordingNotifiable::notify(modified_time);
            observed.unsubscribe(target);
        }
    };

    struct AddingNotifiable : RecordingNotifiable
    {
        hgraph::TSDataView observed{};
        hgraph::Notifiable *target{nullptr};
        bool added{false};

        void notify(hgraph::DateTime modified_time) override
        {
            RecordingNotifiable::notify(modified_time);
            if (!added)
            {
                observed.subscribe(target);
                added = true;
            }
        }
    };

    struct ReplacingNotifiable : RecordingNotifiable
    {
        hgraph::TSDataView observed{};
        hgraph::Notifiable *removed{nullptr};
        hgraph::Notifiable *replacement{nullptr};

        void notify(hgraph::DateTime modified_time) override
        {
            RecordingNotifiable::notify(modified_time);
            observed.unsubscribe(removed);
            observed.unsubscribe(this);
            observed.subscribe(replacement);
        }
    };

    struct MoveTrackedScalar
    {
        std::int32_t value{0};

        static inline std::size_t copy_construct_count{0};
        static inline std::size_t move_construct_count{0};
        static inline std::size_t copy_assign_count{0};
        static inline std::size_t move_assign_count{0};
        static inline std::size_t default_construct_count{0};
        static inline std::size_t destroy_count{0};

        MoveTrackedScalar() { ++default_construct_count; }
        explicit MoveTrackedScalar(std::int32_t v) : value{v} {}

        MoveTrackedScalar(const MoveTrackedScalar &other) : value(other.value)
        {
            ++copy_construct_count;
        }

        MoveTrackedScalar(MoveTrackedScalar &&other) noexcept : value(other.value)
        {
            other.value = 0;
            ++move_construct_count;
        }

        MoveTrackedScalar &operator=(const MoveTrackedScalar &other)
        {
            value = other.value;
            ++copy_assign_count;
            return *this;
        }

        MoveTrackedScalar &operator=(MoveTrackedScalar &&other) noexcept
        {
            value = other.value;
            other.value = 0;
            ++move_assign_count;
            return *this;
        }

        ~MoveTrackedScalar() { ++destroy_count; }

        [[nodiscard]] friend bool operator==(const MoveTrackedScalar &lhs,
                                             const MoveTrackedScalar &rhs) noexcept
        {
            return lhs.value == rhs.value;
        }

        [[nodiscard]] friend bool operator<(const MoveTrackedScalar &lhs,
                                            const MoveTrackedScalar &rhs) noexcept
        {
            return lhs.value < rhs.value;
        }

        static void reset_counts() noexcept
        {
            copy_construct_count = 0;
            move_construct_count = 0;
            copy_assign_count = 0;
            move_assign_count = 0;
            default_construct_count = 0;
            destroy_count = 0;
        }
    };
}

TEST_CASE("TSOutput owns root TSData and exposes TS validity")
{
    using namespace hgraph;

    static_assert(!std::is_copy_constructible_v<TSDataView>);
    static_assert(!std::is_copy_assignable_v<TSDataView>);
    static_assert(std::is_move_constructible_v<TSDataView>);
    static_assert(!std::is_copy_constructible_v<TSDataMutationView>);
    static_assert(!std::is_copy_constructible_v<TSSDataView>);
    static_assert(!std::is_copy_constructible_v<TSDDataView>);
    static_assert(!std::is_copy_constructible_v<TSBDataView>);
    static_assert(!std::is_copy_constructible_v<TSLDataView>);
    static_assert(!std::is_copy_constructible_v<TSWDataView>);
    static_assert(!std::is_copy_constructible_v<TSOutputView>);
    static_assert(!std::is_copy_assignable_v<TSOutputView>);
    static_assert(!std::is_copy_constructible_v<TSSOutputView>);
    static_assert(!std::is_copy_constructible_v<TSDOutputView>);
    static_assert(!std::is_copy_constructible_v<TSBOutputView>);
    static_assert(!std::is_copy_constructible_v<TSLOutputView>);
    static_assert(!std::is_copy_constructible_v<TSWOutputView>);
    static_assert(std::is_copy_constructible_v<TSOutputHandle>);

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    TSOutput output{*ts_int};
    REQUIRE(output.has_value());
    REQUIRE(output.schema() == ts_int);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    auto initial = output.view(t1);
    REQUIRE(initial.type_ref().record() != nullptr);
    REQUIRE(initial.type_ref().record() == output.type_ref().record());
    REQUIRE(initial.bound());
    REQUIRE(initial.evaluation_time() == t1);
    REQUIRE_FALSE(initial.valid());
    REQUIRE_FALSE(initial.all_valid());
    REQUIRE_FALSE(initial.modified());
    REQUIRE(initial.last_modified_time() == MIN_DT);
    REQUIRE(initial.value().checked_as<std::int32_t>() == 0);
    REQUIRE_FALSE(initial.delta_value().has_value());

    Value forty_two{42};
    {
        auto mutation = output.begin_mutation(t1);
        REQUIRE(mutation.current_mutation_time() == t1);
        REQUIRE(mutation.copy_value_from(forty_two.view()));
        REQUIRE(mutation.modified());
    }

    auto modified = output.view(t1);
    REQUIRE(modified.valid());
    REQUIRE(modified.all_valid());
    REQUIRE(modified.modified());
    REQUIRE_FALSE(output.view(t2).modified());
    REQUIRE(modified.last_modified_time() == t1);
    REQUIRE(modified.value().checked_as<std::int32_t>() == 42);
    REQUIRE(modified.delta_value().checked_as<std::int32_t>() == 42);
    REQUIRE_FALSE(output.view(t2).delta_value().has_value());
}

TEST_CASE("TSOutput move mutation moves an owned value without copy assignment")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *meta     = registry.register_scalar<MoveTrackedScalar>("MoveTrackedScalar");
    const auto *ts_meta  = registry.ts(meta);

    TSOutput output{*ts_meta};
    const auto t1 = MIN_ST;

    Value source{MoveTrackedScalar{42}};
    MoveTrackedScalar::reset_counts();
    {
        auto mutation = output.begin_mutation(t1);
        REQUIRE(mutation.move_value_from(std::move(source)));
        REQUIRE(mutation.modified());
    }

    REQUIRE(MoveTrackedScalar::copy_assign_count == 0);
    REQUIRE(MoveTrackedScalar::move_assign_count == 1);
    REQUIRE(output.view(t1).value().checked_as<MoveTrackedScalar>().value == 42);
    REQUIRE(output.view(t1).delta_value().checked_as<MoveTrackedScalar>().value == 42);
}

TEST_CASE("TSOutput move mutation uses writable borrowed views but rejects read-only views")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *meta     = registry.register_scalar<MoveTrackedScalar>("MoveTrackedScalar");
    const auto *ts_meta  = registry.ts(meta);
    const auto binding   = ValuePlanFactory::instance().type_for(meta);
    REQUIRE(binding != nullptr);

    TSOutput output{*ts_meta};
    MoveTrackedScalar source{73};
    const MoveTrackedScalar read_only_source{91};

    {
        auto mutation = output.begin_mutation(MIN_ST);
        ValueView read_only{binding, static_cast<const void *>(&read_only_source)};
        REQUIRE_THROWS_AS(mutation.move_value_from(std::move(read_only)), std::invalid_argument);
    }

    MoveTrackedScalar::reset_counts();
    {
        auto mutation = output.begin_mutation(MIN_ST);
        ValueView writable{binding, static_cast<void *>(&source)};
        REQUIRE(mutation.move_value_from(std::move(writable)));
    }

    REQUIRE(MoveTrackedScalar::copy_assign_count == 0);
    REQUIRE(MoveTrackedScalar::move_assign_count == 1);
    REQUIRE(output.view(MIN_ST).value().checked_as<MoveTrackedScalar>().value == 73);
}

TEST_CASE("TSOutput fixed TSB move mutation moves owned child fields")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *meta     = registry.register_scalar<MoveTrackedScalar>("MoveTrackedScalar");
    const auto *ts_meta  = registry.ts(meta);
    const auto *tsb_meta = registry.tsb("MoveTrackedBundle", {{"a", ts_meta}, {"b", ts_meta}});
    const auto binding  = ValuePlanFactory::instance().type_for(tsb_meta->value_schema);
    REQUIRE(binding != nullptr);

    BundleBuilder builder{binding};
    builder.set("a", Value{MoveTrackedScalar{10}});
    builder.set("b", Value{MoveTrackedScalar{20}});
    Value source = builder.build();

    TSOutput output{*tsb_meta};
    const auto t1 = MIN_ST;

    MoveTrackedScalar::reset_counts();
    {
        auto mutation = output.begin_mutation(t1);
        REQUIRE(mutation.move_value_from(std::move(source)));
        REQUIRE(mutation.modified());
    }

    auto value = output.view(t1).value().as_bundle();
    REQUIRE(value.at("a").checked_as<MoveTrackedScalar>().value == 10);
    REQUIRE(value.at("b").checked_as<MoveTrackedScalar>().value == 20);
    REQUIRE(MoveTrackedScalar::copy_assign_count == 0);
    REQUIRE(MoveTrackedScalar::move_assign_count == 2);
}

TEST_CASE("TSOutput dynamic TSL move mutation moves owned child fields")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *meta     = registry.register_scalar<MoveTrackedScalar>("MoveTrackedScalar");
    const auto *ts_meta  = registry.ts(meta);
    const auto *tsl_meta = registry.tsl(ts_meta, 0);
    const auto binding  = ValuePlanFactory::instance().type_for(meta);
    REQUIRE(binding != nullptr);

    ListBuilder builder{binding};
    builder.push_back(MoveTrackedScalar{10});
    builder.push_back(MoveTrackedScalar{20});
    builder.push_back(MoveTrackedScalar{30});
    Value source = builder.build();

    TSOutput output{*tsl_meta};
    const auto t1 = MIN_ST;

    MoveTrackedScalar::reset_counts();
    {
        auto mutation = output.begin_mutation(t1);
        REQUIRE(mutation.move_value_from(std::move(source)));
        REQUIRE(mutation.modified());
    }

    auto value = output.view(t1).value().as_list();
    REQUIRE(value.size() == 3);
    REQUIRE(value.at(0).checked_as<MoveTrackedScalar>().value == 10);
    REQUIRE(value.at(1).checked_as<MoveTrackedScalar>().value == 20);
    REQUIRE(value.at(2).checked_as<MoveTrackedScalar>().value == 30);
    REQUIRE(MoveTrackedScalar::copy_assign_count == 0);
    REQUIRE(MoveTrackedScalar::move_assign_count == 3);
}

TEST_CASE("TSOutput TSW move mutation moves owned list elements")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *meta     = registry.register_scalar<MoveTrackedScalar>("MoveTrackedScalar");
    const auto *tsw_meta = registry.tsw(meta, 3, 1);
    const auto binding  = ValuePlanFactory::instance().type_for(meta);
    REQUIRE(binding != nullptr);

    ListBuilder builder{binding};
    builder.push_back(MoveTrackedScalar{10});
    builder.push_back(MoveTrackedScalar{20});
    builder.push_back(MoveTrackedScalar{30});
    Value source = builder.build();

    TSOutput output{*tsw_meta};
    const auto t1 = MIN_ST;

    MoveTrackedScalar::reset_counts();
    {
        auto mutation = output.begin_mutation(t1);
        REQUIRE(mutation.move_value_from(std::move(source)));
        REQUIRE(mutation.modified());
    }

    auto output_view = output.view(t1);
    auto window = output_view.as_window();
    REQUIRE(window.size() == 3);
    REQUIRE(window.time_at(0) == t1);
    REQUIRE(window.time_at(1) == t1);
    REQUIRE(window.time_at(2) == t1);
    REQUIRE(window.at(0).checked_as<MoveTrackedScalar>().value == 10);
    REQUIRE(window.at(1).checked_as<MoveTrackedScalar>().value == 20);
    REQUIRE(window.at(2).checked_as<MoveTrackedScalar>().value == 30);
    REQUIRE(MoveTrackedScalar::copy_construct_count == 0);
    REQUIRE(MoveTrackedScalar::move_construct_count == 3);
    REQUIRE(MoveTrackedScalar::copy_assign_count == 0);
}

TEST_CASE("TSOutput TSS move mutation moves owned keys without removal copies")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *meta     = registry.register_scalar<TSSMoveTrackedKey>("TSSMoveTrackedKey");
    const auto *tss_meta = registry.tss(meta);
    const auto binding  = ValuePlanFactory::instance().type_for(meta);
    REQUIRE(binding != nullptr);

    SetBuilder initial_builder{binding};
    initial_builder.insert(TSSMoveTrackedKey{10});
    initial_builder.insert(TSSMoveTrackedKey{99});
    Value initial = initial_builder.build();

    SetBuilder source_builder{binding};
    source_builder.insert(TSSMoveTrackedKey{10});
    source_builder.insert(TSSMoveTrackedKey{20});
    Value source = source_builder.build();

    TSOutput output{*tss_meta};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    {
        auto mutation = output.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(initial.view()));
    }

    TSSMoveTrackedKey::reset_counts();
    {
        auto mutation = output.begin_mutation(t2);
        REQUIRE(mutation.move_value_from(std::move(source)));
        REQUIRE(mutation.modified());
    }

    Value key10{TSSMoveTrackedKey{10}};
    Value key20{TSSMoveTrackedKey{20}};
    Value key99{TSSMoveTrackedKey{99}};
    auto  value = output.view(t2).value().as_set();
    REQUIRE(value.size() == 2);
    REQUIRE(value.contains(key10.view()));
    REQUIRE(value.contains(key20.view()));
    REQUIRE_FALSE(value.contains(key99.view()));
    REQUIRE(TSSMoveTrackedKey::copy_construct_count == 0);
    REQUIRE(TSSMoveTrackedKey::move_construct_count == 1);
}

TEST_CASE("TSOutput TSD move mutation moves keys and child values without removal copies")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *key_meta = registry.register_scalar<TSSMoveTrackedKey>("TSSMoveTrackedKey");
    const auto *value_meta = registry.register_scalar<MoveTrackedScalar>("MoveTrackedScalar");
    const auto *ts_value = registry.ts(value_meta);
    const auto *tsd_meta = registry.tsd(key_meta, ts_value);
    const auto key_binding = ValuePlanFactory::instance().type_for(key_meta);
    const auto value_binding = ValuePlanFactory::instance().type_for(value_meta);
    REQUIRE(key_binding != nullptr);
    REQUIRE(value_binding != nullptr);

    MapBuilder initial_builder{key_binding, value_binding};
    initial_builder.set_item(TSSMoveTrackedKey{10}, MoveTrackedScalar{1});
    initial_builder.set_item(TSSMoveTrackedKey{99}, MoveTrackedScalar{99});
    Value initial = initial_builder.build();

    MapBuilder source_builder{key_binding, value_binding};
    source_builder.set_item(TSSMoveTrackedKey{10}, MoveTrackedScalar{100});
    source_builder.set_item(TSSMoveTrackedKey{20}, MoveTrackedScalar{200});
    Value source = source_builder.build();

    TSOutput output{*tsd_meta};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    {
        auto output_view = output.view(t1);
        auto dict = output_view.as_dict();
        auto mutation = dict.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(initial.view()));
    }

    TSSMoveTrackedKey::reset_counts();
    MoveTrackedScalar::reset_counts();
    {
        auto mutation = output.begin_mutation(t2);
        REQUIRE(mutation.move_value_from(std::move(source)));
        REQUIRE(mutation.modified());
    }

    Value key10{TSSMoveTrackedKey{10}};
    Value key20{TSSMoveTrackedKey{20}};
    Value key99{TSSMoveTrackedKey{99}};
    auto  value = output.view(t2).value().as_map();
    REQUIRE(value.size() == 2);
    REQUIRE(value.at(key10.view()).checked_as<MoveTrackedScalar>().value == 100);
    REQUIRE(value.at(key20.view()).checked_as<MoveTrackedScalar>().value == 200);
    REQUIRE_FALSE(value.contains(key99.view()));
    REQUIRE(TSSMoveTrackedKey::copy_construct_count == 0);
    REQUIRE(TSSMoveTrackedKey::move_construct_count == 1);
    REQUIRE(MoveTrackedScalar::copy_assign_count == 0);
    REQUIRE(MoveTrackedScalar::move_assign_count == 2);
}

TEST_CASE("TSData dynamic storage metrics include TSS capacity and nested TSD children", "[memory]")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *str_meta = registry.register_scalar<std::string>("string");
    const auto *tss_int = registry.tss(int_meta);
    const auto *outer_tsd = registry.tsd(str_meta, tss_int);
    const auto t1 = MIN_ST;

    TSOutput set_output{*tss_int};
    const auto set_before = set_output.data_view().dynamic_storage_metrics();
    {
        auto set_data = set_output.data_view();
        auto set = set_data.as_set();
        auto mutation = set.begin_mutation(t1);
        mutation.reserve(64);
    }
    const auto set_after = set_output.data_view().dynamic_storage_metrics();
    CHECK(set_after.live_bytes > set_before.live_bytes);
    CHECK(set_after.reserved_bytes > set_before.reserved_bytes);
    CHECK(set_after.reserved_bytes >= set_after.live_bytes);

    TSOutput outer{*outer_tsd};
    Value key{std::string{"nested"}};
    {
        auto outer_data = outer.data_view();
        auto dict = outer_data.as_dict();
        auto mutation = dict.begin_mutation(t1);
        static_cast<void>(mutation.at(key.view()));
    }

    auto outer_data = outer.data_view();
    auto dict = outer_data.as_dict();
    auto child = dict.at(key.view());
    const auto outer_before = outer.data_view().dynamic_storage_metrics();
    const auto child_before = child.dynamic_storage_metrics();
    {
        auto child_set = child.as_set();
        auto mutation = child_set.begin_mutation(t1);
        mutation.reserve(64);
    }
    const auto outer_after = outer.data_view().dynamic_storage_metrics();
    const auto child_after = child.dynamic_storage_metrics();

    REQUIRE(child_after.reserved_bytes > child_before.reserved_bytes);
    CHECK(outer_after.reserved_bytes - outer_before.reserved_bytes ==
          child_after.reserved_bytes - child_before.reserved_bytes);
    CHECK(outer_after.live_bytes - outer_before.live_bytes ==
          child_after.live_bytes - child_before.live_bytes);
    CHECK(outer_after.reserved_bytes >= outer_after.live_bytes);
}

TEST_CASE("TSOutput dynamic storage metrics include multi-observer allocation", "[memory]")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    TSOutput output{*registry.ts(int_meta)};
    RecordingNotifiable first;
    RecordingNotifiable second;

    const auto empty = output.dynamic_storage_metrics();
    output.subscribe(&first);
    const auto single = output.dynamic_storage_metrics();
    CHECK(single.live_bytes == empty.live_bytes);
    CHECK(single.reserved_bytes == empty.reserved_bytes);

    output.subscribe(&second);
    const auto many = output.dynamic_storage_metrics();
    CHECK(many.live_bytes > single.live_bytes);
    CHECK(many.reserved_bytes >= many.live_bytes);

    output.unsubscribe(&second);
    output.unsubscribe(&first);
    const auto cleared = output.dynamic_storage_metrics();
    CHECK(cleared.live_bytes == empty.live_bytes);
    CHECK(cleared.reserved_bytes == empty.reserved_bytes);
}

TEST_CASE("TSOutputHandle stores output identity without evaluation time")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    TSOutput output{*ts_int};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    Value value{7};
    {
        auto mutation = output.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    const auto view = output.view(t1);
    auto       handle = view.handle();
    REQUIRE(handle.bound());
    REQUIRE(handle.output() == &output);
    REQUIRE(handle.storage_type() == view.storage_type());
    REQUIRE(handle.schema() == view.schema());

    const TSOutputHandle from_view{view};
    REQUIRE(handle.same_as(from_view));

    const auto replay_at_t1 = handle.view(t1);
    const auto replay_at_t2 = handle.view(t2);
    REQUIRE(replay_at_t1.output() == &output);
    REQUIRE(replay_at_t1.data_view().data() == view.data_view().data());
    REQUIRE(replay_at_t1.evaluation_time() == t1);
    REQUIRE(replay_at_t1.modified());
    REQUIRE(replay_at_t2.evaluation_time() == t2);
    REQUIRE_FALSE(replay_at_t2.modified());

    handle.reset();
    REQUIRE_FALSE(handle.bound());
}

TEST_CASE("TSOutput non-peered TSD can hold forwarding value slots")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *str_meta = registry.register_scalar<std::string>("string");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd_int  = registry.tsd(str_meta, ts_int);

    TSOutput source{*ts_int};
    TSOutput forwarding_dict{
        TSEndpointSchema::non_peered_dict(tsd_int, TSEndpointSchema::peered(ts_int))};

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    const auto t4 = t3 + TimeDelta{1};
    Value      key{std::string{"a"}};
    Value      one{1};
    Value      two{2};

    {
        auto mutation = source.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(one.view()));
    }
    {
        auto dict_view = forwarding_dict.view(t1);
        auto mutation = dict_view.as_dict().begin_mutation(t1);
        auto element  = mutation.at(key.view());
        REQUIRE(element.valid());
    }

    auto dict_at_t1 = forwarding_dict.view(t1);
    auto element = dict_at_t1.as_dict().at(key.view());
    const auto slot = dict_at_t1.as_dict().find_slot(key.view());
    const auto *element_address = element.data_view().data();
    REQUIRE(element.forwarding());
    element.bind_forwarding_target(source.view(t1));
    REQUIRE(element.valid());
    REQUIRE(element.value().checked_as<std::int32_t>() == 1);
    const auto subscribed_count = source.data_view().observer_count();

    {
        auto mutation = source.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(two.view()));
    }

    auto dict_at_t2 = forwarding_dict.view(t2);
    auto after = dict_at_t2.as_dict().at(key.view());
    REQUIRE(after.valid());
    REQUIRE(after.value().checked_as<std::int32_t>() == 2);

    {
        auto dict_view = forwarding_dict.view(t3);
        auto mutation = dict_view.as_dict().begin_mutation(t3);
        REQUIRE(mutation.erase(key.view()));
    }
    auto removed_view = forwarding_dict.view(t3);
    auto removed_dict = removed_view.as_dict();
    REQUIRE_FALSE(removed_dict.slot_live(slot));
    REQUIRE(removed_dict.slot_occupied(slot));
    auto removed_child = removed_dict.at_slot(slot);
    REQUIRE(removed_child.data_view().data() == element_address);
    REQUIRE_FALSE(removed_child.data_view().has_current_value());
    REQUIRE(source.data_view().observer_count() < subscribed_count);

    {
        auto dict_view = forwarding_dict.view(t4);
        auto mutation = dict_view.as_dict().begin_mutation(t4);
        auto resurrected = mutation.at(key.view());
        REQUIRE(resurrected.data() == element_address);
        REQUIRE(resurrected.child_id() == slot);
        REQUIRE_FALSE(resurrected.has_current_value());
    }
    auto resurrected_view = forwarding_dict.view(t4);
    auto resurrected_dict = resurrected_view.as_dict();
    REQUIRE(resurrected_dict.slot_live(slot));
    REQUIRE(resurrected_dict.slot_occupied(slot));
}

TEST_CASE("TSD output structural ranges do not repeat a prior removal on a forwarding retick")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_integer = registry.ts(integer);
    const auto *dict_schema = registry.tsd(integer, ts_integer);

    TSOutput first{*dict_schema};
    TSOutput second{*dict_schema};
    TSOutput forwarding{TSEndpointSchema::peered(dict_schema)};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    Value removed_key{std::int32_t{1}};
    Value surviving_key{std::int32_t{2}};
    Value initial{std::int32_t{10}};

    for (TSOutput *source : {&first, &second})
    {
        auto output_view = source->view(t1);
        auto dict = output_view.as_dict();
        auto mutation = dict.begin_mutation(t1);
        mutation.set(removed_key.view(), initial.view());
        mutation.set(surviving_key.view(), initial.view());
    }
    for (TSOutput *source : {&first, &second})
    {
        auto output_view = source->view(t2);
        auto dict = output_view.as_dict();
        auto mutation = dict.begin_mutation(t2);
        REQUIRE(mutation.erase(removed_key.view()));
    }

    forwarding.view(t1).bind_forwarding_target(first.view(t1));
    forwarding.view(t3).bind_forwarding_target(second.view(t3));

    auto output_view = forwarding.view(t3);
    auto current = output_view.as_dict();
    REQUIRE(current.modified());
    auto added_keys = current.added_keys();
    auto added_values = current.added_values();
    auto added_items = current.added_items();
    auto removed_keys = current.removed_keys();
    auto removed_values = current.removed_values();
    auto removed_items = current.removed_items();
    CHECK(added_keys.begin() == added_keys.end());
    CHECK(added_values.begin() == added_values.end());
    CHECK(added_items.begin() == added_items.end());
    CHECK(removed_keys.begin() == removed_keys.end());
    CHECK(removed_values.begin() == removed_values.end());
    CHECK(removed_items.begin() == removed_items.end());
}

TEST_CASE("TSOutput TSD same-cycle resurrection does not reconstruct element storage")
{
    using namespace hgraph;

    auto       &registry   = TypeRegistry::instance();
    const auto *key_meta   = registry.register_scalar<std::string>("string");
    const auto *value_meta = registry.register_scalar<MoveTrackedScalar>("MoveTrackedScalar");
    const auto *ts_value   = registry.ts(value_meta);
    const auto *tsd_value  = registry.tsd(key_meta, ts_value);

    TSOutput output{*tsd_value};
    Value    key{std::string{"a"}};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    const void *child_address = nullptr;
    std::size_t child_slot = TS_DATA_NO_CHILD_ID;
    std::size_t capacity = 0;
    {
        auto view = output.view(t1);
        auto mutation = view.as_dict().begin_mutation(t1);
        auto child = mutation.at(key.view());
        child_address = child.data();
        child_slot = child.child_id();
        capacity = mutation.slot_capacity();
    }

    const auto constructions = MoveTrackedScalar::default_construct_count;
    const auto destructions = MoveTrackedScalar::destroy_count;
    {
        auto view = output.view(t2);
        auto mutation = view.as_dict().begin_mutation(t2);
        REQUIRE(mutation.erase(key.view()));
        REQUIRE_FALSE(mutation.slot_live(child_slot));
        REQUIRE(mutation.slot_occupied(child_slot));

        auto resurrected = mutation.at(key.view());
        REQUIRE(resurrected.data() == child_address);
        REQUIRE(resurrected.child_id() == child_slot);
        REQUIRE(mutation.slot_capacity() == capacity);
    }

    REQUIRE(MoveTrackedScalar::default_construct_count == constructions);
    REQUIRE(MoveTrackedScalar::destroy_count == destructions);
}

TEST_CASE("TSOutput REF stores TimeSeriesReference as value and delta")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *ref_int  = registry.ref(ts_int);

    TSOutput target{*ts_int};
    TSOutput ref_output{*ref_int};

    const auto t1 = MIN_ST + TimeDelta{1};
    const TimeSeriesReference reference{target.view(t1)};
    Value                     wrapped{reference};

    {
        auto mutation = ref_output.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(wrapped.view()));
    }

    auto view = ref_output.view(t1);
    REQUIRE(view.valid());
    REQUIRE(view.modified());

    const auto  stored_value = view.value();
    const auto &stored       = stored_value.checked_as<TimeSeriesReference>();
    REQUIRE(stored.has_output());
    REQUIRE(stored.target_schema() == ts_int);
    REQUIRE(stored == reference);

    const auto  delta_value = view.delta_value();
    const auto &delta       = delta_value.checked_as<TimeSeriesReference>();
    REQUIRE(delta.has_output());
    REQUIRE(delta == reference);
}

TEST_CASE("TSOutput slot deltas are reclaimed lazily on the next mutation")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tss      = registry.tss(int_meta);

    TSOutput output{*tss};
    auto     root = output.data_view();
    auto     set = root.as_set();

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    Value      one{1};

    // There is no eager cleanup: after adding at t1 the add delta stays physically
    // present (the data-layer accessor is raw) until the next mutation reclaims it.
    {
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
    }
    REQUIRE(range_count(set.added()) == 1);
    REQUIRE(set.contains(one.view()));

    // The next mutation (at t2) reclaims the t1 add delta via prepare_delta, then
    // records its own removal — no explicit cleanup call required.
    {
        auto mutation = set.begin_mutation(t2);
        REQUIRE(mutation.remove(one.view()));
    }
    REQUIRE(range_count(set.added()) == 0);
    REQUIRE(range_count(set.removed()) == 1);
    REQUIRE_FALSE(set.contains(one.view()));

    // Same-cycle add+remove cancels structurally; the next mutation reclaims it.
    const auto t3 = t2 + TimeDelta{1};
    {
        auto mutation = set.begin_mutation(t3);
        REQUIRE(mutation.add(one.view()));
        REQUIRE(mutation.remove(one.view()));
    }
    REQUIRE(output.view(t3).modified());
    REQUIRE(output.view(t3).valid());
    REQUIRE(range_count(set.removed()) == 0);
    REQUIRE_THROWS_AS(output.begin_mutation(MIN_DT), std::invalid_argument);
}

TEST_CASE("forwarding TSS outputs preserve slot observer remove and erase protocol across rebinds")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tss      = registry.tss(int_meta);

    TSOutput first{*tss};
    TSOutput second{*tss};
    TSOutput forwarding{TSEndpointSchema::peered(tss)};

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    Value      one{1};
    Value      two{2};

    auto link = forwarding.view(t1);
    link.bind_forwarding_target(first.view(t1));

    RecordingSlotObserver observer;
    auto link_data = link.data_view().borrowed_ref();
    auto link_set = link_data.as_set();
    link_set.subscribe_slot_observer(&observer);

    {
        auto first_data = first.data_view();
        auto mutation = first_data.as_set().begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
    }
    REQUIRE(std::ranges::find(observer.events, "insert:0") != observer.events.end());

    {
        auto first_data = first.data_view();
        auto mutation = first_data.as_set().begin_mutation(t2);
        REQUIRE(mutation.remove(one.view()));
    }
    REQUIRE(std::ranges::find(observer.events, "remove:0") != observer.events.end());

    {
        auto first_data = first.data_view();
        auto mutation = first_data.as_set().begin_mutation(t3);
        REQUIRE(mutation.add(two.view()));
    }
    REQUIRE(std::ranges::find(observer.events, "erase:0") != observer.events.end());

    link = forwarding.view(t3);
    link.bind_forwarding_target(second.view(t3));
    REQUIRE(observer.events.back() == "clear");

    observer.events.clear();
    {
        auto second_data = second.data_view();
        auto mutation = second_data.as_set().begin_mutation(t3);
        REQUIRE(mutation.add(one.view()));
    }
    REQUIRE(std::ranges::find(observer.events, "insert:0") != observer.events.end());

    link_data = link.data_view().borrowed_ref();
    link_set = link_data.as_set();
    link_set.unsubscribe_slot_observer(&observer);
}

TEST_CASE("forwarding TSS inputs preserve realized bindings for derived keys and rebind deltas")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *text = registry.value_type("str");
    const auto *base = registry.bundle(
        "tests.output", "PolymorphicSetBase", {{"id", integer}}, {}, true);
    const auto *small = registry.bundle(
        "tests.output", "PolymorphicSetSmall",
        {{"id", integer}, {"label", text}}, {base});
    const auto *large = registry.bundle(
        "tests.output", "PolymorphicSetLarge",
        {{"id", integer}, {"quantity", integer}}, {base});
    const auto *tss = registry.tss(base);

    const auto make_key = [&](const ValueTypeMetaData *schema, std::int32_t id) {
        BundleBuilder builder{ValuePlanFactory::instance().type_for(schema)};
        builder.set("id", Value{id});
        if (schema == small) builder.set("label", Value{Str{"small"}});
        if (schema == large) builder.set("quantity", Value{std::int32_t{10}});
        return builder.build();
    };
    const Value small_key = make_key(small, 1);
    const Value large_key = make_key(large, 2);

    const auto snapshot = TypeRealizationSnapshot::capture(registry);
    TypeRealizationScope realization_scope{snapshot.get()};
    TSOutput first{*tss};
    TSOutput second{*tss};
    TSOutput forwarding{TSEndpointSchema::peered(tss)};

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    forwarding.view(t1).bind_forwarding_target(first.view(t1));

    {
        auto first_data = first.data_view();
        auto mutation = first_data.as_set().begin_mutation(t1);
        REQUIRE(mutation.add(small_key.view()));
        REQUIRE(mutation.add(large_key.view()));
    }
    auto forwarding_data = forwarding.data_view();
    auto linked = forwarding_data.as_set();
    REQUIRE(linked.layout().key_binding == ValuePlanFactory::instance().type_for(base));
    std::vector<const ValueTypeMetaData *> added;
    for (const auto &key : linked.added()) added.push_back(key.concrete().schema());
    REQUIRE(added == std::vector<const ValueTypeMetaData *>{small, large});

    {
        auto first_data = first.data_view();
        auto mutation = first_data.as_set().begin_mutation(t2);
        REQUIRE(mutation.remove(small_key.view()));
    }
    std::vector<const ValueTypeMetaData *> removed;
    for (const auto &key : linked.removed()) removed.push_back(key.concrete().schema());
    REQUIRE(removed == std::vector<const ValueTypeMetaData *>{small});

    {
        auto second_data = second.data_view();
        auto mutation = second_data.as_set().begin_mutation(t3);
        REQUIRE(mutation.add(small_key.view()));
    }
    forwarding.view(t3).bind_forwarding_target(second.view(t3));
    forwarding_data = forwarding.data_view();
    linked = forwarding_data.as_set();
    std::vector<const ValueTypeMetaData *> current;
    for (const auto &key : linked.values()) current.push_back(key.concrete().schema());
    REQUIRE(current == std::vector<const ValueTypeMetaData *>{small});
}

TEST_CASE("sampled forwarding rebind publishes a valid-to-invalid transition")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts       = registry.ts(int_meta);

    TSOutput valid_source{*ts};
    TSOutput invalid_source{*ts};
    TSOutput forwarding{TSEndpointSchema::peered(ts)};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    Value value{11};
    REQUIRE(valid_source.begin_mutation(t1).copy_value_from(value.view()));
    forwarding.view(t1).bind_forwarding_target(valid_source.view(t1));
    REQUIRE(forwarding.view(t1).valid());

    RecordingNotifiable observer;
    forwarding.data_view().subscribe(&observer);
    forwarding.view(t2).bind_forwarding_target_sampled(invalid_source.view(t2));

    REQUIRE_FALSE(forwarding.view(t2).valid());
    REQUIRE(observer.notified == std::vector<DateTime>{t2});
    forwarding.data_view().unsubscribe(&observer);
}

TEST_CASE("TSOutput non-peered TSD realizes closed-union keys")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *base = registry.bundle(
        "tests.output", "ForwardingBaseKey", {{"id", int_meta}});
    const auto *derived = registry.bundle(
        "tests.output", "ForwardingDerivedKey",
        {{"id", int_meta}, {"priority", int_meta}}, {base});
    const auto *ts_int = registry.ts(int_meta);
    const auto *tsd = registry.tsd(base, ts_int);

    BundleBuilder key_builder{ValuePlanFactory::instance().type_for(derived)};
    key_builder.set("id", Value{std::int32_t{7}}.view());
    key_builder.set("priority", Value{std::int32_t{3}}.view());
    Value key = key_builder.build();

    const auto snapshot = TypeRealizationSnapshot::capture(registry);
    TypeRealizationScope realization_scope{snapshot.get()};
    TSOutput output{
        TSEndpointSchema::non_peered_dict(tsd, TSEndpointSchema::peered(ts_int))};

    auto output_data = output.data_view();
    auto output_dict = output_data.as_dict();
    REQUIRE(output_dict.layout().key_binding == snapshot->type_for(base));
    auto output_view = output.view(MIN_ST);
    auto mutation = output_view.as_dict().begin_mutation(MIN_ST);
    auto element = mutation.at(key.view());
    REQUIRE(element.valid());
    auto current_view = output.view(MIN_ST);
    REQUIRE(current_view.as_dict().contains(key.view()));
}

TEST_CASE("TSOutput root parent is reattached after copy and move")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};

    Value one{1};
    Value two{2};
    Value three{3};

    TSOutput source{*ts_int};
    {
        auto mutation = source.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(one.view()));
    }

    TSOutput copied{source};
    REQUIRE(copied.data_view().has_parent());
    {
        auto mutation = copied.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(two.view()));
    }
    REQUIRE(copied.view(t2).modified());

    TSOutput moved{std::move(copied)};
    REQUIRE(moved.data_view().has_parent());
    {
        auto mutation = moved.begin_mutation(t3);
        REQUIRE(mutation.copy_value_from(three.view()));
    }
    REQUIRE(moved.view(t3).modified());
}

TEST_CASE("TSOutputView all_valid recurses through fixed bundle children")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsb      = registry.tsb("TSOutputAllValidBundle", {{"a", ts_int}, {"b", ts_int}});

    TSOutput output{*tsb};

    const auto t1 = MIN_ST;
    Value      one{1};
    Value      two{2};

    auto root = output.data_view();
    auto bundle = root.as_bundle();
    auto a = bundle.field("a");
    auto b = bundle.field("b");

    {
        auto mutation = a.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(one.view()));
    }

    auto partially_valid = output.view(t1);
    REQUIRE(partially_valid.valid());
    REQUIRE_FALSE(partially_valid.all_valid());

    {
        auto mutation = b.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(two.view()));
    }

    auto fully_valid = output.view(t1);
    REQUIRE(fully_valid.valid());
    REQUIRE(fully_valid.all_valid());

    auto fully_valid_bundle = fully_valid.as_bundle();
    REQUIRE(fully_valid_bundle.field("a").value().checked_as<std::int32_t>() == 1);
    REQUIRE(fully_valid_bundle.field("b").value().checked_as<std::int32_t>() == 2);
}

TEST_CASE("TSBOutputView keys range retains the output view as its context")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsb      = registry.tsb(
        "TSOutputKeysBundle", {{"left", ts_int}, {"right", ts_int}});

    TSOutput output{*tsb};
    auto     root   = output.view(MIN_ST);
    auto     bundle = root.as_bundle();

    std::vector<std::string> keys;
    for (std::string_view key : bundle.keys()) { keys.emplace_back(key); }

    REQUIRE(keys == std::vector<std::string>{"left", "right"});
}

TEST_CASE("TSData observers notify at the modified level and bubble to parents")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsb      = registry.tsb("TSOutputObserverBundle", {{"a", ts_int}, {"b", ts_int}});

    TSOutput output{*tsb};
    auto     root = output.data_view();
    auto     bundle = root.as_bundle();
    auto     a = bundle.field("a");
    auto     b = bundle.field("b");

    RecordingNotifiable root_observer;
    RecordingNotifiable a_observer;
    RecordingNotifiable a_second_observer;
    RecordingNotifiable b_observer;

    output.subscribe(&root_observer);
    a.subscribe(&a_observer);
    a.subscribe(&a_second_observer);
    b.subscribe(&b_observer);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};

    Value one{1};
    Value two{2};
    Value three{3};

    {
        auto mutation = a.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(one.view()));
    }

    CHECK(root_observer.notified == std::vector<DateTime>{t1});
    CHECK(a_observer.notified == std::vector<DateTime>{t1});
    CHECK(a_second_observer.notified == std::vector<DateTime>{t1});
    CHECK(b_observer.notified.empty());

    {
        auto mutation = a.begin_mutation(t1);
        REQUIRE_FALSE(mutation.copy_value_from(two.view()));
    }

    CHECK(root_observer.notified == std::vector<DateTime>{t1});
    CHECK(a_observer.notified == std::vector<DateTime>{t1});
    CHECK(a_second_observer.notified == std::vector<DateTime>{t1});

    {
        auto mutation = b.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(three.view()));
    }

    CHECK(root_observer.notified == std::vector<DateTime>{t1});
    CHECK(b_observer.notified == std::vector<DateTime>{t1});

    a.unsubscribe(&a_observer);
    {
        auto mutation = a.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(two.view()));
    }

    CHECK(root_observer.notified == std::vector<DateTime>{t1, t2});
    CHECK(a_observer.notified == std::vector<DateTime>{t1});
    CHECK(a_second_observer.notified == std::vector<DateTime>{t1, t2});

    a.unsubscribe(&a_second_observer);
    {
        auto mutation = a.begin_mutation(t3);
        REQUIRE(mutation.copy_value_from(three.view()));
    }

    CHECK(root_observer.notified == std::vector<DateTime>{t1, t2, t3});
    CHECK(a_second_observer.notified == std::vector<DateTime>{t1, t2});

    output.unsubscribe(&root_observer);
    b.unsubscribe(&b_observer);
}

TEST_CASE("TSData observers support reentrant subscribe and unsubscribe")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(int_meta);

    TSOutput output{*ts_int};
    auto     observed = output.data_view();

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    Value      one{1};
    Value      two{2};
    Value      three{3};

    SelfUnsubscribingNotifiable self_unsubscribing;
    RecordingNotifiable         survivor;
    self_unsubscribing.observed = observed.borrowed_ref();
    observed.subscribe(&self_unsubscribing);
    observed.subscribe(&survivor);
    REQUIRE(observed.observer_count() == 2);

    {
        auto mutation = output.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(one.view()));
    }

    CHECK(self_unsubscribing.notified == std::vector<DateTime>{t1});
    CHECK(survivor.notified == std::vector<DateTime>{t1});
    CHECK(observed.observer_count() == 1);

    {
        auto mutation = output.begin_mutation(t2);
        REQUIRE(mutation.copy_value_from(two.view()));
    }

    CHECK(self_unsubscribing.notified == std::vector<DateTime>{t1});
    CHECK(survivor.notified == std::vector<DateTime>{t1, t2});
    observed.unsubscribe(&survivor);
    REQUIRE_FALSE(observed.has_observers());

    RemovingNotifiable remover;
    RecordingNotifiable removed_before_notify;
    RecordingNotifiable after_removed;
    remover.observed = observed.borrowed_ref();
    remover.target   = &removed_before_notify;
    observed.subscribe(&remover);
    observed.subscribe(&removed_before_notify);
    observed.subscribe(&after_removed);

    {
        auto mutation = output.begin_mutation(t3);
        REQUIRE(mutation.copy_value_from(three.view()));
    }

    CHECK(remover.notified == std::vector<DateTime>{t3});
    CHECK(removed_before_notify.notified.empty());
    CHECK(after_removed.notified == std::vector<DateTime>{t3});
    CHECK(observed.observer_count() == 2);

    observed.unsubscribe(&remover);
    observed.unsubscribe(&after_removed);
    REQUIRE_FALSE(observed.has_observers());

    const auto t4 = t3 + TimeDelta{1};
    const auto t5 = t4 + TimeDelta{1};
    Value      four{4};
    Value      five{5};
    AddingNotifiable adding;
    RecordingNotifiable added_later;
    adding.observed = observed.borrowed_ref();
    adding.target   = &added_later;
    observed.subscribe(&adding);

    {
        auto mutation = output.begin_mutation(t4);
        REQUIRE(mutation.copy_value_from(four.view()));
    }

    CHECK(adding.notified == std::vector<DateTime>{t4});
    CHECK(added_later.notified.empty());
    CHECK(observed.observer_count() == 2);

    {
        auto mutation = output.begin_mutation(t5);
        REQUIRE(mutation.copy_value_from(five.view()));
    }

    CHECK(adding.notified == std::vector<DateTime>{t4, t5});
    CHECK(added_later.notified == std::vector<DateTime>{t5});

    observed.unsubscribe(&adding);
    observed.unsubscribe(&added_later);

    const auto t6 = t5 + TimeDelta{1};
    const auto t7 = t6 + TimeDelta{1};
    Value      six{6};
    Value      seven{7};
    ReplacingNotifiable replacing;
    RecordingNotifiable replaced;
    RecordingNotifiable replacement;
    replacing.observed    = observed.borrowed_ref();
    replacing.removed     = &replaced;
    replacing.replacement = &replacement;
    observed.subscribe(&replacing);
    observed.subscribe(&replaced);

    {
        auto mutation = output.begin_mutation(t6);
        REQUIRE(mutation.copy_value_from(six.view()));
    }

    CHECK(replacing.notified == std::vector<DateTime>{t6});
    CHECK(replaced.notified.empty());
    CHECK(replacement.notified.empty());
    CHECK(observed.observer_count() == 1);

    {
        auto mutation = output.begin_mutation(t7);
        REQUIRE(mutation.copy_value_from(seven.view()));
    }

    CHECK(replacement.notified == std::vector<DateTime>{t7});
    observed.unsubscribe(&replacement);
}

TEST_CASE("TSOutputView delegates validity through slot TSData ops")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tss      = registry.tss(int_meta);
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd      = registry.tsd(int_meta, ts_int);

    const auto t1 = MIN_ST;
    Value      one{1};

    TSOutput set_output{*tss};
    REQUIRE(set_output.view(t1).type_ref() == set_output.type_ref());
    REQUIRE_FALSE(set_output.view(t1).valid());
    REQUIRE_FALSE(set_output.view(t1).all_valid());

    auto set_root = set_output.data_view();
    auto set      = set_root.as_set();
    {
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
    }

    REQUIRE(set_output.view(t1).valid());
    REQUIRE(set_output.view(t1).all_valid());

    TSOutput dict_output{*tsd};
    auto     dict_root = dict_output.data_view();
    auto     dict      = dict_root.as_dict();
    Value    key{7};
    Value    value{42};

    {
        auto mutation = dict.begin_mutation(t1);
        static_cast<void>(mutation.at(key.view()));
    }

    REQUIRE(dict_output.view(t1).valid());
    // A TSD's ``all_valid`` is its ``valid``: it does not walk its values.
    // This matches upstream, where TSD declares no ``all_valid`` override and
    // inherits ``PythonTimeSeriesOutput.all_valid``. A key whose value has not
    // been written yet is therefore not detected here - use the value's own
    // ``valid`` if that matters to a node.
    REQUIRE(dict_output.view(t1).all_valid());

    {
        auto child = dict.at(key.view());
        auto mutation = child.begin_mutation(t1);
        REQUIRE(mutation.copy_value_from(value.view()));
    }

    REQUIRE(dict_output.view(t1).all_valid());
}

TEST_CASE("TSOutput shape casts return endpoint views for slot collections")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tss      = registry.tss(int_meta);
    const auto *ts_int   = registry.ts(int_meta);
    const auto *tsd      = registry.tsd(int_meta, ts_int);

    const auto t1 = MIN_ST;
    Value      one{1};
    Value      key{7};
    Value      value{42};

    TSOutput set_output{*tss};
    auto     set_view = set_output.view(t1);
    auto     set = set_view.as_set();
    REQUIRE(set.base().type_ref() == set_output.type_ref());
    {
        auto mutation = set.begin_mutation(t1);
        REQUIRE(mutation.add(one.view()));
    }
    auto current_set_view = set_output.view(t1);
    auto current_set = current_set_view.as_set();
    REQUIRE(current_set.contains(one.view()));
    REQUIRE(range_count(current_set.data_view().values()) == 1);

    TSOutput dict_output{*tsd};
    auto     dict_view = dict_output.view(t1);
    auto     dict = dict_view.as_dict();
    REQUIRE(dict.base().type_ref() == dict_output.type_ref());
    {
        auto mutation = dict.begin_mutation(t1);
        auto child = mutation.at(key.view());
        auto child_mutation = child.begin_mutation(t1);
        REQUIRE(child_mutation.copy_value_from(value.view()));
    }

    auto current_dict_view = dict_output.view(t1);
    auto current_dict = current_dict_view.as_dict();
    REQUIRE(current_dict.contains(key.view()));
    auto child = current_dict.at(key.view());
    REQUIRE(child.valid());
    REQUIRE(child.value().checked_as<std::int32_t>() == 42);
    REQUIRE(range_count(current_dict.values()) == 1);
    REQUIRE(range_count(current_dict.items()) == 1);
    auto data_values = current_dict.data_view().values();
    auto data_items  = current_dict.data_view().items();
    REQUIRE(range_count(data_values) == 1);
    REQUIRE(range_count(data_items) == 1);
}

TEST_CASE("TSOutputView delegates window all_valid through TSData ops")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *tsw      = registry.tsw(int_meta, 2, 2);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    Value      one{1};
    Value      two{2};

    TSOutput output{*tsw};
    auto     root = output.data_view();
    auto     window = root.as_window();

    {
        auto mutation = window.begin_mutation(t1);
        mutation.push(one.view());
    }

    // A window has a current value from its first element. The minimum period
    // controls recursive/full validity, not whether the window exists.
    REQUIRE(output.view(t1).valid());
    REQUIRE_FALSE(output.view(t1).all_valid());

    {
        auto mutation = window.begin_mutation(t2);
        mutation.push(two.view());
    }

    REQUIRE(output.view(t2).valid());
    REQUIRE(output.view(t2).all_valid());
    auto current_view   = output.view(t2);
    auto current_window = current_view.as_window();
    auto values         = current_window.data_view().values();
    auto time_values    = current_window.data_view().time_values();
    auto value_times    = current_window.data_view().value_times();
    REQUIRE(range_count(values) == 2);
    REQUIRE(range_count(time_values) == 2);
    REQUIRE(range_count(value_times) == 2);
}

// Delta clocks are monotonic (GitHub issue #38): a record carrying an older
// time — a freshly bound link replaying its source's historical timestamp —
// must neither rewind tracking state nor rebase a slot store's delta window
// (that would erase sibling marks already recorded this cycle).
TEST_CASE("TSDataTracking ignores records older than the current time")
{
    using namespace hgraph;

    TSDataTracking tracking{};
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    REQUIRE(tracking.record_modified(t2));
    REQUIRE(tracking.last_modified_time == t2);
    REQUIRE_FALSE(tracking.record_modified(t1));
    REQUIRE(tracking.last_modified_time == t2);
    REQUIRE_FALSE(tracking.record_modified(t2));
}

TEST_CASE("TSD delta window survives a stale child record within the cycle")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *key_meta = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_int   = registry.ts(key_meta);
    const auto *tsd_meta = registry.tsd(key_meta, ts_int);

    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};

    TSOutput output{*tsd_meta};
    Value    key_a{10};
    Value    key_b{20};
    Value    one{1};
    Value    two{2};
    {
        auto output_view = output.view(t2);
        auto dict = output_view.as_dict();
        auto mutation = dict.begin_mutation(t2);
        mutation.set(key_a.view(), one.view());
        mutation.set(key_b.view(), two.view());
    }

    auto data = output.data_view();
    auto dict = data.as_dict();
    const auto slot_a = dict.find_slot(key_a.view());
    const auto slot_b = dict.find_slot(key_b.view());
    REQUIRE(dict.slot_added(slot_a));
    REQUIRE(dict.slot_added(slot_b));
    REQUIRE(dict.slot_modified(slot_a));
    REQUIRE(dict.slot_modified(slot_b));

    // Replay a stale (t1) child record into the t2 window — the pre-fix
    // behaviour rebased the delta window and wiped the sibling marks.
    const auto &table = *data.storage_type().ops();
    table.record_child_modified_impl(table.context, const_cast<void *>(data.data()), slot_a, t1);

    REQUIRE(dict.slot_added(slot_a));
    REQUIRE(dict.slot_added(slot_b));
    REQUIRE(dict.slot_modified(slot_a));
    REQUIRE(dict.slot_modified(slot_b));
    REQUIRE(data.modified(t2));
}

TEST_CASE("TSD structural ranges ignore stale bits when only the root reticks")
{
    using namespace hgraph;

    auto       &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<std::int32_t>("int32");
    const auto *ts_integer = registry.ts(integer);
    const auto *dict_schema = registry.tsd(integer, ts_integer);
    const auto t1 = MIN_ST;
    const auto t2 = t1 + TimeDelta{1};
    const auto t3 = t2 + TimeDelta{1};
    Value removed_key{std::int32_t{1}};
    Value surviving_key{std::int32_t{2}};
    Value initial{std::int32_t{10}};

    TSOutput output{*dict_schema};
    {
        auto output_view = output.view(t1);
        auto mutation = output_view.as_dict().begin_mutation(t1);
        mutation.set(removed_key.view(), initial.view());
        mutation.set(surviving_key.view(), initial.view());
    }
    {
        auto output_view = output.view(t2);
        auto mutation = output_view.as_dict().begin_mutation(t2);
        REQUIRE(mutation.erase(removed_key.view()));
    }

    auto data = output.data_view();
    REQUIRE(data.as_dict().structural_delta_current(t2));

    // A referenced child can stamp the exposed root without advancing the
    // root's TSD delta window. The old removal bits must not be republished.
    const auto &ops = *data.storage_type().ops();
    auto *tracking = ops.mutable_tracking_impl(ops.context, const_cast<void *>(data.data()));
    REQUIRE(tracking != nullptr);
    REQUIRE(tracking->record_modified(t3));
    auto output_view = output.view(t3);
    auto current = output_view.as_dict();
    REQUIRE(current.modified());
    REQUIRE_FALSE(current.data_view().structural_delta_current(t3));
    CHECK(range_count(current.added_keys()) == 0);
    CHECK(range_count(current.added_values()) == 0);
    CHECK(range_count(current.added_items()) == 0);
    CHECK(range_count(current.removed_keys()) == 0);
    CHECK(range_count(current.removed_values()) == 0);
    CHECK(range_count(current.removed_items()) == 0);
}
