#include <hgraph/runtime/distributed_boundary.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value_builder.h>

#include <limits>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/generators/catch_generators.hpp>

namespace
{
    using namespace hgraph;
    using namespace hgraph::distributed;

    struct BoundaryFixture
    {
        const TSValueTypeMetaData *schema;
        TSOutput source;
        TSInput input;
        TSOutput target;
        BoundaryTransfer transfer;
        explicit BoundaryFixture(const TSValueTypeMetaData *type)
            : schema(type), source(type),
              input(TSInputBuilderFactory::checked_builder_for(*type, TSEndpointSchema::peered(type))),
              target(type), transfer(type)
        { input.view(nullptr, MIN_ST - MIN_TD).bind_output(source.view(MIN_ST - MIN_TD)); }

        Value relay(DateTime time, bool full = false)
        {
            const auto captured = transfer.capture(input.view(nullptr, time), full);
            transfer.apply(target.view(time), captured.view());
            return captured;
        }
    };
    TSDOutputView dict_at(TSOutput &output, DateTime time)
    { auto view = output.view(time); return view.as_dict(); }
    TSLOutputView list_at(TSOutput &output, DateTime time)
    { auto view = output.view(time); return view.as_list(); }
    TSWOutputView window_at(TSOutput &output, DateTime time)
    { auto view = output.view(time); return view.as_window(); }

    void set(const TSOutputView &out, Int value)
    {
        auto mutation = out.begin_mutation(out.evaluation_time());
        static_cast<void>(mutation.copy_value_from(Value{value}.view()));
    }
}

TEST_CASE("distributed boundary retains added invalid dictionary children and removals")
{
    const auto *schema = schema_descriptor<TSD<Int, TS<Int>>>::ts_meta();
    BoundaryFixture fixture{schema};
    const Value first{Int{1}}, second{Int{2}};
    {
        auto mutation = dict_at(fixture.source, MIN_ST).begin_mutation(MIN_ST);
        static_cast<void>(mutation.at(first.view()));
        const auto child = mutation.at(second.view());
        set(TSOutputView{&fixture.source, child, MIN_ST}, 42);
        mutation.touch();
    }
    REQUIRE(dict_at(fixture.source, MIN_ST).size() == 2);
    fixture.relay(MIN_ST);
    auto dict = dict_at(fixture.target, MIN_ST);
    REQUIRE(dict.size() == 2);
    CHECK_FALSE(dict.at(first.view()).valid());
    CHECK(dict.at(second.view()).value().checked_as<Int>() == 42);

    const auto later = MIN_ST + MIN_TD;
    {
        auto mutation = dict_at(fixture.source, later).begin_mutation(later);
        static_cast<void>(mutation.erase(second.view()));
    }
    fixture.relay(later);
    auto updated = dict_at(fixture.target, later);
    CHECK(updated.size() == 1);
    CHECK(updated.contains(first.view()));
    CHECK_FALSE(updated.contains(second.view()));
    const auto removal_time = later + MIN_TD;
    {
        auto mutation = dict_at(fixture.source, removal_time).begin_mutation(removal_time);
        static_cast<void>(mutation.erase(first.view()));
    }
    fixture.relay(removal_time);
    CHECK(dict_at(fixture.target, removal_time).empty());
}

TEST_CASE("distributed boundary retains dynamic list holes and shrink")
{
    const auto *schema = TypeRegistry::instance().tsl(schema_descriptor<TS<Int>>::ts_meta());
    BoundaryFixture fixture{schema};
    {
        auto list = list_at(fixture.source, MIN_ST);
        list.resize(4);
        set(list.at(1), 17);
    }
    fixture.relay(MIN_ST);
    auto list = list_at(fixture.target, MIN_ST);
    REQUIRE(list.size() == 4);
    CHECK_FALSE(list.at(0).valid());
    CHECK(list.at(1).value().checked_as<Int>() == 17);
    CHECK_FALSE(list.at(3).valid());
    const auto later = MIN_ST + MIN_TD;
    list_at(fixture.source, later).resize(0);
    fixture.relay(later);
    CHECK(list_at(fixture.target, later).size() == 0);
}

TEST_CASE("distributed boundary merges only the selected worker list indices")
{
    BoundaryFixture fixture{schema_descriptor<TSL<TS<Int>, 4>>::ts_meta()};
    auto source = list_at(fixture.source, MIN_ST);
    for (std::size_t index = 0; index < 4; ++index) set(source.at(index), static_cast<Int>(index + 1));
    const auto in = fixture.input.view(nullptr, MIN_ST);
    const auto even = fixture.transfer.capture(in, true, 0, 2);
    const auto odd = fixture.transfer.capture(in, true, 1, 2);
    fixture.transfer.apply(fixture.target.view(MIN_ST), even.view(), true);
    auto intermediate = list_at(fixture.target, MIN_ST);
    CHECK(intermediate.at(0).value().checked_as<Int>() == 1);
    CHECK_FALSE(intermediate.at(1).valid());
    fixture.transfer.apply(fixture.target.view(MIN_ST), odd.view(), true);
    auto complete = list_at(fixture.target, MIN_ST);
    for (std::size_t index = 0; index < 4; ++index)
        CHECK(complete.at(index).value().checked_as<Int>() == static_cast<Int>(index + 1));
}

TEST_CASE("distributed boundary windows stream constant-size deltas and preserve sampled history")
{
    const bool duration = GENERATE(false, true);
    const bool warmup = GENERATE(false, true);
    auto &registry = TypeRegistry::instance();
    const auto *scalar = registry.register_scalar<Int>("int");
    const auto *schema = duration ? registry.tsw_duration(scalar, MIN_TD * 500, warmup ? MIN_TD * 500 : MIN_TD)
                                  : registry.tsw(scalar, std::size_t{100}, warmup ? std::size_t{100} : std::size_t{1});
    BoundaryFixture fixture{schema};
    std::size_t first_size = 0;
    for (Int index = 0; index < 80; ++index)
    {
        const auto time = MIN_ST + MIN_TD * index;
        window_at(fixture.source, time).begin_mutation(time).push(Value{index}.view());
        const auto payload = fixture.relay(time);
        if (index == 1) first_size = payload.view().checked_as<Str>().size();
        if (index > 1) CHECK(payload.view().checked_as<Str>().size() == first_size);
    }
    const auto sampled_time = MIN_ST + MIN_TD * 100;
    TSOutput sampled{schema};
    const auto snapshot = fixture.transfer.capture(fixture.input.view(nullptr, sampled_time), true);
    fixture.transfer.apply(sampled.view(sampled_time), snapshot.view());
    auto restored = window_at(sampled, sampled_time);
    REQUIRE(restored.size() == 80);
    CHECK(restored.time_at(0) == MIN_ST);
    CHECK(restored.time_at(79) == MIN_ST + MIN_TD * 79);
    CHECK(restored.at(79).checked_as<Int>() == 79);

    const auto clear_time = MIN_ST + MIN_TD * 101;
    window_at(fixture.source, clear_time).begin_mutation(clear_time).clear();
    fixture.relay(clear_time);
    CHECK(window_at(fixture.target, clear_time).size() == 0);
    CHECK(window_at(fixture.target, clear_time).data_view().cleared(clear_time));
    const auto next_time = clear_time + MIN_TD;
    {
        auto mutation = window_at(fixture.source, next_time).begin_mutation(next_time);
        mutation.clear();
        mutation.push(Value{Int{99}}.view());
    }
    fixture.relay(next_time);
    CHECK(window_at(fixture.target, next_time).size() == 1);
    CHECK(window_at(fixture.target, next_time).at(0).checked_as<Int>() == 99);
}

TEST_CASE("distributed boundary transports set membership incrementally and on rebind")
{
    BoundaryFixture fixture{schema_descriptor<TSS<Int>>::ts_meta()};
    const Value one{Int{1}}, two{Int{2}};
    {
        auto output = fixture.source.view(MIN_ST);
        auto mutation = output.as_set().begin_mutation(MIN_ST);
        (void)mutation.add(one.view());
        (void)mutation.add(two.view());
    }
    fixture.relay(MIN_ST);
    auto output = fixture.target.view(MIN_ST);
    CHECK(output.as_set().size() == 2);
    const auto later = MIN_ST + MIN_TD;
    {
        auto source = fixture.source.view(later);
        auto mutation = source.as_set().begin_mutation(later);
        (void)mutation.remove(one.view());
    }
    fixture.relay(later);
    auto updated = fixture.target.view(later);
    CHECK_FALSE(updated.as_set().contains(one.view()));
    CHECK(updated.as_set().contains(two.view()));
    fixture.relay(later + MIN_TD, true);
    auto sampled = fixture.target.view(later + MIN_TD);
    CHECK(sampled.as_set().size() == 1);
    CHECK(sampled.as_set().contains(two.view()));
}

TEST_CASE("distributed boundary preserves dictionary membership across child invalidation")
{
    BoundaryFixture fixture{schema_descriptor<TSD<Int, TS<Int>>>::ts_meta()};
    const Value key{Int{4}};
    {
        auto mutation = dict_at(fixture.source, MIN_ST).begin_mutation(MIN_ST);
        const auto child = mutation.at(key.view());
        set(TSOutputView{&fixture.source, child, MIN_ST}, 12);
    }
    fixture.relay(MIN_ST);
    const auto invalid_time = MIN_ST + MIN_TD;
    {
        auto child = dict_at(fixture.source, invalid_time).at(key.view());
        auto mutation = child.begin_mutation(invalid_time);
        static_cast<void>(mutation.invalidate());
    }
    fixture.relay(invalid_time);
    auto invalid = dict_at(fixture.target, invalid_time);
    CHECK(invalid.contains(key.view()));
    CHECK_FALSE(invalid.at(key.view()).valid());
    const auto valid_time = invalid_time + MIN_TD;
    {
        auto child = dict_at(fixture.source, valid_time).at(key.view());
        set(child, 19);
    }
    fixture.relay(valid_time);
    CHECK(dict_at(fixture.target, valid_time).at(key.view()).value().checked_as<Int>() == 19);
}

TEST_CASE("distributed boundary membership handles same-cycle cancellation and slot reuse")
{
    BoundaryFixture fixture{schema_descriptor<TSD<Int, TS<Int>>>::ts_meta()};
    const Value one{Int{1}}, two{Int{2}}, three{Int{3}};
    {
        auto mutation = dict_at(fixture.source, MIN_ST).begin_mutation(MIN_ST);
        (void)mutation.at(one.view());
    }
    fixture.relay(MIN_ST);
    const auto t1 = MIN_ST + MIN_TD;
    {
        auto mutation = dict_at(fixture.source, t1).begin_mutation(t1);
        (void)mutation.erase(one.view());
        (void)mutation.at(one.view());
        (void)mutation.at(two.view());
        (void)mutation.erase(two.view());
    }
    fixture.relay(t1);
    CHECK(dict_at(fixture.target, t1).size() == 1);
    CHECK(dict_at(fixture.target, t1).contains(one.view()));
    const auto t2 = t1 + MIN_TD;
    {
        auto mutation = dict_at(fixture.source, t2).begin_mutation(t2);
        (void)mutation.erase(one.view());
        (void)mutation.at(two.view());
    }
    fixture.relay(t2);
    CHECK(dict_at(fixture.target, t2).size() == 1);
    CHECK(dict_at(fixture.target, t2).contains(two.view()));
    const auto t3 = t2 + MIN_TD;
    {
        auto mutation = dict_at(fixture.source, t3).begin_mutation(t3);
        (void)mutation.erase(two.view());
        (void)mutation.at(three.view());
    }
    fixture.relay(t3);
    CHECK(dict_at(fixture.target, t3).size() == 1);
    CHECK(dict_at(fixture.target, t3).contains(three.view()));
}

TEST_CASE("distributed boundary unbound fixed list preserves shape while invalidating values")
{
    BoundaryFixture fixture{schema_descriptor<TSL<TS<Int>, 3>>::ts_meta()};
    auto list = list_at(fixture.source, MIN_ST);
    set(list.at(0), 1);
    fixture.relay(MIN_ST);
    const auto time = MIN_ST + MIN_TD;
    fixture.input.view(nullptr, time).unbind_output();
    fixture.relay(time);
    auto target = list_at(fixture.target, time);
    CHECK(target.size() == 3);
    CHECK_FALSE(target.at(0).valid());
}

TEST_CASE("distributed boundary rejects oversized sparse list extents before resizing")
{
    const auto *schema = TypeRegistry::instance().tsl(schema_descriptor<TS<Int>>::ts_meta());
    BoundaryFixture fixture{schema};
    auto target = list_at(fixture.target, MIN_ST);
    target.resize(2);
    set(target.at(0), 42);
    std::string bytes(1, '\1');
    write_varint(std::numeric_limits<std::uint64_t>::max(), bytes);
    bytes.push_back('\0');
    const Value payload{bytes};
    CHECK_THROWS_AS(fixture.transfer.apply(fixture.target.view(MIN_ST + MIN_TD), payload.view()), std::runtime_error);
    CHECK_THROWS_AS(fixture.transfer.root_list_size(payload.view()), std::runtime_error);
    CHECK(target.size() == 2);
    CHECK(target.at(0).value().checked_as<Int>() == 42);
}

TEST_CASE("distributed boundary validates a complete delta before publishing any changes")
{
    const bool merge = GENERATE(false, true);
    const auto *schema = TypeRegistry::instance().tsl(schema_descriptor<TS<Int>>::ts_meta());
    BoundaryFixture fixture{schema};
    auto source = list_at(fixture.source, MIN_ST);
    source.resize(4);
    set(source.at(0), 99);
    set(source.at(3), 100);
    auto target = list_at(fixture.target, MIN_ST);
    target.resize(2);
    set(target.at(0), 42);
    const auto captured = fixture.transfer.capture(fixture.input.view(nullptr, MIN_ST), true);
    auto bytes = captured.view().checked_as<Str>();
    const bool trailing = GENERATE(false, true);
    if (trailing) bytes.push_back('\0');
    else bytes.pop_back(); // missing terminator after otherwise valid children
    const Value malformed{bytes};
    if (trailing)
        CHECK_THROWS_AS(fixture.transfer.apply(fixture.target.view(MIN_ST + MIN_TD), malformed.view(), merge), std::invalid_argument);
    else
        CHECK_THROWS_AS(fixture.transfer.apply(fixture.target.view(MIN_ST + MIN_TD), malformed.view(), merge), std::runtime_error);
    CHECK(target.size() == 2);
    CHECK(target.at(0).value().checked_as<Int>() == 42);
    CHECK_FALSE(fixture.target.view(MIN_ST + MIN_TD).modified());
}

TEST_CASE("distributed boundary rejects nested oversized lists before adding dictionary membership")
{
    const auto *schema = TypeRegistry::instance().tsd(TypeRegistry::instance().register_scalar<Int>("int"),
        TypeRegistry::instance().tsl(schema_descriptor<TS<Int>>::ts_meta()));
    BoundaryFixture fixture{schema};
    std::string bytes{"\1\0\1", 3}; // live dict, no removals, one added child
    bind_binary_converter(TypeRegistry::instance().register_scalar<Int>("int")).write(Value{Int{7}}.view(), bytes);
    bytes.push_back('\1'); // live list
    write_varint(std::numeric_limits<std::uint64_t>::max(), bytes);
    bytes.append(2, '\0');
    const Value malformed{bytes};
    CHECK_THROWS_AS(fixture.transfer.apply(fixture.target.view(MIN_ST), malformed.view()), std::runtime_error);
    CHECK(dict_at(fixture.target, MIN_ST).empty());
}

TEST_CASE("distributed boundary list extent shares the configurable decode work budget")
{
    const auto *schema = TypeRegistry::instance().tsl(schema_descriptor<TS<Int>>::ts_meta());
    BoundaryFixture fixture{schema};
    list_at(fixture.source, MIN_ST).resize(20);
    const auto payload = fixture.transfer.capture(fixture.input.view(nullptr, MIN_ST), true);
    const BoundaryTransfer limited{schema, BinaryDecodeLimits{.max_work = 10}};
    CHECK_THROWS_AS(limited.apply(fixture.target.view(MIN_ST), payload.view()), std::runtime_error);
    CHECK(list_at(fixture.target, MIN_ST).empty());
    fixture.transfer.apply(fixture.target.view(MIN_ST), payload.view());
    CHECK(list_at(fixture.target, MIN_ST).size() == 20);
}

TEST_CASE("distributed boundary malformed set delta does not remove live membership")
{
    BoundaryFixture fixture{schema_descriptor<TSS<Int>>::ts_meta()};
    const Value key{Int{7}};
    auto source = fixture.source.view(MIN_ST);
    (void)source.as_set().begin_mutation(MIN_ST).add(key.view());
    fixture.relay(MIN_ST);
    const auto later = MIN_ST + MIN_TD;
    source = fixture.source.view(later);
    (void)source.as_set().begin_mutation(later).remove(key.view());
    auto payload = fixture.transfer.capture(fixture.input.view(nullptr, later));
    auto bytes = payload.view().checked_as<Str>();
    bytes.pop_back();
    const Value truncated{bytes};
    CHECK_THROWS_AS(fixture.transfer.apply(fixture.target.view(later), truncated.view()), std::runtime_error);
    auto target = fixture.target.view(later);
    CHECK(target.as_set().contains(key.view()));
    CHECK_FALSE(target.modified());
}

TEST_CASE("distributed boundary malformed window delta does not clear existing history")
{
    const auto *schema = TypeRegistry::instance().tsw(
        TypeRegistry::instance().register_scalar<Int>("int"), std::size_t{4}, std::size_t{1});
    BoundaryFixture fixture{schema};
    window_at(fixture.source, MIN_ST).begin_mutation(MIN_ST).push(Value{Int{42}}.view());
    fixture.relay(MIN_ST);
    const auto later = MIN_ST + MIN_TD;
    // Live window, clear then push, but no encoded sample.
    const Value truncated{std::string{"\1\1\1", 3}};
    CHECK_THROWS_AS(fixture.transfer.apply(fixture.target.view(later), truncated.view()), std::runtime_error);
    auto target = window_at(fixture.target, later);
    CHECK(target.size() == 1);
    CHECK(target.at(0).checked_as<Int>() == 42);
    CHECK(target.time_at(0) == MIN_ST);
    CHECK_FALSE(fixture.target.view(later).modified());
}

TEST_CASE("distributed boundary validates full window samples before changing an earlier sibling")
{
    const int invalid = GENERATE(0, 1, 2, 3, 4, 5);
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<Int>("int");
    const auto *window = invalid == 4
        ? registry.tsw_duration(integer, TimeDelta{10}, TimeDelta{0})
        : registry.tsw(integer, std::size_t(invalid == 0 ? 1 : 2), std::size_t{1});
    const auto *schema = registry.un_named_tsb({{"first", schema_descriptor<TS<Int>>::ts_meta()},
                                               {"window", window}});
    BoundaryFixture fixture{schema};
    auto target = fixture.target.view(MIN_ST);
    set(target.indexed_child_at(0), 42);
    auto prior_window = target.indexed_child_at(1);
    prior_window.as_window().begin_mutation(MIN_ST).push(Value{Int{7}}.view());
    const auto later = MIN_ST + TimeDelta{100};
    std::vector<DateTime> times;
    switch (invalid)
    {
        case 0: times = {MIN_ST, MIN_ST}; break; // more samples than the count period
        case 1: times = {MIN_DT}; break;
        case 2: times = {later - MIN_TD, later - MIN_TD - MIN_TD}; break;
        case 3: times = {later + MIN_TD}; break;
        case 5: times = {MIN_DT - MIN_TD}; break;
        default: times = {MIN_ST, later}; break; // wider than the duration period
    }
    const auto values = bind_binary_converter(integer);
    const auto timestamps = bind_binary_converter(scalar_descriptor<DateTime>::value_meta());
    std::string bytes{"\1\1\0\1", 4}; // bundle: update first scalar
    values.write(Value{Int{99}}.view(), bytes);
    bytes.append("\1\1\3", 3); // update second child with a full live window
    write_varint(times.size(), bytes);
    for (const auto time : times)
    {
        timestamps.write(Value{time}.view(), bytes);
        values.write(Value{Int{100}}.view(), bytes);
    }
    bytes.push_back('\0');
    const Value malformed{bytes};
    CHECK_THROWS_AS(fixture.transfer.apply(fixture.target.view(later), malformed.view()), std::invalid_argument);
    CHECK(target.indexed_child_at(0).value().checked_as<Int>() == 42);
    CHECK(prior_window.as_window().size() == 1);
    CHECK(prior_window.as_window().at(0).checked_as<Int>() == 7);
    CHECK_FALSE(fixture.target.view(later).modified());
}

TEST_CASE("distributed boundary rejects duplicate window child updates before mutation")
{
    const int shape = GENERATE(0, 1, 2); // list, bundle, dictionary
    auto &registry = TypeRegistry::instance();
    const auto *integer = registry.register_scalar<Int>("int");
    const auto *window = registry.tsw(integer, std::size_t{4}, std::size_t{1});
    const auto *schema = shape == 0 ? registry.tsl(window)
        : shape == 1 ? registry.un_named_tsb({{"window", window}}) : registry.tsd(integer, window);
    BoundaryFixture fixture{schema};
    const Value key{Int{7}};
    auto child = [&]() -> TSOutputView {
        auto target = fixture.target.view(MIN_ST);
        if (shape == 0)
        {
            auto list = target.as_list();
            list.resize(1);
            return list.at(0);
        }
        if (shape == 1) return target.indexed_child_at(0);
        auto mutation = target.as_dict().begin_mutation(MIN_ST);
        return TSOutputView{&fixture.target, mutation.at(key.view()), MIN_ST};
    }();
    child.as_window().begin_mutation(MIN_ST).push(Value{Int{42}}.view());
    const auto values = bind_binary_converter(integer);
    std::string bytes(1, '\1');
    if (shape == 0) write_varint(1, bytes);
    if (shape == 2) bytes.push_back('\0'); // no removals
    for (int duplicate = 0; duplicate < 2; ++duplicate)
    {
        bytes.push_back('\1');
        if (shape == 2) values.write(key.view(), bytes);
        else write_varint(0, bytes);
        bytes.append("\1\0\1", 3); // live window, no clear, one push
        values.write(Value{Int{99}}.view(), bytes);
    }
    bytes.push_back('\0');
    const Value malformed{bytes};
    const auto later = MIN_ST + MIN_TD;
    CHECK_THROWS_AS(fixture.transfer.apply(fixture.target.view(later), malformed.view()), std::invalid_argument);
    CHECK(child.as_window().size() == 1);
    CHECK(child.as_window().at(0).checked_as<Int>() == 42);
    CHECK_FALSE(fixture.target.view(later).modified());
}

namespace
{
    // A scalar nobody has given a wire form.
    struct BoundaryOrphan
    {
        std::string text{};

        friend bool operator==(const BoundaryOrphan &, const BoundaryOrphan &) = default;
    };
}  // namespace

TEST_CASE("distributed boundary refuses a scalar with no wire form while it is being wired")
{
    auto       &registry = TypeRegistry::instance();
    const auto *orphan = registry.register_scalar<BoundaryOrphan>("tests.distributed_boundary.Orphan");
    const auto *schema = registry.tsd(registry.register_scalar<Int>("int"), registry.ts(orphan));

    // Building the transfer IS wiring: nothing has run, no value exists, and the
    // message says what needed the wire form and the whole schema it sits in.
    try
    {
        const distributed::BoundaryTransfer transfer{schema, "dmap_ input 2"};
        FAIL("a boundary was wired around a scalar that cannot cross it");
    }
    catch (const BinaryWireFormError &error)
    {
        CHECK(error.scalar() == "tests.distributed_boundary.Orphan");
        CHECK_THAT(error.what(), Catch::Matchers::ContainsSubstring("required by dmap_ input 2"));
        CHECK_THAT(error.what(), Catch::Matchers::ContainsSubstring(std::string{schema->name()}));
        CHECK_THAT(error.what(), Catch::Matchers::ContainsSubstring("register_binary_atom"));
    }
    CHECK_THROWS_AS(distributed::BoundaryTransfer{schema}, BinaryWireFormError);
}

