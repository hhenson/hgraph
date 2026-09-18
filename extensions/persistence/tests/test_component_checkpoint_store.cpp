#include <hgraph/persistence/component_checkpoint_store.h>
#include "checkpoint_v1_fixture.h"
#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/static_schema.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>

#include <bit>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <limits>
#include <string>

using namespace hgraph;
using namespace hgraph::persistence;

namespace
{
    using DurableCounterState = TSB<"durable_checkpoint_counter", Field<"total", TS<Int>>>;

    struct DurableCounter
    {
        static constexpr auto name = "durable_checkpoint_counter";
        static void start(RecordableState<DurableCounterState> state)
        {
            auto total = state.field<"total">();
            if (!total.valid()) { total.set(Int{0}); }
        }
        static void eval(In<"ts", TS<Int>> input, RecordableState<DurableCounterState> state,
                         Out<TS<Int>> out)
        {
            if (input.value() == -999) { throw std::runtime_error("failed durable day"); }
            auto total = state.field<"total">();
            const auto value = total.value().checked_as<Int>() + input.value();
            total.set(value);
            out.set(value);
        }
    };

    struct DurableCounterStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> input)
        {
            return wire<DurableCounter>(w, input);
        }
    };

    struct DurableComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return stdlib::component<DurableCounterStrategy>(w, "durable-strategy", input);
        }
    };

    struct FreezeStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"predicate", TS<Bool>> predicate,
                                    NamedPort<"value", TS<Int>> value)
        {
            return wire<stdlib::freeze>(w, predicate, value).as<TS<Int>>();
        }
    };

    struct FreezeComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> predicate, Port<TS<Int>> value)
        {
            return stdlib::component<FreezeStrategy>(w, "activity-strategy", predicate, value);
        }
    };

    struct UntilTrueStrategy
    {
        static Port<TS<Bool>> compose(Wiring &w, NamedPort<"value", TS<Bool>> value)
        {
            return wire<stdlib::until_true>(w, value).as<TS<Bool>>();
        }
    };

    struct UntilTrueComponent
    {
        static Port<TS<Bool>> compose(Wiring &w, Port<TS<Bool>> value)
        {
            return stdlib::component<UntilTrueStrategy>(w, "activity-strategy", value);
        }
    };

    testing::EvalNodeRunOptions interval(Int begin, Int end)
    {
        return {.start_time = MIN_ST + MIN_TD * begin, .end_time = MIN_ST + MIN_TD * end};
    }

    struct TemporaryDirectory
    {
        std::filesystem::path path = std::filesystem::temp_directory_path() /
            ("hgraph-component-checkpoint-" + std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()));
        ~TemporaryDirectory()
        {
            std::error_code ignored;
            std::filesystem::remove_all(path, ignored);
        }
    };

    ComponentCheckpoint fixture()
    {
        ComponentCheckpoint checkpoint;
        checkpoint.component_id = "strategy";
        checkpoint.graph_signature = "example-revision-1";
        checkpoint.cut = MIN_ST + MIN_TD * 2;
        checkpoint.completed_until = MIN_ST + MIN_TD * 3;
        TSCheckpointImage image;
        image.schema = schema_descriptor<TS<Int>>::ts_meta();
        image.last_modified_time = checkpoint.cut;
        image.payload = Value{Int{17}};
        NodeCheckpointImage node;
        node.id = "total";
        node.signature = "running-total-v1";
        node.output = image;
        node.recordable_state = image;
        checkpoint.graph.nodes.push_back(std::move(node));
        return checkpoint;
    }

    std::string image_bytes(const Frame &frame)
    {
        const auto &array = static_cast<const arrow::LargeBinaryArray &>(*frame.table->column(0)->chunk(0));
        return std::string{array.GetView(0)};
    }

    Frame with_image_bytes(const Frame &frame, std::string_view bytes)
    {
        arrow::LargeBinaryBuilder builder;
        REQUIRE(builder.Append(bytes).ok());
        std::shared_ptr<arrow::Array> array;
        REQUIRE(builder.Finish(&array).ok());
        return Frame{arrow::Table::Make(frame.table->schema(), {std::move(array)})};
    }

    /** A published envelope assembled by hand: one image cell and its metadata. */
    Frame envelope(std::string_view format, std::string_view predecessor, std::string_view bytes)
    {
        arrow::LargeBinaryBuilder builder;
        REQUIRE(builder.Append(bytes).ok());
        std::shared_ptr<arrow::Array> array;
        REQUIRE(builder.Finish(&array).ok());
        const auto metadata = arrow::key_value_metadata(
            {"hgraph.checkpoint.format", "hgraph.checkpoint.predecessor"},
            {std::string{format}, std::string{predecessor}});
        return Frame{arrow::Table::Make(
            arrow::schema({arrow::field("checkpoint", arrow::large_binary(), false)}, metadata),
            {std::move(array)})};
    }

    constexpr std::string_view format_v1 = "hgraph.component-checkpoint.v1";
    constexpr std::string_view format_v2 = "hgraph.component-checkpoint.v2";
}

TEST_CASE("component checkpoint store: native wired days resume from a durable complete image")
{
    using namespace hgraph::testing;
    TemporaryDirectory directory;
    store::FrameStoreConfig config;
    config.location = store::LocalLocation{directory.path.string()};
    {
        GlobalContext context;
        const ComponentCheckpointStore checkpoints{config};
        persistence::configure_component_recovery(
            context.state().view(), checkpoints, "durable-strategy", "day-one");
        CHECK_OUTPUT(eval_node_with_options<DurableComponent>(interval(0, 2), values<Int>(1, 2)), {1, 3});
        CHECK(checkpoints.contains("day-one"));
    }
    {
        GlobalContext context;
        const ComponentCheckpointStore reopened{config};
        persistence::configure_component_recovery(
            context.state().view(), reopened, "durable-strategy", "day-two", "day-one");
        CHECK_OUTPUT(eval_node_with_options<DurableComponent>(interval(2, 5), values<Int>(none, 3, 4)),
                     {none, 6, 10});
        CHECK(reopened.contains("day-two"));
    }
    {
        GlobalContext context;
        const ComponentCheckpointStore reopened{config};
        persistence::configure_component_recovery(
            context.state().view(), reopened, "durable-strategy", "day-three", "day-two");
        CHECK_THROWS_WITH(
            eval_node_with_options<DurableComponent>(interval(5, 7), values<Int>(5, -999)),
            Catch::Matchers::ContainsSubstring("failed durable day"));
        CHECK_FALSE(reopened.contains("day-three"));
        CHECK(reopened.contains("day-two"));
    }
}

TEST_CASE("component checkpoint store: one immutable image survives store reconstruction")
{
    TemporaryDirectory directory;
    store::FrameStoreConfig config;
    config.location = store::LocalLocation{directory.path.string()};
    const auto checkpoint = fixture();
    {
        const ComponentCheckpointStore store{config};
        store.write("strategy/day-one", checkpoint);
        CHECK(store.contains("strategy/day-one"));
        CHECK_THROWS(store.write("strategy/day-one", checkpoint));
    }
    const ComponentCheckpointStore reopened{config};
    const auto restored = reopened.read("strategy/day-one");
    CHECK(restored.version == 1);
    CHECK(restored.component_id == checkpoint.component_id);
    CHECK(restored.graph_signature == checkpoint.graph_signature);
    CHECK(restored.cut == checkpoint.cut);
    CHECK(restored.completed_until == checkpoint.completed_until);
    REQUIRE(restored.graph.nodes.size() == 1);
    const auto &node = restored.graph.nodes.front();
    CHECK(node.id == "total");
    CHECK(node.signature == "running-total-v1");
    REQUIRE(node.recordable_state);
    CHECK(node.recordable_state->version == 1);
    CHECK(node.recordable_state->payload == Value{Int{17}});
    CHECK(node.recordable_state->last_modified_time == checkpoint.cut);
    CHECK(node.recordable_state->schema == schema_descriptor<TS<Int>>::ts_meta());
    CHECK_FALSE(node.error);
}

TEST_CASE("component checkpoint store: frozen input subscriptions survive durable restart")
{
    using namespace hgraph::testing;
    stdlib::register_standard_operators();
    TemporaryDirectory directory;
    store::FrameStoreConfig config;
    config.location = store::LocalLocation{directory.path.string()};
    const bool freeze = GENERATE(false, true);
    {
        GlobalContext context;
        const ComponentCheckpointStore checkpoints{config};
        persistence::configure_component_recovery(
            context.state().view(), checkpoints, "activity-strategy", "one");
        if (freeze)
        {
            CHECK_OUTPUT(eval_node_with_options<FreezeComponent>(
                             interval(0, 2), values<Bool>(false, true), values<Int>(1, 2)),
                         {1, 2});
        }
        else
        {
            CHECK_OUTPUT(eval_node_with_options<UntilTrueComponent>(
                             interval(0, 2), values<Bool>(false, true)),
                         values<Bool>(false, true));
        }
    }
    {
        GlobalContext context;
        const ComponentCheckpointStore reopened{config};
        persistence::configure_component_recovery(
            context.state().view(), reopened, "activity-strategy", "two", "one");
        if (freeze)
        {
            CHECK_OUTPUT(eval_node_with_options<FreezeComponent>(
                             interval(2, 4), values<Bool>(false, false), values<Int>(3, 4)),
                         values<Int>(none, none));
            CHECK(reopened.read("two").graph.nodes.back().output->payload == Value{Int{2}});
        }
        else
        {
            CHECK_OUTPUT(eval_node_with_options<UntilTrueComponent>(
                             interval(2, 4), values<Bool>(false, false)),
                         values<Bool>(none, none));
            CHECK(reopened.read("two").graph.nodes.back().output->payload == Value{Bool{true}});
        }
    }
}

TEST_CASE("component checkpoint store: recursive topology and endpoint metadata round trip")
{
    auto checkpoint = fixture();
    auto &node = checkpoint.graph.nodes.front();
    TSCheckpointImage keyed;
    keyed.schema = schema_descriptor<TSD<Str, TS<Int>>>::ts_meta();
    keyed.last_modified_time = checkpoint.cut;
    keyed.key_set_last_modified_time = MIN_ST;
    keyed.slot_capacity = 3;
    keyed.keys.push_back(Value{Str{"alpha"}});
    keyed.slots = {1};
    keyed.free_slots = {2, 0};
    keyed.published = {true};
    keyed.children.push_back(*node.output);
    node.output = std::move(keyed);
    ChildGraphCheckpoint child;
    child.slot = 1;
    child.key = Value{Str{"alpha"}};
    child.key_last_modified_time = MIN_ST;
    child.graph = std::make_shared<GraphCheckpointImage>(fixture().graph);
    node.custom.children.push_back(std::move(child));
    node.custom.payload = Value{Int{4}};
    node.ingress = node.output;
    node.input_activity = {
        {.path = {0, 1}, .mode = TSInputActivityMode::Value},
        {.path = {1}, .mode = TSInputActivityMode::Structural},
    };

    const ComponentCheckpointStore store;
    store.write("one", checkpoint);
    const auto restored = store.read("one");
    const auto &actual = restored.graph.nodes.front();
    REQUIRE(actual.output);
    CHECK(actual.output->slot_capacity == 3);
    CHECK(actual.output->slots == std::vector<std::size_t>{1});
    CHECK(actual.output->free_slots == std::vector<std::size_t>{2, 0});
    CHECK(actual.output->published == std::vector<bool>{true});
    CHECK(actual.output->key_set_last_modified_time == MIN_ST);
    REQUIRE(actual.output->keys.size() == 1);
    CHECK(actual.output->keys.front() == Value{Str{"alpha"}});
    CHECK(actual.output->children.front().payload == Value{Int{17}});
    CHECK(actual.custom.payload == Value{Int{4}});
    REQUIRE(actual.custom.children.size() == 1);
    CHECK(actual.custom.children.front().slot == 1);
    CHECK(actual.custom.children.front().key == Value{Str{"alpha"}});
    CHECK(actual.custom.children.front().key_last_modified_time == MIN_ST);
    REQUIRE(actual.custom.children.front().graph);
    CHECK(actual.custom.children.front().graph->nodes.front().id == "total");
    REQUIRE(actual.ingress);
    CHECK(actual.ingress->slots == std::vector<std::size_t>{1});
    CHECK(actual.ingress->free_slots == std::vector<std::size_t>{2, 0});
    CHECK(actual.ingress->children.front().payload == Value{Int{17}});
    REQUIRE(actual.input_activity.size() == 2);
    CHECK(actual.input_activity[0].path == std::vector<std::size_t>{0, 1});
    CHECK(actual.input_activity[0].mode == TSInputActivityMode::Value);
    CHECK(actual.input_activity[1].path == std::vector<std::size_t>{1});
    CHECK(actual.input_activity[1].mode == TSInputActivityMode::Structural);
}

TEST_CASE("component checkpoint store: predecessor selection and immutable configuration are explicit")
{
    using Catch::Matchers::ContainsSubstring;
    store::FrameStoreConfig mutable_config;
    mutable_config.immutable = false;
    CHECK_THROWS_WITH(ComponentCheckpointStore{mutable_config}, ContainsSubstring("immutable"));
    const ComponentCheckpointStore store;
    const auto checkpoint = fixture();
    CHECK_THROWS_WITH(store.read("missing"), ContainsSubstring("not found"));
    CHECK_THROWS_WITH(store.write("two", checkpoint, "one"), ContainsSubstring("predecessor"));
    CHECK_FALSE(store.contains("two"));
    store.write("one", checkpoint);
    CHECK_THROWS(store.write("one", checkpoint, "one"));
    store.write("two", checkpoint, "one");
    CHECK(store.contains("one"));
    CHECK(store.contains("two"));
    GlobalState state;
    CHECK_THROWS(configure_component_recovery(state.view(), store, "strategy", "two", "one"));
    CHECK_THROWS(configure_component_recovery(state.view(), store, "strategy", "three", "three"));
}

TEST_CASE("component checkpoint store: failed encoding publishes no partial day")
{
    const ComponentCheckpointStore store;
    const auto first = fixture();
    store.write("one", first);
    auto failed = first;
    failed.graph.nodes.front().recordable_state->schema = nullptr;
    CHECK_THROWS(store.write("two", failed, "one"));
    CHECK_FALSE(store.contains("two"));
    CHECK(store.read("one").graph.nodes.front().recordable_state->payload == Value{Int{17}});
    failed = first;
    failed.graph.nodes.front().input_activity = {
        {.path = {0}, .mode = static_cast<TSInputActivityMode>(0)},
    };
    CHECK_THROWS_WITH(store.write("two", failed, "one"),
                      Catch::Matchers::ContainsSubstring("unsupported input activity mode"));
    CHECK_FALSE(store.contains("two"));
}

TEST_CASE("component checkpoint store: malformed envelope never yields a checkpoint")
{
    using Catch::Matchers::ContainsSubstring;
    TemporaryDirectory directory;
    store::FrameStoreConfig config;
    config.location = store::LocalLocation{directory.path.string()};
    const ComponentCheckpointStore checkpoints{config};
    const auto frames = store::make_frame_store(config);
    checkpoints.write("valid", fixture());
    const auto frame = frames.read("valid");
    const auto bytes = image_bytes(frame);

    // The envelope names the format; an unknown one is refused, and so is an
    // image that does not belong to the format its envelope claims.
    for (const std::string_view format : {"hgraph.component-checkpoint.v0", "hgraph.component-checkpoint.v3"})
    {
        const auto key = "unsupported-" + std::string{format.substr(format.size() - 2)};
        frames.write(key, envelope(format, "", bytes));
        CHECK_THROWS_WITH(checkpoints.read(key), ContainsSubstring("unsupported format"));
    }
    frames.write("relabelled", envelope(format_v1, "", bytes));
    CHECK_THROWS_WITH(checkpoints.read("relabelled"), ContainsSubstring("invalid component checkpoint"));
    frames.write("substituted", envelope(format_v2, "", test::checkpoint_v1_fixture()));
    CHECK_THROWS_WITH(checkpoints.read("substituted"), ContainsSubstring("unsupported format"));

    // The image states its own version after the marker: 2 and 3 are read,
    // and the envelope's format name is about the envelope, not the image.
    const auto marker = bytes.find("hgraph.checkpoint-image");
    REQUIRE(marker != std::string::npos);
    for (const char version : {char{0}, char{1}, char{4}})
    {
        auto unsupported = bytes;
        unsupported[marker + std::string_view{"hgraph.checkpoint-image"}.size()] = version;
        const auto key = "unsupported-version-" + std::to_string(static_cast<int>(version));
        frames.write(key, with_image_bytes(frame, unsupported));
        CHECK_THROWS_WITH(checkpoints.read(key), ContainsSubstring("unsupported image version"));
    }

    for (const auto length : {std::size_t{0}, std::size_t{7}, bytes.size() - 1})
    {
        const auto key = "truncated-" + std::to_string(length);
        frames.write(key, with_image_bytes(frame, std::string_view{bytes}.substr(0, length)));
        CHECK_THROWS_WITH(checkpoints.read(key), ContainsSubstring("invalid component checkpoint"));
    }

    // Every byte is covered by the checksum, so appended, flipped and
    // rewritten content are all detected before anything is interpreted.
    frames.write("trailing", with_image_bytes(frame, bytes + "extra"));
    CHECK_THROWS_WITH(checkpoints.read("trailing"), ContainsSubstring("checksum mismatch"));
    for (const auto position : {bytes.size() / 2, bytes.size() - 9, bytes.size() - 1})
    {
        auto flipped = bytes;
        flipped[position] = static_cast<char>(flipped[position] ^ 0x01);
        const auto key = "flipped-" + std::to_string(position);
        frames.write(key, with_image_bytes(frame, flipped));
        CHECK_THROWS_WITH(checkpoints.read(key), ContainsSubstring("checksum mismatch"));
    }
    CHECK(checkpoints.read("valid").graph.nodes.front().recordable_state->payload == Value{Int{17}});
}

TEST_CASE("component checkpoint store: images published by hgraph 0.8.25-0.8.27 still load")
{
    using Catch::Matchers::ContainsSubstring;
    TemporaryDirectory directory;
    store::FrameStoreConfig config;
    config.location = store::LocalLocation{directory.path.string()};
    const ComponentCheckpointStore checkpoints{config};
    const auto frames = store::make_frame_store(config);
    const auto released = std::string{test::checkpoint_v1_fixture()};
    frames.write("released", envelope(format_v1, "", released));

    const auto loaded = checkpoints.read("released");
    CHECK(loaded.component_id == "strategy");
    CHECK(loaded.graph_signature == "v1-fixture-signature");
    CHECK(loaded.cut == MIN_ST + MIN_TD);
    CHECK(loaded.completed_until == MIN_ST + MIN_TD * 2);
    REQUIRE(loaded.graph.nodes.size() == 2);
    const auto &total = loaded.graph.nodes[0];
    CHECK(total.id == "strategy:total");
    REQUIRE(total.recordable_state);
    CHECK(total.recordable_state->payload == Value{Int{17}});
    REQUIRE(total.input_activity.size() == 1);
    CHECK(total.input_activity.front().path == std::vector<std::size_t>{0});

    const auto &keyed = loaded.graph.nodes[1];
    REQUIRE(keyed.output);
    // Sparse live slots and a free stack whose order is semantic.
    CHECK(keyed.output->slot_capacity == 8);
    CHECK(keyed.output->slots == std::vector<std::size_t>{0, 2});
    CHECK(keyed.output->free_slots == std::vector<std::size_t>{7, 6, 5, 4, 3, 1});
    REQUIRE(keyed.output->keys.size() == 2);
    CHECK(keyed.output->keys[1] == Value{Int{30}});
    CHECK(keyed.output->children[1].payload == Value{Float{45.0}});
    REQUIRE(keyed.custom.endpoints.size() == 1);
    CHECK(keyed.custom.endpoints.front().window_times == std::vector<DateTime>{MIN_ST, MIN_ST + MIN_TD});
    REQUIRE(keyed.custom.children.size() == 1);
    CHECK(keyed.custom.children.front().slot == 2);
    CHECK(keyed.custom.children.front().graph->nodes.front().id == "strategy:inner");

    // A released image restores into live endpoints exactly as a current one.
    TSOutput target{keyed.output->schema};
    auto target_view = target.data_view();
    restore_ts_checkpoint(target_view, *keyed.output);
    CHECK(target_view.as_dict().size() == 2);

    // Its successor is published in the current format and agrees with it.
    checkpoints.write("successor", loaded, "released");
    CHECK(frames.read("successor").table->schema()->metadata()->Get("hgraph.checkpoint.format").ValueOrDie() ==
          format_v2);
    const auto successor = checkpoints.read("successor");
    CHECK(successor.graph.nodes[1].output->free_slots == keyed.output->free_slots);
    CHECK(successor.graph.nodes[1].output->children[1].payload == Value{Float{45.0}});

    // The retained reader keeps its own refusals.
    frames.write("released-parent", envelope(format_v1, "someone-else", released));
    CHECK_THROWS_WITH(checkpoints.read("released-parent"), ContainsSubstring("inconsistent format metadata"));
    frames.write("released-truncated", envelope(format_v1, "", std::string_view{released}.substr(0, released.size() - 1)));
    CHECK_THROWS_WITH(checkpoints.read("released-truncated"), ContainsSubstring("invalid component checkpoint"));
    frames.write("released-trailing", envelope(format_v1, "", released + "extra"));
    CHECK_THROWS_WITH(checkpoints.read("released-trailing"), ContainsSubstring("trailing data"));
}

TEST_CASE("component checkpoint store: nonfinite and signed-zero values recover bit for bit")
{
    const ComponentCheckpointStore checkpoints;
    auto checkpoint = fixture();
    const auto quiet = std::numeric_limits<Float>::quiet_NaN();
    checkpoint.graph.nodes.front().custom.payload = stdlib::make_list<Float>(
        {Float{1.2345678901234567}, Float{-0.0}, std::numeric_limits<Float>::infinity(),
         -std::numeric_limits<Float>::infinity(), quiet});
    checkpoint.graph.nodes.front().output->schema = schema_descriptor<TS<Float>>::ts_meta();
    checkpoint.graph.nodes.front().output->payload = Value{quiet};
    const bool verify = GENERATE(false, true);
    const auto key = verify ? "verified" : "plain";
    checkpoints.write(key, checkpoint, std::nullopt, verify);

    const auto restored = checkpoints.read(key);
    const auto bits = [](Float value) { return std::bit_cast<std::uint64_t>(value); };
    const auto payload = restored.graph.nodes.front().custom.payload.view().as_list();
    const auto source = checkpoint.graph.nodes.front().custom.payload.view().as_list();
    REQUIRE(payload.size() == 5);
    for (std::size_t index = 0; index < payload.size(); ++index)
        CHECK(bits(payload.at(index).checked_as<Float>()) == bits(source.at(index).checked_as<Float>()));
    CHECK(bits(restored.graph.nodes.front().output->payload.view().checked_as<Float>()) == bits(quiet));
}

TEST_CASE("component checkpoint store: reference locators and adapter clocks survive durable roundtrip")
{
    TemporaryDirectory directory;
    store::FrameStoreConfig config;
    config.location = store::LocalLocation{directory.path.string()};
    auto checkpoint = fixture();
    const auto *scalar = schema_descriptor<TS<Int>>::ts_meta();
    const auto *target = TypeRegistry::instance().tsl(scalar, 2);
    TSCheckpointLocator locator{
        .graph_path = {2, 7, 3, 11},
        .node = 5,
        .endpoint = 4,
        .custom_endpoint = 2,
        .endpoint_path = {4, 1},
        .bindings = {{TypeRegistry::instance().ref(target), {}}, {target, {1}}},
    };
    TSReferenceCheckpointImage peered{
        .kind = TSReferenceCheckpointKind::Peered,
        .target_schema = scalar,
        .target = locator,
    };
    auto &node = checkpoint.graph.nodes.front();
    node.output = TSCheckpointImage{
        .schema = TypeRegistry::instance().ref(target),
        .last_modified_time = checkpoint.cut,
        .reference = TSReferenceCheckpointImage{
            .kind = TSReferenceCheckpointKind::NonPeered,
            .target_schema = target,
            .items = {peered, TSReferenceCheckpointImage{.target_schema = scalar}},
        },
    };
    node.alternatives.push_back(EndpointBindingCheckpoint{
        .binding = locator,
        .clocks = TSCheckpointImage{.schema = scalar, .last_modified_time = checkpoint.cut - MIN_TD},
    });
    {
        const ComponentCheckpointStore store{config};
        store.write("references", checkpoint);
    }
    const ComponentCheckpointStore reopened{config};
    const auto loaded = reopened.read("references");
    const auto &restored = loaded.graph.nodes.front();
    REQUIRE(restored.output);
    CHECK(restored.output->reference == node.output->reference);
    CHECK_FALSE(restored.output->payload.has_value());
    REQUIRE(restored.alternatives.size() == 1);
    CHECK(restored.alternatives.front().binding == locator);
    CHECK(restored.alternatives.front().clocks.last_modified_time == checkpoint.cut - MIN_TD);
}

TEST_CASE("component checkpoint store: malformed reference images are never published")
{
    TemporaryDirectory directory;
    store::FrameStoreConfig config;
    config.location = store::LocalLocation{directory.path.string()};
    const ComponentCheckpointStore store{config};
    for (int malformed = 0; malformed < 6; ++malformed)
    {
        auto checkpoint = fixture();
        const auto *scalar = schema_descriptor<TS<Int>>::ts_meta();
        auto &image = *checkpoint.graph.nodes.front().output;
        image.schema = TypeRegistry::instance().ref(scalar);
        image.payload = {};
        image.reference = TSReferenceCheckpointImage{
            .kind = TSReferenceCheckpointKind::Peered,
            .target_schema = scalar,
            .target = TSCheckpointLocator{},
        };
        switch (malformed)
        {
            case 0: image.reference->target.reset(); break;
            case 1: image.reference->target->graph_path = {2}; break;
            case 2: image.reference->target->endpoint = 5; break;
            case 3: image.reference->kind = TSReferenceCheckpointKind::Empty; break;
            case 4: image.reference->target->bindings.emplace_back(); break;
            case 5: image.payload = Value{Int{3}}; break;
        }
        const auto key = "bad-reference-" + std::to_string(malformed);
        CHECK_THROWS(store.write(key, checkpoint));
        CHECK_FALSE(store.contains(key));
    }
}
