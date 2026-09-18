#include <hgraph/runtime/checkpoint_codec.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/series.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <arrow/api.h>

#include <cstring>
#include <string>

// RFC 0039: the canonical byte form of checkpoint images. These cases fix the
// properties the format promises independently of any store: what an image
// costs, that every part of one survives, and that a damaged one is refused
// without being interpreted.
namespace
{
    using namespace hgraph;
    using Catch::Matchers::ContainsSubstring;

    using FrameRow = Bundle<"tests.checkpoint_codec::Row", Field<"a", Int>>;
    using FrameMeta = Bundle<"tests.checkpoint_codec::Meta", Field<"revision", Int>>;

    Frame one_column_frame(std::vector<std::int64_t> values)
    {
        arrow::Int64Builder builder;
        REQUIRE(builder.AppendValues(values).ok());
        std::shared_ptr<arrow::Array> array;
        REQUIRE(builder.Finish(&array).ok());
        return Frame{arrow::Table::Make(arrow::schema({arrow::field("a", arrow::int64())}), {std::move(array)})};
    }

    TSCheckpointImage scalar_image(Int value, DateTime time)
    {
        TSCheckpointImage image;
        image.schema = schema_descriptor<TS<Int>>::ts_meta();
        image.last_modified_time = time;
        image.payload = Value{value};
        return image;
    }

    ComponentCheckpoint component(NodeCheckpointImage node)
    {
        ComponentCheckpoint checkpoint;
        checkpoint.component_id = "codec";
        checkpoint.graph_signature = "codec-signature";
        checkpoint.cut = MIN_ST + MIN_TD * 5;
        checkpoint.completed_until = MIN_ST + MIN_TD * 6;
        checkpoint.graph.nodes.push_back(std::move(node));
        return checkpoint;
    }

    std::string encoded(const ComponentCheckpoint &checkpoint)
    {
        std::string bytes;
        encode_component_checkpoint(checkpoint, bytes);
        return bytes;
    }

    // Re-seal an image whose content a test has deliberately damaged, so the
    // structural refusals behind the checksum can be reached.
    std::string resealed(std::string bytes)
    {
        const std::string_view content{bytes.data(), bytes.size() - 8};
        std::uint64_t hash{0xcbf29ce484222325ull};
        std::size_t at{0};
        for (; at + 8 <= content.size(); at += 8)
        {
            std::uint64_t word{};
            std::memcpy(&word, content.data() + at, 8);
            hash = (hash ^ word) * 0x100000001b3ull;
        }
        for (; at < content.size(); ++at) { hash = (hash ^ static_cast<unsigned char>(content[at])) * 0x100000001b3ull; }
        std::memcpy(bytes.data() + bytes.size() - 8, &hash, 8);
        return bytes;
    }
}

TEST_CASE("checkpoint codec: a keyed endpoint costs its keys, values and little else", "[checkpoint][codec]")
{
    auto &registry = TypeRegistry::instance();
    const auto *schema = registry.tsd(scalar_descriptor<Int>::value_meta(), schema_descriptor<TS<Float>>::ts_meta());
    constexpr std::size_t count = 1000;
    TSOutput source{schema};
    auto view = source.data_view();
    {
        auto mutation = view.as_dict().begin_mutation(MIN_ST);
        for (std::size_t index = 0; index < count; ++index)
        {
            const Value key{static_cast<Int>(index)};
            const Value value{static_cast<Float>(index)};
            (void)mutation.at(key.view()).begin_mutation(MIN_ST).copy_value_from(value.view());
        }
    }
    NodeCheckpointImage node;
    node.id = "codec:dict";
    node.signature = "dict";
    const auto captured = capture_ts_checkpoint(view);
    node.output = captured;
    const auto bytes = encoded(component(std::move(node)));
    // Eight bytes of key, eight of value, a flag byte and a time byte. A
    // schema, slot or published entry per key would break this bound.
    CHECK(bytes.size() < count * 19 + 512);

    const auto restored = decode_component_checkpoint(bytes);
    const auto &image = *restored.graph.nodes.front().output;
    REQUIRE(image.keys.size() == count);
    CHECK(image.slot_capacity == captured.slot_capacity);
    CHECK(image.slots == captured.slots);
    CHECK(image.free_slots == captured.free_slots);
    CHECK(image.published == captured.published);
    CHECK(image.children[count - 1].payload == Value{static_cast<Float>(count - 1)});
    CHECK(image.children[count - 1].schema == schema_descriptor<TS<Float>>::ts_meta());
    TSOutput target{schema};
    auto target_view = target.data_view();
    restore_ts_checkpoint(target_view, image);
    CHECK(target_view.as_dict().size() == count);
}

TEST_CASE("checkpoint codec: every optional part of an image survives", "[checkpoint][codec]")
{
    auto &registry = TypeRegistry::instance();
    const auto cut = MIN_ST + MIN_TD * 5;
    NodeCheckpointImage node;
    node.id = "codec:node";
    node.signature = std::string{"sig\0with\0nulls", 14};

    TSCheckpointImage keyed;
    keyed.schema = schema_descriptor<TSD<Str, TS<Int>>>::ts_meta();
    keyed.last_modified_time = cut;
    keyed.key_set_last_modified_time = MIN_ST;
    keyed.slot_capacity = 6;
    keyed.keys = {Value{Str{"alpha"}}, Value{Str{"beta"}}, Value{Str{"gamma"}}};
    keyed.slots = {4, 1, 5};                 // not ascending
    keyed.free_slots = {3, 0, 2};            // order is semantic
    keyed.published = {true, false, true};   // not every entry published
    keyed.children = {scalar_image(1, cut), TSCheckpointImage{.schema = schema_descriptor<TS<Int>>::ts_meta()},
                      scalar_image(3, MIN_ST + MIN_TD)};
    node.output = keyed;

    // A payload of a schema the endpoint does not imply, and a child whose
    // schema its parent does not imply, both take the explicit form.
    TSCheckpointImage odd = scalar_image(7, cut);
    odd.payload = Value{Str{"not the declared type"}};
    node.error = odd;
    TSCheckpointImage bundle;
    bundle.schema = registry.un_named_tsb({{"x", schema_descriptor<TS<Int>>::ts_meta()}});
    bundle.last_modified_time = cut;
    bundle.children.push_back(TSCheckpointImage{
        .schema = schema_descriptor<TS<Float>>::ts_meta(), .last_modified_time = cut, .payload = Value{Float{2.5}}});
    node.recordable_state = bundle;

    TSCheckpointImage window;
    window.schema = registry.tsw(scalar_descriptor<Int>::value_meta(), 4, 1);
    window.last_modified_time = cut;
    ListBuilder samples{registry.scalar_type<Int>()};
    samples.push_back(Int{1});
    samples.push_back(Int{2});
    window.payload = samples.build();
    window.window_times = {MIN_ST, cut};
    node.custom.endpoints.push_back(window);

    ChildGraphCheckpoint first{.slot = 9, .key = Value{Str{"alpha"}}, .key_last_modified_time = MIN_ST,
                               .graph = std::make_shared<GraphCheckpointImage>()};
    ChildGraphCheckpoint second{.slot = 2, .key = Value{Str{"beta"}}, .key_last_modified_time = MIN_DT,
                                .graph = std::make_shared<GraphCheckpointImage>()};
    NodeCheckpointImage inner;
    inner.id = "codec:inner";
    inner.signature = node.signature;
    inner.output = scalar_image(11, cut);
    first.graph->nodes.push_back(inner);
    second.graph->nodes.push_back(inner);
    node.custom.children = {first, second};
    node.custom.payload = Value{Int{4}};
    node.input_activity = {{.path = {0, 1}, .mode = TSInputActivityMode::Value},
                           {.path = {}, .mode = TSInputActivityMode::Structural}};

    const auto restored = decode_component_checkpoint(encoded(component(node)));
    CHECK(restored.component_id == "codec");
    CHECK(restored.graph_signature == "codec-signature");
    CHECK(restored.cut == cut);
    CHECK(restored.completed_until == cut + MIN_TD);
    const auto &actual = restored.graph.nodes.front();
    CHECK(actual.signature == node.signature);
    CHECK(actual.output->slots == keyed.slots);
    CHECK(actual.output->free_slots == keyed.free_slots);
    CHECK(actual.output->published == keyed.published);
    CHECK(actual.output->slot_capacity == 6);
    CHECK(actual.output->key_set_last_modified_time == MIN_ST);
    CHECK(actual.output->keys[2] == Value{Str{"gamma"}});
    CHECK(actual.output->children[1].last_modified_time == MIN_DT);
    CHECK_FALSE(actual.output->children[1].payload.has_value());
    CHECK(actual.output->children[2].last_modified_time == MIN_ST + MIN_TD);
    CHECK(actual.error->payload == Value{Str{"not the declared type"}});
    CHECK(actual.recordable_state->children.front().schema == schema_descriptor<TS<Float>>::ts_meta());
    CHECK(actual.recordable_state->children.front().payload == Value{Float{2.5}});
    CHECK(actual.custom.endpoints.front().window_times == window.window_times);
    CHECK(actual.custom.endpoints.front().payload.view().as_indexed_view().size() == 2);
    CHECK(actual.custom.payload == Value{Int{4}});
    CHECK(actual.input_activity == node.input_activity);
    REQUIRE(actual.custom.children.size() == 2);
    CHECK(actual.custom.children[0].slot == 9);
    CHECK(actual.custom.children[1].slot == 2);
    CHECK(actual.custom.children[1].key == Value{Str{"beta"}});
    CHECK(actual.custom.children[0].key_last_modified_time == MIN_ST);
    CHECK(actual.custom.children[1].key_last_modified_time == MIN_DT);
    CHECK(actual.custom.children[1].graph->nodes.front().output->payload == Value{Int{11}});
}

TEST_CASE("checkpoint codec: frames and series are values like any other", "[checkpoint][codec]")
{
    auto &registry = TypeRegistry::instance();
    const auto cut = MIN_ST + MIN_TD * 5;
    const auto table = one_column_frame({1, 2, 3});
    const auto *row = scalar_descriptor<FrameRow>::value_meta();
    const auto *metadata = scalar_descriptor<FrameMeta>::value_meta();

    NodeCheckpointImage node;
    node.id = "codec:frames";
    node.signature = "frames";
    const auto frame_image = [&](const ValueTypeMetaData *schema) {
        TSCheckpointImage image;
        image.schema = registry.ts(schema);
        image.last_modified_time = cut;
        image.payload = Value{ValuePlanFactory::instance().type_for(schema), &table};
        return image;
    };
    node.output = frame_image(scalar_descriptor<Frame>::value_meta());
    node.recordable_state = frame_image(registry.frame(row));            // Frame[Row]
    node.ingress = frame_image(registry.frame(row, metadata));           // Frame[Row, Meta]

    const auto values = one_column_frame({7, 8}).table->column(0)->chunk(0);
    const Series series{values};
    TSCheckpointImage series_image;
    series_image.schema = registry.ts(registry.series(scalar_descriptor<Int>::value_meta()));   // Series[int]
    series_image.last_modified_time = cut;
    series_image.payload = Value{ValuePlanFactory::instance().type_for(series_image.schema->value_type), &series};
    node.custom.endpoints.push_back(series_image);

    const auto restored = decode_component_checkpoint(encoded(component(node)));
    const auto &actual = restored.graph.nodes.front();
    for (const auto *image : {&*actual.output, &*actual.recordable_state, &*actual.ingress})
        CHECK(image->payload.view().checked_as<Frame>().table->Equals(*table.table, true));
    CHECK(actual.output->schema == node.output->schema);
    CHECK(actual.recordable_state->schema == node.recordable_state->schema);
    CHECK(actual.ingress->schema == node.ingress->schema);
    CHECK(actual.custom.endpoints.front().schema == series_image.schema);
    CHECK(actual.custom.endpoints.front().payload.view().checked_as<Series>().array->Equals(values));
}

TEST_CASE("checkpoint codec: an Any payload names its schema in the image's own table", "[checkpoint][codec]")
{
    auto &registry = TypeRegistry::instance();
    const auto cut = MIN_ST + MIN_TD * 5;
    const auto *any = registry.any();

    const auto boxed = [&](Value content) {
        Value box{ValuePlanFactory::instance().type_for(any)};
        box.as_any().begin_mutation().set(std::move(content));
        return box;
    };
    const auto any_image = [&](Value payload) {
        TSCheckpointImage image;
        image.schema = registry.ts(any);
        image.last_modified_time = cut;
        image.payload = std::move(payload);
        return image;
    };

    NodeCheckpointImage node;
    node.id = "codec:any";
    node.signature = "any";
    node.output = any_image(boxed(Value{Int{42}}));
    node.recordable_state = any_image(boxed(Value{Str{"boxed"}}));
    node.ingress = any_image(Value{ValuePlanFactory::instance().type_for(any)});   // an empty box

    const auto restored = decode_component_checkpoint(encoded(component(node)));
    const auto &actual = restored.graph.nodes.front();
    CHECK(actual.output->payload.view() == node.output->payload.view());
    CHECK(actual.recordable_state->payload.view() == node.recordable_state->payload.view());
    CHECK(actual.ingress->payload.view() == node.ingress->payload.view());
    CHECK(actual.output->payload.as_any().get().checked_as<Int>() == 42);
    CHECK_FALSE(actual.ingress->payload.as_any().has_value());
}

TEST_CASE("checkpoint codec: a graph image travels without a component", "[checkpoint][codec]")
{
    GraphCheckpointImage graph;
    NodeCheckpointImage node;
    node.id = "worker:0";
    node.signature = "worker";
    node.output = scalar_image(5, MIN_ST + MIN_TD * 3);
    graph.nodes.push_back(node);
    for (const auto base : {MIN_DT, MIN_ST + MIN_TD * 3, MAX_DT})
    {
        std::string bytes;
        encode_graph_checkpoint(graph, bytes, base);
        CHECK(is_checkpoint_image(bytes));
        const auto restored = decode_graph_checkpoint(bytes);
        REQUIRE(restored.nodes.size() == 1);
        CHECK(restored.nodes.front().output->last_modified_time == MIN_ST + MIN_TD * 3);
        CHECK(restored.nodes.front().output->payload == Value{Int{5}});
        CHECK_THROWS_WITH(decode_component_checkpoint(bytes), ContainsSubstring("unexpected image kind"));
    }
    CHECK_FALSE(is_checkpoint_image("hgraph.component-checkpoint.v1"));
    CHECK_FALSE(is_checkpoint_image(""));
}

TEST_CASE("checkpoint codec: an image that cannot be represented is refused before any byte is produced",
          "[checkpoint][codec]")
{
    NodeCheckpointImage node;
    node.id = "codec:bad";
    node.signature = "bad";
    node.output = scalar_image(1, MIN_ST);
    auto checkpoint = component(node);
    std::string bytes{"prefix"};

    auto unsupported = checkpoint;
    unsupported.graph.nodes.front().output->version = 2;
    CHECK_THROWS_WITH(encode_component_checkpoint(unsupported, bytes), ContainsSubstring("endpoint image version"));
    auto unschooled = checkpoint;
    unschooled.graph.nodes.front().output->schema = nullptr;
    CHECK_THROWS_WITH(encode_component_checkpoint(unschooled, bytes), ContainsSubstring("missing endpoint schema"));
    auto childless = checkpoint;
    childless.graph.nodes.front().custom.children.push_back({.slot = 0, .key = Value{Int{1}}});
    CHECK_THROWS_WITH(encode_component_checkpoint(childless, bytes), ContainsSubstring("missing child graph image"));
    auto unbounded = checkpoint;
    unbounded.completed_until = unbounded.cut;
    CHECK_THROWS_WITH(encode_component_checkpoint(unbounded, bytes), ContainsSubstring("completed component boundary"));
    CHECK(bytes == "prefix");
}

TEST_CASE("checkpoint codec: a damaged image is refused without being interpreted", "[checkpoint][codec]")
{
    NodeCheckpointImage node;
    node.id = "codec:node";
    node.signature = "node";
    node.output = scalar_image(1, MIN_ST);
    const auto bytes = encoded(component(node));

    for (std::size_t length = 0; length < bytes.size(); ++length)
        CHECK_THROWS(decode_component_checkpoint(std::string_view{bytes}.substr(0, length)));
    for (std::size_t position = 0; position < bytes.size(); ++position)
    {
        auto flipped = bytes;
        flipped[position] = static_cast<char>(flipped[position] ^ 0x40);
        CHECK_THROWS(decode_component_checkpoint(flipped));
    }
    CHECK_THROWS_WITH(decode_component_checkpoint(bytes + "x"), ContainsSubstring("checksum mismatch"));

    // Behind a valid checksum, a count that exceeds the remaining input is
    // refused before it sizes an allocation. The node count is the first byte
    // of the body, which the body-length prefix locates exactly.
    auto oversized = bytes;
    const auto body_start = oversized.size() - 8 - [&] {
        // marker(1+23) version(1) kind(1) base(8) id(1+5) signature(1+15) completed(1) then the body length.
        return static_cast<std::size_t>(static_cast<unsigned char>(bytes[24 + 1 + 1 + 8 + 6 + 16 + 1]));
    }();
    REQUIRE(oversized[body_start] == 1);
    oversized[body_start] = 0x7f;
    CHECK_THROWS_WITH(decode_component_checkpoint(resealed(oversized)), ContainsSubstring("invalid sequence size"));

    // A table index that names nothing.
    auto dangling = bytes;
    REQUIRE(dangling[body_start + 1] == 0);   // the node id's string-table index
    dangling[body_start + 1] = 0x55;
    CHECK_THROWS_WITH(decode_component_checkpoint(resealed(dangling)), ContainsSubstring("unknown string index"));
}
