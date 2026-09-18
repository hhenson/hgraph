#include <hgraph/persistence/component_checkpoint_store.h>
#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/static_schema.h>

#include <catch2/catch_test_macros.hpp>

#include <arrow/api.h>
#include <arrow/util/key_value_metadata.h>

#include <chrono>
#include <filesystem>
#include <numeric>
#include <string>
#include <type_traits>
#include <vector>

// RFC 0039: a Frame is ordinary component state. It is held here in every
// place an image can carry a value -- a node output, recordable state, and the
// ingress baseline of a component input -- both untyped and as Frame[Row].
namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace hgraph::persistence;

    using Row = Bundle<"tests.checkpoint_frame::Row", Field<"a", Int>, Field<"b", Int>>;

    void require_arrow(const arrow::Status &status)
    {
        if (!status.ok()) { throw std::runtime_error(status.ToString()); }
    }

    Frame frame(std::vector<std::int64_t> a, std::vector<std::int64_t> b)
    {
        arrow::Int64Builder a_builder, b_builder;
        require_arrow(a_builder.AppendValues(a));
        require_arrow(b_builder.AppendValues(b));
        std::shared_ptr<arrow::Array> a_array, b_array;
        require_arrow(a_builder.Finish(&a_array));
        require_arrow(b_builder.Finish(&b_array));
        // Arrow schema metadata is part of the value and must survive.
        const auto metadata = arrow::key_value_metadata({"origin"}, {"checkpoint-frame-test"});
        return Frame{arrow::Table::Make(
            arrow::schema({arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())}, metadata),
            {std::move(a_array), std::move(b_array)})};
    }

    Frame concatenate(const Frame &head, const Frame &tail)
    {
        auto joined = arrow::ConcatenateTables({head.table, tail.table});
        require_arrow(joined.status());
        auto combined = (*joined)->CombineChunks();
        require_arrow(combined.status());
        return Frame{*combined};
    }

    Int column_sum(const Frame &value, int column)
    {
        Int total{0};
        for (const auto &chunk : value.table->column(column)->chunks())
        {
            const auto &numbers = static_cast<const arrow::Int64Array &>(*chunk);
            for (std::int64_t index = 0; index < numbers.length(); ++index) { total += numbers.Value(index); }
        }
        return total;
    }

    // A named bundle is one schema per name, so each frame flavour has its own.
    using UntypedBook = TSB<"checkpoint_frame_book", Field<"book", TS<Frame>>, Field<"batches", TS<Int>>>;
    using TypedBook = TSB<"checkpoint_typed_frame_book", Field<"book", TS<FrameOf<Row>>>, Field<"batches", TS<Int>>>;
    template <typename FrameTS>
    using BookState = std::conditional_t<std::is_same_v<FrameTS, TS<Frame>>, UntypedBook, TypedBook>;

    // Appends each batch to a frame kept in recordable state and republishes it.
    template <typename FrameTS>
    struct AccumulateFrames
    {
        static constexpr auto name = "checkpoint_accumulate_frames";
        static void eval(In<"batch", TS<Frame>> batch, RecordableState<BookState<FrameTS>> state, Out<FrameTS> out)
        {
            auto book = state.template field<"book">();
            auto batches = state.template field<"batches">();
            const Frame &incoming = batch.value();
            Frame next = book.valid() ? concatenate(book.value().template checked_as<Frame>(), incoming) : incoming;
            batches.set((batches.valid() ? batches.value().template checked_as<Int>() : Int{0}) + 1);
            book.set(next);
            out.set(next);
        }
    };

    // Reads the published frame only when the trigger ticks, so after a restart
    // it can only be answering from the restored output.
    template <typename FrameTS>
    struct WeighBook
    {
        static constexpr auto name = "checkpoint_weigh_book";
        static void eval(In<"trigger", TS<Int>> trigger, In<"book", FrameTS, InputActivity::Passive> book,
                         Out<TS<Int>> out)
        {
            const Frame &value = book.value();
            out.set(column_sum(value, 0) * trigger.value() + column_sum(value, 1));
        }
    };

    template <typename FrameTS>
    struct FrameStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"batch", TS<Frame>> batch,
                                     NamedPort<"trigger", TS<Int>> trigger)
        {
            auto book = wire<AccumulateFrames<FrameTS>>(w, batch);
            return wire<WeighBook<FrameTS>>(w, trigger, book);
        }
    };

    template <typename FrameTS>
    struct FrameComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Frame>> batch, Port<TS<Int>> trigger)
        {
            return stdlib::component<FrameStrategy<FrameTS>>(w, "frame-strategy", batch, trigger);
        }
    };

    EvalNodeRunOptions interval(Int begin, Int end)
    {
        return {.start_time = MIN_ST + MIN_TD * begin, .end_time = MIN_ST + MIN_TD * end};
    }

    struct TemporaryDirectory
    {
        std::filesystem::path path = std::filesystem::temp_directory_path() /
            ("hgraph-checkpoint-frame-" + std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()));
        ~TemporaryDirectory()
        {
            std::error_code ignored;
            std::filesystem::remove_all(path, ignored);
        }
    };

    void collect_frames(const TSCheckpointImage &image, std::vector<Frame> &found)
    {
        if (image.payload.has_value())
            if (const auto *value = image.payload.view().try_as<Frame>()) { found.push_back(*value); }
        for (const auto &child : image.children) { collect_frames(child, found); }
    }

    template <typename FrameTS>
    void frames_restart()
    {
        const auto first = frame({1, 2}, {10, 20});
        const auto second = frame({3}, {30});
        const auto third = frame({4}, {40});

        // One uninterrupted run is the oracle for the restarted pair.
        std::vector<std::optional<Int>> uninterrupted;
        {
            GlobalContext context;
            uninterrupted = eval_node_with_options<FrameComponent<FrameTS>>(
                interval(0, 4), values<Frame>(first, second, none, third), values<Int>(none, 1, 2, 3));
        }
        CHECK_OUTPUT(uninterrupted, values<Int>(none, 66, 72, 130));

        TemporaryDirectory directory;
        store::FrameStoreConfig config;
        config.location = store::LocalLocation{directory.path.string()};
        {
            GlobalContext context;
            const ComponentCheckpointStore checkpoints{config};
            persistence::configure_component_recovery(context.state().view(), checkpoints, "frame-strategy", "one");
            CHECK_OUTPUT(eval_node_with_options<FrameComponent<FrameTS>>(
                             interval(0, 2), values<Frame>(first, second), values<Int>(none, 1)),
                         values<Int>(none, 66));
        }
        {
            // The published image holds the frame itself: contents, Arrow
            // schema and schema metadata, in the output, the recordable state
            // and the input baseline.
            const ComponentCheckpointStore reopened{config};
            const auto saved = reopened.read("one");
            const auto expected = concatenate(first, second);
            std::size_t books{0}, baselines{0};
            for (const auto &node : saved.graph.nodes)
            {
                std::vector<Frame> owned, ingress;
                if (node.output) { collect_frames(*node.output, owned); }
                if (node.recordable_state) { collect_frames(*node.recordable_state, owned); }
                if (node.ingress) { collect_frames(*node.ingress, ingress); }
                for (const auto &found : owned)
                {
                    CHECK(found.table->Equals(*expected.table, /*check_metadata=*/true));
                    ++books;
                }
                for (const auto &found : ingress)
                {
                    CHECK(found.table->Equals(*second.table, /*check_metadata=*/true));
                    ++baselines;
                }
            }
            CHECK(books == 2);
            CHECK(baselines == 1);
        }
        {
            GlobalContext context;
            const ComponentCheckpointStore reopened{config};
            persistence::configure_component_recovery(
                context.state().view(), reopened, "frame-strategy", "two", "one");
            // The first resumed tick has no batch: the answer can only come
            // from the restored output. The second extends the restored state.
            CHECK_OUTPUT(eval_node_with_options<FrameComponent<FrameTS>>(
                             interval(2, 4), values<Frame>(none, third), values<Int>(2, 3)),
                         values<Int>(72, 130));
            CHECK(reopened.contains("two"));
        }
    }
}

TEST_CASE("component checkpoint store: a Frame in output, state and input baseline survives a durable restart")
{
    stdlib::register_standard_operators();
    SECTION("untyped Frame") { frames_restart<TS<Frame>>(); }
    SECTION("typed Frame[Row]") { frames_restart<TS<FrameOf<Row>>>(); }
}
