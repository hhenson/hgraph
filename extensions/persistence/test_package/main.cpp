#include <hgraph/persistence/frame_store.h>
#include <hgraph/persistence/object_store.h>
#include <hgraph/persistence/recording_store.h>

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/table_codec.h>

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace
{
    namespace hg = hgraph;
    namespace hgp = hgraph::persistence;

    void require(bool condition, const char *what)
    {
        if (!condition)
        {
            throw std::runtime_error(what);
        }
    }

    /** The stream both halves of the round trip carry, at the engine times
        replay must re-emit it on (cycle 0 and cycle 2 - the gap is part of
        what a recorded stream preserves). */
    [[nodiscard]] const std::vector<std::pair<hg::DateTime, hg::Int>> &round_trip_rows()
    {
        static const std::vector<std::pair<hg::DateTime, hg::Int>> rows{
            {hg::MIN_ST, hg::Int{11}},
            {hg::MIN_ST + hg::MIN_TD * 2, hg::Int{22}},
        };
        return rows;
    }

    /** A bitemporal recording of those rows, built through the installed
        table codec rather than by hand: the frame layout replay reads is the
        backend's, so the consumer must not re-spell it. */
    [[nodiscard]] hg::Frame seeded_recording()
    {
        hg::FrameRecorder recorder{
            hg::table_converter(hg::scalar_descriptor<hg::Int>::value_meta())};
        for (const auto &[when, value] : round_trip_rows())
        {
            const hg::Value cell{value};
            recorder.append(when, when, cell.view());
        }
        return recorder.finish();
    }

    void check_frame_backend_round_trip()
    {
        using namespace hgraph;

        // A real graph over the DURABLE backend: replay reads the seeded
        // recording out of the state-selected frame store and record writes
        // it back under a second key. Both overloads live in the installed
        // extension, so the second key exists only if the extension's
        // operator overloads resolve and evaluate across the install boundary.
        Wiring     wiring;
        const auto wiring_state = wiring.global_state();
        hgp::set_frame_store(wiring_state,
                             hgp::store::make_frame_store(hgp::store::FrameStoreConfig{}));
        record_replay::set_config(
            wiring_state,
            record_replay::RecordReplayConfig{.backend = std::string{hgp::FRAME_BACKEND}});
        hgp::store_write(wiring_state, "consumer.in", seeded_recording());

        auto replayed =
            wire<stdlib::replay, TS<Int>>(wiring, Str{"in"}, arg<"recordable_id">(Str{"consumer"}));
        wire<stdlib::record>(wiring, replayed, Str{"out"}, arg<"recordable_id">(Str{"consumer"}));

        GraphExecutorBuilder executor_builder;
        executor_builder.graph_builder(std::move(wiring).finish())
            .start_time(MIN_ST)
            .end_time(MIN_ST + MIN_TD * 6);
        GraphExecutorValue executor = executor_builder.make_executor();
        const auto         view     = executor.view();
        view.run();

        // The store outlives the run: what the recording sink wrote is read
        // back out of it and must match the replayed stream row for row.
        const auto state = view.graph().global_state();
        require(hgp::store_contains(state, "consumer.out"),
                "the run recorded through the durable frame backend");
        const Frame recorded  = hgp::store_read(state, "consumer.out");
        const auto &converter = table_converter(scalar_descriptor<Int>::value_meta());
        require(frame_rows(recorded) == static_cast<std::int64_t>(round_trip_rows().size()),
                "every replayed row was recorded again");
        for (std::size_t row = 0; row < round_trip_rows().size(); ++row)
        {
            const auto &[when, value] = round_trip_rows()[row];
            const auto index          = static_cast<std::int64_t>(row);
            require(frame_value_time(converter, recorded, index) == when,
                    "each row replayed at its recorded time");
            require(read_row(converter, recorded, index).view().checked_as<Int>() == value,
                    "each row replayed its recorded value");
        }

        // ``reset_all_registries()`` is deliberately NOT called here: it
        // invalidates the interned metadata every live value above still
        // points at, so a full reset cannot run mid-consumer (the documented
        // test-only reset hazard). The installer replay this extension
        // registers is covered by the in-tree installer tests and the Python
        // extension consumer.
    }

    void check_object_store_contract()
    {
        using namespace hgraph::persistence::store;
        auto              store = make_object_store(ObjectStoreConfig{});
        const std::string value{"installed-object"};
        const auto        data = std::as_bytes(std::span{value.data(), value.size()});

        const auto created = store.put_immutable("consumer/object", data);
        require(created.status == ImmutableWriteStatus::Created,
                "installed object store created an immutable object");
        const auto loaded = store.get("consumer/object");
        require(loaded.has_value() && loaded->data.size() == data.size(),
                "installed object store read the immutable object");

        const auto head = store.compare_exchange_ref("consumer/latest", {}, data);
        require(head.exchanged && head.current.has_value(),
                "installed object store created a conditional reference");
        require(store.list("consumer/", {}, 10).objects.size() == 2,
                "installed object store listed its ordered namespace");
    }
}  // namespace

int main()
{
    try
    {
        // The standard operators first, so the built-in record/replay
        // backends are registered alongside the durable one: the round trip
        // below then proves the extension's overloads were SELECTED, not
        // merely the only candidates left.
        hg::stdlib::register_standard_operators();
        // The installed extension registers its backend through the shared
        // runtime's keyed-installer mechanism.
        hgp::register_frame_backend();
        require(!hg::OperatorRegistry::instance()
                     .overload_signatures("record")
                     .empty(),
                "durable record overload registered");

        {
            // The GlobalState-scoped store round-trips a frame with immutable
            // keys through the installed SDK.
            hg::GlobalContext context;
            const auto        state = context.state().view();
            hgp::set_frame_store(state,
                                 hgp::store::make_frame_store(hgp::store::FrameStoreConfig{}));
            hgp::store_write(state, "consumer/frame", hg::Frame{});
            require(hgp::store_contains(state, "consumer/frame"),
                    "stored frame is retrievable");
            require(!hgp::store_contains(state, "consumer/absent"),
                    "absent keys read as absent");
        }

        // The segmented-recording protocol survives the installed boundary.
        const hg::Frame marker = hgp::segmented_recording_marker();
        require(hgp::is_segmented_recording(marker),
                "segmented-recording marker recognised");
        require(hgp::segment_key("k", 2) == "k.2", "segment key shape");

        check_object_store_contract();

        // The store and the protocol exist for the operators built on them:
        // run those operators in a graph.
        check_frame_backend_round_trip();

        std::cout << "hgraph-persistence installed consumer passed\n";
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
