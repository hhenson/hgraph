#include <hgraph/fabric/fabric.h>

#include <hgraph/types/utils/counted_mutex.h>

#include <hgraph/persistence/frame_store.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>

#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace
{
    namespace hg   = hgraph;
    namespace hgf  = hgraph::fabric;
    namespace hgps = hgraph::persistence::store;

    constexpr hg::DateTime TIME_1{hg::TimeDelta{1'767'323'045'006'007}};
    constexpr hg::DateTime TIME_2{hg::TimeDelta{1'767'323'045'006'007}};
    constexpr hg::DateTime TIME_3{hg::TimeDelta{1'767'323'045'006'008}};

    [[nodiscard]] hg::Frame frame(std::int64_t value,
                                  std::string field_name = "value")
    {
        arrow::Int64Builder builder;
        REQUIRE(builder.Append(value).ok());
        auto result = builder.Finish();
        REQUIRE(result.ok());
        return hg::Frame{arrow::Table::Make(
            arrow::schema({arrow::field(std::move(field_name), arrow::int64())}),
            {std::move(result).ValueOrDie()})};
    }

    [[nodiscard]] hgf::PublicationInput output(
        std::int64_t value, hg::DateTime now = TIME_1,
        std::vector<hgf::DataDependencyInput> dependencies = {})
    {
        return hgf::PublicationInput{
            .output = frame(value),
            .dependencies = std::move(dependencies),
            .system_time = now,
        };
    }

    [[nodiscard]] hgf::PublicationInput inputs_only(
        std::vector<hgf::DataDependencyInput> dependencies,
        hg::DateTime now = TIME_3)
    {
        return hgf::PublicationInput{
            .dependencies = std::move(dependencies),
            .system_time = now,
        };
    }

    hgf::PublicationState drive(hgf::PublisherStateMachine &machine)
    {
        for (int count = 0; count < 16 &&
                            !hgf::publication_terminal(machine.state());
             ++count)
        {
            machine.advance();
        }
        REQUIRE(hgf::publication_terminal(machine.state()));
        return machine.state();
    }

    [[nodiscard]] hgf::DataRevisionInput stored_revision(
        const hgf::FabricConfig &config, std::string_view data_id,
        hgf::RevisionId revision)
    {
        const auto stored = config.objects.get(
            hgf::revision_key(config.prefix, data_id, revision));
        REQUIRE(stored.has_value());
        return hgf::data_revision_input(config.values.decode(hgf::data_revision_meta(), stored->data).view());
    }

    [[nodiscard]] hgf::RevisionId stored_latest(
        const hgf::FabricConfig &config, std::string_view data_id)
    {
        const auto stored =
            config.objects.get(hgf::latest_key(config.prefix, data_id));
        REQUIRE(stored.has_value());
        return hgf::revision_reference_value(config.reference_codec, hgf::MetadataObjectKind::Latest, stored->data);
    }

    struct ControlledDelivery
    {
        hgf::NotificationDeliveryResult result{};
    };

    struct ControlledNotifier
    {
        std::shared_ptr<ControlledDelivery> current{};
        std::vector<hgf::RevisionNotification> notifications{};
    };

    [[nodiscard]] const hgf::NotificationDeliveryOps &controlled_delivery_ops()
    {
        static const hgf::NotificationDeliveryOps ops{
            [](void *context) {
                return static_cast<ControlledDelivery *>(context)->result;
            },
        };
        return ops;
    }

    [[nodiscard]] const hgf::NotifierOps &controlled_notifier_ops()
    {
        static const hgf::NotifierOps ops{
            [](void *) { return hgf::NotificationSubscription{}; },
            [](void *context, hgf::RevisionNotification notification) {
                auto &notifier = *static_cast<ControlledNotifier *>(context);
                notifier.notifications.push_back(std::move(notification));
                notifier.current = std::make_shared<ControlledDelivery>();
                return hgf::NotificationDelivery{notifier.current,
                                                 controlled_delivery_ops()};
            },
        };
        return ops;
    }

    [[nodiscard]] hgf::FabricConfig controlled_config(
        std::shared_ptr<ControlledNotifier> notifier)
    {
        auto config = hgf::make_memory_fabric_config("tests/fabric");
        config.notifications =
            hgf::Notifier{std::move(notifier), controlled_notifier_ops()};
        return config;
    }

    struct FailingFrameStore
    {
    };

    [[nodiscard]] const hgps::FrameStoreOps &failing_frame_ops()
    {
        static const hgps::FrameStoreOps ops{
            [](void *, std::string_view, hg::Frame,
               std::optional<hgps::Compression>) {
                throw std::runtime_error("injected Frame write failure");
            },
            [](void *, std::string_view) { return hg::Frame{}; },
            [](void *, std::string_view) { return false; },
            [](void *) {},
        };
        return ops;
    }
}  // namespace

TEST_CASE("fabric durable keys are canonical reversible and numerically ordered")
{
    const std::string encoded = hgf::encode_data_id_segment("prices/α");
    CHECK(encoded == "bcHJpY2VzL86x");
    CHECK(hgf::decode_data_id_segment(encoded) == "prices/α");
    for (const std::string value : {"a", "ab", "abc", "abcd", "αβγ"})
    {
        CHECK(hgf::decode_data_id_segment(hgf::encode_data_id_segment(value)) ==
              value);
    }
    CHECK_THROWS_AS(hgf::decode_data_id_segment("bYR"),
                    std::invalid_argument);
    CHECK_THROWS_AS(hgf::decode_data_id_segment("bcHJpY2VzL86="),
                    std::invalid_argument);

    CHECK(hgf::encode_fabric_ordinal(9) < hgf::encode_fabric_ordinal(10));
    CHECK(hgf::decode_fabric_ordinal(hgf::encode_fabric_ordinal(
              std::numeric_limits<hg::Int>::max())) ==
          std::numeric_limits<hg::Int>::max());
    CHECK_THROWS_AS(hgf::decode_fabric_ordinal("1"), std::invalid_argument);

    CHECK(hgf::data_version_key("fabric/root", "prices/α", 42) ==
          "fabric/root/bcHJpY2VzL86x/data/0000000000000000042");
    CHECK(hgf::revision_key("fabric/root", "prices/α", 3) ==
          "fabric/root/bcHJpY2VzL86x/revision/0000000000000000003");
    CHECK(hgf::as_of_key("fabric/root", "prices/α", TIME_1) ==
          "fabric/root/bcHJpY2VzL86x/as_of/0001767323045006007");
    CHECK(hgf::latest_key("fabric/root", "prices/α") ==
          "fabric/root/bcHJpY2VzL86x/latest");
    CHECK_THROWS_AS(hgf::data_version_key("bad/", "x", 1),
                    std::invalid_argument);
    CHECK_THROWS_WITH(
        hgf::data_version_key("fabric/root",
                              std::string(hgf::MAX_DATA_ID_BYTES, 'x'), 1),
        "fabric durable key exceeds the portable 1024-byte limit");
}

TEST_CASE("publication makes the accepted revision durable before advertising it")
{
    auto config = hgf::make_memory_fabric_config("tests/fabric");
    auto notices = config.notifications.subscribe();
    hgf::PublisherStateMachine machine{config, "result"};
    machine.begin(output(7));

    REQUIRE(machine.advance() == hgf::PublicationState::FrameDurable);
    const auto candidate = machine.candidate_revision();
    REQUIRE(candidate.has_value());
    CHECK(candidate->revision == 1);
    CHECK(candidate->output_version == 1'767'323'045'006);
    CHECK(config.frames.contains(hgf::data_version_key(
        config.prefix, "result", candidate->output_version)));
    CHECK_FALSE(config.objects.get(
        hgf::revision_key(config.prefix, "result", 1)));

    CHECK(machine.advance() == hgf::PublicationState::RevisionDurable);
    CHECK(config.objects.get(hgf::revision_key(config.prefix, "result", 1)));
    CHECK(machine.accepted_revision() == candidate);
    CHECK(notices.pending() == 0);
    CHECK_FALSE(config.objects.get(
        hgf::as_of_key(config.prefix, "result", candidate->as_of)));
    CHECK_FALSE(config.objects.get(hgf::latest_key(config.prefix, "result")));

    CHECK(machine.advance() == hgf::PublicationState::AsOfDurable);
    CHECK(config.objects.get(
        hgf::as_of_key(config.prefix, "result", candidate->as_of)));
    CHECK_FALSE(config.objects.get(hgf::latest_key(config.prefix, "result")));
    CHECK(machine.advance() == hgf::PublicationState::LatestDurable);
    CHECK(stored_latest(config, "result") == 1);
    CHECK(notices.pending() == 0);
    CHECK(machine.advance() == hgf::PublicationState::NotificationPending);
    CHECK(notices.pending() == 1);
    const auto notice = notices.try_pop();
    REQUIRE(notice.has_value());
    CHECK(notice->data_id == "result");
    CHECK(hgf::data_revision_input(config.values.decode(hgf::data_revision_meta(), notice->revision).view()) ==
          *candidate);
    CHECK(machine.advance() ==
          hgf::PublicationState::NotificationAcknowledged);
    CHECK(machine.advance() == hgf::PublicationState::Published);
    CHECK(machine.accepted_revision() == candidate);
}

TEST_CASE("publication acknowledgement is asynchronous and retries the accepted revision")
{
    auto notifier = std::make_shared<ControlledNotifier>();
    auto config = controlled_config(notifier);
    hgf::PublisherStateMachine machine{config, "result"};
    machine.begin(output(7));
    CHECK(machine.advance() == hgf::PublicationState::FrameDurable);
    CHECK(machine.advance() == hgf::PublicationState::RevisionDurable);
    CHECK(machine.advance() == hgf::PublicationState::AsOfDurable);
    CHECK(machine.advance() == hgf::PublicationState::LatestDurable);
    CHECK(machine.advance() == hgf::PublicationState::NotificationPending);
    REQUIRE(notifier->notifications.size() == 1);
    REQUIRE(notifier->current);

    CHECK(machine.advance() == hgf::PublicationState::NotificationPending);
    CHECK(config.objects.get(hgf::revision_key(config.prefix, "result", 1)));
    CHECK(stored_latest(config, "result") == 1);
    notifier->current->result.status = hgf::NotificationDeliveryStatus::Delivered;
    CHECK(machine.advance() ==
          hgf::PublicationState::NotificationAcknowledged);
    CHECK(machine.advance() == hgf::PublicationState::Published);

    auto failed_notifier = std::make_shared<ControlledNotifier>();
    auto failed_config = controlled_config(failed_notifier);
    hgf::PublisherStateMachine failed{failed_config, "failed"};
    failed.begin(output(9));
    CHECK(failed.advance() == hgf::PublicationState::FrameDurable);
    CHECK(failed.advance() == hgf::PublicationState::RevisionDurable);
    CHECK(failed.advance() == hgf::PublicationState::AsOfDurable);
    CHECK(failed.advance() == hgf::PublicationState::LatestDurable);
    CHECK(failed.advance() == hgf::PublicationState::NotificationPending);
    failed_notifier->current->result = {
        hgf::NotificationDeliveryStatus::Failed, "broker rejected revision"};
    CHECK_THROWS_WITH(failed.advance(),
                      "fabric revision notification delivery failed: broker rejected revision");
    CHECK(failed.state() == hgf::PublicationState::LatestDurable);
    CHECK(failed_config.objects.get(
        hgf::revision_key(failed_config.prefix, "failed", 1)));
    CHECK(stored_latest(failed_config, "failed") == 1);
    REQUIRE(failed.accepted_revision());

    CHECK(failed.advance() == hgf::PublicationState::NotificationPending);
    REQUIRE(failed_notifier->notifications.size() == 2);
    REQUIRE(failed_notifier->current);
    failed_notifier->current->result.status =
        hgf::NotificationDeliveryStatus::Delivered;
    CHECK(failed.advance() == hgf::PublicationState::NotificationAcknowledged);
    CHECK(failed.advance() == hgf::PublicationState::Published);
}

TEST_CASE("graph transport acknowledges the durable candidate without invoking a notifier")
{
    auto config = hgf::make_memory_fabric_config("tests/fabric-graph-notification");
    auto notices = config.notifications.subscribe();
    hgf::PublisherStateMachine machine{config, "result"};
    CHECK_THROWS_AS(machine.acknowledge_notification(), std::logic_error);

    machine.begin(output(7));
    CHECK(machine.advance() == hgf::PublicationState::FrameDurable);
    CHECK(machine.advance() == hgf::PublicationState::RevisionDurable);
    CHECK(machine.advance() == hgf::PublicationState::AsOfDurable);
    CHECK(machine.advance() == hgf::PublicationState::LatestDurable);
    CHECK(notices.pending() == 0);

    machine.acknowledge_notification();
    CHECK(machine.state() == hgf::PublicationState::NotificationAcknowledged);
    CHECK(machine.advance() == hgf::PublicationState::Published);
    CHECK(notices.pending() == 0);
}

TEST_CASE("input-only revisions retain output and identical tuples are suppressed")
{
    auto config = hgf::make_memory_fabric_config("tests/fabric");
    hgf::PublisherStateMachine machine{config, "result"};
    machine.begin(output(7));
    REQUIRE(drive(machine) == hgf::PublicationState::Published);
    const auto first = machine.accepted_revision();
    REQUIRE(first.has_value());

    machine.begin(inputs_only({{"input", 2}}));
    REQUIRE(drive(machine) == hgf::PublicationState::Published);
    const auto second = machine.accepted_revision();
    REQUIRE(second.has_value());
    CHECK(second->revision == 2);
    CHECK(second->output_version == first->output_version);
    CHECK(second->dependencies ==
          std::vector<hgf::DataDependencyInput>{{"input", 2}});

    machine.begin(inputs_only({{"input", 2}}, TIME_3 + hg::TimeDelta{1}));
    CHECK(machine.advance() == hgf::PublicationState::Unchanged);
    CHECK(stored_latest(config, "result") == 2);

    machine.begin(output(7, TIME_2, {{"input", 2}}));
    REQUIRE(drive(machine) == hgf::PublicationState::Published);
    const auto third = machine.accepted_revision();
    REQUIRE(third.has_value());
    CHECK(third->revision == 3);
    CHECK(third->output_version == first->output_version + 1);
}

TEST_CASE("the first accepted Frame fixes exact schema")
{
    auto config = hgf::make_memory_fabric_config("tests/fabric");
    hgf::PublisherStateMachine machine{config, "result"};
    machine.begin(output(7));
    REQUIRE(drive(machine) == hgf::PublicationState::Published);

    hgf::PublicationInput changed = output(8, TIME_3);
    changed.output = frame(8, "different");
    machine.begin(std::move(changed));
    CHECK_THROWS_WITH(
        machine.advance(),
        "fabric output Frame schema does not match the first accepted schema");
    CHECK(stored_latest(config, "result") == 1);
}

TEST_CASE("publication races are first-writer-wins and losers never become the next revision")
{
    SECTION("different candidate versions leave one accepted winner")
    {
        auto config = hgf::make_memory_fabric_config("tests/fabric");
        auto notices = config.notifications.subscribe();
        hgf::PublisherStateMachine first{config, "result"};
        hgf::PublisherStateMachine second{config, "result"};
        first.begin(output(1, TIME_1));
        second.begin(output(2, TIME_1 + hg::TimeDelta{1'000}));
        REQUIRE(first.advance() == hgf::PublicationState::FrameDurable);
        REQUIRE(second.advance() == hgf::PublicationState::FrameDurable);
        REQUIRE(first.advance() == hgf::PublicationState::RevisionDurable);
        CHECK(second.advance() == hgf::PublicationState::LostRace);
        REQUIRE(second.accepted_revision());
        CHECK(second.accepted_revision()->output_version ==
              first.candidate_revision()->output_version);
        CHECK_FALSE(config.objects.get(
            hgf::revision_key(config.prefix, "result", 2)));
        CHECK(drive(first) == hgf::PublicationState::Published);
        CHECK(notices.pending() == 1);
        const auto notice = notices.try_pop();
        REQUIRE(notice);
        CHECK(hgf::data_revision_input(
                  config.values.decode(hgf::data_revision_meta(), notice->revision).view()) ==
              *first.candidate_revision());
    }

    SECTION("conflicting Frames at one millisecond lose before notification")
    {
        auto config = hgf::make_memory_fabric_config("tests/fabric");
        auto notices = config.notifications.subscribe();
        hgf::PublisherStateMachine first{config, "result"};
        hgf::PublisherStateMachine second{config, "result"};
        first.begin(output(1, TIME_1));
        second.begin(output(2, TIME_1));
        CHECK(first.advance() == hgf::PublicationState::FrameDurable);
        CHECK(second.advance() == hgf::PublicationState::LostRace);
        CHECK(notices.pending() == 0);
        CHECK_FALSE(config.objects.get(
            hgf::revision_key(config.prefix, "result", 1)));
    }

    SECTION("identical Frame retries can still compete for the slot")
    {
        auto config = hgf::make_memory_fabric_config("tests/fabric");
        hgf::PublisherStateMachine first{config, "result"};
        hgf::PublisherStateMachine second{config, "result"};
        first.begin(output(1, TIME_1));
        second.begin(output(1, TIME_1));
        CHECK(first.advance() == hgf::PublicationState::FrameDurable);
        CHECK(second.advance() == hgf::PublicationState::FrameDurable);
    }
}

TEST_CASE("startup repairs contiguous revision slots and stale derived indexes")
{
    SECTION("revision durable but both derived indexes missing")
    {
        auto config = hgf::make_memory_fabric_config("tests/fabric");
        {
            hgf::PublisherStateMachine crashed{config, "result"};
            crashed.begin(output(7));
            while (crashed.state() != hgf::PublicationState::RevisionDurable)
            {
                crashed.advance();
            }
            REQUIRE(crashed.candidate_revision());
            CHECK_FALSE(config.objects.get(hgf::as_of_key(
                config.prefix, "result", crashed.candidate_revision()->as_of)));
            CHECK_FALSE(config.objects.get(
                hgf::latest_key(config.prefix, "result")));
        }

        hgf::PublisherStateMachine recovered{config, "result"};
        recovered.begin(inputs_only({}));
        CHECK(recovered.advance() == hgf::PublicationState::Unchanged);
        CHECK(stored_latest(config, "result") == 1);
        const auto revision = stored_revision(config, "result", 1);
        CHECK(config.objects.get(
            hgf::as_of_key(config.prefix, "result", revision.as_of)));
    }

    SECTION("latest lags a later contiguous slot")
    {
        auto config = hgf::make_memory_fabric_config("tests/fabric");
        hgf::PublisherStateMachine publisher{config, "result"};
        publisher.begin(output(7));
        REQUIRE(drive(publisher) == hgf::PublicationState::Published);
        publisher.begin(inputs_only({{"input", 2}}));
        REQUIRE(drive(publisher) == hgf::PublicationState::Published);
        REQUIRE(stored_latest(config, "result") == 2);

        const std::string key = hgf::latest_key(config.prefix, "result");
        const auto current = config.objects.get(key);
        REQUIRE(current);
        const auto stale = config.objects.compare_exchange_ref(
            key, current->version_token,
            hgf::encode_reference(config.reference_codec, hgf::MetadataObjectKind::Latest, 1));
        REQUIRE(stale.exchanged);

        hgf::PublisherStateMachine recovered{config, "result"};
        recovered.begin(inputs_only({{"input", 2}}, TIME_3 + hg::TimeDelta{2}));
        CHECK(recovered.advance() == hgf::PublicationState::Unchanged);
        CHECK(stored_latest(config, "result") == 2);
    }
}

TEST_CASE("each publication crash boundary has the RFC recovery outcome")
{
    const std::vector<hgf::PublicationState> boundaries{
        hgf::PublicationState::FrameDurable,
        hgf::PublicationState::RevisionDurable,
        hgf::PublicationState::AsOfDurable,
        hgf::PublicationState::LatestDurable,
        hgf::PublicationState::NotificationAcknowledged,
    };
    for (const auto boundary : boundaries)
    {
        DYNAMIC_SECTION("boundary " << static_cast<int>(boundary))
        {
            auto config = hgf::make_memory_fabric_config("tests/fabric");
            {
                hgf::PublisherStateMachine crashed{config, "result"};
                crashed.begin(output(7));
                while (crashed.state() != boundary) { crashed.advance(); }
            }

            const bool committed =
                boundary == hgf::PublicationState::RevisionDurable ||
                boundary == hgf::PublicationState::AsOfDurable ||
                boundary == hgf::PublicationState::LatestDurable ||
                boundary == hgf::PublicationState::NotificationAcknowledged;
            hgf::PublisherStateMachine recovered{config, "result"};
            recovered.begin(committed ? inputs_only({}) : output(7));
            REQUIRE(drive(recovered) ==
                    (committed ? hgf::PublicationState::Unchanged
                               : hgf::PublicationState::Published));
            CHECK(stored_latest(config, "result") == 1);
            CHECK(stored_revision(config, "result", 1).output_version ==
                  1'767'323'045'006);
        }
    }
}

TEST_CASE("Frame failure and corrupt or non-contiguous histories never expose a head")
{
    SECTION("Frame write failure leaves no notification or revision")
    {
        auto config = hgf::make_memory_fabric_config("tests/fabric");
        auto notices = config.notifications.subscribe();
        config.frames = hgps::FrameStore{std::make_shared<FailingFrameStore>(),
                                         failing_frame_ops()};
        hgf::PublisherStateMachine machine{config, "result"};
        machine.begin(output(7));
        CHECK_THROWS_WITH(machine.advance(), "injected Frame write failure");
        CHECK(notices.pending() == 0);
        CHECK_FALSE(config.objects.get(
            hgf::revision_key(config.prefix, "result", 1)));
        CHECK_FALSE(config.objects.get(
            hgf::latest_key(config.prefix, "result")));
    }

    SECTION("a revision cannot make a missing Frame visible")
    {
        auto config = hgf::make_memory_fabric_config("tests/fabric");
        auto value = hgf::make_data_revision(hgf::DataRevisionInput{
            .data_id = "result",
            .revision = 1,
            .output_version = 99,
            .as_of = TIME_1,
        });
        REQUIRE(config.objects.put_immutable(
                    hgf::revision_key(config.prefix, "result", 1),
                    config.values.encode(value.view()))
                    .status == hgps::ImmutableWriteStatus::Created);
        hgf::PublisherStateMachine machine{config, "result"};
        machine.begin(inputs_only({}));
        CHECK_THROWS_WITH(
            machine.advance(),
            "fabric revision references a missing durable Frame: result:99");
        CHECK_FALSE(config.objects.get(
            hgf::latest_key(config.prefix, "result")));
    }

    SECTION("a gap is malformed rather than an alternate history")
    {
        auto config = hgf::make_memory_fabric_config("tests/fabric");
        config.frames.write(hgf::data_version_key(config.prefix, "result", 2),
                            frame(2));
        auto value = hgf::make_data_revision(hgf::DataRevisionInput{
            .data_id = "result",
            .revision = 2,
            .output_version = 2,
            .as_of = TIME_1,
        });
        REQUIRE(config.objects.put_immutable(
                    hgf::revision_key(config.prefix, "result", 2),
                    config.values.encode(value.view()))
                    .status == hgps::ImmutableWriteStatus::Created);
        hgf::PublisherStateMachine machine{config, "result"};
        machine.begin(inputs_only({}));
        CHECK_THROWS_WITH(
            machine.advance(),
            "fabric revision history contains a non-contiguous or malformed slot");
        CHECK_FALSE(config.objects.get(
            hgf::latest_key(config.prefix, "result")));
    }
}

TEST_CASE("the codec a wired fabric carries resolves nothing per value")
{
    // The enforceable form of the single-threaded evaluation ruling: every
    // type-system mutex is counted, so a codec bound at wiring time must leave
    // the counter untouched no matter how many values go through it.
    // bind_metadata_codecs resolves the json converter once, in the
    // configuration path; before that, to_json_string resolved it per value and
    // locked to do it.
    //
    // This asserts the codec's contribution only. A whole publication still
    // takes locks -- roughly 300 at the time of writing, down from 910 -- and
    // those come from value construction and field reads (bundle plan lookup,
    // checked_as per field) rather than from serialization. Removing them needs
    // the same bind-at-wiring-time treatment applied to value plans, which is
    // core-level and tracked separately; pinning a whole-publication number
    // here would be a brittle proxy for a property this test can state exactly.
    auto config = hgf::make_memory_fabric_config("tests/fabric");
    const hg::Value revision = hgf::make_data_revision(hgf::DataRevisionInput{
        .data_id = "result", .revision = 1, .output_version = 1,
        .as_of = hg::MIN_ST});

    // Warm once: the converter is composed on first sight of the schema.
    static_cast<void>(config.revision_codec.encode(revision.view()));

    const auto before = hgraph::type_system_lock_count();
    for (int index = 0; index < 64; ++index)
    {
        static_cast<void>(config.revision_codec.encode(revision.view()));
    }
    CHECK(hgraph::type_system_lock_count() == before);
}

TEST_CASE("diagnostic: where publication still locks", "[.diagnostic]")
{
    auto config = hgf::make_memory_fabric_config("tests/fabric");
    {
        hgf::PublisherStateMachine warmup{config, "warm"};
        warmup.begin(output(7));
        while (warmup.advance() != hgf::PublicationState::Published) {}
    }
    const hgf::DataRevisionInput input{
        .data_id = "probe", .revision = 1, .output_version = 1,
        .as_of = hg::MIN_ST};

    auto measure = [](auto &&fn) {
        const auto before = hgraph::type_system_lock_count();
        fn();
        return hgraph::type_system_lock_count() - before;
    };

    hg::Value revision = hgf::make_data_revision(input);
    const auto build = measure([&] { static_cast<void>(hgf::make_data_revision(input)); });
    const auto encode = measure([&] {
        static_cast<void>(config.revision_codec.encode(revision.view()));
    });
    const auto encoded = config.revision_codec.encode(revision.view());
    const auto decode = measure([&] {
        static_cast<void>(config.revision_codec.decode(encoded));
    });
    const auto to_input = measure([&] {
        static_cast<void>(hgf::data_revision_input(revision.view()));
    });
    const auto reference = measure([&] {
        static_cast<void>(hgf::encode_reference(config.reference_codec,
                                                hgf::MetadataObjectKind::Latest, 1));
    });
    WARN("make_data_revision=" << build << " encode=" << encode
         << " decode=" << decode << " data_revision_input=" << to_input
         << " encode_reference=" << reference);
}
