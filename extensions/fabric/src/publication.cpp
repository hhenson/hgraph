#include <hgraph/fabric/publication.h>

#include <hgraph/fabric/keys.h>
#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/value_builders.h>

#include <arrow/table.h>

#include <algorithm>
#include <exception>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph::fabric
{
    namespace
    {
        using persistence::store::ImmutableWriteStatus;
        using persistence::store::ObjectBytes;
        using persistence::store::StoredObject;

        [[nodiscard]] bool same_schema(const Frame &frame,
                                       const std::shared_ptr<arrow::Schema> &schema)
        {
            return frame.has_value() && schema != nullptr &&
                   frame.table->schema()->Equals(*schema, true);
        }

        [[nodiscard]] bool same_frame(const Frame &lhs, const Frame &rhs)
        {
            return lhs.has_value() && rhs.has_value() &&
                   lhs.table->schema()->Equals(*rhs.table->schema(), true) &&
                   lhs.table->Equals(*rhs.table);
        }

        [[nodiscard]] bool same_tuple(const DataRevisionInput &lhs,
                                      const DataRevisionInput &rhs)
        {
            return lhs.output_version == rhs.output_version &&
                   lhs.dependencies == rhs.dependencies &&
                   lhs.self_predecessor == rhs.self_predecessor;
        }

        [[nodiscard]] Int checked_increment(Int value, std::string_view field)
        {
            if (value == std::numeric_limits<Int>::max())
            {
                throw std::overflow_error("fabric " + std::string{field} +
                                          " is exhausted");
            }
            return value + 1;
        }

        [[nodiscard]] DataVersion allocate_version(DateTime now,
                                                   DataVersion previous)
        {
            const auto milliseconds = now.time_since_epoch().count() / 1'000;
            if (milliseconds <= 0)
            {
                throw std::invalid_argument(
                    "fabric publication system time must be after the Unix epoch");
            }
            return std::max(static_cast<DataVersion>(milliseconds),
                            previous == 0 ? DataVersion{1}
                                          : checked_increment(previous, "data version"));
        }

        [[nodiscard]] DateTime allocate_as_of(DateTime now,
                                              std::optional<DateTime> previous)
        {
            const auto current = now.time_since_epoch().count();
            if (current <= 0)
            {
                throw std::invalid_argument(
                    "fabric publication system time must be after the Unix epoch");
            }
            if (!previous.has_value()) { return now; }
            const auto prior = previous->time_since_epoch().count();
            if (prior == std::numeric_limits<TimeDelta::rep>::max())
            {
                throw std::overflow_error("fabric publication as-of is exhausted");
            }
            return DateTime{TimeDelta{std::max(current, prior + 1)}};
        }
    }  // namespace

    struct PublisherStateMachine::Impl
    {
        FabricConfig config;
        Str          data_id;
        PublicationState state{PublicationState::Idle};
        PublicationInput input{};
        std::optional<DataRevisionInput> accepted{};
        std::optional<DataRevisionInput> candidate{};
        ObjectBytes candidate_bytes{};
        NotificationDelivery delivery{};
        std::shared_ptr<arrow::Schema> fixed_schema{};

        Impl(FabricConfig configured, Str id)
            : config(std::move(configured)), data_id(std::move(id))
        {
            require_valid_config(config);
            require_data_id(data_id);
        }

        [[nodiscard]] std::optional<StoredObject> metadata(std::string_view key) const
        {
            return config.objects.get(key);
        }

        [[nodiscard]] DataRevisionInput decode_slot(RevisionId revision) const
        {
            const auto object = metadata(revision_key(config.prefix, data_id, revision));
            if (!object.has_value())
            {
                throw std::runtime_error("fabric accepted revision slot is missing: " +
                                         data_id + ":" + std::to_string(revision));
            }
            DataRevisionInput decoded =
                data_revision_input(decode_revision(object->data).view());
            if (decoded.data_id != data_id || decoded.revision != revision)
            {
                throw std::runtime_error(
                    "fabric revision payload does not match its durable key");
            }
            return decoded;
        }

        [[nodiscard]] Frame load_frame(const DataRevisionInput &revision) const
        {
            Frame frame = config.frames.read(
                data_version_key(config.prefix, data_id, revision.output_version));
            if (!frame.has_value())
            {
                throw std::runtime_error(
                    "fabric revision references a missing durable Frame: " + data_id +
                    ":" + std::to_string(revision.output_version));
            }
            return frame;
        }

        void validate_and_accept(DataRevisionInput next)
        {
            if (accepted.has_value())
            {
                if (next.revision != checked_increment(accepted->revision, "revision id"))
                {
                    throw std::runtime_error("fabric revision history is not contiguous");
                }
                if (next.as_of <= accepted->as_of)
                {
                    throw std::runtime_error("fabric revision as-of history is not monotonic");
                }
                if (next.output_version < accepted->output_version)
                {
                    throw std::runtime_error("fabric output version history regressed");
                }
                if (same_tuple(next, *accepted))
                {
                    throw std::runtime_error(
                        "fabric revision history contains a duplicate accepted tuple");
                }
            }
            else if (next.revision != 1)
            {
                throw std::runtime_error("fabric revision history does not start at one");
            }

            Frame frame = load_frame(next);
            if (fixed_schema == nullptr)
            {
                fixed_schema = frame.table->schema();
            }
            else if (!same_schema(frame, fixed_schema))
            {
                throw std::runtime_error(
                    "fabric accepted Frame violates the fixed schema for data id '" +
                    data_id + "'");
            }
            accepted = std::move(next);
        }

        void repair_as_of(const DataRevisionInput &revision) const
        {
            const ObjectBytes desired = encode_revision_reference(
                MetadataObjectKind::AsOf, revision.revision);
            const auto result = config.objects.put_immutable(
                as_of_key(config.prefix, data_id, revision.as_of), desired);
            if (result.status == ImmutableWriteStatus::Conflict)
            {
                throw std::runtime_error(
                    "fabric as-of entry conflicts with the accepted revision");
            }
        }

        [[nodiscard]] std::optional<RevisionId> latest_revision() const
        {
            const auto current = metadata(latest_key(config.prefix, data_id));
            if (!current.has_value()) { return std::nullopt; }
            return decode_revision_reference(MetadataObjectKind::Latest, current->data);
        }

        void advance_latest(RevisionId target) const
        {
            const std::string key = latest_key(config.prefix, data_id);
            const ObjectBytes desired =
                encode_revision_reference(MetadataObjectKind::Latest, target);
            for (;;)
            {
                const auto current = metadata(key);
                if (current.has_value())
                {
                    const RevisionId current_revision = decode_revision_reference(
                        MetadataObjectKind::Latest, current->data);
                    if (current_revision >= target) { return; }
                }
                const auto result = config.objects.compare_exchange_ref(
                    key,
                    current.has_value()
                        ? std::optional<std::string_view>{current->version_token}
                        : std::nullopt,
                    desired);
                if (result.exchanged) { return; }
                if (!result.current.has_value()) { continue; }
                const RevisionId winner = decode_revision_reference(
                    MetadataObjectKind::Latest, result.current->data);
                if (winner >= target) { return; }
            }
        }

        void require_no_gap(RevisionId expected) const
        {
            const std::string prefix = revision_key_prefix(config.prefix, data_id);
            const std::optional<std::string> start_after =
                expected == 1
                    ? std::nullopt
                    : std::optional<std::string>{
                          revision_key(config.prefix, data_id, expected - 1)};
            const auto page = config.objects.list(
                prefix,
                start_after.has_value()
                    ? std::optional<std::string_view>{*start_after}
                    : std::nullopt,
                1);
            if (page.objects.empty()) { return; }
            const std::string expected_key =
                revision_key(config.prefix, data_id, expected);
            if (page.objects.front().key != expected_key)
            {
                throw std::runtime_error(
                    "fabric revision history contains a non-contiguous or malformed slot");
            }
        }

        void recover()
        {
            const std::optional<RevisionId> indexed_latest = latest_revision();
            RevisionId next = accepted.has_value()
                                  ? checked_increment(accepted->revision, "revision id")
                                  : RevisionId{1};
            if (indexed_latest.has_value() &&
                (!accepted.has_value() || *indexed_latest > accepted->revision))
            {
                while (next <= *indexed_latest)
                {
                    DataRevisionInput revision = decode_slot(next);
                    validate_and_accept(std::move(revision));
                    repair_as_of(*accepted);
                    next = checked_increment(next, "revision id");
                }
            }

            for (;;)
            {
                const auto object =
                    metadata(revision_key(config.prefix, data_id, next));
                if (!object.has_value()) { break; }
                DataRevisionInput revision =
                    data_revision_input(decode_revision(object->data).view());
                if (revision.data_id != data_id || revision.revision != next)
                {
                    throw std::runtime_error(
                        "fabric revision payload does not match its durable key");
                }
                validate_and_accept(std::move(revision));
                repair_as_of(*accepted);
                next = checked_increment(next, "revision id");
            }
            require_no_gap(next);

            if (accepted.has_value())
            {
                repair_as_of(*accepted);
                advance_latest(accepted->revision);
            }
            else if (indexed_latest.has_value())
            {
                throw std::runtime_error("fabric latest refers to an empty history");
            }
        }

        void write_frame(const Frame &frame, DataVersion version)
        {
            const std::string key =
                data_version_key(config.prefix, data_id, version);
            try
            {
                config.frames.write(key, frame);
                return;
            }
            catch (...)
            {
                const std::exception_ptr write_failure = std::current_exception();
                Frame existing;
                try
                {
                    existing = config.frames.read(key);
                }
                catch (...)
                {
                    std::rethrow_exception(write_failure);
                }
                if (!existing.has_value())
                {
                    std::rethrow_exception(write_failure);
                }
                if (same_frame(existing, frame)) { return; }
                state = PublicationState::LostRace;
            }
        }

        void prepare()
        {
            recover();
            if (input.system_time <= MIN_DT)
            {
                throw std::invalid_argument(
                    "fabric publication requires a real system time");
            }
            if (!input.output.has_value() && !accepted.has_value())
            {
                state = PublicationState::AwaitingFirstOutput;
                return;
            }
            if (input.output.has_value() && fixed_schema != nullptr &&
                !same_schema(input.output, fixed_schema))
            {
                throw std::invalid_argument(
                    "fabric output Frame schema does not match the first accepted schema");
            }

            const DataVersion output_version =
                input.output.has_value()
                    ? allocate_version(input.system_time,
                                       accepted.has_value() ? accepted->output_version : 0)
                    : accepted->output_version;
            const RevisionId revision =
                accepted.has_value()
                    ? checked_increment(accepted->revision, "revision id")
                    : RevisionId{1};
            DataRevisionInput proposed{
                .data_id = data_id,
                .revision = revision,
                .output_version = output_version,
                .dependencies = input.dependencies,
                .self_predecessor = input.self_predecessor,
                .as_of = allocate_as_of(
                    input.system_time,
                    accepted.has_value()
                        ? std::optional<DateTime>{accepted->as_of}
                        : std::nullopt),
            };
            Value canonical = make_data_revision(std::move(proposed));
            candidate = data_revision_input(canonical.view());
            candidate_bytes = encode_revision(canonical.view());

            if (!input.output.has_value() && same_tuple(*candidate, *accepted))
            {
                candidate.reset();
                candidate_bytes.clear();
                state = PublicationState::Unchanged;
                return;
            }
            if (input.output.has_value())
            {
                write_frame(input.output, output_version);
                if (state == PublicationState::LostRace) { return; }
            }
            state = PublicationState::FrameDurable;
        }

        void publish_notification()
        {
            delivery = config.notifications.publish(
                RevisionNotification{data_id, candidate_bytes});
            state = PublicationState::NotificationPending;
        }

        void poll_notification()
        {
            const NotificationDeliveryResult result = delivery.poll();
            if (result.status == NotificationDeliveryStatus::Pending) { return; }
            if (result.status == NotificationDeliveryStatus::Failed)
            {
                throw std::runtime_error(
                    result.message.empty()
                        ? "fabric revision notification delivery failed"
                        : "fabric revision notification delivery failed: " +
                              result.message);
            }
            state = PublicationState::NotificationAcknowledged;
        }

        void commit_revision()
        {
            const auto result = config.objects.put_immutable(
                revision_key(config.prefix, data_id, candidate->revision),
                candidate_bytes);
            if (result.status == ImmutableWriteStatus::Conflict)
            {
                DataRevisionInput winner = decode_slot(candidate->revision);
                validate_and_accept(std::move(winner));
                state = PublicationState::LostRace;
                return;
            }
            state = PublicationState::RevisionDurable;
        }

        PublicationState advance()
        {
            switch (state)
            {
                case PublicationState::Idle:
                    throw std::logic_error(
                        "fabric publication has no active attempt");
                case PublicationState::Preparing:
                    prepare();
                    break;
                case PublicationState::FrameDurable:
                    publish_notification();
                    break;
                case PublicationState::NotificationPending:
                    poll_notification();
                    break;
                case PublicationState::NotificationAcknowledged:
                    commit_revision();
                    break;
                case PublicationState::RevisionDurable:
                    repair_as_of(*candidate);
                    state = PublicationState::AsOfDurable;
                    break;
                case PublicationState::AsOfDurable:
                    advance_latest(candidate->revision);
                    state = PublicationState::LatestDurable;
                    break;
                case PublicationState::LatestDurable:
                    validate_and_accept(*candidate);
                    state = PublicationState::Published;
                    break;
                case PublicationState::Published:
                case PublicationState::Unchanged:
                case PublicationState::AwaitingFirstOutput:
                case PublicationState::LostRace:
                    break;
            }
            return state;
        }
    };

    PublisherStateMachine::PublisherStateMachine(FabricConfig config, Str data_id)
        : impl_(std::make_unique<Impl>(std::move(config), std::move(data_id)))
    {
    }

    PublisherStateMachine::~PublisherStateMachine() = default;
    PublisherStateMachine::PublisherStateMachine(PublisherStateMachine &&) noexcept = default;
    PublisherStateMachine &PublisherStateMachine::operator=(
        PublisherStateMachine &&) noexcept = default;

    void PublisherStateMachine::begin(PublicationInput input)
    {
        if (impl_->state != PublicationState::Idle &&
            !publication_terminal(impl_->state))
        {
            throw std::logic_error(
                "fabric publication attempt is still in progress");
        }
        impl_->input = std::move(input);
        impl_->candidate.reset();
        impl_->candidate_bytes.clear();
        impl_->delivery.reset();
        impl_->state = PublicationState::Preparing;
    }

    PublicationState PublisherStateMachine::advance() { return impl_->advance(); }

    PublicationState PublisherStateMachine::state() const noexcept
    {
        return impl_->state;
    }

    std::optional<DataRevisionInput>
    PublisherStateMachine::accepted_revision() const
    {
        return impl_->accepted;
    }

    std::optional<DataRevisionInput>
    PublisherStateMachine::candidate_revision() const
    {
        return impl_->candidate;
    }

    const Str &PublisherStateMachine::data_id() const noexcept
    {
        return impl_->data_id;
    }
}  // namespace hgraph::fabric
