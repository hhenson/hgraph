#include <hgraph/runtime/race_tsd_node.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_input/base_view.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/time_series/ts_input/dict_view.h>
#include <hgraph/types/time_series/ts_output/base_view.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/util/date_time.h>
#include <hgraph/util/scope.h>

#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <stdexcept>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr const char *race_tsd_storage_field_name = "race_tsd";
        struct RaceTsdStorage;

        struct RaceTsdNotifier final : Notifiable
        {
            GraphValue     *graph{nullptr};
            RaceTsdStorage *storage{nullptr};
            std::size_t     node_index{0};

            void notify(DateTime modified_time) override
            {
                if (graph == nullptr) { return; }
                const DateTime when = modified_time != MIN_DT
                                          ? std::max(modified_time, graph->view().evaluation_time())
                                          : graph->view().evaluation_time();
                graph->schedule_node(node_index, when);
            }

            void source_invalidated(const TSDataTracking *source) noexcept override;
        };

        struct RaceTsdEntry
        {
            DateTime                    first_valid{MAX_DT};
            TimeSeriesReference         source{};
            TimeSeriesReference         last_item{};   // identity: a NEW reference re-stamps first-valid
            std::vector<TSOutputHandle> subscribed{};
            bool                        seen{false};
        };

        struct ValueKeyHash
        {
            [[nodiscard]] std::size_t operator()(const Value &value) const { return value.view().hash(); }
        };

        struct ValueKeyEq
        {
            [[nodiscard]] bool operator()(const Value &lhs, const Value &rhs) const
            {
                return lhs.view().equals(rhs.view());
            }
        };

        /** One race per FIELD (hgraph's reduce_tsd_of_bundles_with_race);
            a non-bundle OUT is the single-field degenerate case. */
        struct RaceFieldState
        {
            ankerl::unordered_dense::map<Value, RaceTsdEntry, ValueKeyHash, ValueKeyEq> entries{};
            Value winner{};
        };

        struct RaceTsdStorage
        {
            struct Subscription
            {
                TSOutputHandle       handle{};
                const TSDataTracking *tracking{nullptr};
                std::size_t           references{0};
            };

            RaceTsdNotifier notifier{};
            std::vector<RaceFieldState> fields{};
            std::vector<Subscription>   subscriptions{};
            TimeSeriesReference         published{};

            void subscribe(RaceTsdEntry &entry, TSOutputHandle handle)
            {
                if (!handle.bound() || std::ranges::any_of(
                        entry.subscribed,
                        [&](const TSOutputHandle &existing) { return existing.same_as(handle); }))
                {
                    return;
                }

                const auto *tracking = &handle.data_view().tracking();
                auto subscription = std::ranges::find_if(
                    subscriptions,
                    [&](const Subscription &existing) { return existing.tracking == tracking; });
                if (subscription == subscriptions.end())
                {
                    handle.data_view().subscribe(&notifier);
                    subscriptions.push_back(Subscription{handle, tracking, 1});
                }
                else { ++subscription->references; }
                entry.subscribed.push_back(std::move(handle));
            }

            void unsubscribe(RaceTsdEntry &entry) noexcept
            {
                for (TSOutputHandle &handle : entry.subscribed)
                {
                    static_cast<void>(fallback_on_exception(false, [&] {
                        const auto *tracking = &handle.data_view().tracking();
                        const auto subscription = std::ranges::find_if(
                            subscriptions,
                            [&](const Subscription &existing) { return existing.tracking == tracking; });
                        if (subscription == subscriptions.end()) { return true; }
                        if (--subscription->references == 0)
                        {
                            subscription->handle.data_view().unsubscribe(&notifier);
                            subscriptions.erase(subscription);
                        }
                        return true;
                    }));
                }
                entry.subscribed.clear();
            }

            void source_invalidated(const TSDataTracking *source) noexcept
            {
                const auto subscription = std::ranges::find_if(
                    subscriptions,
                    [&](const Subscription &existing) { return existing.tracking == source; });
                if (subscription == subscriptions.end()) { return; }

                for (auto &field : fields)
                {
                    for (auto &[key, entry] : field.entries)
                    {
                        std::erase_if(entry.subscribed, [&](const TSOutputHandle &handle) {
                            return &handle.data_view().tracking() == source;
                        });
                    }
                }
                subscriptions.erase(subscription);
            }

            void unsubscribe_all() noexcept
            {
                for (auto &field : fields)
                {
                    for (auto &[key, entry] : field.entries) { unsubscribe(entry); }
                }
                subscriptions.clear();
            }

            ~RaceTsdStorage() noexcept { unsubscribe_all(); }
        };

        void RaceTsdNotifier::source_invalidated(const TSDataTracking *source) noexcept
        {
            if (storage != nullptr) { storage->source_invalidated(source); }
            if (graph == nullptr) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                graph->schedule_node(node_index, graph->view().evaluation_time());
                return true;
            }));
        }

        struct RaceTsdContext
        {
            std::size_t storage_offset{0};
            std::size_t field_count{1};
        };

        void subscribe_output_tree(RaceTsdStorage &storage, RaceTsdEntry &entry,
                                   TSOutputHandle handle)
        {
            if (!handle.bound() || std::ranges::any_of(
                    entry.subscribed,
                    [&](const TSOutputHandle &existing) { return existing.same_as(handle); }))
            {
                return;
            }

            storage.subscribe(entry, handle);
            auto view = handle.view(MIN_DT);
            if (view.forwarding())
            {
                if (view.forwarding_bound())
                {
                    subscribe_output_tree(storage, entry, view.forwarding_target());
                }
                return;
            }

            const auto *schema = handle.schema();
            const std::size_t child_count =
                schema != nullptr && schema->kind == TSTypeKind::TSB
                    ? schema->field_count()
                : schema != nullptr && schema->kind == TSTypeKind::TSL
                    ? schema->fixed_size()
                    : 0;
            for (std::size_t index = 0; index < child_count; ++index)
            {
                auto data = handle.data_view();
                TSDataView child;
                if (detail::has_input_children(data))
                {
                    auto projection = detail::input_child_projection(data, index);
                    child = projection.target_link.valid()
                                ? std::move(projection.target_link)
                                : std::move(projection.visible);
                }
                else if (index < data.indexed_child_count())
                {
                    child = data.indexed_child_at(index);
                }
                if (child.valid())
                {
                    subscribe_output_tree(storage, entry, TSOutputHandle{handle.output(), child});
                }
            }
        }

        /** Subscribe the notifier to every endpoint inside ``reference``. */
        void subscribe_reference_targets(RaceTsdStorage &storage, RaceTsdEntry &entry,
                                         const TimeSeriesReference &reference)
        {
            if (reference.is_empty()) { return; }
            if (reference.is_peered())
            {
                subscribe_output_tree(storage, entry, reference.target_output());
                return;
            }
            for (const TimeSeriesReference &item : reference.items())
            {
                subscribe_reference_targets(storage, entry, item);
            }
        }

        void publish_reference(const NodeView &view, DateTime evaluation_time, const TimeSeriesReference &reference)
        {
            Value value{reference};
            auto  output   = view.output(evaluation_time);
            auto  mutation = output.begin_mutation(evaluation_time);
            if (!mutation.move_value_from(std::move(value)))
            {
                throw std::logic_error("race_tsd failed to publish the winner reference");
            }
        }

        struct RaceTsdView
        {
            NodeView    view;
            void       *storage{nullptr};
            const void *context{nullptr};

            [[nodiscard]] static const void *node_view_type_id() noexcept
            {
                static const char token{};
                return &token;
            }

            [[nodiscard]] static RaceTsdView from_node(NodeView view, const void *context)
            {
                const auto &typed = *static_cast<const RaceTsdContext *>(context);
                void       *storage = MemoryUtils::advance(view.data(), typed.storage_offset);
                return RaceTsdView{std::move(view), storage, context};
            }
        };

        /** The per-key FIELD reference: peered TSB refs project the field
            child; non-peered refs index their items. Whole (non-bundle)
            mode passes the reference through. */
        [[nodiscard]] TimeSeriesReference field_item(const TimeSeriesReference &reference, std::size_t field,
                                                     std::size_t field_count)
        {
            if (field_count == 1 && (reference.target_schema() == nullptr ||
                                     reference.target_schema()->kind != TSTypeKind::TSB))
            {
                return reference;
            }
            if (reference.is_empty()) { return TimeSeriesReference{}; }
            if (reference.is_non_peered())
            {
                return field < reference.items().size() ? reference[field] : TimeSeriesReference{};
            }
            // Peered TSB reference: the field child of the target output.
            const TSOutputHandle &target = reference.target_output();
            auto data = target.data_view();
            TSDataView child_data;
            if (detail::has_input_children(data))
            {
                auto projection = detail::input_child_projection(data, field);
                child_data = projection.target_link.valid()
                                 ? std::move(projection.target_link)
                                 : std::move(projection.visible);
            }
            else
            {
                if (field >= data.indexed_child_count()) { return TimeSeriesReference{}; }
                child_data = data.indexed_child_at(field);
            }
            TSOutputHandle child{target.output(), child_data};
            return child.bound()
                       ? TimeSeriesReference{std::move(child)}
                       : TimeSeriesReference::empty(
                             data.schema()->fields()[field].type);
        }

        bool race_tsd_evaluate(const NodeView &view, DateTime evaluation_time, RaceTsdStorage &storage,
                               std::size_t field_count)
        {
            auto  root    = view.input(evaluation_time);
            auto  bundle  = root.as_bundle();
            auto  tsd_ts  = bundle[0];
            auto  tsd     = tsd_ts.as_dict();

            if (storage.fields.size() != field_count) { storage.fields.resize(field_count); }

            for (auto &field : storage.fields)
            {
                for (auto &[key, entry] : field.entries) { entry.seen = false; }
            }

            // Scan the live key set, per field (hgraph's per-field loop with
            // reference-identity re-stamping).
            for (auto &&[key, child] : tsd.items())
            {
                const bool has_reference = child.valid();
                const TimeSeriesReference reference =
                    has_reference ? child.value().checked_as<TimeSeriesReference>() : TimeSeriesReference{};

                for (std::size_t f = 0; f < field_count; ++f)
                {
                    auto &field = storage.fields[f];
                    auto &entry = field.entries[Value{key}];
                    entry.seen  = true;
                    entry.source = reference;

                    const TimeSeriesReference item = field_item(reference, f, field_count);
                    const bool item_valid = !item.is_empty() && item.is_valid(evaluation_time);
                    if (item_valid)
                    {
                        if (entry.first_valid == MAX_DT || !(entry.last_item == item))
                        {
                            entry.first_valid = evaluation_time;
                            entry.last_item   = item;
                        }
                        storage.unsubscribe(entry);
                    }
                    else
                    {
                        entry.first_valid = MAX_DT;
                        entry.last_item   = TimeSeriesReference{};
                        storage.unsubscribe(entry);
                    }
                }
            }

            for (auto &field : storage.fields)
            {
                for (auto it = field.entries.begin(); it != field.entries.end();)
                {
                    if (!it->second.seen)
                    {
                        storage.unsubscribe(it->second);
                        it = field.entries.erase(it);
                    }
                    else { ++it; }
                }
            }

            // Per-field winners: keep a still-valid winner; else the earliest
            // first-valid key (smallest key on ties). With no winner, PENDING
            // candidates (valid reference, invalid target) subscribe so a
            // target becoming valid wakes the node.
            bool any_field_unwon = false;
            for (auto &field : storage.fields)
            {
                const auto winner_it = field.winner.has_value() ? field.entries.find(field.winner)
                                                                : field.entries.end();
                const bool winner_ok = winner_it != field.entries.end() && winner_it->second.first_valid != MAX_DT;
                if (winner_ok) { continue; }

                const Value *best      = nullptr;
                DateTime     best_time = MAX_DT;
                for (const auto &[key, entry] : field.entries)
                {
                    if (entry.first_valid < best_time ||
                        (entry.first_valid == best_time && best != nullptr &&
                         key.view().compare(best->view()) == std::partial_ordering::less))
                    {
                        best_time = entry.first_valid;
                        best      = &key;
                    }
                }
                if (best != nullptr) { field.winner = Value{best->view()}; }
                else
                {
                    field.winner = Value{};
                    any_field_unwon = true;
                }
            }

            // The REF value is stable while its target can invalidate. Keep
            // the winning field observed so a silent target unbind schedules
            // the node to choose the next eligible key.
            for (auto &field : storage.fields)
            {
                if (!field.winner.has_value()) { continue; }
                auto winner = field.entries.find(field.winner);
                if (winner == field.entries.end() || !winner->second.subscribed.empty()) { continue; }
                subscribe_reference_targets(storage, winner->second, winner->second.source);
                subscribe_reference_targets(storage, winner->second, winner->second.last_item);
            }

            if (any_field_unwon)
            {
                const std::size_t f_count = storage.fields.size();
                for (auto &&[key, child] : tsd.items())
                {
                    if (!child.valid()) { continue; }
                    const TimeSeriesReference reference = child.value().checked_as<TimeSeriesReference>();
                    for (std::size_t f = 0; f < f_count; ++f)
                    {
                        auto &field = storage.fields[f];
                        if (field.winner.has_value()) { continue; }
                        const TimeSeriesReference item = field_item(reference, f, f_count);
                        if (item.is_empty() || item.is_valid(evaluation_time)) { continue; }
                        auto &entry = field.entries[Value{key}];
                        if (entry.subscribed.empty())
                        {
                            subscribe_reference_targets(storage, entry, reference);
                            subscribe_reference_targets(storage, entry, item);
                        }
                    }
                }
            }

            // Assemble + publish (same-reference dedup: an unchanged assembly
            // must not re-tick consumers - rebinds sample every field).
            TimeSeriesReference assembled{};
            if (field_count == 1)
            {
                auto &field = storage.fields[0];
                if (field.winner.has_value()) { assembled = field.entries[field.winner].last_item; }
            }
            else
            {
                std::vector<TimeSeriesReference> items;
                items.reserve(field_count);
                bool any = false;
                for (auto &field : storage.fields)
                {
                    if (field.winner.has_value())
                    {
                        items.push_back(field.entries[field.winner].last_item);
                        any = true;
                    }
                    else { items.push_back(TimeSeriesReference{}); }
                }
                if (any)
                {
                    const auto *out_schema = view.schema()->output_schema;
                    assembled = TimeSeriesReference::non_peered(
                        out_schema != nullptr ? out_schema->referenced_ts() : nullptr, std::move(items));
                }
            }

            if (!assembled.is_empty() && !(assembled == storage.published))
            {
                storage.published = assembled;
                publish_reference(view, evaluation_time, assembled);
            }
            return true;
        }

        // Contexts intern by (offset, field-count) - node builders are
        // long-lived registry artifacts (immortal).
        const RaceTsdContext &intern_race_context(std::size_t offset, std::size_t field_count)
        {
            static auto *contexts = new std::vector<RaceTsdContext *>{};
            for (const auto *context : *contexts)
            {
                if (context->storage_offset == offset && context->field_count == field_count) { return *context; }
            }
            auto *context = new RaceTsdContext{offset, field_count};
            contexts->push_back(context);
            return *context;
        }

        bool race_tsd_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            auto typed = view.as<RaceTsdView>();
            const auto &context = *static_cast<const RaceTsdContext *>(typed.context);
            return race_tsd_evaluate(view, evaluation_time, *MemoryUtils::cast<RaceTsdStorage>(typed.storage),
                                     context.field_count);
        }

        void race_tsd_start(const NodeView &view, DateTime)
        {
            auto typed = view.as<RaceTsdView>();
            auto &storage = *MemoryUtils::cast<RaceTsdStorage>(typed.storage);
            storage.notifier.graph      = view.graph_value();
            storage.notifier.storage    = &storage;
            storage.notifier.node_index = view.node_index();
        }

        void race_tsd_stop(const NodeView &view, DateTime)
        {
            auto typed = view.as<RaceTsdView>();
            MemoryUtils::cast<RaceTsdStorage>(typed.storage)->unsubscribe_all();
        }
    }  // namespace

    NodeBuilder make_race_tsd_node(const TSValueTypeMetaData &tsd_schema)
    {
        if (tsd_schema.kind != TSTypeKind::TSD || tsd_schema.element_ts() == nullptr)
        {
            throw std::invalid_argument("race_tsd requires a TSD input schema");
        }

        auto       &registry       = TypeRegistry::instance();
        const auto *element        = tsd_schema.element_ts();
        const auto *out_target     = registry.dereference(element);
        const auto *output_schema  = element->kind == TSTypeKind::REF
                                         ? element
                                         : registry.ref(out_target);
        const auto *race_tsd_schema = element->kind == TSTypeKind::REF
                                          ? &tsd_schema
                                          : registry.tsd(tsd_schema.key_type(), output_schema);
        const auto *input_schema = registry.un_named_tsb({{"tsd", race_tsd_schema}});

        NodeTypeMetaData schema;
        schema.display_name  = "reduce_tsd_with_race";
        schema.input_schema  = input_schema;
        schema.output_schema = output_schema;
        schema.node_kind     = NodeKind::Compute;
        schema.active_inputs = std::vector<std::size_t>{0};
        // NO validity gating: an empty/invalid dict still races (to none).
        schema.valid_inputs.emplace();

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(schema);

        const std::array fields{NodeStorageField{
            .name = race_tsd_storage_field_name,
            .plan = &MemoryUtils::plan_for<RaceTsdStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const std::size_t field_count =
            out_target != nullptr && out_target->kind == TSTypeKind::TSB ? out_target->field_count() : 1;
        const auto &context = intern_race_context(
            descriptor.storage_plan->component(race_tsd_storage_field_name).offset, field_count);

        descriptor.callbacks.start          = &race_tsd_start;
        descriptor.callbacks.stop           = &race_tsd_stop;
        descriptor.ops.evaluate_impl        = &race_tsd_evaluate_impl;
        descriptor.ops.extended_view_type_id = RaceTsdView::node_view_type_id();
        descriptor.ops.extended_view_context = &context;

        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
