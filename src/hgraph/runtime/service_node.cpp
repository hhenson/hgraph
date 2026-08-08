#include <hgraph/runtime/service_node.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/base_view.h>
#include <hgraph/types/time_series/ts_output/base_view.h>
#include <hgraph/types/time_series/ts_output/dict_view.h>
#include <hgraph/types/time_series/ts_output/set_view.h>

#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    Int next_request_id() noexcept
    {
        static std::atomic<Int> next{0};
        return next.fetch_add(1, std::memory_order_relaxed) + 1;
    }

    namespace
    {
        constexpr std::string_view subscription_key_source_storage_field{"subscription_key_source"};
        constexpr std::string_view subscription_key_capture_storage_field{"subscription_key_capture"};
        constexpr std::string_view subscription_response_gate_storage_field{"subscription_response_gate"};
        constexpr std::string_view request_input_source_storage_field{"request_input_source"};
        constexpr std::string_view request_input_capture_storage_field{"request_input_capture"};

        struct ValueKeyHash
        {
            using is_transparent = void;

            [[nodiscard]] std::size_t operator()(const Value &value) const
            {
                return value.has_value() ? value.hash() : 0;
            }
        };

        struct ValueKeyEqual
        {
            using is_transparent = void;

            [[nodiscard]] bool operator()(const Value &lhs, const Value &rhs) const
            {
                if (lhs.has_value() != rhs.has_value()) { return false; }
                return !lhs.has_value() || lhs.equals(rhs);
            }
        };

        struct SubscriptionKeyChange
        {
            Value    key{};
            DateTime observed_at{};
            bool     add{true};
        };

        struct SubscriptionKeySourceStorage
        {
            ankerl::unordered_dense::map<Value, std::size_t, ValueKeyHash, ValueKeyEqual> counts{};
            std::vector<SubscriptionKeyChange>                                            pending{};
        };

        struct SubscriptionKeySourceContext
        {
            std::string path{};
            std::size_t storage_offset{0};
        };

        struct SubscriptionKeyCaptureStorage
        {
            Value       previous_key{};
            NodePtr     source{};
            TSInputView input{};
            bool        has_previous{false};
        };

        struct SubscriptionKeyCaptureContext
        {
            std::string path{};
            std::size_t storage_offset{0};
            bool        same_cycle{true};
        };

        struct SubscriptionResponseGateContext
        {
            std::size_t storage_offset{0};
            bool        response_same_cycle{false};
            bool        forwards_structure{false};
        };

        struct SubscriptionResponseGateStorage
        {
            Value previous_key{};
            Value pending_response{};
            bool  has_previous{false};
            bool  awaiting_fresh_response{false};
            bool  awaiting_subscription{false};
        };

        struct RequestInputSourceContext
        {
            std::string path{};
            std::size_t storage_offset{0};
        };

        struct RequestInputChange
        {
            Int      request_id{0};
            Value    delta{};
            DateTime observed_at{};
            bool     remove{false};
        };

        struct RequestInputSourceStorage
        {
            std::vector<RequestInputChange> pending{};
            DateTime                        publish_time{MAX_DT};
        };

        struct RequestInputCaptureStorage
        {
            NodePtr     source{};
            TSInputView input{};
            Int         request_id{0};
            bool        live{false};
        };

        struct RequestInputCaptureContext
        {
            std::string path{};
            std::size_t storage_offset{0};
            bool        same_cycle{false};
        };

        [[nodiscard]] std::vector<std::unique_ptr<SubscriptionKeySourceContext>> &
        subscription_key_source_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<SubscriptionKeySourceContext>>;
            return *contexts;
        }

        [[nodiscard]] const SubscriptionKeySourceContext &register_subscription_key_source_context(
            std::string path,
            std::size_t storage_offset)
        {
            auto &contexts = subscription_key_source_contexts();
            const auto existing = std::ranges::find_if(
                contexts,
                [&](const auto &context) {
                    return context->path == path && context->storage_offset == storage_offset;
                });
            if (existing != contexts.end()) { return **existing; }
            auto context = std::make_unique<SubscriptionKeySourceContext>(SubscriptionKeySourceContext{
                .path           = std::move(path),
                .storage_offset = storage_offset,
            });
            const auto *result = context.get();
            contexts.push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] std::vector<std::unique_ptr<SubscriptionKeyCaptureContext>> &
        subscription_key_capture_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<SubscriptionKeyCaptureContext>>;
            return *contexts;
        }

        [[nodiscard]] const SubscriptionKeyCaptureContext &register_subscription_key_capture_context(
            std::string path,
            std::size_t storage_offset,
            bool same_cycle)
        {
            auto &contexts = subscription_key_capture_contexts();
            const auto existing = std::ranges::find_if(
                contexts,
                [&](const auto &context) {
                    return context->path == path && context->storage_offset == storage_offset
                        && context->same_cycle == same_cycle;
                });
            if (existing != contexts.end()) { return **existing; }
            auto context = std::make_unique<SubscriptionKeyCaptureContext>(SubscriptionKeyCaptureContext{
                .path           = std::move(path),
                .storage_offset = storage_offset,
                .same_cycle     = same_cycle,
            });
            const auto *result = context.get();
            contexts.push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] std::vector<std::unique_ptr<SubscriptionResponseGateContext>> &
        subscription_response_gate_contexts() noexcept
        {
            static auto *contexts =
                new std::vector<std::unique_ptr<SubscriptionResponseGateContext>>;
            return *contexts;
        }

        [[nodiscard]] const SubscriptionResponseGateContext &
        register_subscription_response_gate_context(
            std::size_t storage_offset,
            bool response_same_cycle,
            bool forwards_structure)
        {
            auto &contexts = subscription_response_gate_contexts();
            const auto existing = std::ranges::find_if(
                contexts,
                [&](const auto &context) {
                    return context->storage_offset == storage_offset
                        && context->response_same_cycle == response_same_cycle
                        && context->forwards_structure == forwards_structure;
                });
            if (existing != contexts.end()) { return **existing; }
            auto context = std::make_unique<SubscriptionResponseGateContext>(
                SubscriptionResponseGateContext{
                    .storage_offset = storage_offset,
                    .response_same_cycle = response_same_cycle,
                    .forwards_structure = forwards_structure,
                });
            const auto *result = context.get();
            contexts.push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] SubscriptionKeySourceStorage &source_storage_of(
            const NodeView &view,
            const SubscriptionKeySourceContext &context)
        {
            return *MemoryUtils::cast<SubscriptionKeySourceStorage>(
                MemoryUtils::advance(view.data(), context.storage_offset));
        }

        [[nodiscard]] SubscriptionKeyCaptureStorage &capture_storage_of(
            const NodeView &view,
            const SubscriptionKeyCaptureContext &context)
        {
            return *MemoryUtils::cast<SubscriptionKeyCaptureStorage>(
                MemoryUtils::advance(view.data(), context.storage_offset));
        }

        [[nodiscard]] SubscriptionResponseGateStorage &response_gate_storage_of(
            const NodeView &view,
            std::size_t storage_offset)
        {
            return *MemoryUtils::cast<SubscriptionResponseGateStorage>(
                MemoryUtils::advance(view.data(), storage_offset));
        }

        [[nodiscard]] std::vector<std::unique_ptr<RequestInputSourceContext>> &
        request_input_source_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<RequestInputSourceContext>>;
            return *contexts;
        }

        [[nodiscard]] const RequestInputSourceContext &register_request_input_source_context(
            std::string path,
            std::size_t storage_offset)
        {
            auto &contexts = request_input_source_contexts();
            const auto existing = std::ranges::find_if(
                contexts,
                [&](const auto &context) {
                    return context->path == path && context->storage_offset == storage_offset;
                });
            if (existing != contexts.end()) { return **existing; }
            auto context = std::make_unique<RequestInputSourceContext>(RequestInputSourceContext{
                .path           = std::move(path),
                .storage_offset = storage_offset,
            });
            const auto *result = context.get();
            contexts.push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] RequestInputSourceStorage &source_storage_of(
            const NodeView &view,
            const RequestInputSourceContext &context)
        {
            return *MemoryUtils::cast<RequestInputSourceStorage>(
                MemoryUtils::advance(view.data(), context.storage_offset));
        }

        [[nodiscard]] std::vector<std::unique_ptr<RequestInputCaptureContext>> &
        request_input_capture_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<RequestInputCaptureContext>>;
            return *contexts;
        }

        [[nodiscard]] const RequestInputCaptureContext &register_request_input_capture_context(
            std::string path,
            std::size_t storage_offset,
            bool same_cycle)
        {
            auto &contexts = request_input_capture_contexts();
            const auto existing = std::ranges::find_if(
                contexts,
                [&](const auto &context) {
                    return context->path == path && context->storage_offset == storage_offset
                        && context->same_cycle == same_cycle;
                });
            if (existing != contexts.end()) { return **existing; }
            auto context = std::make_unique<RequestInputCaptureContext>(RequestInputCaptureContext{
                .path           = std::move(path),
                .storage_offset = storage_offset,
                .same_cycle     = same_cycle,
            });
            const auto *result = context.get();
            contexts.push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] RequestInputCaptureStorage &capture_storage_of(
            const NodeView &view,
            const RequestInputCaptureContext &context)
        {
            return *MemoryUtils::cast<RequestInputCaptureStorage>(
                MemoryUtils::advance(view.data(), context.storage_offset));
        }

        class SubscriptionKeySourceView
        {
          public:
            [[nodiscard]] static const void *node_view_type_id() noexcept
            {
                static const char token{};
                return &token;
            }

            [[nodiscard]] static SubscriptionKeySourceView from_node(NodeView view, const void *context)
            {
                if (context == nullptr)
                {
                    throw std::logic_error("SubscriptionKeySourceView requires a typed view context");
                }
                return SubscriptionKeySourceView{
                    std::move(view),
                    static_cast<const SubscriptionKeySourceContext *>(context),
                };
            }

            void enqueue(Value key, bool add, DateTime schedule_time, DateTime observed_at) const
            {
                if (!key.has_value()) { return; }
                auto &pending = source_storage_of(view_, *context_).pending;
                pending.push_back(SubscriptionKeyChange{
                    .key         = std::move(key),
                    .observed_at = observed_at,
                    .add         = add,
                });

                GraphValue *graph = view_.graph_value();
                if (graph == nullptr)
                {
                    throw std::logic_error("subscription key source node is not attached to a graph");
                }
                graph->schedule_node(view_.node_index(), schedule_time);
            }

            [[nodiscard]] std::size_t node_index() const noexcept { return view_.node_index(); }

          private:
            SubscriptionKeySourceView(NodeView view, const SubscriptionKeySourceContext *context) noexcept
                : view_(std::move(view)),
                  context_(context)
            {
            }

            NodeView                            view_{};
            const SubscriptionKeySourceContext *context_{nullptr};
        };

        class RequestInputSourceView
        {
          public:
            [[nodiscard]] static const void *node_view_type_id() noexcept
            {
                static const char token{};
                return &token;
            }

            [[nodiscard]] static RequestInputSourceView from_node(NodeView view, const void *context)
            {
                if (context == nullptr)
                {
                    throw std::logic_error("RequestInputSourceView requires a typed view context");
                }
                return RequestInputSourceView{
                    std::move(view),
                    static_cast<const RequestInputSourceContext *>(context),
                };
            }

            void set(Int request_id, Value delta, DateTime schedule_time,
                     DateTime observed_at) const
            {
                auto &pending = source_storage_of(view_, *context_).pending;
                auto existing = std::ranges::find_if(
                    pending, [&](const RequestInputChange &change) {
                        return change.request_id == request_id
                            && change.observed_at == observed_at;
                    });
                if (existing != pending.end())
                {
                    existing->delta  = std::move(delta);
                    existing->remove = false;
                    schedule_publication(schedule_time);
                    return;
                }
                pending.push_back(RequestInputChange{
                    .request_id  = request_id,
                    .delta       = std::move(delta),
                    .observed_at = observed_at,
                    .remove      = false,
                });
                schedule_publication(schedule_time);
            }

            void remove(Int request_id, DateTime schedule_time,
                        DateTime observed_at) const
            {
                auto &pending = source_storage_of(view_, *context_).pending;
                auto existing = std::ranges::find_if(
                    pending, [&](const RequestInputChange &change) {
                        return change.request_id == request_id
                            && change.observed_at == observed_at;
                    });
                if (existing != pending.end())
                {
                    existing->delta  = Value{};
                    existing->remove = true;
                    schedule_publication(schedule_time);
                    return;
                }
                pending.push_back(RequestInputChange{
                    .request_id  = request_id,
                    .delta       = Value{},
                    .observed_at = observed_at,
                    .remove      = true,
                });
                schedule_publication(schedule_time);
            }

            void schedule_publication(DateTime schedule_time) const
            {
                auto &storage = source_storage_of(view_, *context_);
                if (schedule_time >= storage.publish_time) { return; }

                GraphValue *graph = view_.graph_value();
                if (graph == nullptr)
                {
                    throw std::logic_error("request input source node is not attached to a graph");
                }
                graph->schedule_node(view_.node_index(), schedule_time);
                storage.publish_time = schedule_time;
            }

            [[nodiscard]] std::size_t node_index() const noexcept { return view_.node_index(); }

          private:
            RequestInputSourceView(NodeView view, const RequestInputSourceContext *context) noexcept
                : view_(std::move(view)),
                  context_(context)
            {
            }

            NodeView                         view_{};
            const RequestInputSourceContext *context_{nullptr};
        };

        void initialize_subscription_capture(
            const NodeView &capture,
            DateTime evaluation_time,
            SubscriptionKeyCaptureStorage &storage)
        {
            if (storage.source && storage.input.schema() != nullptr) { return; }

            auto input         = capture.input(evaluation_time);
            auto bundle        = input.as_bundle();
            // Retained view: carry the prepared route so per-tick reads
            // resolve through the trie handle (issue #203).
            storage.input      = input.routed_child_at(0);
            auto subscriptions = bundle.at(1);
            if (!subscriptions.bound())
            {
                throw std::logic_error("subscription key capture requires a bound subscriptions source");
            }

            NodeView source = subscriptions.bound_output().owner_node();
            if (!source.valid() || !source.is<SubscriptionKeySourceView>())
            {
                throw std::logic_error("subscription key capture is not bound to a subscription key source");
            }
            storage.source = source.pointer();
        }

        void initialize_request_capture(
            const NodeView &capture,
            DateTime evaluation_time,
            RequestInputCaptureStorage &storage)
        {
            if (storage.source && storage.input.schema() != nullptr) { return; }

            auto input    = capture.input(evaluation_time);
            auto bundle   = input.as_bundle();
            storage.input = input.routed_child_at(0);
            auto requests = bundle.at(1);
            auto request_id = bundle.at(2);
            if (!requests.bound())
            {
                throw std::logic_error("request input capture requires a bound requests source");
            }
            if (!request_id.valid())
            {
                throw std::logic_error("request input capture requires a valid runtime request id");
            }

            NodeView source = requests.bound_output().owner_node();
            if (!source.valid() || !source.is<RequestInputSourceView>())
            {
                throw std::logic_error("request input capture is not bound to a request input source");
            }
            storage.source = source.pointer();
            storage.request_id = request_id.value().checked_as<Int>();
        }

        /**
         * Request/reply inputs publish on the next cycle and intentionally omit
         * service ranking because that temporal break permits recursive graphs.
         * Root capture during ``start`` can schedule for the current engine
         * time because evaluation has not begun. A dynamically started nested
         * capture schedules the next cycle because the outer source rank may
         * already have passed. Subscription and service-adaptor sends are the
         * rank-correct same-cycle cases and do not use this helper.
         */
        [[nodiscard]] DateTime request_stub_forward_time(
            const NodeView &view,
            DateTime evaluation_time,
            bool start_phase)
        {
            // A nested graph can start while its parent is already being
            // evaluated. The outer transport source may therefore have passed
            // its rank for this engine time; schedule the hand-off for the
            // next cycle. Root graph start runs before evaluation begins and
            // can still publish at the current engine time.
            return start_phase && !view.graph().is_nested()
                       ? evaluation_time
                       : evaluation_time + MIN_TD;
        }

        void apply_pending_subscription_key_changes(
            SubscriptionKeySourceStorage &storage,
            TSSDataMutationView &mutation)
        {
            const DateTime publication = storage.pending.front().observed_at;
            std::vector<SubscriptionKeyChange> deferred;
            for (SubscriptionKeyChange &change : storage.pending)
            {
                if (change.observed_at != publication)
                {
                    deferred.push_back(std::move(change));
                    continue;
                }
                if (!change.key.has_value()) { continue; }

                if (change.add)
                {
                    auto [it, inserted] = storage.counts.emplace(change.key, 0U);
                    if (inserted) { static_cast<void>(mutation.add(it->first.view())); }
                    ++it->second;
                    continue;
                }

                auto it = storage.counts.find(change.key);
                if (it == storage.counts.end()) { continue; }
                if (it->second > 1U)
                {
                    --it->second;
                    continue;
                }

                Value removed_key{it->first};
                storage.counts.erase(it);
                static_cast<void>(mutation.remove(removed_key.view()));
            }
            storage.pending = std::move(deferred);
        }

        void apply_pending_request_input_changes(
            RequestInputSourceStorage &storage,
            const TSOutputView &output,
            DateTime evaluation_time)
        {
            auto dict     = output.as_dict();
            auto mutation = dict.begin_mutation(evaluation_time);
            std::vector<Int> applied_request_ids{};
            std::vector<RequestInputChange> deferred{};
            for (RequestInputChange &change : storage.pending)
            {
                if (std::ranges::find(applied_request_ids, change.request_id)
                    != applied_request_ids.end())
                {
                    deferred.push_back(std::move(change));
                    continue;
                }
                applied_request_ids.push_back(change.request_id);

                Value request_id{change.request_id};
                if (change.remove)
                {
                    static_cast<void>(mutation.erase(request_id.view()));
                    continue;
                }

                auto child = mutation.at(request_id.view());
                apply_delta(
                    TSOutputView{output.output(), child, evaluation_time},
                    change.delta.view());
            }
            mutation.touch();
            storage.pending = std::move(deferred);
        }

        bool subscription_key_source_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            const auto &typed_context = *static_cast<const SubscriptionKeySourceContext *>(
                view.type().ops_ref().extended_view_context);
            auto       &storage       = source_storage_of(view, typed_context);
            if (storage.pending.empty()) { return true; }

            auto output   = view.output(evaluation_time);
            auto set      = output.as_set();
            auto mutation = set.begin_mutation(evaluation_time);
            apply_pending_subscription_key_changes(storage, mutation);
            if (!storage.pending.empty())
            {
                view.graph().schedule_node(view.node_index(), evaluation_time + MIN_TD);
            }
            return true;
        }

        bool request_input_source_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            const auto &context = *static_cast<const RequestInputSourceContext *>(
                view.type().ops_ref().extended_view_context);
            auto &storage = source_storage_of(view, context);
            storage.publish_time = MAX_DT;
            if (storage.pending.empty()) { return true; }

            apply_pending_request_input_changes(storage, view.output(evaluation_time), evaluation_time);
            if (!storage.pending.empty())
            {
                RequestInputSourceView::from_node(NodeView{view.pointer()}, &context).schedule_publication(
                    evaluation_time + MIN_TD);
            }
            return true;
        }

        void subscription_key_source_stop(const NodeView &view, DateTime evaluation_time)
        {
            const auto *context = static_cast<const SubscriptionKeySourceContext *>(
                view.type().ops_ref().extended_view_context);
            auto &storage = source_storage_of(view, *context);

            storage.counts.clear();
            storage.pending.clear();

            auto output   = view.output(evaluation_time);
            auto set      = output.as_set();
            auto mutation = set.begin_mutation(evaluation_time);
            mutation.clear();
        }

        void request_input_source_stop(const NodeView &view, DateTime evaluation_time)
        {
            const auto *context = static_cast<const RequestInputSourceContext *>(
                view.type().ops_ref().extended_view_context);
            auto &storage = source_storage_of(view, *context);
            storage.pending.clear();
            storage.publish_time = MAX_DT;

            auto output   = view.output(evaluation_time);
            auto dict     = output.as_dict();
            auto mutation = dict.begin_mutation(evaluation_time);
            mutation.clear();
        }

        void record_subscription_key(
            const SubscriptionKeySourceView &source,
            SubscriptionKeyCaptureStorage &storage,
            const TSInputView &key_input,
            DateTime schedule_time,
            DateTime observed_at)
        {
            if (!key_input.valid())
            {
                if (storage.has_previous)
                {
                    source.enqueue(std::move(storage.previous_key), false, schedule_time, observed_at);
                }
                storage.previous_key = Value{};
                storage.has_previous = false;
                return;
            }

            Value key{key_input.value()};
            if (storage.has_previous && storage.previous_key.equals(key)) { return; }

            if (storage.has_previous)
            {
                source.enqueue(std::move(storage.previous_key), false, schedule_time, observed_at);
            }
            source.enqueue(key, true, schedule_time, observed_at);
            storage.previous_key = std::move(key);
            storage.has_previous = true;
        }

        void capture_subscription_key(
            const SubscriptionKeyCaptureContext &context,
            const NodeView &view,
            DateTime evaluation_time,
            bool start_phase)
        {
            auto &storage = capture_storage_of(view, context);
            initialize_subscription_capture(view, evaluation_time, storage);
            auto key    = storage.input.borrowed_ref(evaluation_time);
            auto source = NodeView{storage.source}.as<SubscriptionKeySourceView>();
            static_cast<void>(start_phase);
            const DateTime schedule_time = context.same_cycle
                                               ? (view.graph().is_nested()
                                                      ? evaluation_time + MIN_TD
                                                      : evaluation_time)
                                               : request_stub_forward_time(
                                                     view, evaluation_time, start_phase);
            record_subscription_key(source, storage, key, schedule_time, evaluation_time);
        }

        [[nodiscard]] bool response_key_changed(
            SubscriptionResponseGateStorage &storage,
            const TSInputView &key)
        {
            if (!key.valid())
            {
                const bool changed = storage.has_previous;
                storage.previous_key = Value{};
                storage.has_previous = false;
                return changed;
            }

            Value current{key.value()};
            if (storage.has_previous && storage.previous_key.equals(current)) { return false; }
            storage.previous_key = std::move(current);
            storage.has_previous = true;
            return true;
        }

        void evaluate_subscription_response_gate(
            const SubscriptionResponseGateContext &context,
            const NodeView &view,
            DateTime evaluation_time)
        {
            auto input_root = view.input(evaluation_time);
            auto input = input_root.as_bundle();
            auto value = input.at(0);
            auto key   = input.at(1);
            auto subscriptions_input = input.at(2);
            auto subscriptions = subscriptions_input.as_set();
            auto &storage = response_gate_storage_of(view, context.storage_offset);
            std::optional<NodeScheduler> scheduler;
            if (!context.response_same_cycle)
            {
                scheduler.emplace(
                    view.scheduler_state(), view.graph_value(), view.node_index(), evaluation_time);
            }
            auto output = view.output(evaluation_time);

            const auto clear_response = [&]() {
                if (context.forwards_structure)
                {
                    static_cast<void>(clear_forwarding_output_tree(
                        output.borrowed_ref(), true));
                }
                else
                {
                    auto mutation = output.begin_mutation(evaluation_time);
                    static_cast<void>(mutation.invalidate());
                }
            };

            const auto publish_response_source = [&]() {
                if (!context.forwards_structure)
                {
                    if (!value.valid())
                    {
                        clear_response();
                    }
                    else if (!output.valid())
                    {
                        apply_current_value(output, value.value());
                    }
                    else if (value.modified())
                    {
                        apply_delta(output, capture_delta(value).view());
                    }
                    return;
                }
                auto source = value.bound_output();
                if (!source.bound())
                {
                    clear_response();
                    return;
                }
                static_cast<void>(bind_forwarding_output_tree_to_source(
                    output.borrowed_ref(), source, true));
            };

            const auto key_was_added = [&]() {
                if (!key.valid()) { return false; }
                return std::ranges::any_of(
                    subscriptions.added(),
                    [&](const ValueView &added) { return added.equals(key.value()); });
            };

            const auto key_is_live = [&]() {
                return key.valid() && subscriptions.contains(key.value());
            };

            const auto stage_fresh_response = [&]() {
                if (!value.valid() || !value.modified()) { return; }
                if (context.response_same_cycle)
                {
                    if (context.forwards_structure)
                    {
                        publish_response_source();
                    }
                    else
                    {
                        apply_current_value(output, value.value());
                    }
                    storage.pending_response = Value{};
                    storage.awaiting_fresh_response = false;
                    return;
                }
                storage.pending_response = Value{value.value()};
                scheduler->schedule(MIN_TD);
            };

            if (response_key_changed(storage, key))
            {
                storage.pending_response = Value{};
                storage.awaiting_subscription = false;
                storage.awaiting_fresh_response = false;
                clear_response();
                if (!key.valid()) { return; }
                if (!key_is_live())
                {
                    storage.awaiting_subscription = true;
                    return;
                }
                storage.awaiting_fresh_response = key_was_added();
                if (storage.awaiting_fresh_response)
                {
                    stage_fresh_response();
                    return;
                }
            }

            if (storage.awaiting_subscription)
            {
                if (!key_is_live()) { return; }
                storage.awaiting_subscription = false;
                storage.awaiting_fresh_response = key_was_added();
                if (storage.awaiting_fresh_response)
                {
                    stage_fresh_response();
                    return;
                }
            }

            if (storage.awaiting_fresh_response)
            {
                if (!context.response_same_cycle && scheduler->is_scheduled_now()
                    && storage.pending_response.has_value())
                {
                    if (!value.valid())
                    {
                        storage.pending_response = Value{};
                        return;
                    }
                    // A newer first-generation value in the release cycle wins;
                    // Python exposes the child's current value at this boundary.
                    if (value.modified())
                    {
                        storage.pending_response = Value{value.value()};
                    }
                    if (context.forwards_structure)
                    {
                        publish_response_source();
                    }
                    else
                    {
                        apply_current_value(output, storage.pending_response.view());
                    }
                    storage.pending_response = Value{};
                    storage.awaiting_fresh_response = false;
                    return;
                }
                stage_fresh_response();
                return;
            }

            // Once a response has been admitted, retain its endpoint identity
            // through ordinary value changes and transient invalidity. A
            // source replacement rebinds the forwarding tree; an unbound
            // source clears it. This preserves hgraph's REF topology while
            // the target itself supplies value/delta propagation.
            publish_response_source();
        }

        bool subscription_key_capture_evaluate_impl(
            const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }
            const auto *context = static_cast<const SubscriptionKeyCaptureContext *>(
                view.type().ops_ref().extended_view_context);
            capture_subscription_key(*context, view, evaluation_time, false);
            return true;
        }

        void capture_request_input(
            const RequestInputCaptureContext &context,
            const NodeView &view,
            DateTime evaluation_time,
            bool start_phase)
        {
            auto &storage = capture_storage_of(view, context);
            initialize_request_capture(view, evaluation_time, storage);
            auto request = storage.input.borrowed_ref(evaluation_time);
            auto source  = NodeView{storage.source}.as<RequestInputSourceView>();
            const DateTime schedule_time = context.same_cycle
                                               ? (view.graph().is_nested()
                                                      ? evaluation_time + MIN_TD
                                                      : evaluation_time)
                                               : request_stub_forward_time(view, evaluation_time, start_phase);

            if (request.valid())
            {
                const bool first_value = !storage.live;
                source.set(
                    storage.request_id,
                    first_value ? capture_current_delta(request) : capture_delta(request),
                    schedule_time,
                    evaluation_time);
                storage.live = true;
                return;
            }

            if (storage.live)
            {
                source.remove(storage.request_id, schedule_time, evaluation_time);
                storage.live = false;
            }
        }

        bool request_input_capture_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }
            const auto *context = static_cast<const RequestInputCaptureContext *>(
                view.type().ops_ref().extended_view_context);
            capture_request_input(*context, view, evaluation_time, false);
            return true;
        }

        void subscription_key_capture_stop(const NodeView &view, DateTime evaluation_time)
        {
            const auto *context = static_cast<const SubscriptionKeyCaptureContext *>(
                view.type().ops_ref().extended_view_context);
            auto &storage = capture_storage_of(view, *context);
            if (!storage.has_previous)
            {
                storage.source = NodePtr{};
                storage.input  = TSInputView{};
                return;
            }

            initialize_subscription_capture(view, evaluation_time, storage);
            auto source = NodeView{storage.source}.as<SubscriptionKeySourceView>();
            source.enqueue(
                std::move(storage.previous_key), false,
                context->same_cycle && !view.graph().is_nested()
                    ? evaluation_time
                    : evaluation_time + MIN_TD,
                evaluation_time);
            storage.previous_key = Value{};
            storage.has_previous = false;
            storage.source       = NodePtr{};
            storage.input        = TSInputView{};
        }

        void request_input_capture_stop(const NodeView &view, DateTime evaluation_time)
        {
            const auto *context = static_cast<const RequestInputCaptureContext *>(
                view.type().ops_ref().extended_view_context);
            auto &storage = capture_storage_of(view, *context);
            if (!storage.live)
            {
                storage.source = NodePtr{};
                storage.input  = TSInputView{};
                return;
            }

            initialize_request_capture(view, evaluation_time, storage);
            auto source = NodeView{storage.source}.as<RequestInputSourceView>();
            source.remove(
                storage.request_id,
                context->same_cycle && !view.graph().is_nested()
                    ? evaluation_time
                    : evaluation_time + MIN_TD,
                evaluation_time);
            storage.live   = false;
            storage.source = NodePtr{};
            storage.input  = TSInputView{};
        }

        [[nodiscard]] std::string subscription_key_path(std::string path)
        {
            if (path.empty()) { throw std::invalid_argument("subscription key path must not be empty"); }
            return path;
        }

        [[nodiscard]] std::string request_input_path(std::string path)
        {
            if (path.empty()) { throw std::invalid_argument("request input path must not be empty"); }
            return path;
        }
    }  // namespace

    NodeBuilder make_request_id_source_node()
    {
        NodeTypeMetaData schema;
        schema.display_name  = "request_id_source";
        schema.output_schema = TypeRegistry::instance().ts(scalar_descriptor<Int>::value_meta());
        schema.node_kind     = NodeKind::PullSource;

        NodeCallbacks callbacks;
        callbacks.start = [](const NodeView &view, DateTime evaluation_time) {
            const Int request_id = next_request_id();
            auto mutation = view.output(evaluation_time).begin_mutation(evaluation_time);
            if (!mutation.move_value_from(Value{request_id}))
            {
                throw std::logic_error("request id source failed to publish its id");
            }
        };
        static const std::byte runtime_type_id{};
        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(schema);
        descriptor.callbacks = std::move(callbacks);
        NodeBuilder builder = NodeBuilder::from_canonical_descriptor(
            std::move(descriptor), &runtime_type_id);
        builder.label("request_id_source");
        return builder;
    }

    NodeBuilder make_subscription_key_source_node(std::string path, const ValueTypeMetaData &key_schema)
    {
        path = subscription_key_path(std::move(path));

        auto       &registry      = TypeRegistry::instance();
        const auto *output_schema = registry.tss(&key_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema.display_name  = "subscription_key_source";
        descriptor.schema.output_schema = output_schema;
        descriptor.schema.node_kind     = NodeKind::PullSource;

        const std::array fields{NodeStorageField{
            .name = subscription_key_source_storage_field,
            .plan = &MemoryUtils::plan_for<SubscriptionKeySourceStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const auto *context = &register_subscription_key_source_context(
            path, descriptor.storage_plan->component(subscription_key_source_storage_field).offset);

        descriptor.callbacks.stop            = &subscription_key_source_stop;
        descriptor.ops.evaluate_impl         = &subscription_key_source_evaluate_impl;
        descriptor.ops.extended_view_type_id = SubscriptionKeySourceView::node_view_type_id();
        descriptor.ops.extended_view_context = context;

        NodeBuilder builder = NodeBuilder::from_canonical_descriptor(
            std::move(descriptor), context);
        builder.label(std::string{"subscription_key_source:"} + context->path);
        return builder;
    }

    NodeBuilder make_subscription_key_capture_node(
        std::string path, const ValueTypeMetaData &key_schema, bool same_cycle)
    {
        path = subscription_key_path(std::move(path));

        auto       &registry            = TypeRegistry::instance();
        const auto *key_ts_schema       = registry.ts(&key_schema);
        const auto *subscription_schema = registry.tss(&key_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema.display_name = "subscription_key_capture";
        descriptor.schema.input_schema = registry.un_named_tsb({
            {"key", key_ts_schema},
            {"subscriptions", subscription_schema},
        });
        descriptor.schema.node_kind     = NodeKind::Sink;
        descriptor.schema.active_inputs = std::vector<std::size_t>{0};
        descriptor.schema.valid_inputs  = std::vector<std::size_t>{};

        const std::array fields{NodeStorageField{
            .name = subscription_key_capture_storage_field,
            .plan = &MemoryUtils::plan_for<SubscriptionKeyCaptureStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const auto *context = &register_subscription_key_capture_context(
            path, descriptor.storage_plan->component(subscription_key_capture_storage_field).offset,
            same_cycle);

        descriptor.callbacks.start = [context](const NodeView &view, DateTime evaluation_time) {
            capture_subscription_key(*context, view, evaluation_time, true);
        };
        descriptor.callbacks.stop            = &subscription_key_capture_stop;
        descriptor.ops.evaluate_impl         = &subscription_key_capture_evaluate_impl;
        descriptor.ops.extended_view_context = context;

        NodeBuilder builder = NodeBuilder::from_canonical_descriptor(
            std::move(descriptor), context);
        builder.label(std::string{"subscription_key_capture:"} + context->path);
        return builder;
    }

    NodeBuilder make_subscription_response_gate_node(
        const ValueTypeMetaData &key_schema,
        const TSValueTypeMetaData &response_schema,
        bool response_same_cycle)
    {
        auto &registry = TypeRegistry::instance();
        const auto *key_ts_schema = registry.ts(&key_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema.display_name = "subscription_response_gate";
        descriptor.schema.input_schema = registry.un_named_tsb({
            {"value", &response_schema},
            {"key", key_ts_schema},
            {"subscriptions", registry.tss(&key_schema)},
        });
        descriptor.schema.output_schema = &response_schema;
        descriptor.schema.node_kind = NodeKind::Compute;
        descriptor.schema.uses_scheduler = !response_same_cycle;
        descriptor.schema.active_inputs = std::vector<std::size_t>{0, 1, 2};
        descriptor.schema.valid_inputs = std::vector<std::size_t>{};

        const std::array fields{NodeStorageField{
            .name = subscription_response_gate_storage_field,
            .plan = &MemoryUtils::plan_for<SubscriptionResponseGateStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);
        const auto *context = &register_subscription_response_gate_context(
            descriptor.storage_plan->component(subscription_response_gate_storage_field).offset,
            response_same_cycle,
            requires_structural_forwarding(response_schema));

        descriptor.callbacks.evaluate = [context](
            const NodeView &view, DateTime evaluation_time) {
            evaluate_subscription_response_gate(*context, view, evaluation_time);
        };

        NodeBuilder builder = NodeBuilder::from_canonical_descriptor(
            std::move(descriptor), context);
        if (context->forwards_structure)
        {
            builder.output_endpoint(forwarding_output_endpoint_schema(&response_schema));
        }
        builder.label("subscription_response_gate");
        return builder;
    }

    NodeBuilder make_request_input_source_node(std::string path, const TSValueTypeMetaData &request_schema)
    {
        path = request_input_path(std::move(path));

        auto       &registry      = TypeRegistry::instance();
        const auto *request_id    = registry.register_scalar<Int>("int");
        const auto *output_schema = registry.tsd(request_id, &request_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema.display_name  = "request_input_source";
        descriptor.schema.output_schema = output_schema;
        descriptor.schema.node_kind     = NodeKind::PullSource;

        const std::array fields{NodeStorageField{
            .name = request_input_source_storage_field,
            .plan = &MemoryUtils::plan_for<RequestInputSourceStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const auto *context = &register_request_input_source_context(
            path, descriptor.storage_plan->component(request_input_source_storage_field).offset);

        descriptor.callbacks.stop            = &request_input_source_stop;
        descriptor.ops.evaluate_impl         = &request_input_source_evaluate_impl;
        descriptor.ops.extended_view_type_id = RequestInputSourceView::node_view_type_id();
        descriptor.ops.extended_view_context = context;

        NodeBuilder builder = NodeBuilder::from_canonical_descriptor(
            std::move(descriptor), context);
        builder.label(std::string{"request_input_source:"} + context->path);
        return builder;
    }

    NodeBuilder make_request_input_capture_node(
        std::string path,
        const TSValueTypeMetaData &request_schema,
        bool same_cycle)
    {
        path = request_input_path(std::move(path));

        auto       &registry        = TypeRegistry::instance();
        const auto *request_id_meta = registry.register_scalar<Int>("int");
        const auto *requests_schema = registry.tsd(request_id_meta, &request_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema.display_name = "request_input_capture";
        descriptor.schema.input_schema = registry.un_named_tsb({
            {"request", &request_schema},
            {"requests", requests_schema},
            {"request_id", registry.ts(request_id_meta)},
        });
        descriptor.schema.node_kind     = NodeKind::Sink;
        descriptor.schema.active_inputs = std::vector<std::size_t>{0, 2};
        descriptor.schema.valid_inputs  = std::vector<std::size_t>{};

        const std::array fields{NodeStorageField{
            .name = request_input_capture_storage_field,
            .plan = &MemoryUtils::plan_for<RequestInputCaptureStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const auto *context = &register_request_input_capture_context(
            path,
            descriptor.storage_plan->component(request_input_capture_storage_field).offset,
            same_cycle);

        descriptor.callbacks.start = [context](const NodeView &view, DateTime evaluation_time) {
            // A request_id operator publishes during evaluation rather than
            // node startup. Its input is active, so defer initial capture until
            // that first tick instead of requiring a startup-valid ID.
            auto input = view.input(evaluation_time);
            auto bundle = input.as_bundle();
            if (bundle.at(2).valid())
            {
                capture_request_input(*context, view, evaluation_time, true);
            }
        };
        descriptor.callbacks.stop            = &request_input_capture_stop;
        descriptor.ops.evaluate_impl         = &request_input_capture_evaluate_impl;
        descriptor.ops.extended_view_context = context;

        NodeBuilder builder = NodeBuilder::from_canonical_descriptor(
            std::move(descriptor), context);
        builder.label(std::string{"request_input_capture:"} + context->path);
        return builder;
    }
}  // namespace hgraph
