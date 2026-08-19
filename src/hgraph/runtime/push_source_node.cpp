#include <hgraph/runtime/push_source_node.h>

#include <hgraph/runtime/executor.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/util/scope.h>

#include <array>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace detail
    {
        const DateTime push_conflation_time{MIN_ST};

        struct PushSourcePolicyOps
        {
            const MemoryUtils::StoragePlan *storage_plan{nullptr};

            const ValueTypeMetaData &(*sender_schema_impl)(const void *context) = nullptr;
            bool (*output_compatible_impl)(const void *context,
                                                         const TSValueTypeMetaData &output_schema) = nullptr;
            void (*start_impl)(const void *context,
                               void *storage,
                               const TSValueTypeMetaData &output_schema) = nullptr;
            void (*stop_impl)(const void *context, void *storage) = nullptr;
            bool (*send_impl)(const void *context, void *storage, Value value) = nullptr;
            bool (*emit_next_impl)(const void *context,
                                                 void *storage,
                                                 const TSOutputView &output) = nullptr;
            std::size_t (*pending_items_impl)(const void *context,
                                             const void *storage) noexcept = nullptr;
        };

        struct PushSourceQueuePop
        {
            Value value{};
            bool  more_pending{false};
        };

        struct PushSourcePolicyContext
        {
            /** The OBSERVED delta schema; what ``sender_schema()`` reports. */
            const ValueTypeMetaData *sender_schema{nullptr};
            /** The AUTHORED counterpart, equal to ``sender_schema`` unless the
                output contains a ``TSD`` (see ``authored_delta_schema``). */
            const ValueTypeMetaData *authored_schema{nullptr};
        };


        /**
         * A pushed delta may be OBSERVED or AUTHORED.
         *
         * ``py_to_delta`` escalates to the authored shape only when the caller
         * actually expresses strict intent, so an ordinary push matches
         * ``sender_schema`` exactly. A ``{key: REMOVE}`` push arrives on the
         * authored schema, which ``apply_delta`` already accepts - and which
         * may differ from the observed one at ANY depth (a ``TSL`` of ``TSD``
         * escalates its element type), so this compares whole schemas rather
         * than inspecting the top-level fields.
         */
        [[nodiscard]] bool push_value_schema_acceptable(const PushSourcePolicyContext &context,
                                                        const ValueTypeMetaData &value) noexcept
        {
            return context.sender_schema == &value || context.authored_schema == &value;
        }

        struct QueuePolicyStorage
        {
            void start()
            {
                std::lock_guard lock{mutex};
                accepting = true;
                values.clear();
            }

            void stop()
            {
                std::lock_guard lock{mutex};
                accepting = false;
                values.clear();
            }

            [[nodiscard]] bool send(const PushSourcePolicyContext &context, Value value)
            {
                std::lock_guard lock{mutex};
                if (!accepting) { return false; }
                if (!value.has_value())
                {
                    throw std::invalid_argument("PushSourceSender requires a live value payload");
                }
                const auto &value_schema = *value.schema();
                if (context.sender_schema == nullptr ||
                    !push_value_schema_acceptable(context, value_schema))
                {
                    throw std::invalid_argument(
                        "PushSourceSender value schema does not match the push-source sender schema");
                }

                values.push_back(std::move(value));
                return true;
            }

            [[nodiscard]] std::optional<PushSourceQueuePop> try_pop()
            {
                std::lock_guard lock{mutex};
                if (values.empty()) { return std::nullopt; }

                PushSourceQueuePop result{
                    .value = std::move(values.front()),
                    .more_pending = false,
                };
                values.pop_front();
                result.more_pending = !values.empty();
                return result;
            }

            [[nodiscard]] std::size_t pending_items() const noexcept
            {
                std::lock_guard lock{mutex};
                return values.size();
            }

            mutable std::mutex mutex{};
            std::deque<Value>  values{};
            bool               accepting{false};
        };

        struct ConflatingPolicyStorage
        {
            void start(const TSValueTypeMetaData &schema)
            {
                std::lock_guard lock{mutex};
                output_schema = &schema;
                accumulator = TSOutput{schema};
                accepting = true;
                pending = false;
                next_mutation_time = MIN_ST;
            }

            void stop()
            {
                std::lock_guard lock{mutex};
                accepting = false;
                pending = false;
                accumulator = TSOutput{};
                output_schema = nullptr;
                next_mutation_time = MIN_ST;
            }

            [[nodiscard]] bool send(const PushSourcePolicyContext &context, Value value)
            {
                std::lock_guard lock{mutex};
                if (!accepting) { return false; }
                if (!value.has_value())
                {
                    throw std::invalid_argument("PushSourceSender requires a live value payload");
                }
                const auto &value_schema = *value.schema();
                if (context.sender_schema == nullptr ||
                    !push_value_schema_acceptable(context, value_schema))
                {
                    throw std::invalid_argument(
                        "PushSourceSender value schema does not match the push-source sender schema");
                }

                const DateTime mutation_time = next_mutation_time;
                next_mutation_time += MIN_TD;
                apply_delta(accumulator.view(mutation_time), value.view());
                pending = pending || accumulator.view(mutation_time).modified();
                return pending;
            }

            [[nodiscard]] std::optional<TSOutput> take_accumulated()
            {
                std::lock_guard lock{mutex};
                if (!pending) { return std::nullopt; }

                std::optional<TSOutput> result{std::move(accumulator)};
                accumulator = TSOutput{*output_schema};
                pending = false;
                next_mutation_time = MIN_ST;
                return result;
            }

            [[nodiscard]] std::size_t pending_items() const noexcept
            {
                std::lock_guard lock{mutex};
                return pending ? 1U : 0U;
            }

            mutable std::mutex          mutex{};
            TSOutput                    accumulator{};
            const TSValueTypeMetaData  *output_schema{nullptr};
            DateTime                    next_mutation_time{MIN_ST};
            bool                        accepting{false};
            bool                        pending{false};
        };
    }  // namespace detail

    namespace
    {
        constexpr std::string_view push_source_policy_field_name{"push_source_policy"};

        [[noreturn]] void throw_unconfigured_policy()
        {
            throw std::logic_error("PushSourcePolicy is not configured");
        }

        [[nodiscard]] const ValueTypeMetaData &default_sender_schema_impl(const void *)
        {
            throw_unconfigured_policy();
        }

        [[nodiscard]] bool default_output_compatible_impl(const void *, const TSValueTypeMetaData &)
        {
            return false;
        }

        void default_policy_start(const void *, void *, const TSValueTypeMetaData &)
        {
            throw_unconfigured_policy();
        }

        void default_policy_stop(const void *, void *)
        {
        }

        [[nodiscard]] bool default_policy_send(const void *, void *, Value)
        {
            throw_unconfigured_policy();
        }

        [[nodiscard]] bool default_policy_emit_next(const void *, void *, const TSOutputView &)
        {
            throw_unconfigured_policy();
        }

        [[nodiscard]] std::size_t default_policy_pending_items(
            const void *, const void *) noexcept
        {
            return 0;
        }

        [[nodiscard]] const detail::PushSourcePolicyOps &default_push_source_policy_ops()
        {
            static const detail::PushSourcePolicyOps ops{
                .storage_plan = &MemoryUtils::plan_for<std::byte>(),
                .sender_schema_impl = &default_sender_schema_impl,
                .output_compatible_impl = &default_output_compatible_impl,
                .start_impl = &default_policy_start,
                .stop_impl = &default_policy_stop,
                .send_impl = &default_policy_send,
                .emit_next_impl = &default_policy_emit_next,
                .pending_items_impl = &default_policy_pending_items,
            };
            return ops;
        }

        [[nodiscard]] const detail::PushSourcePolicyContext &policy_context(const void *context)
        {
            return *static_cast<const detail::PushSourcePolicyContext *>(context);
        }

        [[nodiscard]] std::vector<std::unique_ptr<detail::PushSourcePolicyContext>> &policy_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<detail::PushSourcePolicyContext>>;
            return *contexts;
        }

        [[nodiscard]] const detail::PushSourcePolicyContext &register_policy_context(
            const ValueTypeMetaData &sender_schema, const ValueTypeMetaData *authored_schema)
        {
            auto context = std::make_unique<detail::PushSourcePolicyContext>(
                detail::PushSourcePolicyContext{
                    .sender_schema  = &sender_schema,
                    .authored_schema = authored_schema != nullptr ? authored_schema : &sender_schema});
            const auto *result = context.get();
            policy_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] const ValueTypeMetaData &sender_schema_impl(const void *context)
        {
            return *policy_context(context).sender_schema;
        }

        [[nodiscard]] bool delta_output_compatible(const void *context,
                                                   const TSValueTypeMetaData &output_schema)
        {
            return output_schema.delta_value_schema == policy_context(context).sender_schema;
        }

        void queue_policy_start(const void *, void *storage, const TSValueTypeMetaData &)
        {
            MemoryUtils::cast<detail::QueuePolicyStorage>(storage)->start();
        }

        void queue_policy_stop(const void *, void *storage)
        {
            MemoryUtils::cast<detail::QueuePolicyStorage>(storage)->stop();
        }

        [[nodiscard]] bool queue_policy_send(const void *context, void *storage, Value value)
        {
            return MemoryUtils::cast<detail::QueuePolicyStorage>(storage)->send(
                policy_context(context),
                std::move(value));
        }

        [[nodiscard]] bool queue_policy_emit_next(const void *,
                                                  void *storage,
                                                  const TSOutputView &output)
        {
            auto item = MemoryUtils::cast<detail::QueuePolicyStorage>(storage)->try_pop();
            if (!item.has_value()) { return false; }

            apply_delta(output, item->value.view());
            return item->more_pending;
        }

        [[nodiscard]] std::size_t queue_policy_pending_items(
            const void *, const void *storage) noexcept
        {
            return MemoryUtils::cast<const detail::QueuePolicyStorage>(storage)
                ->pending_items();
        }

        void conflating_policy_start(const void *, void *storage, const TSValueTypeMetaData &output_schema)
        {
            MemoryUtils::cast<detail::ConflatingPolicyStorage>(storage)->start(output_schema);
        }

        void conflating_policy_stop(const void *, void *storage)
        {
            MemoryUtils::cast<detail::ConflatingPolicyStorage>(storage)->stop();
        }

        [[nodiscard]] bool conflating_policy_send(const void *context, void *storage, Value value)
        {
            return MemoryUtils::cast<detail::ConflatingPolicyStorage>(storage)->send(
                policy_context(context),
                std::move(value));
        }

        [[nodiscard]] bool conflating_policy_emit_next(const void *,
                                                       void *storage,
                                                       const TSOutputView &output)
        {
            auto accumulated = MemoryUtils::cast<detail::ConflatingPolicyStorage>(storage)->take_accumulated();
            if (!accumulated.has_value()) { return false; }

            apply_current_value(output, accumulated->view(detail::push_conflation_time).value());
            return false;
        }

        [[nodiscard]] std::size_t conflating_policy_pending_items(
            const void *, const void *storage) noexcept
        {
            return MemoryUtils::cast<const detail::ConflatingPolicyStorage>(storage)
                ->pending_items();
        }

        [[nodiscard]] const detail::PushSourcePolicyOps &queue_policy_ops()
        {
            static const detail::PushSourcePolicyOps ops{
                .storage_plan = &MemoryUtils::plan_for<detail::QueuePolicyStorage>(),
                .sender_schema_impl = &sender_schema_impl,
                .output_compatible_impl = &delta_output_compatible,
                .start_impl = &queue_policy_start,
                .stop_impl = &queue_policy_stop,
                .send_impl = &queue_policy_send,
                .emit_next_impl = &queue_policy_emit_next,
                .pending_items_impl = &queue_policy_pending_items,
            };
            return ops;
        }

        [[nodiscard]] const detail::PushSourcePolicyOps &conflating_policy_ops()
        {
            static const detail::PushSourcePolicyOps ops{
                .storage_plan = &MemoryUtils::plan_for<detail::ConflatingPolicyStorage>(),
                .sender_schema_impl = &sender_schema_impl,
                .output_compatible_impl = &delta_output_compatible,
                .start_impl = &conflating_policy_start,
                .stop_impl = &conflating_policy_stop,
                .send_impl = &conflating_policy_send,
                .emit_next_impl = &conflating_policy_emit_next,
                .pending_items_impl = &conflating_policy_pending_items,
            };
            return ops;
        }

        struct PushSourceNodeContext
        {
            const TSValueTypeMetaData *output_schema{nullptr};
            PushSourcePolicy           policy{};
            std::size_t                policy_storage_offset{0};
            PushSourceStartViewCallback on_start{};
            std::function<void(const NodeView &)> on_stop{};
        };

        [[nodiscard]] std::vector<std::unique_ptr<PushSourceNodeContext>> &push_source_node_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<PushSourceNodeContext>>;
            return *contexts;
        }

        [[nodiscard]] const PushSourceNodeContext &register_push_source_node_context(
            const TSValueTypeMetaData &output_schema,
            PushSourcePolicy policy,
            std::size_t policy_storage_offset,
            PushSourceStartViewCallback on_start,
            std::function<void(const NodeView &)> on_stop)
        {
            auto context = std::make_unique<PushSourceNodeContext>(PushSourceNodeContext{
                .output_schema = &output_schema,
                .policy = policy,
                .policy_storage_offset = policy_storage_offset,
                .on_start = std::move(on_start),
                .on_stop = std::move(on_stop),
            });
            const auto *result = context.get();
            push_source_node_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] void *policy_storage(const PushSourceNodeContext &context, void *memory)
        {
            return MemoryUtils::advance(memory, context.policy_storage_offset);
        }

        [[nodiscard]] const void *policy_storage(
            const PushSourceNodeContext &context, const void *memory)
        {
            return MemoryUtils::advance(memory, context.policy_storage_offset);
        }

        [[nodiscard]] NodeInspectionMetrics push_source_inspection_metrics(
            const void *raw_context, const void *memory) noexcept
        {
            const auto &context = *static_cast<const PushSourceNodeContext *>(raw_context);
            return NodeInspectionMetrics{
                .pending_items = detail::PushSourcePolicyAccess::pending_items(
                    context.policy, policy_storage(context, memory)),
            };
        }

        void push_source_start(const PushSourceNodeContext &context, const NodeView &view,
                               DateTime evaluation_time)
        {
            void *storage = policy_storage(context, view.data());
            detail::PushSourcePolicyAccess::start(context.policy, storage, *context.output_schema);
            auto rollback = UnwindCleanupGuard([&] {
                detail::PushSourcePolicyAccess::stop(context.policy, storage);
            });
            if (context.on_start)
            {
                context.on_start(detail::PushSourcePolicyAccess::make_sender(
                    context.policy,
                    storage,
                    view.graph().root().executor().push_queue_engine(),
                    view.graph().type_realization()), view, evaluation_time);
            }
            rollback.release();
        }

        void push_source_eval(const PushSourceNodeContext &context, const NodeView &view, DateTime evaluation_time)
        {
            void *storage = policy_storage(context, view.data());
            const bool more_pending = detail::PushSourcePolicyAccess::emit_next(
                context.policy,
                storage,
                view.output(evaluation_time));
            if (more_pending)
            {
                view.graph().root().executor().push_queue_engine().mark_push_update_pending();
            }
        }

        void push_source_stop(const PushSourceNodeContext &context, const NodeView &view)
        {
            detail::PushSourcePolicyAccess::stop(context.policy, policy_storage(context, view.data()));
            if (context.on_stop) { context.on_stop(view); }
        }

        [[nodiscard]] PushSourcePolicy make_policy(const detail::PushSourcePolicyOps &ops,
                                                   const ValueTypeMetaData &sender_schema,
                                                   const ValueTypeMetaData *authored_schema)
        {
            return detail::PushSourcePolicyAccess::make_policy(
                &ops,
                &register_policy_context(sender_schema, authored_schema));
        }
    }  // namespace

    PushSourcePolicy::PushSourcePolicy() noexcept
        : PushSourcePolicy(&default_push_source_policy_ops(), nullptr)
    {
    }

    PushSourcePolicy::PushSourcePolicy(const detail::PushSourcePolicyOps *ops,
                                       const void *context) noexcept
        : ops_(ops != nullptr ? ops : &default_push_source_policy_ops()),
          context_(context)
    {
    }

    bool PushSourcePolicy::valid() const noexcept
    {
        return ops_ != &default_push_source_policy_ops();
    }

    const ValueTypeMetaData &PushSourcePolicy::sender_schema() const
    {
        return ops_->sender_schema_impl(context_);
    }

    bool PushSourcePolicy::output_compatible(const TSValueTypeMetaData &output_schema) const
    {
        return ops_->output_compatible_impl(context_, output_schema);
    }

    bool detail::PushSourcePolicyStorageRef::bound() const noexcept
    {
        return storage != nullptr;
    }

    void detail::PushSourcePolicyStorageRef::send(Value value) const
    {
        if (!bound()) { throw std::logic_error("PushSourceSender requires live push-source policy storage"); }
        if (push_engine.stop_requested()) { return; }
        if (policy.ops_->send_impl(policy.context_, storage, std::move(value)))
        {
            push_engine.mark_push_update_pending();
        }
    }

    PushSourcePolicy detail::PushSourcePolicyAccess::make_policy(
        const PushSourcePolicyOps *ops,
        const void *context) noexcept
    {
        return PushSourcePolicy{ops, context};
    }

    const MemoryUtils::StoragePlan &detail::PushSourcePolicyAccess::storage_plan(
        const PushSourcePolicy &policy)
    {
        return *policy.ops_->storage_plan;
    }

    PushSourceSender detail::PushSourcePolicyAccess::make_sender(
        PushSourcePolicy policy,
        void *storage,
        PushQueueEngineView push_engine,
        const TypeRealizationSnapshot *type_realization) noexcept
    {
        return PushSourceSender{PushSourcePolicyStorageRef{
            .policy = policy,
            .storage = storage,
            .push_engine = std::move(push_engine),
            .type_realization = type_realization,
        }};
    }

    void detail::PushSourcePolicyAccess::start(const PushSourcePolicy &policy,
                                               void *storage,
                                               const TSValueTypeMetaData &output_schema)
    {
        policy.ops_->start_impl(policy.context_, storage, output_schema);
    }

    void detail::PushSourcePolicyAccess::stop(const PushSourcePolicy &policy, void *storage)
    {
        policy.ops_->stop_impl(policy.context_, storage);
    }

    bool detail::PushSourcePolicyAccess::emit_next(const PushSourcePolicy &policy,
                                                   void *storage,
                                                   const TSOutputView &output)
    {
        return policy.ops_->emit_next_impl(policy.context_, storage, output);
    }

    std::size_t detail::PushSourcePolicyAccess::pending_items(
        const PushSourcePolicy &policy, const void *storage) noexcept
    {
        return policy.ops_->pending_items_impl(policy.context_, storage);
    }

    PushSourceSender::PushSourceSender(detail::PushSourcePolicyStorageRef policy_storage) noexcept
        : policy_storage_(policy_storage)
    {
    }

    bool PushSourceSender::valid() const noexcept
    {
        return policy_storage_.bound();
    }

    const TypeRealizationSnapshot *PushSourceSender::type_realization() const noexcept
    {
        return policy_storage_.type_realization;
    }

    void PushSourceSender::send(Value value) const
    {
        policy_storage_.send(std::move(value));
    }

    PushSourcePolicy make_push_source_policy(PushSourcePolicyKind kind,
                                             const ValueTypeMetaData &sender_schema)
    {
        return make_push_source_policy(kind, sender_schema, nullptr);
    }

    PushSourcePolicy make_push_source_policy(PushSourcePolicyKind kind,
                                             const ValueTypeMetaData &sender_schema,
                                             const ValueTypeMetaData *authored_schema)
    {
        switch (kind)
        {
            case PushSourcePolicyKind::Queue:
                return make_policy(queue_policy_ops(), sender_schema, authored_schema);
            case PushSourcePolicyKind::Conflating:
                return make_policy(conflating_policy_ops(), sender_schema, authored_schema);
        }
        throw std::invalid_argument("Unknown push-source policy kind");
    }

    PushSourcePolicy make_push_source_queue_policy(const ValueTypeMetaData &sender_schema)
    {
        return make_push_source_policy(PushSourcePolicyKind::Queue, sender_schema, nullptr);
    }

    PushSourcePolicy make_push_source_queue_policy(const TSValueTypeMetaData &output_schema)
    {
        return make_push_source_policy(PushSourcePolicyKind::Queue, *output_schema.delta_value_schema,
                                       output_schema.authored_delta_schema);
    }

    PushSourcePolicy make_push_source_conflating_policy(const ValueTypeMetaData &sender_schema)
    {
        return make_push_source_policy(PushSourcePolicyKind::Conflating, sender_schema, nullptr);
    }

    PushSourcePolicy make_push_source_conflating_policy(const TSValueTypeMetaData &output_schema)
    {
        return make_push_source_policy(PushSourcePolicyKind::Conflating, *output_schema.delta_value_schema,
                                       output_schema.authored_delta_schema);
    }

    namespace
    {
        NodeBuilder make_push_source_node_with_capability(
            const TSValueTypeMetaData &output_schema,
            PushSourcePolicy policy,
            PushSourceNodeExtension extension,
            bool requires_phase_runner,
            bool simulation_capable)
        {
            if (!policy.output_compatible(output_schema))
            {
                throw std::invalid_argument("Push source policy is not compatible with the output schema");
            }

            NodeTypeMetaData schema;
            schema.display_name = "push_source";
            schema.output_schema = &output_schema;
            schema.node_kind = NodeKind::PushSource;
            schema.simulation_capable_push_source = simulation_capable;
            schema.requires_phase_runner = requires_phase_runner;
            schema.state_schema = extension.state_schema;
            schema.scalar_schema = extension.scalar_schema;
            schema.uses_scheduler = extension.uses_scheduler;
            schema.uses_global_state = extension.uses_global_state;
            schema.uses_evaluation_clock = extension.uses_evaluation_clock;
            schema.uses_python_values = extension.uses_python_values;

            const std::array fields{
                NodeStorageField{
                    push_source_policy_field_name,
                    &detail::PushSourcePolicyAccess::storage_plan(policy),
                },
            };
            const auto &plan = node_storage_plan_for(schema, fields);
            const auto *context = &register_push_source_node_context(
                output_schema,
                policy,
                plan.component(push_source_policy_field_name).offset,
                std::move(extension.on_start),
                std::move(extension.on_stop));

            NodeCallbacks callbacks;
            callbacks.start = [context](const NodeView &view, DateTime evaluation_time) {
                push_source_start(*context, view, evaluation_time);
            };
            callbacks.evaluate = [context](const NodeView &view, DateTime evaluation_time) {
                push_source_eval(*context, view, evaluation_time);
            };
            callbacks.stop = [context](const NodeView &view, DateTime) {
                push_source_stop(*context, view);
            };

            NodeTypeDescriptor descriptor;
            descriptor.schema = std::move(schema);
            descriptor.storage_plan = &plan;
            descriptor.callbacks = std::move(callbacks);
            descriptor.ops.inspection_metrics_impl = &push_source_inspection_metrics;
            descriptor.ops.extended_view_context = context;
            return NodeBuilder::from_descriptor(std::move(descriptor));
        }
    }  // namespace

    NodeBuilder make_push_source_node(const TSValueTypeMetaData &output_schema,
                                      PushSourcePolicy policy,
                                      PushSourceStartCallback on_start,
                                      bool requires_phase_runner)
    {
        return make_push_source_node_with_capability(
            output_schema, std::move(policy),
            PushSourceNodeExtension{
                .on_start = [on_start = std::move(on_start)](
                    PushSourceSender sender, const NodeView &, DateTime) {
                    if (on_start) { on_start(std::move(sender)); }
                },
            },
            requires_phase_runner, false);
    }

    NodeBuilder make_push_source_node_with_view(
        const TSValueTypeMetaData &output_schema,
        PushSourcePolicy policy,
        PushSourceNodeExtension extension,
        bool requires_phase_runner)
    {
        return make_push_source_node_with_capability(
            output_schema, std::move(policy), std::move(extension),
            requires_phase_runner, false);
    }

    NodeBuilder make_push_source_node(const TSValueTypeMetaData &output_schema,
                                      PushSourceStartCallback on_start)
    {
        return make_push_source_node(
            output_schema,
            make_push_source_queue_policy(output_schema),
            std::move(on_start),
            false);
    }

    NodeBuilder make_simulation_capable_push_source_node(
        const TSValueTypeMetaData &output_schema,
        PushSourcePolicy policy,
        PushSourceStartCallback on_start,
        bool requires_phase_runner)
    {
        return make_push_source_node_with_capability(
            output_schema, std::move(policy),
            PushSourceNodeExtension{
                .on_start = [on_start = std::move(on_start)](
                    PushSourceSender sender, const NodeView &, DateTime) {
                    if (on_start) { on_start(std::move(sender)); }
                },
            },
            requires_phase_runner, true);
    }

    NodeBuilder make_simulation_capable_push_source_node(
        const TSValueTypeMetaData &output_schema,
        PushSourceStartCallback on_start)
    {
        return make_simulation_capable_push_source_node(
            output_schema,
            make_push_source_queue_policy(output_schema),
            std::move(on_start),
            false);
    }
}  // namespace hgraph
