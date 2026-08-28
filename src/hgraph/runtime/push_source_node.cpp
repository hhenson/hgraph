#include <hgraph/runtime/push_source_node.h>

#include <hgraph/runtime/executor.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

#include <array>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace detail
    {
        const DateTime push_conflation_time{MIN_ST};

        struct PushSourceSendResult
        {
            bool accepted{false};
            bool wake_required{false};
        };

        struct PushSourcePolicyOps
        {
            const MemoryUtils::StoragePlan *storage_plan{nullptr};

            const ValueTypeMetaData &(*sender_schema_impl)(const void *context) = nullptr;
            bool (*output_compatible_impl)(const void *context,
                                           const TSValueTypeMetaData &output_schema) = nullptr;
            void (*start_impl)(const void *context, void *storage,
                               const TSValueTypeMetaData &output_schema) = nullptr;
            void (*stop_impl)(const void *context, void *storage) = nullptr;
            PushSourceSendResult (*try_send_impl)(const void *context, void *storage,
                                                  Value value) = nullptr;
            PushSourceSendResult (*send_blocking_impl)(const void *context, void *storage,
                                                       Value value) = nullptr;
            bool (*emit_next_impl)(const void *context, void *storage,
                                   const TSOutputView &output) = nullptr;
            std::size_t (*pending_items_impl)(const void *context,
                                              const void *storage) noexcept = nullptr;
        };

        /** Private sender facade dispatch. The public handle retains erased
         * ownership separately and always binds a non-null table. */
        struct PushSourceSenderOps
        {
            bool (*valid_impl)(const void *control) noexcept = nullptr;
            const TypeRealizationSnapshot *(*type_realization_impl)(
                const void *control) noexcept = nullptr;
            bool (*try_send_impl)(void *control, Value value) = nullptr;
            bool (*send_blocking_impl)(void *control, Value value) = nullptr;
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
            std::size_t              max_pending{0};
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
            void start(const PushSourcePolicyContext &context,
                       const TSValueTypeMetaData *burst_output_schema = nullptr)
            {
                std::lock_guard lock{mutex};
                values.clear();
                max_pending          = context.max_pending;
                burst_element_binding = {};
                burst_value_binding   = {};
                if (burst_output_schema != nullptr)
                {
                    burst_element_binding =
                        value_type_for_active_realization(context.sender_schema);
                    burst_value_binding = compact_list_type(
                        burst_element_binding, *burst_output_schema->value_schema);
                }
                consumer_thread = std::this_thread::get_id();
                accepting       = true;
            }

            void stop()
            {
                {
                    std::lock_guard lock{mutex};
                    accepting = false;
                    values.clear();
                    burst_element_binding = {};
                    burst_value_binding = {};
                    consumer_thread = {};
                }
                capacity_available.notify_all();
            }

            [[nodiscard]] PushSourceSendResult try_send(
                const PushSourcePolicyContext &context, Value value)
            {
                std::lock_guard lock{mutex};
                if (!accepting)
                {
                    return {};
                }
                validate(context, value);
                if (full())
                {
                    return {};
                }

                const bool was_empty = values.empty();
                values.push_back(std::move(value));
                return {.accepted = true, .wake_required = was_empty};
            }

            [[nodiscard]] PushSourceSendResult send_blocking(
                const PushSourcePolicyContext &context, Value value)
            {
                std::unique_lock lock{mutex};
                if (!accepting)
                {
                    return {};
                }
                validate(context, value);
                if (full() && consumer_thread == std::this_thread::get_id())
                {
                    throw std::logic_error("PushSourceSender::send_blocking cannot wait on "
                                           "the graph evaluation thread");
                }
                capacity_available.wait(lock, [this] { return !accepting || !full(); });
                if (!accepting)
                {
                    return {};
                }

                const bool was_empty = values.empty();
                values.push_back(std::move(value));
                return {.accepted = true, .wake_required = was_empty};
            }

            [[nodiscard]] std::optional<PushSourceQueuePop> try_pop()
            {
                std::unique_lock lock{mutex};
                if (values.empty())
                {
                    return std::nullopt;
                }

                PushSourceQueuePop result{
                    .value = std::move(values.front()),
                    .more_pending = false,
                };
                values.pop_front();
                result.more_pending = !values.empty();
                lock.unlock();
                capacity_available.notify_one();
                return result;
            }

            [[nodiscard]] std::deque<Value> take_all()
            {
                std::deque<Value> result;
                {
                    std::lock_guard lock{mutex};
                    result.swap(values);
                }
                if (!result.empty())
                {
                    capacity_available.notify_all();
                }
                return result;
            }

            [[nodiscard]] Value make_burst(std::deque<Value> burst_values) const
            {
                if (burst_values.empty())
                {
                    return {};
                }
                if (!burst_element_binding || !burst_value_binding)
                {
                    throw std::logic_error("Burst push-source queue is not initialized");
                }

                ListBuilder builder{burst_element_binding, *burst_value_binding.schema()};
                for (Value &value : burst_values)
                {
                    builder.push_back(std::move(value));
                }
                ListStorage storage = builder.build_storage();
                Value result{burst_value_binding};
                burst_value_binding.ops_ref().move_assign_from(
                    burst_value_binding, const_cast<void *>(result.view().data()),
                    burst_value_binding, &storage);
                return result;
            }

            [[nodiscard]] std::size_t pending_items() const noexcept
            {
                std::lock_guard lock{mutex};
                return values.size();
            }

          private:
            [[nodiscard]] bool full() const noexcept
            {
                return max_pending != 0 && values.size() >= max_pending;
            }

            static void validate(const PushSourcePolicyContext &context, const Value &value)
            {
                if (!value.has_value())
                {
                    throw std::invalid_argument("PushSourceSender requires a live value payload");
                }
                const auto &value_schema = *value.schema();
                if (context.sender_schema == nullptr ||
                    !push_value_schema_acceptable(context, value_schema))
                {
                    throw std::invalid_argument("PushSourceSender value schema does not "
                                                "match the push-source sender schema");
                }
            }

            mutable std::mutex      mutex{};
            std::condition_variable capacity_available{};
            std::deque<Value>       values{};
            ValueTypeRef            burst_element_binding{};
            ValueTypeRef            burst_value_binding{};
            std::size_t             max_pending{0};
            std::thread::id         consumer_thread{};
            bool                    accepting{false};
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

            [[nodiscard]] PushSourceSendResult try_send(
                const PushSourcePolicyContext &context, Value value)
            {
                std::lock_guard lock{mutex};
                if (!accepting)
                {
                    return {};
                }
                if (!value.has_value())
                {
                    throw std::invalid_argument("PushSourceSender requires a live value payload");
                }
                const auto &value_schema = *value.schema();
                if (context.sender_schema == nullptr ||
                    !push_value_schema_acceptable(context, value_schema))
                {
                    throw std::invalid_argument("PushSourceSender value schema does not "
                                                "match the push-source sender schema");
                }

                const DateTime mutation_time = next_mutation_time;
                next_mutation_time += MIN_TD;
                const bool was_pending = pending;
                apply_delta(accumulator.view(mutation_time), value.view());
                pending = pending || accumulator.view(mutation_time).modified();
                return {
                    .accepted = true,
                    .wake_required = pending && !was_pending,
                };
            }

            [[nodiscard]] PushSourceSendResult send_blocking(
                const PushSourcePolicyContext &context, Value value)
            {
                return try_send(context, std::move(value));
            }

            [[nodiscard]] std::optional<TSOutput> take_accumulated()
            {
                std::lock_guard lock{mutex};
                if (!pending)
                {
                    return std::nullopt;
                }

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

        /** Shared lifetime boundary between retained producer handles and the
         * graph-owned policy storage. Closing prevents new calls; graph stop
         * then wakes policy waiters and waits for entered calls before node
         * storage can be destroyed. */
        class PushSourceSenderControl
        {
          public:
            PushSourceSenderControl(PushSourcePolicy policy, void *storage,
                                    PushQueueEngineView push_engine,
                                    const TypeRealizationSnapshot *type_realization) noexcept
                : policy_{policy}, storage_{storage}, push_engine_{std::move(push_engine)},
                  type_realization_{type_realization}
            {
            }

            [[nodiscard]] bool valid() const noexcept
            {
                std::lock_guard lock{mutex_};
                return !closing_ && storage_ != nullptr && push_engine_.valid() &&
                       !push_engine_.stop_requested();
            }

            [[nodiscard]] const TypeRealizationSnapshot *type_realization() const noexcept
            {
                std::lock_guard lock{mutex_};
                return !closing_ && storage_ != nullptr ? type_realization_ : nullptr;
            }

            [[nodiscard]] bool try_send(Value value)
            {
                if (!enter())
                {
                    return false;
                }
                auto leave_call = make_scope_exit([this] { leave(); });
                if (push_engine_.stop_requested())
                {
                    return false;
                }

                const PushSourceSendResult result =
                    policy_.ops_->try_send_impl(policy_.context_, storage_, std::move(value));
                if (result.accepted && result.wake_required)
                {
                    push_engine_.mark_push_update_pending();
                }
                return result.accepted;
            }

            [[nodiscard]] bool send_blocking(Value value)
            {
                if (!enter())
                {
                    return false;
                }
                auto leave_call = make_scope_exit([this] { leave(); });
                if (push_engine_.stop_requested())
                {
                    return false;
                }

                const PushSourceSendResult result =
                    policy_.ops_->send_blocking_impl(policy_.context_, storage_, std::move(value));
                if (!result.accepted)
                {
                    return false;
                }
                if (result.wake_required)
                {
                    push_engine_.mark_push_update_pending();
                }
                return true;
            }

            void begin_close() noexcept
            {
                std::lock_guard lock{mutex_};
                closing_ = true;
            }

            void wait_for_quiescence() noexcept
            {
                std::unique_lock lock{mutex_};
                quiescent_.wait(lock, [this] { return active_calls_ == 0; });
            }

            void detach() noexcept
            {
                std::lock_guard lock{mutex_};
                storage_ = nullptr;
                push_engine_ = {};
                type_realization_ = nullptr;
            }

          private:
            [[nodiscard]] bool enter() noexcept
            {
                std::lock_guard lock{mutex_};
                if (closing_ || storage_ == nullptr)
                {
                    return false;
                }
                ++active_calls_;
                return true;
            }

            void leave() noexcept
            {
                std::lock_guard lock{mutex_};
                if (--active_calls_ == 0)
                {
                    quiescent_.notify_all();
                }
            }

            mutable std::mutex      mutex_{};
            std::condition_variable quiescent_{};
            PushSourcePolicy        policy_{};
            void                   *storage_{nullptr};
            PushQueueEngineView     push_engine_{};
            const TypeRealizationSnapshot *type_realization_{nullptr};
            std::size_t              active_calls_{0};
            bool                     closing_{false};
        };
    }  // namespace detail

    namespace
    {
        constexpr std::string_view push_source_policy_field_name{"push_source_policy"};
        constexpr std::string_view push_source_sender_control_field_name{
            "push_source_sender_control"};

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

        [[nodiscard]] detail::PushSourceSendResult default_policy_try_send(
            const void *, void *, Value)
        {
            throw_unconfigured_policy();
        }

        [[nodiscard]] detail::PushSourceSendResult default_policy_send_blocking(
            const void *, void *, Value)
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
                .try_send_impl = &default_policy_try_send,
                .send_blocking_impl = &default_policy_send_blocking,
                .emit_next_impl = &default_policy_emit_next,
                .pending_items_impl = &default_policy_pending_items,
            };
            return ops;
        }

        [[nodiscard]] bool stopped_sender_valid(const void *) noexcept
        {
            return false;
        }

        [[nodiscard]] const TypeRealizationSnapshot *stopped_sender_type_realization(
            const void *) noexcept
        {
            return nullptr;
        }

        [[nodiscard]] bool stopped_sender_send(void *, Value)
        {
            return false;
        }

        [[nodiscard]] const detail::PushSourceSenderOps &stopped_sender_ops() noexcept
        {
            static constexpr detail::PushSourceSenderOps ops{
                .valid_impl = &stopped_sender_valid,
                .type_realization_impl = &stopped_sender_type_realization,
                .try_send_impl = &stopped_sender_send,
                .send_blocking_impl = &stopped_sender_send,
            };
            return ops;
        }

        [[nodiscard]] bool active_sender_valid(const void *control) noexcept
        {
            return static_cast<const detail::PushSourceSenderControl *>(control)->valid();
        }

        [[nodiscard]] const TypeRealizationSnapshot *active_sender_type_realization(
            const void *control) noexcept
        {
            return static_cast<const detail::PushSourceSenderControl *>(control)
                ->type_realization();
        }

        [[nodiscard]] bool active_sender_try_send(void *control, Value value)
        {
            return static_cast<detail::PushSourceSenderControl *>(control)
                ->try_send(std::move(value));
        }

        [[nodiscard]] bool active_sender_send_blocking(void *control, Value value)
        {
            return static_cast<detail::PushSourceSenderControl *>(control)
                ->send_blocking(std::move(value));
        }

        [[nodiscard]] const detail::PushSourceSenderOps &active_sender_ops() noexcept
        {
            static constexpr detail::PushSourceSenderOps ops{
                .valid_impl = &active_sender_valid,
                .type_realization_impl = &active_sender_type_realization,
                .try_send_impl = &active_sender_try_send,
                .send_blocking_impl = &active_sender_send_blocking,
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
            const ValueTypeMetaData &sender_schema,
            const ValueTypeMetaData *authored_schema,
            std::size_t max_pending)
        {
            auto context = std::make_unique<detail::PushSourcePolicyContext>(
                detail::PushSourcePolicyContext{
                    .sender_schema  = &sender_schema,
                    .authored_schema = authored_schema != nullptr ? authored_schema : &sender_schema,
                    .max_pending = max_pending});
            const auto *result = context.get();
            policy_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] const ValueTypeMetaData &sender_schema_impl(const void *context)
        {
            return *policy_context(context).sender_schema;
        }

        [[nodiscard]] bool delta_output_compatible(
            const void *context, const TSValueTypeMetaData &output_schema)
        {
            return output_schema.delta_value_schema == policy_context(context).sender_schema;
        }

        [[nodiscard]] bool burst_output_compatible(
            const void *context, const TSValueTypeMetaData &output_schema)
        {
            const auto *value_schema = output_schema.value_schema;
            return output_schema.kind == TSTypeKind::TS && value_schema != nullptr &&
                   value_schema->try_value_kind() == ValueTypeKind::List &&
                   value_schema->fixed_size == 0 && value_schema->is_variadic_tuple() &&
                   !value_schema->is_mutable() && !value_schema->is_nullable() &&
                   value_schema->element_type == policy_context(context).sender_schema;
        }

        void queue_policy_start(const void *context, void *storage, const TSValueTypeMetaData &)
        {
            MemoryUtils::cast<detail::QueuePolicyStorage>(storage)->start(policy_context(context));
        }

        void burst_policy_start(
            const void *context, void *storage,
            const TSValueTypeMetaData &output_schema)
        {
            MemoryUtils::cast<detail::QueuePolicyStorage>(storage)->start(policy_context(context), &output_schema);
        }

        void queue_policy_stop(const void *, void *storage)
        {
            MemoryUtils::cast<detail::QueuePolicyStorage>(storage)->stop();
        }

        [[nodiscard]] detail::PushSourceSendResult queue_policy_try_send(
            const void *context, void *storage, Value value)
        {
            return MemoryUtils::cast<detail::QueuePolicyStorage>(storage)->try_send(
                policy_context(context), std::move(value));
        }

        [[nodiscard]] detail::PushSourceSendResult queue_policy_send_blocking(
            const void *context, void *storage, Value value)
        {
            return MemoryUtils::cast<detail::QueuePolicyStorage>(storage)->send_blocking(
                policy_context(context), std::move(value));
        }

        [[nodiscard]] bool queue_policy_emit_next(
            const void *, void *storage, const TSOutputView &output)
        {
            auto item = MemoryUtils::cast<detail::QueuePolicyStorage>(storage)->try_pop();
            if (!item.has_value())
            {
                return false;
            }

            apply_delta(output, item->value.view());
            return item->more_pending;
        }

        [[nodiscard]] bool burst_policy_emit_next(
            const void *, void *storage, const TSOutputView &output)
        {
            auto *queue = MemoryUtils::cast<detail::QueuePolicyStorage>(storage);
            auto values = queue->take_all();
            if (values.empty())
            {
                return false;
            }

            apply_current_value(output, queue->make_burst(std::move(values)).view());
            return false;
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

        [[nodiscard]] detail::PushSourceSendResult conflating_policy_try_send(
            const void *context, void *storage, Value value)
        {
            return MemoryUtils::cast<detail::ConflatingPolicyStorage>(storage)->try_send(
                policy_context(context), std::move(value));
        }

        [[nodiscard]] detail::PushSourceSendResult conflating_policy_send_blocking(
            const void *context, void *storage, Value value)
        {
            return MemoryUtils::cast<detail::ConflatingPolicyStorage>(storage)->send_blocking(
                policy_context(context), std::move(value));
        }

        [[nodiscard]] bool conflating_policy_emit_next(
            const void *, void *storage, const TSOutputView &output)
        {
            auto accumulated = MemoryUtils::cast<detail::ConflatingPolicyStorage>(storage)->take_accumulated();
            if (!accumulated.has_value())
            {
                return false;
            }

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
                .try_send_impl = &queue_policy_try_send,
                .send_blocking_impl = &queue_policy_send_blocking,
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
                .try_send_impl = &conflating_policy_try_send,
                .send_blocking_impl = &conflating_policy_send_blocking,
                .emit_next_impl = &conflating_policy_emit_next,
                .pending_items_impl = &conflating_policy_pending_items,
            };
            return ops;
        }

        [[nodiscard]] const detail::PushSourcePolicyOps &burst_policy_ops()
        {
            static const detail::PushSourcePolicyOps ops{
                .storage_plan = &MemoryUtils::plan_for<detail::QueuePolicyStorage>(),
                .sender_schema_impl = &sender_schema_impl,
                .output_compatible_impl = &burst_output_compatible,
                .start_impl = &burst_policy_start,
                .stop_impl = &queue_policy_stop,
                .try_send_impl = &queue_policy_try_send,
                .send_blocking_impl = &queue_policy_send_blocking,
                .emit_next_impl = &burst_policy_emit_next,
                .pending_items_impl = &queue_policy_pending_items,
            };
            return ops;
        }

        struct PushSourceNodeContext
        {
            const TSValueTypeMetaData *output_schema{nullptr};
            PushSourcePolicy           policy{};
            std::size_t                policy_storage_offset{0};
            std::size_t                sender_control_storage_offset{0};
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
            std::size_t sender_control_storage_offset,
            PushSourceStartViewCallback on_start,
            std::function<void(const NodeView &)> on_stop)
        {
            auto context = std::make_unique<PushSourceNodeContext>(PushSourceNodeContext{
                .output_schema = &output_schema,
                .policy = policy,
                .policy_storage_offset = policy_storage_offset,
                .sender_control_storage_offset = sender_control_storage_offset,
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

        [[nodiscard]] std::shared_ptr<detail::PushSourceSenderControl> &
        sender_control_storage(const PushSourceNodeContext &context, void *memory)
        {
            return *MemoryUtils::cast<std::shared_ptr<detail::PushSourceSenderControl>>(
                MemoryUtils::advance(memory, context.sender_control_storage_offset));
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

        void push_source_start(const PushSourceNodeContext &context,
                               const NodeView &view, DateTime evaluation_time)
        {
            void *storage = policy_storage(context, view.data());
            auto &control = sender_control_storage(context, view.data());
            detail::PushSourcePolicyAccess::start(context.policy, storage, *context.output_schema);
            auto rollback = UnwindCleanupGuard([&] {
                if (control) { control->begin_close(); }
                detail::PushSourcePolicyAccess::stop(context.policy, storage);
                if (control)
                {
                    control->wait_for_quiescence();
                    control->detach();
                    control.reset();
                }
            });
            control = std::make_shared<detail::PushSourceSenderControl>(
                context.policy, storage, view.graph().root().executor().push_queue_engine(),
                view.graph().type_realization());
            if (context.on_start)
            {
                context.on_start(detail::PushSourcePolicyAccess::make_sender(control),
                                 view, evaluation_time);
            }
            rollback.release();
        }

        void push_source_eval(const PushSourceNodeContext &context, const NodeView &view, DateTime evaluation_time)
        {
            void *storage = policy_storage(context, view.data());
            const bool more_pending = detail::PushSourcePolicyAccess::emit_next(
                context.policy, storage, view.output(evaluation_time));
            if (more_pending)
            {
                view.graph().root().executor().push_queue_engine().mark_push_update_pending();
            }
        }

        void push_source_stop(const PushSourceNodeContext &context, const NodeView &view)
        {
            auto &control_slot = sender_control_storage(context, view.data());
            auto control = control_slot;
            if (control)
            {
                control->begin_close();
            }
            detail::PushSourcePolicyAccess::stop(context.policy, policy_storage(context, view.data()));
            if (control)
            {
                control->wait_for_quiescence();
            }
            auto detach = make_scope_exit([&] {
                if (control) { control->detach(); }
                control_slot.reset();
            });
            if (context.on_stop)
            {
                context.on_stop(view);
            }
        }

        [[nodiscard]] PushSourcePolicy make_policy(const detail::PushSourcePolicyOps &ops,
                                                   const ValueTypeMetaData &sender_schema,
                                                   const ValueTypeMetaData *authored_schema,
                                                   std::size_t max_pending = 0)
        {
            return detail::PushSourcePolicyAccess::make_policy(
                &ops, &register_policy_context(sender_schema, authored_schema, max_pending));
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

    PushSourcePolicy detail::PushSourcePolicyAccess::make_policy(
        const PushSourcePolicyOps *ops, const void *context) noexcept
    {
        return PushSourcePolicy{ops, context};
    }

    const MemoryUtils::StoragePlan &detail::PushSourcePolicyAccess::storage_plan(
        const PushSourcePolicy &policy)
    {
        return *policy.ops_->storage_plan;
    }

    PushSourceSender detail::PushSourcePolicyAccess::make_sender(
        std::shared_ptr<PushSourceSenderControl> control) noexcept
    {
        return PushSourceSender{std::move(control)};
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

    std::size_t detail::PushSourcePolicyAccess::pending_items(const PushSourcePolicy &policy,
                                                              const void *storage) noexcept
    {
        return policy.ops_->pending_items_impl(policy.context_, storage);
    }

    PushSourceSender::PushSourceSender() noexcept
        : ops_(&stopped_sender_ops())
    {
    }

    PushSourceSender::PushSourceSender(std::shared_ptr<detail::PushSourceSenderControl> control) noexcept
        : ops_(&active_sender_ops()), control_(std::move(control))
    {
    }

    PushSourceSender::PushSourceSender(PushSourceSender &&other) noexcept
        : ops_(std::exchange(other.ops_, &stopped_sender_ops())),
          control_(std::move(other.control_))
    {
    }

    PushSourceSender &PushSourceSender::operator=(PushSourceSender &&other) noexcept
    {
        if (this != &other)
        {
            control_ = std::move(other.control_);
            ops_ = std::exchange(other.ops_, &stopped_sender_ops());
        }
        return *this;
    }

    bool PushSourceSender::valid() const noexcept
    {
        return ops_->valid_impl(control_.get());
    }

    const TypeRealizationSnapshot *PushSourceSender::type_realization() const noexcept
    {
        return ops_->type_realization_impl(control_.get());
    }

    bool PushSourceSender::try_send(Value value) const
    {
        return ops_->try_send_impl(control_.get(), std::move(value));
    }

    bool PushSourceSender::send_blocking(Value value) const
    {
        return ops_->send_blocking_impl(control_.get(), std::move(value));
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
            case PushSourcePolicyKind::Burst:
                throw std::invalid_argument(
                    "Burst push-source policy requires a complete "
                    "TS[tuple[SCALAR, ...]] output schema");
        }
        throw std::invalid_argument("Unknown push-source policy kind");
    }

    PushSourcePolicy make_push_source_queue_policy(
        const ValueTypeMetaData &sender_schema, std::size_t max_pending)
    {
        return make_policy(queue_policy_ops(), sender_schema, nullptr, max_pending);
    }

    PushSourcePolicy make_push_source_queue_policy(
        const TSValueTypeMetaData &output_schema, std::size_t max_pending)
    {
        return make_policy(
            queue_policy_ops(), *output_schema.delta_value_schema,
            output_schema.authored_delta_schema, max_pending);
    }

    PushSourcePolicy make_push_source_burst_policy(
        const TSValueTypeMetaData &output_schema, std::size_t max_pending)
    {
        const auto *value_schema = output_schema.value_schema;
        if (output_schema.kind != TSTypeKind::TS || value_schema == nullptr ||
            value_schema->try_value_kind() != ValueTypeKind::List ||
            value_schema->fixed_size != 0 || !value_schema->is_variadic_tuple() ||
            value_schema->is_mutable() || value_schema->is_nullable() ||
            value_schema->element_type == nullptr)
        {
            throw std::invalid_argument("Burst push source requires TS[tuple[SCALAR, ...]] output");
        }
        return make_policy(
            burst_policy_ops(), *value_schema->element_type, nullptr, max_pending);
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
        NodeBuilder make_push_source_node_impl(
            const TSValueTypeMetaData &output_schema,
            PushSourcePolicy policy,
            PushSourceNodeExtension extension,
            bool requires_phase_runner)
        {
            if (!policy.output_compatible(output_schema))
            {
                throw std::invalid_argument("Push source policy is not compatible with the output schema");
            }

            NodeTypeMetaData schema;
            schema.display_name = "push_source";
            schema.output_schema = &output_schema;
            schema.node_kind = NodeKind::PushSource;
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
                NodeStorageField{
                    push_source_sender_control_field_name,
                    &MemoryUtils::plan_for<std::shared_ptr<detail::PushSourceSenderControl>>(),
                },
            };
            const auto &plan = node_storage_plan_for(schema, fields);
            const auto *context = &register_push_source_node_context(
                output_schema,
                policy,
                plan.component(push_source_policy_field_name).offset,
                plan.component(push_source_sender_control_field_name).offset,
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

    NodeBuilder make_push_source_node(
        const TSValueTypeMetaData &output_schema,
        PushSourcePolicy policy,
        PushSourceStartCallback on_start,
        bool requires_phase_runner)
    {
        return make_push_source_node_impl(
            output_schema, std::move(policy),
            PushSourceNodeExtension{
                .on_start = [on_start = std::move(on_start)](
                    PushSourceSender sender, const NodeView &, DateTime) {
                    if (on_start) { on_start(std::move(sender)); }
                },
            },
            requires_phase_runner);
    }

    NodeBuilder make_push_source_node_with_view(
        const TSValueTypeMetaData &output_schema,
        PushSourcePolicy policy,
        PushSourceNodeExtension extension,
        bool requires_phase_runner)
    {
        return make_push_source_node_impl(
            output_schema, std::move(policy), std::move(extension),
            requires_phase_runner);
    }

    NodeBuilder make_push_source_node(
        const TSValueTypeMetaData &output_schema,
        PushSourceStartCallback on_start)
    {
        return make_push_source_node(
            output_schema,
            make_push_source_queue_policy(output_schema),
            std::move(on_start),
            false);
    }

}  // namespace hgraph
