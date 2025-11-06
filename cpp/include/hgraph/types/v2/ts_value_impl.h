#pragma once

#include <hgraph/types/ts_traits.h>
#include <hgraph/types/v2/any_value.h>
#include <hgraph/types/v2/ts_value.h>
#include <hgraph/types/v2/ts_value_helpers.h>
#include <hgraph/util/date_time.h>
#include <memory>
#include <typeinfo>
#include <unordered_set>

namespace hgraph
{

    struct DelegateTSValue : TSValue
    {
        explicit DelegateTSValue(TSValue::s_ptr ts_value);

        [[nodiscard]] TsEventAny query_event(engine_time_t t) const override;

        void apply_event(const TsEventAny &event) override;
        void bind_to(TSValue *value) override;
        void unbind() override;
        void reset() override;
        void add_subscriber(Notifiable *subscriber) override;
        void remove_subscriber(Notifiable *subscriber) override;
        void mark_invalid(engine_time_t t) override;

        [[nodiscard]] bool                  has_subscriber(Notifiable *subscriber) const override;
        [[nodiscard]] bool                  modified(engine_time_t t) const override;
        [[nodiscard]] bool                  all_valid() const override;
        [[nodiscard]] bool                  valid() const override;
        [[nodiscard]] engine_time_t         last_modified_time() const override;
        [[nodiscard]] const AnyValue<>     &value() const override;
        [[nodiscard]] const std::type_info &value_type() const override;
        [[nodiscard]] bool                  is_value_instanceof(const std::type_info &value_type) override;
        void                                notify_subscribers(engine_time_t t) override;

        void swap(TSValue::s_ptr other) { std::swap(_ts_value, other); }

        const TSValue::s_ptr &delegate() const { return _ts_value; }

      private:
        TSValue::s_ptr _ts_value;
    };

    struct BaseTSValue : TSValue
    {
        // Shared state (single source of truth)
        TypeId     _value_type;  // Expected value type
        AnyValue<> _value;       // Current value (type-erased)
        TsEventAny _last_event;  // Most recent event (holds timestamp + kind + value)

        explicit BaseTSValue(const std::type_info &type) : _value_type(TypeId{&type}) {}

        void                                apply_event(const TsEventAny &event) override;
        [[nodiscard]] TsEventAny            query_event(engine_time_t t) const override;
        void                                bind_to(TSValue *) override;
        void                                unbind() override;
        void                                reset() override;
        [[nodiscard]] bool                  modified(engine_time_t t) const override;
        [[nodiscard]] bool                  all_valid() const override;
        [[nodiscard]] bool                  valid() const override;
        [[nodiscard]] engine_time_t         last_modified_time() const override;
        [[nodiscard]] const AnyValue<>     &value() const override;
        [[nodiscard]] const std::type_info &value_type() const override;
        void                                mark_invalid(engine_time_t t) override;
        [[nodiscard]] bool                  is_value_instanceof(const std::type_info &value_type) override;
    };

    struct NoneTSValue final : TSValue
    {
        TypeId     _value_type;  // Expected value type
        AnyValue<> _value;       // Current value (type-erased)

        explicit NoneTSValue(const std::type_info &type);

        void                                apply_event(const TsEventAny &event) override;
        [[nodiscard]] TsEventAny            query_event(engine_time_t t) const override;
        void                                bind_to(TSValue *other) override;
        void                                unbind() override;
        void                                reset() override;
        void                                add_subscriber(Notifiable *subscriber) override;
        void                                remove_subscriber(Notifiable *subscriber) override;
        bool                                has_subscriber(Notifiable *subscriber) const override;
        [[nodiscard]] bool                  modified(engine_time_t t) const override;
        [[nodiscard]] bool                  all_valid() const override;
        [[nodiscard]] bool                  valid() const override;
        [[nodiscard]] engine_time_t         last_modified_time() const override;
        [[nodiscard]] const AnyValue<>     &value() const override;
        [[nodiscard]] const std::type_info &value_type() const override;
        void                                mark_invalid(engine_time_t t) override;
        void                                notify_subscribers(engine_time_t t) override;
        [[nodiscard]] bool                  is_value_instanceof(const std::type_info &value_type) override;
    };

    /**
     * @brief Non-bound implementation for inputs not yet bound to an output.
     *
     * This provides the ability for an input to track values if required but has no
     * peer that is producing values. As such, there is no need for a subscriber as the only
     * subscriber in this model is the input.
     *
     * We can reduce "subscription" to marking the _active flag.
     * This will still perform all other tasks.
     *
     * This also assumes that if the input is managing the state, that it is either in an active
     * state or can do it's own notification / scheduling if required.
     */
    struct NonBoundTSValue final : BaseTSValue
    {
        bool _active{false};  // Active state tracked locally

        explicit NonBoundTSValue(const std::type_info &type) : BaseTSValue(type) {}

        void               add_subscriber(Notifiable *subscriber) override;
        void               remove_subscriber(Notifiable *subscriber) override;
        [[nodiscard]] bool has_subscriber(Notifiable *subscriber) const override;
        void               notify_subscribers(engine_time_t t) override;
    };

    /**
     * @brief Simple peered implementation - direct input-output binding, no references.
     *
     * This is the simplest and most common case:
     * - Direct value updates
     * - No reference tracking
     * - Minimal overhead
     */
    struct PeeredTSValue final : BaseTSValue
    {
        // Shared state (single source of truth)
        std::unordered_set<Notifiable *> _subscribers;  // Notification subscribers

        explicit PeeredTSValue(const std::type_info &type) : BaseTSValue(type) {}

        void               add_subscriber(Notifiable *subscriber) override;
        void               remove_subscriber(Notifiable *subscriber) override;
        void               notify_subscribers(engine_time_t t) override;
        [[nodiscard]] bool has_subscriber(Notifiable *subscriber) const override;
    };

    struct SampledTSValue final : DelegateTSValue
    {
        explicit SampledTSValue(TSValue::s_ptr ts_value, engine_time_t sampled_time)
            : DelegateTSValue(ts_value), _sampled_time(sampled_time) {}

        [[nodiscard]] bool          modified(engine_time_t t) const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;

      private:
        engine_time_t _sampled_time;
    };

    /**
     * This is an input side-only wrapper that provides the ability to manage
     * a reference output bound to a non-reference input. In this model, make_active/make_passive
     * and active are by definition expected to be the same notifiable instance.
     * We hold the value of this to be able to correctly track and manage the state with
     * the underlying outputs as they get switched in and out.
     */
    struct ReferencedTSValue final : DelegateTSValue, Notifiable
    {
        explicit ReferencedTSValue(TSValue::s_ptr reference_ts_value, const std::type_info &type, NotifiableContext *context);
        ~ReferencedTSValue() override;

        void               add_subscriber(Notifiable *subscriber) override;
        void               remove_subscriber(Notifiable *subscriber) override;
        [[nodiscard]] bool has_subscriber(Notifiable *subscriber) const override;
        void               notify_subscribers(engine_time_t t) override;
        void notify(engine_time_t et) override;

      protected:
        void update_binding();
        bool bound() const;
        bool is_active() const;
        void mark_sampled();
        engine_time_t current_time() const;

      private:
        TSValue::s_ptr _reference_ts_value;
        NotifiableContext *_context;
        Notifiable    *_active{nullptr};
    };

    inline bool is_sampled(const TSValue::s_ptr &ts_value) { return dynamic_cast<SampledTSValue *>(ts_value.get()) != nullptr; }

    inline bool is_peered(const TSValue::s_ptr &ts_value) { return dynamic_cast<PeeredTSValue *>(ts_value.get()) != nullptr; }

    inline bool is_non_bound(const TSValue::s_ptr &ts_value) { return dynamic_cast<NonBoundTSValue *>(ts_value.get()) != nullptr; }

    inline bool is_none(const TSValue::s_ptr &ts_value) { return dynamic_cast<NoneTSValue *>(ts_value.get()) != nullptr; }

    inline bool is_bound(const TSValue::s_ptr &ts_value) {
        return !is_non_bound(ts_value) && !is_none(ts_value) &&
               (is_sampled(ts_value) ? is_bound(dynamic_cast<SampledTSValue *>(ts_value.get())->delegate()) : true);
    }

}  // namespace hgraph
