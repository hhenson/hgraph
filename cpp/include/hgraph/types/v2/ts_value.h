#pragma once

#include <concepts>
#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/ts_traits.h>
#include <hgraph/types/v2/any_value.h>
#include <hgraph/types/v2/ts_event.h>
#include <hgraph/util/date_time.h>
#include <memory>

namespace hgraph
{

    /**
     * @brief Base implementation class for type-erased time series value storage.
     *
     * This is the core implementation (PIMPL) that holds the actual shared state
     * between TSInput and TSOutput. It uses AnyValue for type-erased
     * storage, eliminating template proliferation.
     *
     * TSValue is the virtual interface. Two implementations exist:
     * - NonBoundImpl: For unbound inputs (returns defaults, tracks active as bool)
     * - SimplePeeredImpl: For bound inputs/outputs (shared state, subscriber management)
     *
     * Design principles:
     * - Single source of truth: All state lives here, shared via shared_ptr
     * - Computed properties: valid() and last_modified_time() derived from _last_event
     * - Swappable implementations: Virtual interface allows different state machine variants
     */
    struct HGRAPH_EXPORT TSValue
    {
        using s_ptr = std::shared_ptr<TSValue>;

        // Virtual interface for variant behavior
        virtual void                     apply_event(const TsEventAny &event) = 0;
        [[nodiscard]] virtual TsEventAny query_event(engine_time_t t) const   = 0;

        virtual void reset() = 0;

        // Subscriber management (for active state)
        virtual void add_subscriber(Notifiable *subscriber)       = 0;
        virtual void remove_subscriber(Notifiable *subscriber)    = 0;
        virtual bool has_subscriber(Notifiable *subscriber) const = 0;

        // State queries
        [[nodiscard]] virtual bool          modified(engine_time_t t) const = 0;
        [[nodiscard]] virtual bool          all_valid() const               = 0;
        [[nodiscard]] virtual bool          valid() const                   = 0;
        [[nodiscard]] virtual engine_time_t last_modified_time() const      = 0;

        // Value access
        [[nodiscard]] virtual const AnyValue<> &value() const = 0;

        // Type information
        [[nodiscard]] virtual const std::type_info &value_type() const = 0;

        // Event generation
        virtual void mark_invalid(engine_time_t t) = 0;

        // Notification
        virtual void notify_subscribers(engine_time_t t) = 0;

        [[nodiscard]] virtual bool is_value_instanceof(const std::type_info &value_type) = 0;

        bool is_value_instanceof(const TSValue &value) { return is_value_instanceof(value.value_type()); }

        bool is_value_instanceof(const s_ptr &value) { return is_value_instanceof(value->value_type()); }

        virtual ~TSValue() = default;
    };

    struct TSInput;
    struct ReferencedTSValue;

    /**
     * @brief Type-erased time series output (event generator).
     *
     * Thin wrapper around TSValue that uses AnyValue for storage.
     * Multiple inputs can bind to the same output, sharing the impl.
     *
     * This is the internal implementation used by TimeSeriesValueOutput<T>.
     */
    struct HGRAPH_EXPORT TSOutput
    {
        // NOTE: An output can be nested, and so needs to be notifiable to notify it's parents.
        using impl_ptr = TSValue::s_ptr;

        // Non-template constructor (implementation in .cpp)
        explicit TSOutput(NotifiableContext *owner, const std::type_info &value_type);

        // Move semantics
        TSOutput(TSOutput &&)            = default;
        TSOutput &operator=(TSOutput &&) = default;

        // Delete copy operations
        TSOutput(const TSOutput &)            = delete;
        TSOutput &operator=(const TSOutput &) = delete;

        // Value access (returns AnyValue)
        [[nodiscard]] const AnyValue<> &value() const;

        // Set value with AnyValue
        void set_value(const AnyValue<> &v);

        // Set value with AnyValue move
        void set_value(AnyValue<> &&v);

        // Invalidate the value
        void invalidate();

        // Resets the state of the time-series, this will clear out the values
        // and remove any this leaves parent and subscriptions in place, no
        // notifications will be sent though
        void reset();

        // Delegate to impl
        [[nodiscard]] bool          modified() const;
        [[nodiscard]] bool          valid() const;
        [[nodiscard]] engine_time_t last_modified_time() const;

        [[nodiscard]] TsEventAny delta_value() const;

        // Current time accessor (delegates to parent)
        [[nodiscard]] engine_time_t current_time() const;

        // The parent can also be considered as the interested observer of this output. In the case of the output, this can be
        // a collection output. For the case of output, the outer nodes notify are a nop in the sense that it does not need to be
        // scheduled. However, the wrapper collection may be tracking observers at their level and also update their last updated
        // state.
        [[nodiscard]] Notifiable *owner() const;
        void                      set_owner(NotifiableContext *owner);

        void subscribe(Notifiable *notifier);
        void unsubscribe(Notifiable *notifier);

        const std::type_info &value_type() const;

      protected:
        void notify_parent(engine_time_t t) const;

      private:
        friend TSInput;
        friend ReferencedTSValue;
        impl_ptr           _impl;    // Shared with bound inputs
        NotifiableContext *_owner;  // Owning node (implements both Notifiable and CurrentTimeProvider)
    };

    /**
     * @brief Type-erased time series input (event consumer).
     *
     * Thin wrapper that binds to a TSOutput by sharing its impl.
     * Provides read-only access to the value. Implements Notifiable to receive
     * notifications when the bound output changes.
     *
     * This is the internal implementation used by TimeSeriesValueInput<T>.
     */
    struct HGRAPH_EXPORT TSInput final : Notifiable
    {
        using impl_ptr = TSValue::s_ptr;

        // Non-template constructor (implementation in .cpp)
        explicit TSInput(NotifiableContext *parent, const std::type_info &value_type);

        // Destructor - unsubscribe from impl if active
        ~TSInput();

        // Move semantics
        TSInput(TSInput &&)            = default;
        TSInput &operator=(TSInput &&) = default;

        // Delete copy operations
        TSInput(const TSInput &)            = delete;
        TSInput &operator=(const TSInput &) = delete;

        // Value access (returns AnyValue)
        [[nodiscard]] const AnyValue<> &value() const;

        // Set value with AnyValue (only if we are in a non-bound state)
        void set_value(const AnyValue<> &v);

        // Set value with AnyValue move (only if we are in a non-bound state)
        void set_value(AnyValue<> &&v);

        // Queries delegate to shared impl
        [[nodiscard]] bool modified() const;

        [[nodiscard]] bool valid() const;

        [[nodiscard]] engine_time_t last_modified_time() const;

        // Active state (computed from subscription)
        [[nodiscard]] bool active() const;

        // Mark input as active (adds to the subscriber-set)
        void make_active();

        // Mark input as passive (removes from the subscriber-set)
        void make_passive();

        // Marks the input as being modified for the current engine time.
        // This will make the input show as modified and will notify the parent.
        // NOTE: This may go away, it is used as we transition from the legacy model
        //       to the new model and is important to reflect current implementation logic.
        void mark_sampled();

        // Notifiable interface
        // This is needed as we use our own reference to mark active, and we will be called when notified.
        // The expected flow is: output gets modified, it calls notify on subscribers, we are a subscriber
        // we get notified, this method then notifies the parent, ultimately this will notify the containing Node
        // which will result in the node owning this Input being scheduled.
        void notify(engine_time_t et) override;

        // Current time accessor (delegates to parent)
        [[nodiscard]] engine_time_t current_time() const;

        // The owner can also be considered as the interested observer of this output. In the case of the output, this can be
        // a collection output. For the case of output, the outer nodes notify are a nop in the sense that it does not need to be
        // scheduled. However, the wrapper collection may be tracking observers at their level and also update their last updated
        // state.
        [[nodiscard]] NotifiableContext *owner() const;
        void                             set_owner(NotifiableContext *owner);

        // The input is associated with a model not owned by this input.
        // This mirrors the concept of having the _output set on the old view of TS
        [[nodiscard]] bool bound() const;

        // Bind to output (shares impl) - implementation in .cpp
        void bind_output(TSOutput &output);
        void copy_from_input(TSInput &input);

        void un_bind();

        void subscribe(Notifiable *notifier);
        void unsubscribe(Notifiable *notifier);

        const std::type_info &value_type() const;

        // bind a reference
        //  Need to decide how a reference looks, I think to start with a reference is just like a normal TS with extra magic
      protected:
        void add_before_evaluation_notification(std::function<void()> &&fn) const;
        void add_after_evaluation_notification(std::function<void()> &&fn) const;
        void bind(impl_ptr &other);

      private:
        impl_ptr                _impl;                  // Shared impl
        NotifiableContext      *_owner;                // Owning node (implements both Notifiable and CurrentTimeProvider)
    };

    // Factory functions for template convenience
    template <typename T> TSOutput make_ts_output(NotifiableContext *owner, const std::type_info &value_type = typeid(T)) {
        return TSOutput(owner, value_type);
    }

    template <typename T> TSInput make_ts_input(NotifiableContext *owner, const std::type_info &value_type = typeid(T)) {
        return TSInput(owner, value_type);
    }

}  // namespace hgraph
