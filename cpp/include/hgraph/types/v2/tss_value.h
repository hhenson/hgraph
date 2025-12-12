#pragma once

#include <concepts>
#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>

#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/ts_traits.h>
#include <hgraph/types/v2/any_value.h>
#include <hgraph/types/v2/ts_value.h>
#include <hgraph/types/v2/tss_event.h>
#include <hgraph/util/date_time.h>

namespace hgraph
{

    /**
     * @brief Base implementation class for type-erased time series set storage.
     *
     * This is the core implementation (PIMPL) that holds the actual shared state
     * between TSSInput and TSSOutput. It uses AnyValue for type-erased item storage,
     * eliminating template proliferation.
     *
     * TSSValue is the virtual interface. Implementations:
     * - NonBoundTSSValue: For unbound inputs (returns defaults, tracks active as bool)
     * - PeeredTSSValue: For bound inputs/outputs (shared state, subscriber management)
     *
     * Design principles:
     * - Single source of truth: All state lives here, shared via shared_ptr
     * - Type erasure: AnyValue<> stores set items without templates
     * - Swappable implementations: Virtual interface allows different state machine variants
     */
    struct HGRAPH_EXPORT TSSValue
    {
        using s_ptr = std::shared_ptr<TSSValue>;

        // Event handling
        virtual void                       apply_event(const TsSetEventAny &event) = 0;
        [[nodiscard]] virtual TsSetEventAny query_event(engine_time_t t) const      = 0;
        virtual void                       reset()                                 = 0;

        // Set operations
        virtual void add_item(const AnyValue<> &item)    = 0;
        virtual void remove_item(const AnyValue<> &item) = 0;
        virtual void clear_items(engine_time_t t)        = 0;

        // State queries
        [[nodiscard]] virtual bool          contains(const AnyValue<> &item) const = 0;
        [[nodiscard]] virtual size_t        size() const                           = 0;
        [[nodiscard]] virtual bool          empty() const                          = 0;
        [[nodiscard]] virtual bool          modified(engine_time_t t) const        = 0;
        [[nodiscard]] virtual bool          all_valid() const                      = 0;
        [[nodiscard]] virtual bool          valid() const                          = 0;
        [[nodiscard]] virtual engine_time_t last_modified_time() const             = 0;

        // Value access - returns reference to internal set representation
        // Note: For iteration, use the visitor pattern or get a copy
        [[nodiscard]] virtual std::vector<AnyValue<>> values() const = 0;

        // Delta access (current cycle)
        [[nodiscard]] virtual const std::vector<AnyValue<>> &added_items() const   = 0;
        [[nodiscard]] virtual const std::vector<AnyValue<>> &removed_items() const = 0;
        [[nodiscard]] virtual bool was_added(const AnyValue<> &item) const         = 0;
        [[nodiscard]] virtual bool was_removed(const AnyValue<> &item) const       = 0;

        // Subscriber management (for active state)
        virtual void add_subscriber(Notifiable *subscriber)       = 0;
        virtual void remove_subscriber(Notifiable *subscriber)    = 0;
        virtual bool has_subscriber(Notifiable *subscriber) const = 0;
        virtual void notify_subscribers(engine_time_t t)          = 0;

        // Type information
        [[nodiscard]] virtual const std::type_info &element_type() const                 = 0;
        [[nodiscard]] virtual bool                  is_element_instanceof(const std::type_info &type) const = 0;

        // Event generation
        virtual void mark_invalid(engine_time_t t) = 0;

        virtual ~TSSValue() = default;
    };

    struct TSSInput;

    /**
     * @brief Manages reference outputs for TSS (contains and is_empty).
     *
     * TSS can provide child TS[bool] outputs:
     * - contains(item): TS[bool] that ticks True when item is in set, False when removed
     * - is_empty(): TS[bool] that tracks whether the set is empty
     */
    struct HGRAPH_EXPORT TSSRefOutputManager
    {
        explicit TSSRefOutputManager(NotifiableContext *owner, const std::type_info &bool_type);
        ~TSSRefOutputManager();

        /// Get or create a contains output for an item
        TSOutput *get_contains_output(const AnyValue<> &item);

        /// Release a contains output (decrement ref count, destroy if zero)
        void release_contains_output(const AnyValue<> &item);

        /// Get the is_empty output (creates on first access)
        TSOutput &is_empty_output();

        /// Called when items are added to the set
        void on_items_added(const std::vector<AnyValue<>> &items, engine_time_t t);

        /// Called when items are removed from the set
        void on_items_removed(const std::vector<AnyValue<>> &items, engine_time_t t);

        /// Called when set is cleared
        void on_cleared(engine_time_t t);

        /// Called when set becomes non-empty
        void on_became_non_empty(engine_time_t t);

        /// Called when set becomes empty
        void on_became_empty(engine_time_t t);

      private:
        NotifiableContext *_owner;

        // Contains outputs: one TS[bool] per tracked item
        struct ContainsEntry
        {
            std::unique_ptr<TSOutput> output;
            size_t                    ref_count{1};
        };

        // Use a custom hash for AnyValue
        struct AnyValueHash
        {
            size_t operator()(const AnyValue<> &v) const { return v.hash_code(); }
        };

        std::unordered_map<AnyValue<>, ContainsEntry, AnyValueHash> _contains_outputs;

        // Single is_empty output
        std::unique_ptr<TSOutput> _is_empty_output;
    };

    /**
     * @brief Type-erased time series set output (event generator).
     *
     * Thin wrapper around TSSValue that uses AnyValue for storage.
     * Multiple inputs can bind to the same output, sharing the impl.
     *
     * Size: 24 bytes (impl ptr + owner ptr + ref manager ptr)
     */
    struct HGRAPH_EXPORT TSSOutput
    {
        using impl_ptr = TSSValue::s_ptr;

        // Construction
        explicit TSSOutput(NotifiableContext *owner, const std::type_info &element_type);

        // Move semantics
        TSSOutput(TSSOutput &&) noexcept;
        TSSOutput &operator=(TSSOutput &&) noexcept;

        // Delete copy operations
        TSSOutput(const TSSOutput &)            = delete;
        TSSOutput &operator=(const TSSOutput &) = delete;

        ~TSSOutput();

        // Set operations
        void add(const AnyValue<> &item);
        void remove(const AnyValue<> &item);
        void clear();

        /// Set multiple items at once (added and removed)
        void set_delta(const std::vector<AnyValue<>> &added, const std::vector<AnyValue<>> &removed);

        /// Set delta from event
        void apply_event(const TsSetEventAny &event);

        /// Invalidate the set
        void invalidate();

        /// Reset the set state (clear values, no notification)
        void reset();

        // State queries
        [[nodiscard]] bool          contains(const AnyValue<> &item) const;
        [[nodiscard]] size_t        size() const;
        [[nodiscard]] bool          empty() const;
        [[nodiscard]] bool          modified() const;
        [[nodiscard]] bool          valid() const;
        [[nodiscard]] engine_time_t last_modified_time() const;

        // Value access
        [[nodiscard]] std::vector<AnyValue<>> values() const;

        // Delta access
        [[nodiscard]] const std::vector<AnyValue<>> &added() const;
        [[nodiscard]] const std::vector<AnyValue<>> &removed() const;
        [[nodiscard]] bool                           was_added(const AnyValue<> &item) const;
        [[nodiscard]] bool                           was_removed(const AnyValue<> &item) const;

        [[nodiscard]] TsSetEventAny delta_value() const;

        // Reference outputs
        [[nodiscard]] TSOutput *get_contains_output(const AnyValue<> &item);
        void                    release_contains_output(const AnyValue<> &item);
        [[nodiscard]] TSOutput &is_empty_output();

        // Current time accessor (delegates to owner)
        [[nodiscard]] engine_time_t current_time() const;

        // Owner access
        [[nodiscard]] Notifiable *owner() const;
        void                      set_owner(NotifiableContext *owner);

        // Subscription management
        void subscribe(Notifiable *notifier);
        void unsubscribe(Notifiable *notifier);

        // Type information
        [[nodiscard]] const std::type_info &element_type() const;

        // Internal access for binding
        [[nodiscard]] impl_ptr get_impl() const { return _impl; }

      protected:
        void notify_parent(engine_time_t t) const;

      private:
        friend TSSInput;
        impl_ptr                              _impl;
        NotifiableContext                    *_owner;
        std::unique_ptr<TSSRefOutputManager> _ref_outputs;
    };

    /**
     * @brief Type-erased time series set input (event consumer).
     *
     * Thin wrapper that binds to a TSSOutput by sharing its impl.
     * Provides read-only access to the set. Implements Notifiable to receive
     * notifications when the bound output changes.
     *
     * Size: 24 bytes (impl ptr + owner ptr + prev_impl ptr)
     */
    struct HGRAPH_EXPORT TSSInput final : Notifiable
    {
        using impl_ptr = TSSValue::s_ptr;

        // Construction
        explicit TSSInput(NotifiableContext *owner, const std::type_info &element_type);

        // Destructor - unsubscribe from impl if active
        ~TSSInput() override;

        // Move semantics
        TSSInput(TSSInput &&) noexcept;
        TSSInput &operator=(TSSInput &&) noexcept;

        // Delete copy operations
        TSSInput(const TSSInput &)            = delete;
        TSSInput &operator=(const TSSInput &) = delete;

        // Binding
        void bind_output(TSSOutput &output);
        void copy_from_input(TSSInput &input);
        void unbind();
        [[nodiscard]] bool bound() const;

        // Active state
        [[nodiscard]] bool active() const;
        void               make_active();
        void               make_passive();

        // State queries (delegate to impl)
        [[nodiscard]] bool          contains(const AnyValue<> &item) const;
        [[nodiscard]] size_t        size() const;
        [[nodiscard]] bool          empty() const;
        [[nodiscard]] bool          modified() const;
        [[nodiscard]] bool          valid() const;
        [[nodiscard]] engine_time_t last_modified_time() const;

        // Value access
        [[nodiscard]] std::vector<AnyValue<>> values() const;

        // Delta access (may compute from prev_output for accurate cross-binding deltas)
        [[nodiscard]] std::vector<AnyValue<>> added() const;
        [[nodiscard]] std::vector<AnyValue<>> removed() const;
        [[nodiscard]] bool                    was_added(const AnyValue<> &item) const;
        [[nodiscard]] bool                    was_removed(const AnyValue<> &item) const;

        [[nodiscard]] TsSetEventAny delta_value() const;

        // Notifiable interface
        void notify(engine_time_t et) override;

        // Current time accessor (delegates to owner)
        [[nodiscard]] engine_time_t current_time() const;

        // Owner access
        [[nodiscard]] NotifiableContext *owner() const;
        void                             set_owner(NotifiableContext *owner);

        // Subscription management (for nested structures)
        void subscribe(Notifiable *notifier);
        void unsubscribe(Notifiable *notifier);

        // Type information
        [[nodiscard]] const std::type_info &element_type() const;

      protected:
        void add_before_evaluation_notification(std::function<void()> &&fn) const;
        void add_after_evaluation_notification(std::function<void()> &&fn) const;
        void bind(impl_ptr &other);

      private:
        impl_ptr           _impl;
        NotifiableContext *_owner;
        impl_ptr           _prev_impl;  // For computing accurate deltas across rebinding
    };

    // Factory functions for template convenience
    template <typename T>
    TSSOutput make_tss_output(NotifiableContext *owner, const std::type_info &element_type = typeid(T)) {
        return TSSOutput(owner, element_type);
    }

    template <typename T>
    TSSInput make_tss_input(NotifiableContext *owner, const std::type_info &element_type = typeid(T)) {
        return TSSInput(owner, element_type);
    }

}  // namespace hgraph
