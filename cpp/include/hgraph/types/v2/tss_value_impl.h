#pragma once

#include <algorithm>
#include <unordered_set>

#include <hgraph/types/ts_traits.h>
#include <hgraph/types/v2/any_value.h>
#include <hgraph/types/v2/tss_event.h>
#include <hgraph/types/v2/tss_value.h>
#include <hgraph/util/date_time.h>

namespace hgraph
{

    /**
     * @brief Hash functor for AnyValue to use in unordered containers.
     */
    struct AnyValueHash
    {
        size_t operator()(const AnyValue<> &v) const { return v.hash_code(); }
    };

    /**
     * @brief Base implementation for TSSValue with shared state.
     *
     * Contains the common state and logic for all TSS implementations:
     * - Set storage (using unordered_set with AnyValue)
     * - Last event tracking
     * - Element type validation
     */
    struct HGRAPH_EXPORT BaseTSSValue : TSSValue
    {
        // Type information
        TypeId _element_type;

        // Set storage - using unordered_set with custom hash
        std::unordered_set<AnyValue<>, AnyValueHash> _value;

        // Last event (holds timestamp, kind, and delta)
        TsSetEventAny _last_event;

        // Validity flag
        bool _valid{false};

        explicit BaseTSSValue(const std::type_info &type) : _element_type(TypeId{&type}) {}

        // Event handling
        void                            apply_event(const TsSetEventAny &event) override;
        [[nodiscard]] TsSetEventAny     query_event(engine_time_t t) const override;
        void                            reset() override;

        // Set operations
        void add_item(const AnyValue<> &item) override;
        void remove_item(const AnyValue<> &item) override;
        void clear_items(engine_time_t t) override;

        // State queries
        [[nodiscard]] bool          contains(const AnyValue<> &item) const override;
        [[nodiscard]] size_t        size() const override;
        [[nodiscard]] bool          empty() const override;
        [[nodiscard]] bool          modified(engine_time_t t) const override;
        [[nodiscard]] bool          all_valid() const override;
        [[nodiscard]] bool          valid() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;

        // Value access
        [[nodiscard]] std::vector<AnyValue<>> values() const override;

        // Delta access
        [[nodiscard]] const std::vector<AnyValue<>> &added_items() const override;
        [[nodiscard]] const std::vector<AnyValue<>> &removed_items() const override;
        [[nodiscard]] bool was_added(const AnyValue<> &item) const override;
        [[nodiscard]] bool was_removed(const AnyValue<> &item) const override;

        // Type information
        [[nodiscard]] const std::type_info &element_type() const override;
        [[nodiscard]] bool is_element_instanceof(const std::type_info &type) const override;

        // Event generation
        void mark_invalid(engine_time_t t) override;

      protected:
        /// Validate that item type matches element type
        void validate_item_type(const AnyValue<> &item) const;

        /// Internal add (no notification)
        bool do_add(const AnyValue<> &item);

        /// Internal remove (no notification)
        bool do_remove(const AnyValue<> &item);
    };

    /**
     * @brief Placeholder implementation for uninitialized TSSValue.
     *
     * Returns defaults for all queries, throws on modification attempts.
     */
    struct HGRAPH_EXPORT NoneTSSValue final : TSSValue
    {
        TypeId _element_type;

        // Empty vectors for delta access
        std::vector<AnyValue<>> _empty_vec;

        explicit NoneTSSValue(const std::type_info &type);

        void                            apply_event(const TsSetEventAny &event) override;
        [[nodiscard]] TsSetEventAny     query_event(engine_time_t t) const override;
        void                            reset() override;

        void add_item(const AnyValue<> &item) override;
        void remove_item(const AnyValue<> &item) override;
        void clear_items(engine_time_t t) override;

        [[nodiscard]] bool          contains(const AnyValue<> &item) const override;
        [[nodiscard]] size_t        size() const override;
        [[nodiscard]] bool          empty() const override;
        [[nodiscard]] bool          modified(engine_time_t t) const override;
        [[nodiscard]] bool          all_valid() const override;
        [[nodiscard]] bool          valid() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;

        [[nodiscard]] std::vector<AnyValue<>> values() const override;

        [[nodiscard]] const std::vector<AnyValue<>> &added_items() const override;
        [[nodiscard]] const std::vector<AnyValue<>> &removed_items() const override;
        [[nodiscard]] bool was_added(const AnyValue<> &item) const override;
        [[nodiscard]] bool was_removed(const AnyValue<> &item) const override;

        void add_subscriber(Notifiable *subscriber) override;
        void remove_subscriber(Notifiable *subscriber) override;
        bool has_subscriber(Notifiable *subscriber) const override;
        void notify_subscribers(engine_time_t t) override;

        [[nodiscard]] const std::type_info &element_type() const override;
        [[nodiscard]] bool is_element_instanceof(const std::type_info &type) const override;

        void mark_invalid(engine_time_t t) override;
    };

    /**
     * @brief Non-bound implementation for inputs not yet bound to an output.
     *
     * Provides the ability for an input to track values if required but has no
     * peer producing values. Subscription is reduced to a simple active flag.
     */
    struct HGRAPH_EXPORT NonBoundTSSValue final : BaseTSSValue
    {
        bool _active{false};

        explicit NonBoundTSSValue(const std::type_info &type) : BaseTSSValue(type) {}

        void               add_subscriber(Notifiable *subscriber) override;
        void               remove_subscriber(Notifiable *subscriber) override;
        [[nodiscard]] bool has_subscriber(Notifiable *subscriber) const override;
        void               notify_subscribers(engine_time_t t) override;
    };

    /**
     * @brief Peered implementation - direct input-output binding.
     *
     * This is the most common case:
     * - Direct value updates
     * - Subscriber set for notifications
     * - Minimal overhead
     */
    struct HGRAPH_EXPORT PeeredTSSValue final : BaseTSSValue
    {
        std::unordered_set<Notifiable *> _subscribers;

        explicit PeeredTSSValue(const std::type_info &type) : BaseTSSValue(type) {}

        void               add_subscriber(Notifiable *subscriber) override;
        void               remove_subscriber(Notifiable *subscriber) override;
        [[nodiscard]] bool has_subscriber(Notifiable *subscriber) const override;
        void               notify_subscribers(engine_time_t t) override;
    };

    /**
     * @brief Delegate implementation that wraps another TSSValue.
     *
     * Used for adding behavior layers (like sampled tracking) without
     * modifying the underlying implementation.
     */
    struct HGRAPH_EXPORT DelegateTSSValue : TSSValue
    {
        explicit DelegateTSSValue(TSSValue::s_ptr delegate);

        [[nodiscard]] TsSetEventAny query_event(engine_time_t t) const override;

        void apply_event(const TsSetEventAny &event) override;
        void reset() override;

        void add_item(const AnyValue<> &item) override;
        void remove_item(const AnyValue<> &item) override;
        void clear_items(engine_time_t t) override;

        [[nodiscard]] bool          contains(const AnyValue<> &item) const override;
        [[nodiscard]] size_t        size() const override;
        [[nodiscard]] bool          empty() const override;
        [[nodiscard]] bool          modified(engine_time_t t) const override;
        [[nodiscard]] bool          all_valid() const override;
        [[nodiscard]] bool          valid() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;

        [[nodiscard]] std::vector<AnyValue<>> values() const override;

        [[nodiscard]] const std::vector<AnyValue<>> &added_items() const override;
        [[nodiscard]] const std::vector<AnyValue<>> &removed_items() const override;
        [[nodiscard]] bool was_added(const AnyValue<> &item) const override;
        [[nodiscard]] bool was_removed(const AnyValue<> &item) const override;

        void add_subscriber(Notifiable *subscriber) override;
        void remove_subscriber(Notifiable *subscriber) override;
        bool has_subscriber(Notifiable *subscriber) const override;
        void notify_subscribers(engine_time_t t) override;

        [[nodiscard]] const std::type_info &element_type() const override;
        [[nodiscard]] bool is_element_instanceof(const std::type_info &type) const override;

        void mark_invalid(engine_time_t t) override;

        void swap(TSSValue::s_ptr other);

        [[nodiscard]] const TSSValue::s_ptr &delegate() const;

      protected:
        TSSValue::s_ptr _delegate;

        // Local subscribers for this delegate layer
        std::set<Notifiable *> _local_subscribers;
    };

    /**
     * @brief Sampled implementation that tracks when input was sampled.
     *
     * Wraps another TSSValue and overrides modified() to return true
     * at the sampled time.
     */
    struct HGRAPH_EXPORT SampledTSSValue final : DelegateTSSValue
    {
        explicit SampledTSSValue(TSSValue::s_ptr delegate, engine_time_t sampled_time)
            : DelegateTSSValue(std::move(delegate)), _sampled_time(sampled_time) {}

        [[nodiscard]] bool          modified(engine_time_t t) const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;

        void               add_subscriber(Notifiable *subscriber) override;
        void               remove_subscriber(Notifiable *subscriber) override;
        [[nodiscard]] bool has_subscriber(Notifiable *subscriber) const override;
        void               notify_subscribers(engine_time_t t) override;

      private:
        engine_time_t _sampled_time;
    };

    // Helper functions for type checking
    inline bool is_sampled_tss(const TSSValue::s_ptr &value) {
        return dynamic_cast<SampledTSSValue *>(value.get()) != nullptr;
    }

    inline bool is_peered_tss(const TSSValue::s_ptr &value) {
        return dynamic_cast<PeeredTSSValue *>(value.get()) != nullptr;
    }

    inline bool is_non_bound_tss(const TSSValue::s_ptr &value) {
        return dynamic_cast<NonBoundTSSValue *>(value.get()) != nullptr;
    }

    inline bool is_none_tss(const TSSValue::s_ptr &value) {
        return dynamic_cast<NoneTSSValue *>(value.get()) != nullptr;
    }

    inline bool is_bound_tss(const TSSValue::s_ptr &value) {
        return !is_non_bound_tss(value) && !is_none_tss(value) &&
               (is_sampled_tss(value) ? is_bound_tss(dynamic_cast<SampledTSSValue *>(value.get())->delegate()) : true);
    }

}  // namespace hgraph
