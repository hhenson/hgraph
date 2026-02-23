//
// Created by Howard Henson on 10/05/2025.
//

#ifndef TSD_H
#define TSD_H

#include <hgraph/builders/input_builder.h>
#include <hgraph/builders/output_builder.h>
#include <hgraph/builders/time_series_types/time_series_dict_input_builder.h>
#include <hgraph/builders/time_series_types/time_series_dict_output_builder.h>
#include <hgraph/types/base_time_series.h>
#include <hgraph/types/feature_extension.h>
#include <hgraph/types/tss.h>
#include <hgraph/types/value/value.h>
#include <ranges>

namespace hgraph
{
    // ValueHash and ValueEqual are defined in feature_extension.h

    // TSDKeyObserver: Used to track additions and removals of parent keys.
    // Since the TSD is dynamic, the inputs associated with an output need to be updated when a key is added or removed
    // to correctly manage its internal state.
    struct TSDKeyObserver
    {
        // Called when a key is added
        virtual void on_key_added(const value::View &key) = 0;

        // Called when a key is removed
        virtual void on_key_removed(const value::View &key) = 0;

        virtual ~TSDKeyObserver() = default;
    };

    template <typename T_TS>
        requires TimeSeriesT<T_TS>
    struct TimeSeriesDict : T_TS
    {
        // Map concrete Base types back to interface types for collections
        using ts_type     = std::conditional_t<std::is_base_of_v<TimeSeriesInput, T_TS>, TimeSeriesInput, TimeSeriesOutput>;
        using ts_type_s_ptr = std::shared_ptr<ts_type>;  // Use interface type for pointers
        using T_TS::T_TS;

        [[nodiscard]] virtual size_t size() const = 0;

        [[nodiscard]] virtual bool has_added() const = 0;

        [[nodiscard]] virtual bool has_removed() const = 0;
    };

    struct TimeSeriesDictOutput : TimeSeriesDict<BaseTimeSeriesOutput>
    {
        using ptr = TimeSeriesDictOutput*;
        using TimeSeriesDict::TimeSeriesDict;

        virtual void py_set_item(const nb::object &key, const nb::object &value) = 0;

        virtual void py_del_item(const nb::object &key) = 0;

        virtual nb::object py_pop(const nb::object &key, const nb::object &default_value) = 0;

        virtual nb::object py_get_ref(const nb::object &key, const nb::object &requester) = 0;

        virtual void py_release_ref(const nb::object &key, const nb::object &requester) = 0;

        // Returns a TimeSeriesSetOutput that tracks the keys in this dict
        [[nodiscard]] virtual TimeSeriesSetOutput &key_set() = 0;

        [[nodiscard]] virtual const TimeSeriesSetOutput &key_set() const = 0;

        VISITOR_SUPPORT()
    };

    struct TimeSeriesDictInput : TimeSeriesDict<BaseTimeSeriesInput>
    {
        using ptr = TimeSeriesDictInput*;
        using TimeSeriesDict<BaseTimeSeriesInput>::TimeSeriesDict;

        // Returns a TimeSeriesSetInput that tracks the keys in this dict
        [[nodiscard]] virtual TimeSeriesSetInput &key_set() = 0;

        [[nodiscard]] virtual const TimeSeriesSetInput &key_set() const = 0;

        VISITOR_SUPPORT()
    };

    struct TimeSeriesDictOutputImpl final : TimeSeriesDictOutput
    {
        using ptr                 = TimeSeriesDictOutputImpl*;
        using value_type          = time_series_output_s_ptr;
        // Non-templated key set - access keys via Value API with elem.as<K>()
        using key_set_type        = TimeSeriesSetOutput;

        // Storage types using Value for type-erased key storage
        using map_type = std::unordered_map<value::Value, value_type, ValueHash, ValueEqual>;
        using reverse_map_type = std::unordered_map<TimeSeriesOutput*, value::Value>;
        // Removed items stores pair<value, was_valid> to preserve validity state from before clearing
        using removed_items_map_type = std::unordered_map<value::Value, std::pair<value_type, bool>, ValueHash, ValueEqual>;
        using item_iterator       = typename map_type::iterator;
        using const_item_iterator = typename map_type::const_iterator;

        explicit TimeSeriesDictOutputImpl(const node_ptr &parent, output_builder_s_ptr ts_builder,
                                          output_builder_s_ptr ts_ref_builder, const value::TypeMeta* key_type);

        explicit TimeSeriesDictOutputImpl(time_series_output_ptr parent, output_builder_s_ptr ts_builder,
                                          output_builder_s_ptr ts_ref_builder, const value::TypeMeta* key_type);

        void py_set_value(const nb::object& value) override;

        void apply_result(const nb::object& value) override;

        bool can_apply_result(const nb::object& result) override;

        void mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) override;

        [[nodiscard]] const map_type &value() const { return _ts_values; }

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        void clear() override;

        void invalidate() override;

        void copy_from_output(const TimeSeriesOutput &output) override;

        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool has_added() const override;

        [[nodiscard]] bool has_removed() const override;

        [[nodiscard]] auto size() const -> size_t override;

        // Value-based API - primary public interface
        [[nodiscard]] bool contains(const value::View &key) const {
            return _ts_values.find(key) != _ts_values.end();
        }

        [[nodiscard]] value_type operator[](const value::View &key);

        [[nodiscard]] value_type operator[](const value::View &key) const;

        [[nodiscard]] const_item_iterator begin() const;

        [[nodiscard]] item_iterator begin();

        [[nodiscard]] const_item_iterator end() const;

        [[nodiscard]] item_iterator end();

        [[nodiscard]] const map_type &modified_items() const { return _modified_items; }

        [[nodiscard]] bool was_modified(const value::View &key) const {
            return _modified_items.find(key) != _modified_items.end();
        }

        [[nodiscard]] const map_type &valid_items() const;

        [[nodiscard]] const map_type &added_items() const;

        [[nodiscard]] bool was_added(const value::View &key) const {
            return key_set().was_added(key);
        }

        [[nodiscard]] const removed_items_map_type &removed_items() const { return _removed_items; }

        [[nodiscard]] bool was_removed(const value::View &key) const {
            return _removed_items.find(key) != _removed_items.end();
        }

        // Returns whether the removed item was valid at the time of removal
        [[nodiscard]] bool was_removed_valid(const value::View &key) const {
            auto it = _removed_items.find(key);
            if (it == _removed_items.end()) { return false; }
            return it->second.second;  // pair<value_type, was_valid>
        }

        [[nodiscard]] TimeSeriesSetOutput &key_set() override;

        [[nodiscard]] const TimeSeriesSetOutput &key_set() const override;

        /**
         * @brief Get the key_set as a shared_ptr for binding operations.
         */
        [[nodiscard]] std::shared_ptr<TimeSeriesSetOutput> key_set_s_ptr() const { return _key_set; }

        void py_set_item(const nb::object &key, const nb::object &value) override;

        void py_del_item(const nb::object &key) override;

        void erase(const value::View &key);

        nb::object py_pop(const nb::object &key, const nb::object &default_value) override;

        nb::object py_get_ref(const nb::object &key, const nb::object &requester) override;

        void py_release_ref(const nb::object &key, const nb::object &requester) override;

        time_series_output_s_ptr& get_ref(const nb::object &key, const void *requester);

        void release_ref(const nb::object &key, const void *requester);

        void add_key_observer(TSDKeyObserver *observer);

        void remove_key_observer(TSDKeyObserver *observer);

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        [[nodiscard]] TimeSeriesKind kind() const override { return TimeSeriesKind::Dict | TimeSeriesKind::Output; }

        [[nodiscard]] value_type get_or_create(const value::View &key);

        [[nodiscard]] bool has_reference() const override;

        VISITOR_SUPPORT()

        // Creates a new time series for the given key and returns it.
        // This allows get_or_create to avoid a second lookup after creation.
        value_type create(const value::View &key);

        [[nodiscard]] const value::TypeMeta* key_type_meta() const { return _key_type; }

        // Return Value-based key from time series pointer
        [[nodiscard]] value::View key_from_ts(TimeSeriesOutput *ts) const;
        [[nodiscard]] value::View key_from_ts(const value_type& ts) const;

    protected:
        friend struct TimeSeriesDictOutputBuilder;

        void _dispose();

        void _clear_key_changes();

        void remove_value(const value::View &key, bool raise_if_not_found);

        // Isolate the modified tracking logic here
        void _clear_key_tracking();

        void _add_key_value(const value::View &key, const value_type &value);

        void _key_updated(const value::View &key);

        void _remove_key_value(const value::View &key, const value_type &value);

    private:
        const value::TypeMeta* _key_type{nullptr};  // Key type schema for Value-based access
        std::shared_ptr<key_set_type> _key_set;

        // Storage using Value keys (type-erased)
        map_type _ts_values;
        reverse_map_type _ts_values_to_keys;
        map_type _modified_items;
        removed_items_map_type _removed_items;  // Stores pair<value, was_valid>
        mutable map_type _valid_items_cache;
        mutable map_type _added_items_cache;  // Instance member instead of static for thread safety
        mutable engine_time_t _valid_items_cache_time{MIN_DT};  // Track when valid_items cache was built
        mutable engine_time_t _added_items_cache_time{MIN_DT};  // Track when added_items cache was built

        output_builder_s_ptr _ts_builder;
        output_builder_s_ptr _ts_ref_builder;

        // Value-based FeatureOutputExtension for ref outputs
        FeatureOutputExtensionValue       _ref_ts_feature;
        std::vector<TSDKeyObserver *>     _key_observers;
        engine_time_t                     _last_cleanup_time{MIN_DT};
        static inline map_type            _empty;
    };

    struct TimeSeriesDictInputImpl : TimeSeriesDictInput, TSDKeyObserver
    {
        using ptr                 = TimeSeriesDictInputImpl*;
        using value_type          = time_series_input_s_ptr;
        // Non-templated key set - access keys via Value API with elem.as<K>()
        using key_set_type        = TimeSeriesSetInput;
        using key_set_type_ptr    = std::shared_ptr<key_set_type>;

        // Storage types using Value for type-erased key storage
        using map_type = std::unordered_map<value::Value, value_type, ValueHash, ValueEqual>;
        using removed_map_type = std::unordered_map<value::Value, std::pair<value_type, bool>, ValueHash, ValueEqual>;
        using reverse_map_type = std::unordered_map<TimeSeriesInput*, value::Value>;
        using item_iterator       = typename map_type::iterator;
        using const_item_iterator = typename map_type::const_iterator;

        explicit TimeSeriesDictInputImpl(const node_ptr &parent, input_builder_s_ptr ts_builder, const value::TypeMeta* key_type);

        explicit TimeSeriesDictInputImpl(time_series_input_ptr parent, input_builder_s_ptr ts_builder, const value::TypeMeta* key_type);

        [[nodiscard]] bool has_peer() const override;

        const_item_iterator begin() const;

        item_iterator begin();

        const_item_iterator end() const;

        item_iterator end();

        [[nodiscard]] size_t size() const override;

        [[nodiscard]] const map_type &value() const { return _ts_values; }

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] bool contains(const value::View &key) const {
            return _ts_values.find(key) != _ts_values.end();
        }

        [[nodiscard]] value_type operator[](const value::View &key) const;

        [[nodiscard]] value_type operator[](const value::View &key);

        [[nodiscard]] const map_type &modified_items() const;

        [[nodiscard]] bool was_modified(const value::View &key) const;

        [[nodiscard]] const map_type &valid_items() const;

        [[nodiscard]] const map_type &added_items() const;

        [[nodiscard]] bool was_added(const value::View &key) const {
            return key_set().was_added(key);
        }

        [[nodiscard]] const map_type &removed_items() const;

        [[nodiscard]] bool was_removed(const value::View &key) const {
            return _removed_items.find(key) != _removed_items.end();
        }

        [[nodiscard]] TimeSeriesSetInput &key_set() override;

        [[nodiscard]] bool has_added() const override;

        [[nodiscard]] bool has_removed() const override;

        [[nodiscard]] const TimeSeriesSetInput &key_set() const override;

        void on_key_added(const value::View &key) override;

        void on_key_removed(const value::View &key) override;

        [[nodiscard]] value_type get_or_create(const value::View &key);

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        [[nodiscard]] TimeSeriesKind kind() const override { return TimeSeriesKind::Dict | TimeSeriesKind::Input; }

        [[nodiscard]] bool has_reference() const override;

        void make_active() override;

        void make_passive() override;

        [[nodiscard]] bool modified() const override;

        /**
         * @brief Override valid() to match Python behavior.
         * TSD input is valid if it was ever sampled (_sample_time > MIN_DT),
         * which allows reading data even when unbound.
         */
        [[nodiscard]] bool valid() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override;

        // Creates a new time series for the given key and returns it.
        value_type create(const value::View &key);

        [[nodiscard]] TimeSeriesDictOutputImpl &output_t();

        [[nodiscard]] const TimeSeriesDictOutputImpl &output_t() const;

        VISITOR_SUPPORT()

        [[nodiscard]] const value::TypeMeta* key_type_meta() const { return _key_type; }

        // Return key from time series pointer
        [[nodiscard]] value::View key_from_ts(TimeSeriesInput *ts) const;
        [[nodiscard]] value::View key_from_ts(value_type ts) const;

    protected:
        void notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;

        bool do_bind_output(time_series_output_s_ptr value) override;

        void do_un_bind_output(bool unbind_refs) override;

        [[nodiscard]] bool was_removed_valid(const value::View &key) const;

        void reset_prev();

        void clear_key_changes();

        void register_clear_key_changes() const;

        void register_clear_key_changes();

        // Isolate modified tracking here.
        void _clear_key_tracking();

        void _add_key_value(const value::View &key, const value_type &value);

        void _key_updated(const value::View &key);

        void _remove_key_value(const value::View &key, const value_type &value);

    private:
        friend struct TimeSeriesDictInputBuilder;

        const value::TypeMeta* _key_type{nullptr};  // Key type schema for Value-based access
        key_set_type_ptr _key_set;

        // Storage using Value keys (type-erased)
        map_type _ts_values;
        reverse_map_type _ts_values_to_keys;
        map_type _modified_items;
        removed_map_type _removed_items;
        mutable map_type _valid_items_cache;
        mutable map_type _added_items_cache;
        mutable map_type _removed_items_cache;
        mutable map_type _modified_items_cache;
        mutable engine_time_t _valid_items_cache_time{MIN_DT};  // Track when valid_items cache was built
        mutable engine_time_t _added_items_cache_time{MIN_DT};  // Track when added_items cache was built
        static inline map_type empty_{};

        input_builder_s_ptr _ts_builder;

        TimeSeriesDictOutputImpl::ptr _prev_output{nullptr};

        engine_time_t _last_modified_time{MIN_DT};
        bool          _has_peer{false};
        mutable bool  _clear_key_changes_registered{false};
    };

} // namespace hgraph

#endif  // TSD_H