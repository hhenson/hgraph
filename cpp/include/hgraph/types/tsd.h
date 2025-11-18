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
#include <hgraph/types/tss.h>
#include <hgraph/types/time_series_visitor.h>
#include <ranges>

namespace hgraph {
    // TSDKeyObserver: Used to track additions and removals of parent keys.
    // Since the TSD is dynamic, the inputs associated with an output need to be updated when a key is added or removed
    // to correctly manage its internal state.
    template<typename K>
    struct TSDKeyObserver {
        // Called when a key is added
        virtual void on_key_added(const K &key) = 0;

        // Called when a key is removed
        virtual void on_key_removed(const K &key) = 0;

        virtual ~TSDKeyObserver() = default;
    };

    template<typename T_TS>
        requires TimeSeriesT<T_TS>
    struct TimeSeriesDict : T_TS {
        // Map concrete Base types back to interface types for collections
        using ts_type = std::conditional_t<
            std::is_base_of_v<TimeSeriesInput, T_TS>,
            TimeSeriesInput,
            TimeSeriesOutput
        >;
        using ts_type_ptr = nb::ref<ts_type>;  // Use interface type for pointers
        using T_TS::T_TS;

        [[nodiscard]] virtual size_t size() const = 0;

        // Create a set of Python-based API, for non-object-based instances there will
        // be typed analogues.
        [[nodiscard]] virtual nb::object py_get_item(const nb::object &item) const = 0;

        [[nodiscard]] virtual nb::object py_get(const nb::object &item, const nb::object &default_value) const = 0;

        [[nodiscard]] virtual nb::object py_get_or_create(const nb::object &key) = 0;

        virtual void py_create(const nb::object &item) = 0;

        [[nodiscard]] virtual nb::iterator py_iter() = 0;

        [[nodiscard]] virtual bool py_contains(const nb::object &item) const = 0;

        [[nodiscard]] virtual nb::object py_key_set() const = 0;

        [[nodiscard]] virtual nb::iterator py_keys() const = 0;

        [[nodiscard]] virtual nb::iterator py_values() const = 0;

        [[nodiscard]] virtual nb::iterator py_items() const = 0;

        [[nodiscard]] virtual nb::iterator py_modified_keys() const = 0;

        [[nodiscard]] virtual nb::iterator py_modified_values() const = 0;

        [[nodiscard]] virtual nb::iterator py_modified_items() const = 0;

        [[nodiscard]] virtual bool py_was_modified(const nb::object &key) const = 0;

        [[nodiscard]] virtual nb::iterator py_valid_keys() const = 0;

        [[nodiscard]] virtual nb::iterator py_valid_values() const = 0;

        [[nodiscard]] virtual nb::iterator py_valid_items() const = 0;

        [[nodiscard]] virtual nb::iterator py_added_keys() const = 0;

        [[nodiscard]] virtual nb::iterator py_added_values() const = 0;

        [[nodiscard]] virtual nb::iterator py_added_items() const = 0;

        [[nodiscard]] virtual bool has_added() const = 0;

        [[nodiscard]] virtual bool py_was_added(const nb::object &key) const = 0;

        [[nodiscard]] virtual nb::iterator py_removed_keys() const = 0;

        [[nodiscard]] virtual nb::iterator py_removed_values() const = 0;

        [[nodiscard]] virtual nb::iterator py_removed_items() const = 0;

        [[nodiscard]] virtual bool has_removed() const = 0;

        [[nodiscard]] virtual bool py_was_removed(const nb::object &key) const = 0;
    };

    struct TimeSeriesDictOutput : TimeSeriesDict<BaseTimeSeriesOutput> {
        using ptr = nb::ref<TimeSeriesDictOutput>;
        using TimeSeriesDict::TimeSeriesDict;

        virtual void py_set_item(const nb::object &key, const nb::object &value) = 0;

        virtual void py_del_item(const nb::object &key) = 0;

        virtual nb::object py_pop(const nb::object &key, const nb::object &default_value) = 0;

        virtual nb::object py_get_ref(const nb::object &key, const nb::object &requester) = 0;

        virtual void py_release_ref(const nb::object &key, const nb::object &requester) = 0;

        // Returns a TimeSeriesSetOutput that tracks the keys in this dict
        [[nodiscard]] virtual TimeSeriesSetOutput &key_set() = 0;
        [[nodiscard]] virtual const TimeSeriesSetOutput &key_set() const = 0;
    };

    struct TimeSeriesDictInput : TimeSeriesDict<BaseTimeSeriesInput> {
        using ptr = nb::ref<TimeSeriesDictInput>;
        using TimeSeriesDict<BaseTimeSeriesInput>::TimeSeriesDict;

        // Returns a TimeSeriesSetInput that tracks the keys in this dict
        [[nodiscard]] virtual TimeSeriesSetInput &key_set() = 0;
        [[nodiscard]] virtual const TimeSeriesSetInput &key_set() const = 0;
    };

    template<typename T_Key>
    using TSDOutBuilder = struct TimeSeriesDictOutputBuilder_T<T_Key>;

    template<typename T_Key>
    struct TimeSeriesDictOutput_T : TimeSeriesDictOutput {
        using ptr = nb::ref<TimeSeriesDictOutput_T>;
        using key_type = T_Key;
        using value_type = time_series_output_ptr;
        using k_set_type = std::unordered_set<key_type>;
        using map_type = std::unordered_map<key_type, value_type>;
        using item_iterator = typename map_type::iterator;
        using const_item_iterator = typename map_type::const_iterator;
        using key_set_type = TimeSeriesSetOutput_T<key_type>;
        // TODO: Currently we are only exposing simple types and nb::object, so this simple strategy is not overly expensive,
        //  If we start using more complicated native types, we may wish to use a pointer so something to that effect to
        //  Track keys. The values have a light weight reference counting cost to store as value_type so leave for the moment as
        //  well.
        // Use raw pointers for reverse lookup to enable efficient lookup from mark_child_modified
        using reverse_map = std::unordered_map<TimeSeriesOutput *, key_type>;

        explicit TimeSeriesDictOutput_T(const node_ptr &parent, output_builder_ptr ts_builder,
                                        output_builder_ptr ts_ref_builder);

        explicit TimeSeriesDictOutput_T(const time_series_type_ptr &parent, output_builder_ptr ts_builder,
                                        output_builder_ptr ts_ref_builder);

        void py_set_value(nb::object value) override;

        void apply_result(nb::object value) override;

        bool can_apply_result(nb::object result) override;

        [[nodiscard]] nb::object py_get(const nb::object &item, const nb::object &default_value) const override;

        void py_create(const nb::object &item) override;

        [[nodiscard]] nb::iterator py_iter() override;

        void mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) override;

        [[nodiscard]] const map_type &value() const;

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        void clear() override;

        void invalidate() override;

        void copy_from_output(const TimeSeriesOutput &output) override;

        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool has_added() const override;

        [[nodiscard]] bool has_removed() const override;

        [[nodiscard]] auto size() const -> size_t override;

        [[nodiscard]] bool py_contains(const nb::object &item) const override;

        [[nodiscard]] bool contains(const key_type &item) const;

        [[nodiscard]] nb::object py_get_item(const nb::object &item) const override;

        [[nodiscard]] nb::object py_get_or_create(const nb::object &key) override;

        [[nodiscard]] ts_type_ptr operator[](const key_type &item);

        [[nodiscard]] ts_type_ptr operator[](const key_type &item) const;

        [[nodiscard]] const_item_iterator begin() const;

        [[nodiscard]] item_iterator begin();

        [[nodiscard]] const_item_iterator end() const;

        [[nodiscard]] item_iterator end();

        [[nodiscard]] nb::iterator py_keys() const override;

        [[nodiscard]] nb::iterator py_values() const override;

        [[nodiscard]] nb::iterator py_items() const override;

        [[nodiscard]] const map_type &modified_items() const;

        [[nodiscard]] nb::iterator py_modified_keys() const override;

        [[nodiscard]] nb::iterator py_modified_values() const override;

        [[nodiscard]] nb::iterator py_modified_items() const override;

        [[nodiscard]] bool py_was_modified(const nb::object &key) const override;

        [[nodiscard]] bool was_modified(const key_type &key) const;

        [[nodiscard]] auto valid_items() const;

        [[nodiscard]] nb::iterator py_valid_keys() const override;

        [[nodiscard]] nb::iterator py_valid_values() const override;

        [[nodiscard]] nb::iterator py_valid_items() const override;

        [[nodiscard]] const k_set_type &added_keys() const;

        [[nodiscard]] nb::iterator py_added_keys() const override;

        [[nodiscard]] nb::iterator py_added_values() const override;

        [[nodiscard]] nb::iterator py_added_items() const override;

        [[nodiscard]] bool py_was_added(const nb::object &key) const override;

        [[nodiscard]] bool was_added(const key_type &key) const;

        [[nodiscard]] const map_type &removed_items() const;

        [[nodiscard]] nb::iterator py_removed_keys() const override;

        [[nodiscard]] nb::iterator py_removed_values() const override;

        [[nodiscard]] nb::iterator py_removed_items() const override;

        [[nodiscard]] bool py_was_removed(const nb::object &key) const override;

        [[nodiscard]] bool was_removed(const key_type &key) const;

        [[nodiscard]] nb::object py_key_set() const override;

        [[nodiscard]] TimeSeriesSetOutput &key_set() override;

        [[nodiscard]] const TimeSeriesSetOutput &key_set() const override;

        [[nodiscard]] TimeSeriesSetOutput_T<key_type> &key_set_t();

        [[nodiscard]] const TimeSeriesSetOutput_T<key_type> &key_set_t() const;

        void py_set_item(const nb::object &key, const nb::object &value) override;

        void py_del_item(const nb::object &key) override;

        void erase(const key_type &key);

        nb::object py_pop(const nb::object &key, const nb::object &default_value) override;

        nb::object py_get_ref(const nb::object &key, const nb::object &requester) override;

        void py_release_ref(const nb::object &key, const nb::object &requester) override;

        time_series_output_ptr get_ref(const key_type &key, const void *requester);

        void release_ref(const key_type &key, const void *requester);

        void add_key_observer(TSDKeyObserver<key_type> *observer);

        void remove_key_observer(TSDKeyObserver<key_type> *observer);

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        // void post_modify() override;

        TimeSeriesOutput::ptr _get_or_create(const key_type &key);

        [[nodiscard]] bool has_reference() const override;

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesOutputVisitor<TimeSeriesDictOutput_T<T_Key>>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesOutputVisitor<TimeSeriesDictOutput_T<T_Key>>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }

    protected:
        friend TSDOutBuilder<T_Key>;

        void _dispose();

        void _clear_key_changes();

        void _create(const key_type &key);

        void remove_value(const key_type &key, bool raise_if_not_found);

        // Isolate the modified tracking logic here
        const key_type &key_from_value(TimeSeriesOutput *value) const;
        void _clear_key_tracking();
        void _add_key_value(const key_type &key, const value_type& value);
        void _key_updated(const key_type& key);
        void _remove_key_value(const key_type &key, const value_type& value);

    private:
        nb::ref<key_set_type> _key_set;
        map_type _ts_values;

        reverse_map _ts_values_to_keys;
        map_type _modified_items;
        map_type _removed_items;
        // This ensures we hold onto the values until we are sure no one needs to reference them.
        mutable map_type _valid_items_cache; // Cache for valid_items() to ensure iterator lifetime safety.

        output_builder_ptr _ts_builder;
        output_builder_ptr _ts_ref_builder;

        FeatureOutputExtension<key_type> _ref_ts_feature;
        std::vector<TSDKeyObserver<key_type> *> _key_observers;
        engine_time_t _last_cleanup_time{MIN_DT};
        static inline map_type _empty;
    };

    template<typename T_Key>
    using TSD_Builder = struct TimeSeriesDictInputBuilder_T<T_Key>;

    template<typename T_Key>
    struct TimeSeriesDictInput_T : TimeSeriesDictInput, TSDKeyObserver<T_Key> {
        using ptr = nb::ref<TimeSeriesDictInput_T>;
        using key_type = T_Key;
        using value_type = time_series_input_ptr;
        using map_type = std::unordered_map<key_type, value_type>;
        using removed_map_type = std::unordered_map<key_type, std::pair<value_type, bool> >;
        using added_map_type = std::unordered_map<key_type, value_type>;
        using modified_map_type = std::unordered_map<key_type, value_type>;
        using item_iterator = typename map_type::iterator;
        using const_item_iterator = typename map_type::const_iterator;
        using key_set_type = TimeSeriesSetInput_T<key_type>;
        using key_set_type_ptr = nb::ref<key_set_type>;
        // Use raw pointers for reverse lookup to enable efficient lookup from notify_parent
        using reverse_map = std::unordered_map<TimeSeriesInput *, key_type>;

        explicit TimeSeriesDictInput_T(const node_ptr &parent, input_builder_ptr ts_builder);

        explicit TimeSeriesDictInput_T(const time_series_type_ptr &parent, input_builder_ptr ts_builder);

        [[nodiscard]] bool has_peer() const override;

        const_item_iterator begin() const;

        item_iterator begin();

        const_item_iterator end() const;

        item_iterator end();

        [[nodiscard]] size_t size() const override;

        [[nodiscard]] const map_type &value() const;

        [[nodiscard]] nb::object py_value() const override;

        [[nodiscard]] nb::object py_delta_value() const override;

        [[nodiscard]] bool py_contains(const nb::object &item) const override;

        [[nodiscard]] bool contains(const key_type &item) const;

        [[nodiscard]] nb::object py_get(const nb::object &item, const nb::object &default_value) const override;

        void py_create(const nb::object &item) override;

        [[nodiscard]] nb::iterator py_iter() override;

        [[nodiscard]] nb::object py_get_item(const nb::object &item) const override;

        [[nodiscard]] nb::object py_get_or_create(const nb::object &key) override;

        [[nodiscard]] value_type operator[](const key_type &item) const;

        [[nodiscard]] value_type operator[](const key_type &item);

        [[nodiscard]] nb::iterator py_keys() const override;

        [[nodiscard]] nb::iterator py_values() const override;

        [[nodiscard]] nb::iterator py_items() const override;

        [[nodiscard]] const map_type &modified_items() const;

        [[nodiscard]] nb::iterator py_modified_keys() const override;

        [[nodiscard]] nb::iterator py_modified_values() const override;

        [[nodiscard]] nb::iterator py_modified_items() const override;

        [[nodiscard]] bool py_was_modified(const nb::object &key) const override;

        [[nodiscard]] bool was_modified(const key_type &key) const;

        [[nodiscard]] const map_type &valid_items() const;

        [[nodiscard]] nb::iterator py_valid_keys() const override;

        [[nodiscard]] nb::iterator py_valid_values() const override;

        [[nodiscard]] nb::iterator py_valid_items() const override;

        [[nodiscard]] const map_type &added_items() const;

        [[nodiscard]] nb::iterator py_added_keys() const override;

        [[nodiscard]] nb::iterator py_added_values() const override;

        [[nodiscard]] nb::iterator py_added_items() const override;

        [[nodiscard]] bool py_was_added(const nb::object &key) const override;

        [[nodiscard]] bool was_added(const key_type &key) const;

        [[nodiscard]] const map_type &removed_items() const;

        [[nodiscard]] nb::iterator py_removed_keys() const override;

        [[nodiscard]] nb::iterator py_removed_values() const override;

        [[nodiscard]] nb::iterator py_removed_items() const override;

        [[nodiscard]] bool py_was_removed(const nb::object &key) const override;

        [[nodiscard]] bool was_removed(const key_type &key) const;

        [[nodiscard]] nb::object py_key_set() const override;

        [[nodiscard]] TimeSeriesSetInput &key_set() override;

        [[nodiscard]] bool has_added() const override;

        [[nodiscard]] bool has_removed() const override;

        [[nodiscard]] const TimeSeriesSetInput &key_set() const override;

        void on_key_added(const key_type &key) override;

        void on_key_removed(const key_type &key) override;

        value_type get_or_create(const key_type &key);

        [[nodiscard]] bool is_same_type(const TimeSeriesType *other) const override;

        [[nodiscard]] bool has_reference() const override;

        void make_active() override;

        void make_passive() override;

        [[nodiscard]] bool modified() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override;

        // Expose this as this is currently used in at least one Python service test.
        void _create(const key_type &key);

        [[nodiscard]] TimeSeriesSetInput_T<key_type> &key_set_t();

        [[nodiscard]] const TimeSeriesSetInput_T<key_type> &key_set_t() const;

        [[nodiscard]] TimeSeriesDictOutput_T<key_type> &output_t();

        [[nodiscard]] const TimeSeriesDictOutput_T<key_type> &output_t() const;

        // Visitor support - Acyclic pattern (runtime dispatch)
        void accept(TimeSeriesVisitor& visitor) override {
            if (auto* typed_visitor = dynamic_cast<TimeSeriesInputVisitor<TimeSeriesDictInput_T<T_Key>>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        void accept(TimeSeriesVisitor& visitor) const override {
            if (auto* typed_visitor = dynamic_cast<ConstTimeSeriesInputVisitor<TimeSeriesDictInput_T<T_Key>>*>(&visitor)) {
                typed_visitor->visit(*this);
            }
        }

        // CRTP visitor support (compile-time dispatch)
        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) {
            return visitor(*this);
        }

        template<typename Visitor>
            requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
        decltype(auto) accept(Visitor& visitor) const {
            return visitor(*this);
        }

    protected:
        void notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override;

        bool do_bind_output(time_series_output_ptr &value) override;

        void do_un_bind_output(bool unbind_refs) override;

        [[nodiscard]] bool was_removed_valid(const key_type &key) const;

        void reset_prev();

        void clear_key_changes();

        void register_clear_key_changes() const;

        void register_clear_key_changes();

        // Isolate modified tracking here.
        [[nodiscard]] const key_type &key_from_value(TimeSeriesInput *value) const;
        [[nodiscard]] const key_type &key_from_value(value_type value) const;
        void _clear_key_tracking();
        void _add_key_value(const key_type &key, const value_type& value);
        void _key_updated(const key_type& key);
        void _remove_key_value(const key_type &key, const value_type& value);

    private:
        friend TSD_Builder<T_Key>;

        key_set_type_ptr _key_set;
        map_type _ts_values;

        reverse_map _ts_values_to_keys;
        mutable map_type _valid_items_cache; // Cache the valid items if called.
        map_type _modified_items; // This is cached for performance reasons.
        mutable map_type _modified_items_cache; // This is cached for performance reasons.
        mutable map_type _added_items_cache;
        mutable map_type _removed_item_cache;
        removed_map_type _removed_items;
        // This ensures we hold onto the values until we are sure no one needs to reference them.
        static inline map_type empty_{};

        input_builder_ptr _ts_builder;

        typename TimeSeriesDictOutput_T<T_Key>::ptr _prev_output;

        engine_time_t _last_modified_time{MIN_DT};
        bool _has_peer{false};
        mutable bool _clear_key_changes_registered{false};
    };

    void tsd_register_with_nanobind(nb::module_ & m);
} // namespace hgraph

#endif  // TSD_H