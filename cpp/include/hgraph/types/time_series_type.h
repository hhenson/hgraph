#ifndef TIME_SERIES_TYPE_H
#define TIME_SERIES_TYPE_H

#include <hgraph/hgraph_base.h>
#include <hgraph/util/reference_count_subscriber.h>
#include <variant>

namespace hgraph {
    struct HGRAPH_EXPORT TimeSeriesType : nb::intrusive_base {
        using ptr = nb::ref<TimeSeriesType>;

        explicit TimeSeriesType(const node_ptr &parent);

        explicit TimeSeriesType(const ptr &parent);

        TimeSeriesType(const TimeSeriesType &) = default;

        TimeSeriesType(TimeSeriesType &&) = default;

        TimeSeriesType &operator=(const TimeSeriesType &) = default;

        TimeSeriesType &operator=(TimeSeriesType &&) = default;

        ~TimeSeriesType() override = default;

        // Pure virtual methods to be implemented in derived classes

        // Method for owning node
        [[nodiscard]] node_ptr owning_node();

        [[nodiscard]] node_ptr owning_node() const;

        // Method for owning graph
        [[nodiscard]] graph_ptr owning_graph();

        [[nodiscard]] graph_ptr owning_graph() const;

        // Method for value - as python object
        [[nodiscard]] virtual nb::object py_value() const = 0;

        // Method for delta value - as python object
        [[nodiscard]] virtual nb::object py_delta_value() const = 0;

        // Method to check if modified
        [[nodiscard]] virtual bool modified() const = 0;

        // Method to check if valid
        [[nodiscard]] virtual bool valid() const = 0;

        /*
        Is there a valid value associated to this time-series input, or loosely, "has this property
        ever ticked?". Note that it is possible for the time-series to become invalid after it has been made valid.
        The invalidation occurs mostly when working with REF values.
        :return: True if there is a valid value associated with this time-series.
         */
        [[nodiscard]] virtual bool all_valid() const = 0;

        // Method for last modified time
        [[nodiscard]] virtual engine_time_t last_modified_time() const = 0;

        /**
        FOR USE IN LIBRARY CODE.

        Change the owning node / time-series container of this time-series.
        This is used when grafting a time-series input from one node / time-series container to another.
        For example, see use in map implementation.
        */
        void re_parent(const node_ptr &parent);

        void re_parent(const ptr &parent);

        /*
         * This is used to deal with the fact we are not tracking the type in the time-series value.
         * We need to deal with reference vs non-reference detection and the 3 methods below help with that.
         */
        [[nodiscard]] virtual bool is_same_type(const TimeSeriesType *other) const = 0;

        [[nodiscard]] virtual bool is_reference() const;

        [[nodiscard]] virtual bool has_reference() const;

        void reset_parent_or_node();

        // // Overload for re_parent with TimeSeries
        // virtual void re_parent(TimeSeriesType::ptr parent) = 0;

        static void register_with_nanobind(nb::module_ &m);

        static inline time_series_type_ptr null_ptr{};

    protected:
        /*
         * Used to manage access to the parent/node variant.
         */
        ptr &_parent_time_series() const;

        ptr &_parent_time_series();

        bool _has_parent_time_series() const;

        void _set_parent_time_series(TimeSeriesType *ts);

        bool has_parent_or_node() const;

    public:
        [[nodiscard]] bool has_owning_node() const;

    private:
        using TsOrNode = std::variant<ptr, node_ptr>;
        std::optional<TsOrNode> _parent_ts_or_node{};

        /*
         * Utility to extract the owning node associated to this time-series.
         * Will raise if no value set.
         */
        node_ptr _owning_node() const;
    };

    struct TimeSeriesInput;
    struct OutputBuilder;

    struct HGRAPH_EXPORT TimeSeriesOutput : TimeSeriesType {
        using ptr = nb::ref<TimeSeriesOutput>;
        using TimeSeriesType::TimeSeriesType;

        // Pure virtual methods to be implemented in derived classes
        [[nodiscard]] virtual bool modified() const = 0;
        [[nodiscard]] virtual engine_time_t last_modified_time() const = 0;
        virtual void mark_invalid() = 0;
        virtual void mark_modified() = 0;
        virtual void mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) = 0;
        [[nodiscard]] virtual bool valid() const = 0;
        [[nodiscard]] virtual bool all_valid() const = 0;
        [[nodiscard]] virtual ptr parent_output() const = 0;
        [[nodiscard]] virtual ptr parent_output() = 0;
        [[nodiscard]] virtual bool has_parent_output() const = 0;
        virtual void subscribe(Notifiable *node) = 0;
        virtual void un_subscribe(Notifiable *node) = 0;
        virtual void builder_release_cleanup() = 0;
        virtual void py_set_value(nb::object value) = 0;
        virtual bool can_apply_result(nb::object value) = 0;
        virtual void apply_result(nb::object value) = 0;
        virtual void copy_from_output(const TimeSeriesOutput &output) = 0;
        virtual void copy_from_input(const TimeSeriesInput &input) = 0;
        virtual void clear() = 0;
        virtual void invalidate() = 0;
        virtual void mark_modified(engine_time_t modified_time) = 0;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT TimeSeriesInput : TimeSeriesType, Notifiable {
        using ptr = nb::ref<TimeSeriesInput>;
        using TimeSeriesType::TimeSeriesType;

        // Pure virtual methods to be implemented in derived classes
        [[nodiscard]] virtual ptr parent_input() const = 0;
        [[nodiscard]] virtual bool has_parent_input() const = 0;
        [[nodiscard]] virtual bool bound() const = 0;
        [[nodiscard]] virtual bool has_peer() const = 0;
        [[nodiscard]] virtual time_series_output_ptr output() const = 0;
        virtual bool bind_output(time_series_output_ptr output_) = 0;
        virtual void un_bind_output(bool unbind_refs) = 0;
        [[nodiscard]] virtual bool active() const = 0;
        virtual void make_active() = 0;
        virtual void make_passive() = 0;
        [[nodiscard]] virtual bool has_output() const = 0;
        virtual void builder_release_cleanup() = 0;
        [[nodiscard]] virtual nb::object py_value() const = 0;
        [[nodiscard]] virtual nb::object py_delta_value() const = 0;
        [[nodiscard]] virtual bool modified() const = 0;
        [[nodiscard]] virtual bool valid() const = 0;
        [[nodiscard]] virtual bool all_valid() const = 0;
        [[nodiscard]] virtual engine_time_t last_modified_time() const = 0;
        [[nodiscard]] virtual time_series_reference_output_ptr reference_output() const = 0;
        [[nodiscard]] virtual const TimeSeriesInput *get_input(size_t index) const = 0;
        [[nodiscard]] virtual TimeSeriesInput *get_input(size_t index) = 0;

        static void register_with_nanobind(nb::module_ &m);
    };
} // namespace hgraph

#endif  // TIME_SERIES_TYPE_H