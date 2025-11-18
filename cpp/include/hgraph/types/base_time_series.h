//
// Base Time Series Input/Output Shim Layer
//
// This file provides BaseTimeSeriesInput and BaseTimeSeriesOutput classes that sit
// between the abstract TimeSeriesInput/TimeSeriesOutput interfaces and the concrete
// implementations. The Base classes hold all the state and concrete behavior.
//
// The goal is to eventually make TimeSeriesInput/TimeSeriesOutput pure virtual interfaces,
// with all implementation details moved to these Base classes.
//

#ifndef BASE_TIME_SERIES_H
#define BASE_TIME_SERIES_H

#include <hgraph/types/time_series_type.h>
#include <unordered_set>
#include <type_traits>

namespace hgraph {
    // Forward declare to avoid circular dependency
    struct TimeSeriesVisitor;
    struct OutputBuilder;

    /**
     * @brief Base class for TimeSeriesOutput implementations
     *
     * This class holds all the state and concrete behavior currently in TimeSeriesOutput.
     * All concrete output types should extend this class rather than TimeSeriesOutput directly.
     * Eventually, TimeSeriesOutput will become a pure virtual interface.
     */
    struct HGRAPH_EXPORT BaseTimeSeriesOutput : TimeSeriesOutput {
        using ptr = nb::ref<BaseTimeSeriesOutput>;

        explicit BaseTimeSeriesOutput(const node_ptr &parent) : _parent_ts_or_node{parent} {}
        explicit BaseTimeSeriesOutput(const TimeSeriesType::ptr &parent) : _parent_ts_or_node{parent} {}

        // Implement TimeSeriesType pure virtuals
        [[nodiscard]] node_ptr owning_node() override;
        [[nodiscard]] node_ptr owning_node() const override;
        [[nodiscard]] graph_ptr owning_graph() override;
        [[nodiscard]] graph_ptr owning_graph() const override;
        [[nodiscard]] bool is_reference() const override;
        [[nodiscard]] bool has_reference() const override;
        void reset_parent_or_node() override;
        [[nodiscard]] bool has_parent_or_node() const override;
        [[nodiscard]] bool has_owning_node() const override;
        void re_parent(const node_ptr &parent) override;
        void re_parent(const TimeSeriesType::ptr &parent) override;

        // Inherited interface implementations
        [[nodiscard]] bool modified() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;
        void mark_invalid() override;
        void mark_modified() override;
        void mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) override;
        [[nodiscard]] bool valid() const override;
        [[nodiscard]] bool all_valid() const override;
        [[nodiscard]] TimeSeriesOutput::ptr parent_output() const override;
        [[nodiscard]] TimeSeriesOutput::ptr parent_output() override;
        [[nodiscard]] bool has_parent_output() const override;

        void subscribe(Notifiable *node) override;
        void un_subscribe(Notifiable *node) override;
        void builder_release_cleanup() override;

        bool can_apply_result(nb::object value) override;
        void clear() override;
        void invalidate() override;
        void mark_modified(engine_time_t modified_time) override;

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

        static void register_with_nanobind(nb::module_ &m);

    protected:
        // State and helpers moved from TimeSeriesType
        TimeSeriesType::ptr &_parent_time_series() const;
        TimeSeriesType::ptr &_parent_time_series();
        bool _has_parent_time_series() const;
        void _set_parent_time_series(TimeSeriesType *ts);
        node_ptr _owning_node() const;

        void _notify(engine_time_t modified_time);
        void _reset_last_modified_time();

    private:
        friend OutputBuilder;
        using TsOrNode = std::variant<TimeSeriesType::ptr, node_ptr>;
        std::optional<TsOrNode> _parent_ts_or_node{};
        std::unordered_set<Notifiable *> _subscribers{};
        engine_time_t _last_modified_time{MIN_DT};
    };

    /**
     * @brief Base class for TimeSeriesInput implementations
     *
     * This class holds all the state and concrete behavior currently in TimeSeriesInput.
     * All concrete input types should extend this class rather than TimeSeriesInput directly.
     * Eventually, TimeSeriesInput will become a pure virtual interface.
     */
    struct HGRAPH_EXPORT BaseTimeSeriesInput : TimeSeriesInput {
        using ptr = nb::ref<BaseTimeSeriesInput>;

        explicit BaseTimeSeriesInput(const node_ptr &parent) : _parent_ts_or_node{parent} {}
        explicit BaseTimeSeriesInput(const TimeSeriesType::ptr &parent) : _parent_ts_or_node{parent} {}

        // Implement TimeSeriesType pure virtuals
        [[nodiscard]] node_ptr owning_node() override;
        [[nodiscard]] node_ptr owning_node() const override;
        [[nodiscard]] graph_ptr owning_graph() override;
        [[nodiscard]] graph_ptr owning_graph() const override;
        [[nodiscard]] bool is_reference() const override;
        [[nodiscard]] bool has_reference() const override;
        void reset_parent_or_node() override;
        [[nodiscard]] bool has_parent_or_node() const override;
        [[nodiscard]] bool has_owning_node() const override;
        void re_parent(const node_ptr &parent) override;
        void re_parent(const TimeSeriesType::ptr &parent) override;

        // Inherited interface implementations
        [[nodiscard]] TimeSeriesInput::ptr parent_input() const override;
        [[nodiscard]] bool has_parent_input() const override;
        [[nodiscard]] bool bound() const override;
        [[nodiscard]] bool has_peer() const override;
        [[nodiscard]] time_series_output_ptr output() const override;

        bool bind_output(time_series_output_ptr output_) override;
        void un_bind_output(bool unbind_refs) override;

        [[nodiscard]] bool active() const override;
        void make_active() override;
        void make_passive() override;
        [[nodiscard]] bool has_output() const override;

        void builder_release_cleanup() override;

        [[nodiscard]] nb::object py_value() const override;
        [[nodiscard]] nb::object py_delta_value() const override;
        [[nodiscard]] bool modified() const override;
        [[nodiscard]] bool valid() const override;
        [[nodiscard]] bool all_valid() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;
        [[nodiscard]] time_series_reference_output_ptr reference_output() const override;

        [[nodiscard]] const TimeSeriesInput *get_input(size_t index) const override;
        [[nodiscard]] TimeSeriesInput *get_input(size_t index) override;

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

        static void register_with_nanobind(nb::module_ &m);

    protected:
        // State and helpers moved from TimeSeriesType
        TimeSeriesType::ptr &_parent_time_series() const;
        TimeSeriesType::ptr &_parent_time_series();
        bool _has_parent_time_series() const;
        void _set_parent_time_series(TimeSeriesType *ts);
        node_ptr _owning_node() const;

        // Protected virtual methods for derived classes to override
        virtual bool do_bind_output(time_series_output_ptr &output_);
        virtual void do_un_bind_output(bool unbind_refs);

        void notify(engine_time_t modified_time) override;
        virtual void notify_parent(TimeSeriesInput *child, engine_time_t modified_time);

        void set_sample_time(engine_time_t sample_time);
        [[nodiscard]] engine_time_t sample_time() const;
        [[nodiscard]] bool sampled() const;

        void reset_output();
        void set_output(time_series_output_ptr output);
        void set_active(bool active);

    private:
        using TsOrNode = std::variant<TimeSeriesType::ptr, node_ptr>;
        std::optional<TsOrNode> _parent_ts_or_node{};
        time_series_output_ptr _output;
        time_series_reference_output_ptr _reference_output;
        bool _active{false};
        engine_time_t _sample_time{MIN_DT};
        engine_time_t _notify_time{MIN_DT};
    };

    // Implementation of CRTP accept forwarding for TimeSeriesInputVisitable
    // This is tricky: we need to call the concrete type's accept(), not BaseTimeSeriesInput's
    // The concrete types (like TimeSeriesValueReferenceInput) override this with their own accept()
    // So we just need a default implementation that's never actually called
    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) TimeSeriesInputVisitable::accept(Visitor& visitor) {
        // This should never be called - concrete types override with CRTP accept
        throw std::runtime_error("CRTP accept() called on TimeSeriesInputVisitable base - missing override in derived class");
    }

    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) TimeSeriesInputVisitable::accept(Visitor& visitor) const {
        // This should never be called - concrete types override with CRTP accept
        throw std::runtime_error("CRTP accept() called on TimeSeriesInputVisitable base - missing override in derived class");
    }

    // Implementation of CRTP accept forwarding for TimeSeriesOutputVisitable
    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) TimeSeriesOutputVisitable::accept(Visitor& visitor) {
        // This should never be called - concrete types override with CRTP accept
        throw std::runtime_error("CRTP accept() called on TimeSeriesOutputVisitable base - missing override in derived class");
    }

    template<typename Visitor>
        requires (!std::is_base_of_v<TimeSeriesVisitor, Visitor>)
    decltype(auto) TimeSeriesOutputVisitable::accept(Visitor& visitor) const {
        // This should never be called - concrete types override with CRTP accept
        throw std::runtime_error("CRTP accept() called on TimeSeriesOutputVisitable base - missing override in derived class");
    }

} // namespace hgraph

#endif  // BASE_TIME_SERIES_H

