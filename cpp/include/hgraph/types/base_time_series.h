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
#include <fmt/format.h>
#include <hgraph/types/node.h>
#include <unordered_set>
#include <type_traits>

namespace hgraph {
    // Forward declare to avoid circular dependency
    struct TimeSeriesVisitor;
    struct OutputBuilder;
    struct TimeSeriesValueReferenceOutput;

    /**
     * @brief Base class for TimeSeriesOutput implementations
     *
     * This class holds all the state and concrete behavior currently in TimeSeriesOutput.
     * All concrete output types should extend this class rather than TimeSeriesOutput directly.
     * Eventually, TimeSeriesOutput will become a pure virtual interface.
     */
    struct HGRAPH_EXPORT BaseTimeSeriesOutput : TimeSeriesOutput {
        using ptr = BaseTimeSeriesOutput*;
        using s_ptr = std::shared_ptr<BaseTimeSeriesOutput>;

        explicit BaseTimeSeriesOutput(node_ptr parent) : _parent_ts_or_node{parent} {}
        explicit BaseTimeSeriesOutput(time_series_output_ptr parent) : _parent_ts_or_node{parent} {}

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
        void re_parent(node_ptr parent) override;
        void re_parent(const time_series_type_ptr parent) override;

        // Inherited interface implementations
        [[nodiscard]] bool modified() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;
        void mark_invalid() override;
        void mark_modified() override;
        void mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) override;
        [[nodiscard]] bool valid() const override;
        [[nodiscard]] bool all_valid() const override;
        [[nodiscard]] TimeSeriesOutput::s_ptr parent_output() const override;
        [[nodiscard]] TimeSeriesOutput::s_ptr parent_output() override;
        [[nodiscard]] bool has_parent_output() const override;

        void subscribe(Notifiable *node) override;
        void un_subscribe(Notifiable *node) override;
        void builder_release_cleanup() override;

        bool can_apply_result(const nb::object& value) override;
        void clear() override;
        void invalidate() override;
        void mark_modified(engine_time_t modified_time) override;

        VISITOR_SUPPORT()

    protected:
        // State and helpers moved from TimeSeriesType
        time_series_output_ptr _parent_output() const;
        time_series_output_ptr _parent_output();
        bool _has_parent_output() const;
        void _set_parent_output(time_series_output_ptr ts);
        node_ptr _owning_node() const;

        void _notify(engine_time_t modified_time);
        void _reset_last_modified_time();

    private:
        friend OutputBuilder;
        using TsOrNode = std::variant<time_series_output_ptr, node_ptr>;
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
    template<typename T>
    requires std::is_base_of_v<TimeSeriesInput, T>
    struct HGRAPH_EXPORT BaseTimeSeriesInput : T {
        using ptr = BaseTimeSeriesInput*;
        using s_ptr = std::shared_ptr<BaseTimeSeriesInput>;

        explicit BaseTimeSeriesInput(node_ptr parent) : _parent_ts_or_node{parent} {}
        explicit BaseTimeSeriesInput(time_series_input_ptr parent) : _parent_ts_or_node{parent} {}

        // Virtual destructor ensures proper cleanup - unsubscribe from any output we're subscribed to
        virtual ~BaseTimeSeriesInput() {
            // Unsubscribe from the output if we're active and bound
            // Use 'this' directly since that's what was used in subscribe()
            if (_active && _output != nullptr) {
                _output->un_subscribe(this);
            }
        }

        // Implement TimeSeriesType pure virtuals
        [[nodiscard]] node_ptr owning_node() override { return _owning_node(); }
        [[nodiscard]] node_ptr owning_node() const override { return _owning_node(); }
        [[nodiscard]] graph_ptr owning_graph() override { return has_owning_node() ? owning_node()->graph() : nullptr; }
        [[nodiscard]] graph_ptr owning_graph() const override { return has_owning_node() ? owning_node()->graph() : nullptr; }
        [[nodiscard]] bool is_reference() const override { return false; }
        [[nodiscard]] bool has_reference() const override { return false; }
        void reset_parent_or_node() override { _parent_ts_or_node.reset(); }
        [[nodiscard]] bool has_parent_or_node() const override { return _parent_ts_or_node.has_value(); }
        [[nodiscard]] bool has_owning_node() const override {
            if (_parent_ts_or_node.has_value()) {
                if (std::holds_alternative<node_ptr>(*_parent_ts_or_node)) {
                    return std::get<node_ptr>(*_parent_ts_or_node) != nullptr;
                }
                return std::get<time_series_input_ptr>(*_parent_ts_or_node)->has_owning_node();
            }
            return false;
        }
        void re_parent(node_ptr parent) override { _parent_ts_or_node = parent; }
        void re_parent(const time_series_type_ptr parent) override { _parent_ts_or_node = static_cast<time_series_input_ptr>(parent); }

        // Inherited interface implementations
        [[nodiscard]] TimeSeriesInput::s_ptr parent_input() const override {
            if (_has_parent_input()) {
                auto p = std::get<time_series_input_ptr>(*_parent_ts_or_node);
                return p ? p->shared_from_this() : time_series_input_s_ptr{};
            }
            return {};
        }
        [[nodiscard]] bool has_parent_input() const override { return _has_parent_input(); }
        [[nodiscard]] bool bound() const override { return _output != nullptr; }
        [[nodiscard]] bool has_peer() const override { return _output != nullptr; }
        [[nodiscard]] time_series_output_s_ptr output() const override { return _output; }

        bool bind_output(const time_series_output_s_ptr &output_) override;
        void un_bind_output(bool unbind_refs) override;

        [[nodiscard]] bool active() const override { return _active; }
        void make_active() override;
        void make_passive() override;
        [[nodiscard]] bool has_output() const override { return _output != nullptr; }

        void builder_release_cleanup() override {
            // Unsubscribe before clearing _output to avoid dangling subscriber pointers
            if (_active && _output != nullptr) {
                _output->un_subscribe(this);
            }
            _output = nullptr;
        }

        [[nodiscard]] nb::object py_value() const override {
            return _output != nullptr ? _output->py_value() : nb::none();
        }
        [[nodiscard]] nb::object py_delta_value() const override {
            return _output != nullptr ? _output->py_delta_value() : nb::none();
        }
        [[nodiscard]] bool modified() const override { return _output != nullptr && (_output->modified() || sampled()); }
        [[nodiscard]] bool valid() const override { return bound() && _output != nullptr && _output->valid(); }
        [[nodiscard]] bool all_valid() const override { return bound() && _output != nullptr && _output->all_valid(); }
        [[nodiscard]] engine_time_t last_modified_time() const override {
            return bound() ? std::max(_output->last_modified_time(), _sample_time) : MIN_DT;
        }
        [[nodiscard]] time_series_reference_output_s_ptr reference_output() const override { return _reference_output; }

        [[nodiscard]] TimeSeriesInput::s_ptr get_input(size_t index) override {
            throw std::runtime_error("BaseTimeSeriesInput [] not supported");
        }

    protected:
        // State and helpers moved from TimeSeriesType
        time_series_input_ptr _parent_input() const {
            return const_cast<BaseTimeSeriesInput *>(this)->_parent_input();
        }
        time_series_input_ptr _parent_input() {
            if (_parent_ts_or_node.has_value() && std::holds_alternative<time_series_input_ptr>(*_parent_ts_or_node)) {
                return std::get<time_series_input_ptr>(*_parent_ts_or_node);
            }
            return nullptr;
        }
        bool _has_parent_input() const {
            return _parent_ts_or_node.has_value() && std::holds_alternative<time_series_input_ptr>(*_parent_ts_or_node);
        }
        void _set_parent_input(time_series_input_ptr ts) {
            if (_parent_ts_or_node.has_value() && std::holds_alternative<time_series_input_ptr>(*_parent_ts_or_node)) {
                std::get<time_series_input_ptr>(*_parent_ts_or_node) = ts;
            } else {
                _parent_ts_or_node = ts;
            }
        }
        node_ptr _owning_node() const {
            if (_parent_ts_or_node.has_value()) {
                return std::visit(
                    []<typename T_>(T_ &&value) -> node_ptr {
                        using TT = std::decay_t<T_>;
                        if constexpr (std::is_same_v<TT, time_series_input_ptr>) {
                            return value->owning_node();
                        } else if constexpr (std::is_same_v<TT, node_ptr>) {
                            return value;
                        } else {
                            throw std::runtime_error("Unknown type");
                        }
                    },
                    _parent_ts_or_node.value());
            } else {
                throw std::runtime_error(fmt::format("No node is accessible (type: {})", typeid(*this).name()));
            }
        }

        // Protected virtual methods for derived classes to override
        virtual bool do_bind_output(time_series_output_s_ptr output_);
        virtual void do_un_bind_output(bool unbind_refs);

        void notify(engine_time_t modified_time) override;
        void notify_parent(TimeSeriesInput *child, engine_time_t modified_time) override {
            notify(modified_time);
        }

        void set_sample_time(engine_time_t sample_time) { _sample_time = sample_time; }
        [[nodiscard]] engine_time_t sample_time() const { return _sample_time; }
        [[nodiscard]] bool sampled() const {
            if (!has_owning_node()) { return false; }
            auto n = owning_node();
            if (n == nullptr) { return false; }
            return _sample_time != MIN_DT && _sample_time == *n->cached_evaluation_time_ptr();
        }

        void reset_output() { _output = nullptr; }
        void set_output(const time_series_output_s_ptr& output) { _output = output; }
        void set_active(bool active) { _active = active; }

    private:
        using TsOrNode = std::variant<time_series_input_ptr, node_ptr>;
        std::optional<TsOrNode> _parent_ts_or_node{};
        time_series_output_s_ptr _output;  // Keep output alive while bound (was nb::ref<TimeSeriesOutput>)
        time_series_reference_output_s_ptr _reference_output;
        std::shared_ptr<TimeSeriesValueReferenceOutput> _v2_reference_output;  // v2 style reference output
        bool _active{false};
        engine_time_t _sample_time{MIN_DT};
        engine_time_t _notify_time{MIN_DT};
    };

} // namespace hgraph

#endif  // BASE_TIME_SERIES_H

// Template method implementations - included separately in base_time_series_impl.h
// which is included after ref.h to avoid circular dependency issues

