#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/access_strategy.h>

namespace hgraph
{
    /**
     * PyTimeSeriesType - Base class for Python time-series wrappers
     *
     * Implementation:
     * - Holds shared_ptr<Node> for lifetime management
     * - Holds const TimeSeriesTypeMeta* for type info and navigation
     * - Derived classes hold TimeSeriesValueView or TSInputView
     */
    struct HGRAPH_EXPORT PyTimeSeriesType
    {
        virtual ~PyTimeSeriesType() = default;

        // Move semantics
        PyTimeSeriesType(PyTimeSeriesType&& other) noexcept
            : _node(std::move(other._node)), _meta(other._meta) {
            other._meta = nullptr;
        }

        PyTimeSeriesType& operator=(PyTimeSeriesType&& other) noexcept {
            if (this != &other) {
                _node = std::move(other._node);
                _meta = other._meta;
                other._meta = nullptr;
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesType(const PyTimeSeriesType&) = delete;
        PyTimeSeriesType& operator=(const PyTimeSeriesType&) = delete;

        // Graph navigation methods
        [[nodiscard]] nb::object owning_node() const;
        [[nodiscard]] nb::object owning_graph() const;
        [[nodiscard]] nb::bool_ has_parent_or_node() const;
        [[nodiscard]] nb::bool_ has_owning_node() const;

        // Access to underlying node (for wrapper factory)
        [[nodiscard]] node_s_ptr node() const { return _node; }

        // Value access - derived classes implement via their view
        [[nodiscard]] virtual nb::object value() const = 0;
        [[nodiscard]] virtual nb::object delta_value() const = 0;

        // Modification state - derived classes implement via their view
        [[nodiscard]] virtual engine_time_t last_modified_time() const = 0;
        [[nodiscard]] virtual nb::bool_ modified() const = 0;
        [[nodiscard]] virtual nb::bool_ valid() const = 0;
        [[nodiscard]] virtual nb::bool_ all_valid() const = 0;

        [[nodiscard]] virtual nb::bool_ is_reference() const;

        static void register_with_nanobind(nb::module_ &m);

      protected:
        PyTimeSeriesType() = default;
        PyTimeSeriesType(node_s_ptr node, const TimeSeriesTypeMeta* meta);

        node_s_ptr _node;
        const TimeSeriesTypeMeta* _meta{nullptr};
    };

    struct PyTimeSeriesInput;

    /**
     * PyTimeSeriesOutput - Python wrapper for time-series outputs
     *
     * Holds TimeSeriesValueView for value access and mutation.
     * Inherits node_s_ptr and meta from base class.
     */
    struct HGRAPH_EXPORT PyTimeSeriesOutput : PyTimeSeriesType
    {
        // Move semantics
        PyTimeSeriesOutput(PyTimeSeriesOutput&& other) noexcept
            : PyTimeSeriesType(std::move(other))
            , _view(std::move(other._view))
            , _output(other._output) {
            other._output = nullptr;
        }

        PyTimeSeriesOutput& operator=(PyTimeSeriesOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesType::operator=(std::move(other));
                _view = std::move(other._view);
                _output = other._output;
                other._output = nullptr;
            }
            return *this;
        }

        // Implement base class pure virtuals
        [[nodiscard]] nb::object value() const override;
        [[nodiscard]] nb::object delta_value() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;
        [[nodiscard]] nb::bool_ modified() const override;
        [[nodiscard]] nb::bool_ valid() const override;
        [[nodiscard]] nb::bool_ all_valid() const override;

        // Output-specific navigation
        [[nodiscard]] nb::object parent_output() const;
        [[nodiscard]] nb::bool_  has_parent_output() const;

        // Mutation operations
        void apply_result(nb::object value);
        virtual void set_value(nb::object value);
        void copy_from_output(const PyTimeSeriesOutput &output);
        void copy_from_input(const PyTimeSeriesInput &input);
        void clear();
        void invalidate();
        bool can_apply_result(nb::object value);

        static void register_with_nanobind(nb::module_ &m);

        // Access to underlying view and output
        [[nodiscard]] value::TimeSeriesValueView& view() { return _view; }
        [[nodiscard]] const value::TimeSeriesValueView& view() const { return _view; }
        [[nodiscard]] ts::TSOutput* output() const { return _output; }

        // Constructor
        PyTimeSeriesOutput(node_s_ptr node, value::TimeSeriesValueView view, ts::TSOutput* output, const TimeSeriesTypeMeta* meta);

      protected:
        value::TimeSeriesValueView _view;
        ts::TSOutput* _output{nullptr};
    };

    /**
     * PyTimeSeriesInput - Python wrapper for time-series inputs
     *
     * Holds TSInputView for value access. The view points to an AccessStrategy
     * and fetches fresh data on each access (never materialized).
     *
     * Optionally holds raw pointer to TSInput for binding operations.
     * Field wrappers don't have a TSInput, only a view.
     *
     * Inherits node_s_ptr and meta from base class.
     */
    struct HGRAPH_EXPORT PyTimeSeriesInput : PyTimeSeriesType
    {
        // Move semantics
        PyTimeSeriesInput(PyTimeSeriesInput&& other) noexcept
            : PyTimeSeriesType(std::move(other))
            , _view(std::move(other._view))
            , _input(other._input) {
            other._input = nullptr;
        }

        PyTimeSeriesInput& operator=(PyTimeSeriesInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesType::operator=(std::move(other));
                _view = std::move(other._view);
                _input = other._input;
                other._input = nullptr;
            }
            return *this;
        }

        // Implement base class pure virtuals
        [[nodiscard]] nb::object value() const override;
        [[nodiscard]] nb::object delta_value() const override;
        [[nodiscard]] engine_time_t last_modified_time() const override;
        [[nodiscard]] nb::bool_ modified() const override;
        [[nodiscard]] nb::bool_ valid() const override;
        [[nodiscard]] nb::bool_ all_valid() const override;

        // Graph navigation
        [[nodiscard]] nb::object parent_input() const;
        [[nodiscard]] nb::bool_  has_parent_input() const;

        // Activation state
        [[nodiscard]] nb::bool_ active() const;
        void                    make_active();
        void                    make_passive();

        // Binding state
        [[nodiscard]] nb::bool_  bound() const;
        [[nodiscard]] nb::bool_  has_peer() const;
        [[nodiscard]] nb::object output() const;
        [[nodiscard]] nb::bool_  has_output() const;
        nb::bool_                bind_output(nb::object output_);
        void                     un_bind_output(bool unbind_refs);

        // Reference support
        [[nodiscard]] nb::object reference_output() const;
        [[nodiscard]] nb::object get_input(size_t index) const;

        static void register_with_nanobind(nb::module_ &m);

        // Access to underlying view and input
        [[nodiscard]] ts::TSInputView& view() { return _view; }
        [[nodiscard]] const ts::TSInputView& view() const { return _view; }
        [[nodiscard]] ts::TSInput* input() const { return _input; }

        // Constructor for direct input wrappers (with TSInput for binding operations)
        PyTimeSeriesInput(node_s_ptr node, ts::TSInputView view, ts::TSInput* input, const TimeSeriesTypeMeta* meta);

        // Constructor for field wrappers (view only, no TSInput)
        PyTimeSeriesInput(node_s_ptr node, ts::TSInputView view, const TimeSeriesTypeMeta* meta);

      private:
        ts::TSInputView _view;
        ts::TSInput* _input{nullptr};
    };

}  // namespace hgraph
