#pragma once

#include "api_ptr.h"

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/types/time_series/ts_input_view.h>

namespace hgraph
{
    /**
     * @brief Base class for Python time-series wrappers.
     *
     * Uses view-based storage only (TSView). Legacy _impl storage has been removed.
     */
    struct HGRAPH_EXPORT PyTimeSeriesType
    {
        virtual ~PyTimeSeriesType() = default;

        // Move constructor
        PyTimeSeriesType(PyTimeSeriesType&& other) noexcept
            : view_(std::move(other.view_)) {}

        // Move assignment
        PyTimeSeriesType& operator=(PyTimeSeriesType&& other) noexcept {
            if (this != &other) {
                view_ = std::move(other.view_);
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

        // Value of this node - for python API
        [[nodiscard]] nb::object value() const;
        [[nodiscard]] nb::object delta_value() const;

        // When was this time-series last modified?
        [[nodiscard]] engine_time_t last_modified_time() const;
        // State of the value checks
        [[nodiscard]] nb::bool_ modified() const;
        [[nodiscard]] nb::bool_ valid() const;
        [[nodiscard]] nb::bool_ all_valid() const;

        [[nodiscard]] virtual nb::bool_ is_reference() const;

        // Get the view
        [[nodiscard]] TSView& view() { return view_; }
        [[nodiscard]] const TSView& view() const { return view_; }

        static void register_with_nanobind(nb::module_ &m);

      protected:
        // View-based constructor
        explicit PyTimeSeriesType(TSView view);

      private:
        TSView view_;
    };

    struct PyTimeSeriesInput;

    struct HGRAPH_EXPORT PyTimeSeriesOutput : PyTimeSeriesType
    {
        // Output-specific navigation of the graph structure.
        [[nodiscard]] nb::object parent_output() const;
        [[nodiscard]] nb::bool_  has_parent_output() const;

        // Mutation operations
        void apply_result(nb::object value);
        void set_value(nb::object value);
        void copy_from_output(const PyTimeSeriesOutput &output);
        void copy_from_input(const PyTimeSeriesInput &input);
        void clear();
        void invalidate();
        bool can_apply_result(nb::object value);

        // Get the output view
        [[nodiscard]] TSOutputView& output_view() { return output_view_; }
        [[nodiscard]] const TSOutputView& output_view() const { return output_view_; }

        static void register_with_nanobind(nb::module_ &m);

      protected:
        // View-based constructor
        explicit PyTimeSeriesOutput(TSOutputView view);

      private:
        TSOutputView output_view_;
    };

    struct HGRAPH_EXPORT PyTimeSeriesInput : PyTimeSeriesType
    {
        // Graph navigation specific to the input
        [[nodiscard]] nb::object parent_input() const;
        [[nodiscard]] nb::bool_  has_parent_input() const;

        // Active state
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

        // Reference and child access
        [[nodiscard]] nb::object reference_output() const;
        [[nodiscard]] nb::object get_input(size_t index) const;

        // Get the input view
        [[nodiscard]] TSInputView& input_view() { return input_view_; }
        [[nodiscard]] const TSInputView& input_view() const { return input_view_; }

        static void register_with_nanobind(nb::module_ &m);

      protected:
        // View-based constructor
        explicit PyTimeSeriesInput(TSInputView view);

      private:
        TSInputView input_view_;
    };

}  // namespace hgraph
