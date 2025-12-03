#pragma once

#include "api_ptr.h"

#include <hgraph/hgraph_base.h>

namespace hgraph
{
    struct HGRAPH_EXPORT PyTimeSeriesType
    {
        using api_ptr = ApiPtr<TimeSeriesType>;

        virtual ~PyTimeSeriesType() = default;

        // Move constructor
        PyTimeSeriesType(PyTimeSeriesType&& other) noexcept
            : _impl(std::move(other._impl)) {}

        // Move assignment
        PyTimeSeriesType& operator=(PyTimeSeriesType&& other) noexcept {
            if (this != &other) {
                _impl = std::move(other._impl);
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesType(const PyTimeSeriesType&) = delete;
        PyTimeSeriesType& operator=(const PyTimeSeriesType&) = delete;

        // Graph navigation methods. These may not be required
        // (Other than for debugging) if we used the context approach
        [[nodiscard]] nb::object owning_node() const;   // PyNode
        [[nodiscard]] nb::object owning_graph() const;  // PyGraph
        // Helper methods can be removed now we use ptr return types?
        [[nodiscard]] nb::bool_ has_parent_or_node() const;
        [[nodiscard]] nb::bool_ has_owning_node() const;

        // Value of this node - for python API
        [[nodiscard]] nb::object value() const;
        [[nodiscard]] nb::object delta_value() const;

        // When was this time-series last modified?
        [[nodiscard]] engine_time_t last_modified_time() const;
        // State of the value checks (related to last_modified_time and event type)
        [[nodiscard]] nb::bool_ modified() const;
        [[nodiscard]] nb::bool_ valid() const;
        [[nodiscard]] nb::bool_ all_valid() const;

        [[nodiscard]] virtual nb::bool_ is_reference() const;

        static void register_with_nanobind(nb::module_ &m);

      protected:
        explicit PyTimeSeriesType(api_ptr impl);

        [[nodiscard]] control_block_ptr control_block() const;

        template <typename U> U *static_cast_impl() const { return _impl.static_cast_<U>(); }

        template <typename U> U *dynamic_cast_impl() const { return _impl.dynamic_cast_<U>(); }

        template <typename U> std::shared_ptr<U> impl_s_ptr() const { return _impl.control_block_typed<U>(); }

      private:
        api_ptr _impl;
    };

    struct PyTimeSeriesInput;

    struct HGRAPH_EXPORT PyTimeSeriesOutput : PyTimeSeriesType
    {
        using api_ptr = ApiPtr<TimeSeriesOutput>;

        // Output-specific navigation of the graph structure.
        [[nodiscard]] nb::object parent_output() const;
        [[nodiscard]] nb::bool_  has_parent_output() const;

        // Mutation operations
        // The apply_result is the core mechanism to apply a python value to the
        // output, this will call py_set_value - If this gets a None, it will just return
        void apply_result(nb::object value);
        // This is the method that does most of the work - if this gets a None, it will call invalidate
        void set_value(nb::object value);
        // These methods were designed to allow C++ to perform a more optimal copy of values given its
        // knowledge of the internal state of the input/output passed. A copy visitor.
        void copy_from_output(const PyTimeSeriesOutput &output);
        void copy_from_input(const PyTimeSeriesInput &input);

        // These seem to have a lot of overlap in terms of behaviour.
        // Clear will remove the value, internal tracking etc.
        void clear();
        // Will reset the state and put the state back to it's unset state.
        void invalidate();

        // This is used by the dequeing logic to work out how much we can de-queue from a push queue.
        // It could be moved into the queuing logic and implemented as a visitor, this would allow us to peek
        // The queue and perform the change, if the change is successful, we then pop the queue.
        bool can_apply_result(nb::object value);

        static void register_with_nanobind(nb::module_ &m);

      protected:
        using PyTimeSeriesType::PyTimeSeriesType;

      private:
        friend time_series_output_s_ptr unwrap_output(const PyTimeSeriesOutput &output_);
        [[nodiscard]] TimeSeriesOutput *impl() const;
    };

    struct HGRAPH_EXPORT PyTimeSeriesInput : PyTimeSeriesType
    {
        using api_ptr = ApiPtr<TimeSeriesInput>;

        // Graph navigation specific to the input
        [[nodiscard]] nb::object parent_input() const;
        [[nodiscard]] nb::bool_  has_parent_input() const;

        // This is used to indicate if the owner of this input is interested in being notified when
        // modifications are made to the value represented by this input.
        [[nodiscard]] nb::bool_ active() const;
        void                    make_active();
        void                    make_passive();

        // Dealing with the various states the time-series can be found in, for the most part this
        // should not need to be exposed as a client facing API, but is used for internal state management.
        [[nodiscard]] nb::bool_  bound() const;
        [[nodiscard]] nb::bool_  has_peer() const;
        [[nodiscard]] nb::object output() const;  // time_series_output_ptr
        [[nodiscard]] nb::bool_  has_output() const;
        nb::bool_                bind_output(nb::object output_);
        void                     un_bind_output(bool unbind_refs);

        // This is a feature used by the BackTrace tooling, this is not something that is generally
        // Useful, it could be handled through a visitor, or some other means of extraction.
        // This exposes internal implementation logic.
        [[nodiscard]] nb::object reference_output() const;  // time_series_ref_ptr

        // This is a hack to support REF time-series binding, this definitely needs to be revisited.
        [[nodiscard]] nb::object get_input(size_t index) const;

        static void register_with_nanobind(nb::module_ &m);

      protected:
        using PyTimeSeriesType::PyTimeSeriesType;

      private:
        [[nodiscard]] TimeSeriesInput *impl() const;
        friend time_series_input_s_ptr unwrap_input(const PyTimeSeriesInput &input_);
    };

}  // namespace hgraph
