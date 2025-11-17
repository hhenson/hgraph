#pragma once

#include <hgraph/hgraph_base.h>

namespace hgraph
{
    struct HGRAPH_EXPORT PyTimeSeriesType
    {
        virtual ~PyTimeSeriesType() = default;

        // Graph navigation methods. These may not be required
        // (Other than for debugging) if we used the context approach
        [[nodiscard]] virtual nb::object owning_node() const  = 0;  // PyNode
        [[nodiscard]] virtual nb::object owning_graph() const = 0;  // PyGraph
        // Helper methods can be removed now we use ptr return types?
        [[nodiscard]] virtual nb::bool_ has_parent_or_node() const = 0;
        [[nodiscard]] virtual nb::bool_ has_owning_node() const    = 0;

        // Value of this node - for python API
        [[nodiscard]] virtual nb::object value() const       = 0;
        [[nodiscard]] virtual nb::object delta_value() const = 0;

        // When was this time-series last modified?
        [[nodiscard]] virtual engine_time_t last_modified_time() const = 0;
        // State of the value checks (related to last_modified_time and event type)
        [[nodiscard]] virtual nb::bool_ modified() const  = 0;
        [[nodiscard]] virtual nb::bool_ valid() const     = 0;
        [[nodiscard]] virtual nb::bool_ all_valid() const = 0;

        /*
         * This is used to deal with the fact we are not tracking the type in the time-series value.
         * We need to deal with reference vs non-reference detection and the 3 methods below help with that.
         */
        [[nodiscard]] virtual nb::bool_ is_same_type(const TimeSeriesType *other) const = 0;
        [[nodiscard]] virtual nb::bool_ is_reference() const                            = 0;

        [[nodiscard]] virtual nb::str py_str() = 0;
        [[nodiscard]] virtual nb::str py_repr() = 0;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesInput;

    struct HGRAPH_EXPORT PyTimeSeriesOutput : PyTimeSeriesType
    {

        // Output-specific navigation of the graph structure.
        [[nodiscard]] virtual nb::object parent_output() const     = 0;
        [[nodiscard]] virtual bool       has_parent_output() const = 0;

        // Mutation operations
        // The apply_result is the core mechanism to apply a python value to the
        // output, this will call py_set_value - If this gets a None, it will just return
        virtual void apply_result(nb::object value) = 0;
        // This is the method that does most of the work - if this gets a None, it will call invalidate
        virtual void set_value(nb::object value) = 0;
        // These methods were designed to allow C++ to perform a more optimal copy of values given its
        // knowledge of the internal state of the input/output passed. A copy visitor.
        virtual void copy_from_output(const PyTimeSeriesOutput &output) = 0;
        virtual void copy_from_input(const PyTimeSeriesInput &input)    = 0;

        // These seem to have a lot of overlap in terms of behaviour.
        // Clear will remove the value, internal tracking etc.
        virtual void clear() = 0;
        // Will reset the state and put the state back to it's unset state.
        virtual void invalidate() = 0;

        // This is used by the dequeing logic to work out how much we can de-queue from a push queue.
        // It could be moved into the queuing logic and implemented as a visitor, this would allow us to peek
        // The queue and perform the change, if the change is successful, we then pop the queue.
        virtual bool can_apply_result(nb::object value) = 0;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct HGRAPH_EXPORT PyTimeSeriesInput : PyTimeSeriesType
    {
        // Graph navigation specific to the input
        [[nodiscard]] virtual nb::object parent_input() const     = 0;
        [[nodiscard]] virtual nb::bool_  has_parent_input() const = 0;

        // This is used to indicate if the owner of this input is interested in being notified when
        // modifications are made to the value represented by this input.
        [[nodiscard]] virtual nb::bool_ active() const = 0;
        virtual void                    make_active()  = 0;
        virtual void                    make_passive() = 0;

        // Dealing with the various states the time-series can be found in, for the most part this
        // should not need to be exposed as a client facing API, but is used for internal state management.
        [[nodiscard]] virtual nb::bool_  bound() const                               = 0;
        [[nodiscard]] virtual nb::bool_  has_peer() const                            = 0;
        [[nodiscard]] virtual nb::object output() const                              = 0;  // time_series_output_ptr
        [[nodiscard]] virtual nb::bool_  has_output() const                          = 0;
        virtual nb::bool_                bind_output(nb::object output_) = 0;
        virtual nb::bool_                un_bind_output(bool unbind_refs)            = 0;

        // This is a feature used by the BackTrace tooling, this is not something that is generally
        // Useful, it could be handled through a visitor, or some other means of extraction.
        // This exposes internal implementation logic.
        [[nodiscard]] virtual nb::object reference_output() const = 0;  // time_series_ref_ptr

        // This is a hack to support REF time-series binding, this definitely needs to be revisited.
        [[nodiscard]] virtual nb::object get_input(size_t index) const = 0;

        static void register_with_nanobind(nb::module_ &m);
    private:
        friend TimeSeriesInput *unwrap_input(const PyTimeSeriesInput &input_);
    };

}  // namespace hgraph
