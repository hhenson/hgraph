#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_view.h>

namespace hgraph
{
    /**
     * @brief Base class for Python time-series wrappers.
     *
     * This is a marker base class with only a virtual destructor.
     * All methods are implemented concretely in derived classes using the type-erased
     * TSView/TSMutableView - no virtual dispatch needed since the view handles polymorphism.
     */
    struct HGRAPH_EXPORT PyTimeSeriesType
    {
        virtual ~PyTimeSeriesType() = default;

        // Default move constructor/assignment
        PyTimeSeriesType(PyTimeSeriesType&& other) noexcept = default;
        PyTimeSeriesType& operator=(PyTimeSeriesType&& other) noexcept = default;

        // Delete copy constructor and assignment
        PyTimeSeriesType(const PyTimeSeriesType&) = delete;
        PyTimeSeriesType& operator=(const PyTimeSeriesType&) = delete;

        static void register_with_nanobind(nb::module_ &m);

      protected:
        PyTimeSeriesType() = default;
    };

    // Forward declaration for copy_from_input
    struct PyTimeSeriesInput;

    struct HGRAPH_EXPORT PyTimeSeriesOutput : PyTimeSeriesType
    {
        // ========== View-based construction ==========

        /**
         * Construct from a TSMutableView.
         */
        explicit PyTimeSeriesOutput(TSMutableView view);

        /**
         * Get the view.
         */
        [[nodiscard]] TSMutableView view() const { return _view; }

        // ========== Common time-series interface ==========
        // All methods use the view layer for type-specific behavior (no virtual dispatch)

        [[nodiscard]] nb::object value() const;
        [[nodiscard]] nb::object delta_value() const;
        [[nodiscard]] nb::object owning_node() const;
        [[nodiscard]] nb::object owning_graph() const;
        [[nodiscard]] engine_time_t last_modified_time() const;
        [[nodiscard]] nb::bool_ modified() const;
        [[nodiscard]] nb::bool_ valid() const;
        [[nodiscard]] nb::bool_ all_valid() const;
        [[nodiscard]] nb::bool_ is_reference() const;

        // ========== Mutation operations ==========

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

        // For REF outputs: bind the output to a target
        // This updates the underlying TimeSeriesReference value to point to the target
        void bind_output(nb::object output);

        // For REF outputs: make the binding active (schedule for notifications)
        void make_active();

        // For REF outputs: make the binding passive (unschedule)
        void make_passive();

        // For TSD element wrappers: set the element key for validity checking
        // When set, valid() will verify the key still exists in the parent TSD
        void set_element_key(nb::object key) { _element_key = std::move(key); }
        [[nodiscard]] nb::object element_key() const { return _element_key; }
        [[nodiscard]] bool has_element_key() const { return _element_key.is_valid() && !_element_key.is_none(); }

        /**
         * @brief Get a stable identity for this output.
         *
         * Returns a unique identifier that can be used to compare if two output
         * wrappers refer to the same underlying time series. This is needed because
         * Python wrapper objects may differ even when referring to the same data.
         */
        [[nodiscard]] uintptr_t output_id() const;

        static void register_with_nanobind(nb::module_ &m);

      protected:
        // View storage - the only data member
        TSMutableView _view;

        // For TSD elements: the key used to access this element
        // If set, valid() will check if this key still exists in the parent TSD
        nb::object _element_key{};
    };

    struct HGRAPH_EXPORT PyTimeSeriesInput : PyTimeSeriesType
    {
        // ========== View-based construction ==========

        /**
         * Construct from a TSView.
         */
        explicit PyTimeSeriesInput(TSView view);

        /**
         * Get the view.
         */
        [[nodiscard]] TSView view() const { return _view; }

        // ========== Common time-series interface ==========
        // All methods use the view layer for type-specific behavior (no virtual dispatch)

        [[nodiscard]] nb::object value() const;
        [[nodiscard]] nb::object delta_value() const;
        [[nodiscard]] nb::object owning_node() const;
        [[nodiscard]] nb::object owning_graph() const;
        [[nodiscard]] engine_time_t last_modified_time() const;
        [[nodiscard]] nb::bool_ modified() const;
        [[nodiscard]] nb::bool_ valid() const;
        [[nodiscard]] nb::bool_ all_valid() const;
        [[nodiscard]] nb::bool_ is_reference() const;

        // ========== Input-specific state methods ==========

        // This is used to indicate if the owner of this input is interested in being notified when
        // modifications are made to the value represented by this input.
        [[nodiscard]] nb::bool_ active() const;
        void                    make_active();
        void                    make_passive();

        // Dealing with the various states the time-series can be found in
        [[nodiscard]] nb::bool_  bound() const;

        // Binding methods (used by valid operator and REF handling)
        void bind_output(nb::object output);
        void un_bind_output();

        // Notification method (called when unbinding, matches Python behavior)
        void notify(nb::object modified_time);

        // Peer property (for REF binding detection)
        [[nodiscard]] nb::bool_ has_peer() const;

        static void register_with_nanobind(nb::module_ &m);

        // Store a bound output TSValue for passthrough inputs
        void set_bound_output(const TSValue* output);
        [[nodiscard]] const TSValue* bound_output() const;

        // Get the Python object this input is bound to (for cleanup in un_bind_output)
        [[nodiscard]] nb::object get_bound_py_output() const { return _bound_py_output; }

      protected:
        // View storage - the only data member
        TSView _view;

        // Binding state - for passthrough inputs (starts false, becomes true after bind_output)
        bool _explicit_bound{false};

        // Pointer to bound output TSValue (for passthrough inputs bound via REF)
        const TSValue* _bound_output{nullptr};

        // Python object bound to this input (for CppKeySetOutputWrapper etc.)
        // This allows checking the output's modified() status for scheduling
        nb::object _bound_py_output{};

        // Track when the binding was last modified (for modified() check)
        engine_time_t _binding_modified_time{MIN_DT};
    };

}  // namespace hgraph
