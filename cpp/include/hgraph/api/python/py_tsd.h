#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/hgraph_base.h>

namespace hgraph
{
    /**
     * @brief Python wrapper for TimeSeriesDictOutput.
     *
     * Uses TSView/TSDView for operations.
     * Note: Some operations are stubs pending full TSD view support.
     */
    struct PyTimeSeriesDictOutput : PyTimeSeriesOutput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesDictOutput(TSMutableView view);

        // Move constructor
        PyTimeSeriesDictOutput(PyTimeSeriesDictOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesDictOutput& operator=(PyTimeSeriesDictOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesDictOutput(const PyTimeSeriesDictOutput&) = delete;
        PyTimeSeriesDictOutput& operator=(const PyTimeSeriesDictOutput&) = delete;

        [[nodiscard]] size_t size() const;
        [[nodiscard]] nb::object get_item(const nb::object &item) const;
        [[nodiscard]] nb::object get(const nb::object &item, const nb::object &default_value) const;
        [[nodiscard]] nb::object get_or_create(const nb::object &key);
        void create(const nb::object &item);
        [[nodiscard]] nb::object iter() const;
        [[nodiscard]] bool contains(const nb::object &item) const;
        [[nodiscard]] nb::object key_set() const;
        [[nodiscard]] nb::object keys() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object items() const;
        [[nodiscard]] nb::object modified_keys() const;
        [[nodiscard]] nb::object modified_values() const;
        [[nodiscard]] nb::object modified_items() const;
        [[nodiscard]] bool was_modified(const nb::object &key) const;
        [[nodiscard]] nb::object valid_keys() const;
        [[nodiscard]] nb::object valid_values() const;
        [[nodiscard]] nb::object valid_items() const;
        [[nodiscard]] nb::object added_keys() const;
        [[nodiscard]] nb::object added_values() const;
        [[nodiscard]] nb::object added_items() const;
        [[nodiscard]] bool has_added() const;
        [[nodiscard]] bool was_added(const nb::object &key) const;
        [[nodiscard]] nb::object removed_keys() const;
        [[nodiscard]] nb::object removed_values() const;
        [[nodiscard]] nb::object removed_items() const;
        [[nodiscard]] bool has_removed() const;
        [[nodiscard]] bool was_removed(const nb::object &key) const;
        [[nodiscard]] nb::object key_from_value(const nb::object &value) const;

        // value() and delta_value() are inherited from base - view layer handles TSD specifics

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
        void set_item(const nb::object &key, const nb::object &value);
        void del_item(const nb::object &key);
        nb::object pop(const nb::object &key, const nb::object &default_value);
        nb::object get_ref(const nb::object &key, const nb::object &requester);
        void release_ref(const nb::object &key, const nb::object &requester);
        void on_key_removed(const nb::object &key);

        /**
         * @brief Update tracked ref outputs for a removed key.
         *
         * Called when a key is removed from the TSD. Updates any tracked
         * reference outputs for that key to point to None.
         * This matches Python's _ref_ts_feature.update(key) behavior.
         */
        void update_ref_output_for_removed_key(const nb::object &key);
    };

    /**
     * @brief Python wrapper for TimeSeriesDictInput.
     *
     * Uses TSView/TSDView for operations.
     */
    struct PyTimeSeriesDictInput : PyTimeSeriesInput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesDictInput(TSView view);

        // Move constructor
        PyTimeSeriesDictInput(PyTimeSeriesDictInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesDictInput& operator=(PyTimeSeriesDictInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesDictInput(const PyTimeSeriesDictInput&) = delete;
        PyTimeSeriesDictInput& operator=(const PyTimeSeriesDictInput&) = delete;

        [[nodiscard]] size_t size() const;
        [[nodiscard]] nb::object get_item(const nb::object &item) const;
        [[nodiscard]] nb::object get(const nb::object &item, const nb::object &default_value) const;
        [[nodiscard]] nb::object get_or_create(const nb::object &key);
        void create(const nb::object &item);
        [[nodiscard]] nb::object iter() const;
        [[nodiscard]] bool contains(const nb::object &item) const;
        [[nodiscard]] nb::object key_set() const;
        [[nodiscard]] nb::object keys() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object items() const;
        [[nodiscard]] nb::object modified_keys() const;
        [[nodiscard]] nb::object modified_values() const;
        [[nodiscard]] nb::object modified_items() const;
        [[nodiscard]] bool was_modified(const nb::object &key) const;
        [[nodiscard]] nb::object valid_keys() const;
        [[nodiscard]] nb::object valid_values() const;
        [[nodiscard]] nb::object valid_items() const;
        [[nodiscard]] nb::object added_keys() const;
        [[nodiscard]] nb::object added_values() const;
        [[nodiscard]] nb::object added_items() const;
        [[nodiscard]] bool has_added() const;
        [[nodiscard]] bool was_added(const nb::object &key) const;
        [[nodiscard]] nb::object removed_keys() const;
        [[nodiscard]] nb::object removed_values() const;
        [[nodiscard]] nb::object removed_items() const;
        [[nodiscard]] bool has_removed() const;
        [[nodiscard]] bool was_removed(const nb::object &key) const;
        [[nodiscard]] nb::object key_from_value(const nb::object &value) const;

        // value() and delta_value() are inherited from base - view layer handles TSD specifics

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
        void on_key_added(const nb::object &key);
        void on_key_removed(const nb::object &key);
    };

    // Forward declarations
    struct CppKeySetOutputWrapper;
    struct CppKeySetIsEmptyOutput;

    /**
     * @brief C++ wrapper providing TSS output interface for TSD's key_set.
     *
     * This provides the expected TSS output interface by delegating to the
     * underlying TSD's key tracking.
     *
     * Note: Stores TSMutableView directly to be independent of Python wrapper lifetime.
     */
    struct CppKeySetOutputWrapper
    {
        explicit CppKeySetOutputWrapper(TSMutableView view);

        // TSS output interface
        [[nodiscard]] nb::object value() const;
        [[nodiscard]] nb::object delta_value() const;
        [[nodiscard]] bool valid() const;
        [[nodiscard]] bool modified() const;
        [[nodiscard]] nb::object last_modified_time() const;
        [[nodiscard]] nb::object added() const;
        [[nodiscard]] nb::object removed() const;
        [[nodiscard]] bool was_added(const nb::object& item) const;
        [[nodiscard]] bool was_removed(const nb::object& item) const;
        [[nodiscard]] size_t size() const;
        [[nodiscard]] bool contains(const nb::object& item) const;
        [[nodiscard]] nb::object values() const;

        // Required for is_empty operator
        [[nodiscard]] nb::object is_empty_output();

        // Node/Graph access
        [[nodiscard]] nb::object owning_node() const;
        [[nodiscard]] nb::object owning_graph() const;

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;

    private:
        TSMutableView _view;  // Direct view storage - independent of Python wrapper
    };

    /**
     * @brief C++ wrapper providing TS[bool] output interface for key_set is_empty.
     *
     * Tracks whether the TSD's key set is empty and provides the time series
     * interface needed for REF[TS[bool]] return.
     *
     * Note: Stores TSMutableView directly to be independent of Python wrapper lifetime.
     */
    struct CppKeySetIsEmptyOutput
    {
        explicit CppKeySetIsEmptyOutput(TSMutableView view);

        // TS[bool] output interface
        [[nodiscard]] bool value() const;
        [[nodiscard]] bool delta_value() const;
        [[nodiscard]] bool valid() const;
        [[nodiscard]] bool modified();
        [[nodiscard]] nb::object last_modified_time() const;
        [[nodiscard]] bool all_valid() const;

        // Node/Graph access
        [[nodiscard]] nb::object owning_node() const;
        [[nodiscard]] nb::object owning_graph() const;

        // Duck-typing support for TimeSeriesReference.make()
        [[nodiscard]] bool has_output() const { return true; }
        [[nodiscard]] nb::object output();  // Returns self

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;

        /**
         * @brief Get a stable identity for this output.
         *
         * Returns a unique identifier that can be used to compare if two output
         * wrappers refer to the same underlying time series. This is needed because
         * Python wrapper objects may differ even when referring to the same data.
         */
        [[nodiscard]] uintptr_t output_id() const;

    private:
        TSMutableView _view;  // Direct view storage - independent of Python wrapper
        std::optional<bool> _last_empty_state;
        engine_time_t _last_check_time{MIN_DT};
        bool _cached_modified{false};
    };

    /**
     * @brief C++ wrapper providing TSS input interface for TSD input's key_set.
     *
     * Note: Stores TSView directly to be independent of Python wrapper lifetime.
     */
    struct CppKeySetInputWrapper
    {
        explicit CppKeySetInputWrapper(TSView view);

        // TSS input interface
        [[nodiscard]] nb::object value() const;
        [[nodiscard]] nb::object delta_value() const;
        [[nodiscard]] bool valid() const;
        [[nodiscard]] bool modified() const;
        [[nodiscard]] nb::object last_modified_time() const;
        [[nodiscard]] nb::object added() const;
        [[nodiscard]] nb::object removed() const;
        [[nodiscard]] bool was_added(const nb::object& item) const;
        [[nodiscard]] bool was_removed(const nb::object& item) const;
        [[nodiscard]] size_t size() const;
        [[nodiscard]] bool contains(const nb::object& item) const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] bool all_valid() const;
        [[nodiscard]] bool bound() const;
        [[nodiscard]] bool has_peer() const;
        [[nodiscard]] nb::object output() const;

        // Node/Graph access
        [[nodiscard]] nb::object owning_node() const;
        [[nodiscard]] nb::object owning_graph() const;

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;

    private:
        TSView _view;  // Direct view storage - independent of Python wrapper
    };

    void tsd_register_with_nanobind(nb::module_ & m);
}  // namespace hgraph
