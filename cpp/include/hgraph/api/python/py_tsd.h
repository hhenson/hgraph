#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    /**
     * PyTimeSeriesDictOutput - Wrapper for dict outputs (TSD)
     *
     * Provides key-value access via view navigation.
     */
    struct PyTimeSeriesDictOutput : PyTimeSeriesOutput
    {
        using PyTimeSeriesOutput::PyTimeSeriesOutput;

        PyTimeSeriesDictOutput(PyTimeSeriesDictOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        PyTimeSeriesDictOutput& operator=(PyTimeSeriesDictOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesDictOutput(const PyTimeSeriesDictOutput&) = delete;
        PyTimeSeriesDictOutput& operator=(const PyTimeSeriesDictOutput&) = delete;

        // Key-value access
        [[nodiscard]] nb::object get_item(const nb::object &item) const;
        [[nodiscard]] nb::object get(const nb::object &item, const nb::object &default_value) const;
        [[nodiscard]] nb::object get_or_create(const nb::object &key);
        void create(const nb::object &item);
        void set_item(const nb::object &key, const nb::object &value);
        void del_item(const nb::object &key);
        nb::object pop(const nb::object &key, const nb::object &default_value);

        // Reference management
        nb::object get_ref(const nb::object &key, const nb::object &requester);
        void release_ref(const nb::object &key, const nb::object &requester);

        // Utility
        [[nodiscard]] nb::object key_from_value(const nb::object &value) const;

        [[nodiscard]] bool contains(const nb::object &item) const;
        [[nodiscard]] size_t size() const;

        // Iteration
        [[nodiscard]] nb::object iter() const;
        [[nodiscard]] nb::object key_set() const;
        [[nodiscard]] nb::object keys() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object items() const;

        // Valid/modified/added/removed subset access
        [[nodiscard]] nb::object valid_keys() const;
        [[nodiscard]] nb::object valid_values() const;
        [[nodiscard]] nb::object valid_items() const;
        [[nodiscard]] nb::object modified_keys() const;
        [[nodiscard]] nb::object modified_values() const;
        [[nodiscard]] nb::object modified_items() const;
        [[nodiscard]] bool was_modified(const nb::object &key) const;

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

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    };

    /**
     * PyTimeSeriesDictInput - Wrapper for dict inputs (TSD)
     *
     * Provides key-value access via view navigation.
     */
    struct PyTimeSeriesDictInput : PyTimeSeriesInput
    {
        using PyTimeSeriesInput::PyTimeSeriesInput;

        PyTimeSeriesDictInput(PyTimeSeriesDictInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        PyTimeSeriesDictInput& operator=(PyTimeSeriesDictInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesDictInput(const PyTimeSeriesDictInput&) = delete;
        PyTimeSeriesDictInput& operator=(const PyTimeSeriesDictInput&) = delete;

        // Key-value access
        [[nodiscard]] nb::object get_item(const nb::object &item) const;
        [[nodiscard]] nb::object get(const nb::object &item, const nb::object &default_value) const;
        [[nodiscard]] nb::object get_or_create(const nb::object &key);
        void create(const nb::object &item);
        [[nodiscard]] bool contains(const nb::object &item) const;
        [[nodiscard]] size_t size() const;

        // Key event callbacks
        void on_key_added(const nb::object &key);
        void on_key_removed(const nb::object &key);

        // Utility
        [[nodiscard]] nb::object key_from_value(const nb::object &value) const;

        // Iteration
        [[nodiscard]] nb::object iter() const;
        [[nodiscard]] nb::object key_set() const;
        [[nodiscard]] nb::object keys() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object items() const;

        // Valid/modified/added/removed subset access
        [[nodiscard]] nb::object valid_keys() const;
        [[nodiscard]] nb::object valid_values() const;
        [[nodiscard]] nb::object valid_items() const;
        [[nodiscard]] nb::object modified_keys() const;
        [[nodiscard]] nb::object modified_values() const;
        [[nodiscard]] nb::object modified_items() const;
        [[nodiscard]] bool was_modified(const nb::object &key) const;

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

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    };

    void tsd_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
