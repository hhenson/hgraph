#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/hgraph_base.h>

namespace hgraph
{
    /**
     * @brief Python wrapper for TimeSeriesBundleOutput.
     *
     * Uses TSView/TSBView for all operations.
     */
    struct PyTimeSeriesBundleOutput : PyTimeSeriesOutput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesBundleOutput(TSMutableView view);

        // Move constructor
        PyTimeSeriesBundleOutput(PyTimeSeriesBundleOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesBundleOutput& operator=(PyTimeSeriesBundleOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesBundleOutput(const PyTimeSeriesBundleOutput&) = delete;
        PyTimeSeriesBundleOutput& operator=(const PyTimeSeriesBundleOutput&) = delete;

        // Default iterator iterates over keys to keep this more consistent with Python (c.f. dict)
        [[nodiscard]] nb::object iter() const;

        [[nodiscard]] nb::object get_item(const nb::handle &key) const;

        [[nodiscard]] nb::object get_attr(const nb::handle &key) const;

        [[nodiscard]] nb::bool_ contains(const nb::handle &key) const;

        [[nodiscard]] nb::object key_from_value(const nb::handle &value) const;

        // Retrieves valid keys
        [[nodiscard]] nb::object keys() const;

        [[nodiscard]] nb::object values() const;

        [[nodiscard]] nb::object valid_keys() const;

        [[nodiscard]] nb::object valid_values() const;

        [[nodiscard]] nb::object modified_keys() const;

        [[nodiscard]] nb::object modified_values() const;

        [[nodiscard]] nb::int_ len() const;

        [[nodiscard]] nb::bool_ empty() const;

        // Retrieves valid items
        [[nodiscard]] nb::object items() const;

        [[nodiscard]] nb::object valid_items() const;

        [[nodiscard]] nb::object modified_items() const;

        // value() and delta_value() are inherited from base - view layer handles TSB specifics

        [[nodiscard]] nb::str py_str();
        [[nodiscard]] nb::str py_repr();
    };

    /**
     * @brief Python wrapper for TimeSeriesBundleInput.
     *
     * Uses TSView/TSBView for all operations.
     */
    struct PyTimeSeriesBundleInput : PyTimeSeriesInput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesBundleInput(TSView view);

        // Move constructor
        PyTimeSeriesBundleInput(PyTimeSeriesBundleInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesBundleInput& operator=(PyTimeSeriesBundleInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesBundleInput(const PyTimeSeriesBundleInput&) = delete;
        PyTimeSeriesBundleInput& operator=(const PyTimeSeriesBundleInput&) = delete;

        // Default iterator iterates over keys to keep this more consistent with Python (c.f. dict)
        [[nodiscard]] nb::object iter() const;

        [[nodiscard]] nb::object get_item(const nb::handle &key) const;

        [[nodiscard]] nb::object get_attr(const nb::handle &key) const;

        [[nodiscard]] nb::bool_ contains(const nb::handle &key) const;

        [[nodiscard]] nb::object key_from_value(const nb::handle &value) const;

        // Retrieves valid keys
        [[nodiscard]] nb::object keys() const;

        [[nodiscard]] nb::object values() const;

        [[nodiscard]] nb::object valid_keys() const;

        [[nodiscard]] nb::object valid_values() const;

        [[nodiscard]] nb::object modified_keys() const;

        [[nodiscard]] nb::object modified_values() const;

        [[nodiscard]] nb::int_ len() const;

        [[nodiscard]] nb::bool_ empty() const;

        // Retrieves valid items
        [[nodiscard]] nb::object items() const;

        [[nodiscard]] nb::object valid_items() const;

        [[nodiscard]] nb::object modified_items() const;

        // value() and delta_value() are inherited from base - view layer handles TSB specifics

        [[nodiscard]] nb::str py_str();
        [[nodiscard]] nb::str py_repr();
    };

    void tsb_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
