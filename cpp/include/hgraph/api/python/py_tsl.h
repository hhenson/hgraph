#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/hgraph_base.h>

namespace hgraph
{
    /**
     * @brief Python wrapper for TimeSeriesListOutput.
     *
     * Uses TSView/TSLView for all operations.
     */
    struct PyTimeSeriesListOutput : PyTimeSeriesOutput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesListOutput(TSMutableView view);

        // Move constructor
        PyTimeSeriesListOutput(PyTimeSeriesListOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesListOutput& operator=(PyTimeSeriesListOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesListOutput(const PyTimeSeriesListOutput&) = delete;
        PyTimeSeriesListOutput& operator=(const PyTimeSeriesListOutput&) = delete;

        // Default iterator iterates over values
        [[nodiscard]] nb::object iter() const;

        [[nodiscard]] nb::object get_item(const nb::handle &key) const;

        // Retrieves keys (indices)
        [[nodiscard]] nb::object keys() const;

        [[nodiscard]] nb::object values() const;

        [[nodiscard]] nb::object valid_keys() const;

        [[nodiscard]] nb::object modified_keys() const;

        [[nodiscard]] nb::int_ len() const;

        // Retrieves items (index, value pairs)
        [[nodiscard]] nb::object items() const;

        [[nodiscard]] nb::object valid_values() const;

        [[nodiscard]] nb::object valid_items() const;

        [[nodiscard]] nb::object modified_values() const;

        [[nodiscard]] nb::object modified_items() const;

        [[nodiscard]] bool empty() const;

        // value() and delta_value() are inherited from base - view layer handles TSL specifics

        [[nodiscard]] nb::str py_str();
        [[nodiscard]] nb::str py_repr();
    };

    /**
     * @brief Python wrapper for TimeSeriesListInput.
     *
     * Uses TSView/TSLView for all operations.
     */
    struct PyTimeSeriesListInput : PyTimeSeriesInput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesListInput(TSView view);

        // Move constructor
        PyTimeSeriesListInput(PyTimeSeriesListInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesListInput& operator=(PyTimeSeriesListInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesListInput(const PyTimeSeriesListInput&) = delete;
        PyTimeSeriesListInput& operator=(const PyTimeSeriesListInput&) = delete;

        // Default iterator iterates over values
        [[nodiscard]] nb::object iter() const;

        [[nodiscard]] nb::object get_item(const nb::handle &key) const;

        // Retrieves keys (indices)
        [[nodiscard]] nb::object keys() const;

        [[nodiscard]] nb::object values() const;

        [[nodiscard]] nb::object valid_keys() const;

        [[nodiscard]] nb::object modified_keys() const;

        [[nodiscard]] nb::int_ len() const;

        // Retrieves items (index, value pairs)
        [[nodiscard]] nb::object items() const;

        [[nodiscard]] nb::object valid_values() const;

        [[nodiscard]] nb::object valid_items() const;

        [[nodiscard]] nb::object modified_values() const;

        [[nodiscard]] nb::object modified_items() const;

        [[nodiscard]] bool empty() const;

        // value() and delta_value() are inherited from base - view layer handles TSL specifics

        [[nodiscard]] nb::str py_str();
        [[nodiscard]] nb::str py_repr();
    };

    void tsl_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
