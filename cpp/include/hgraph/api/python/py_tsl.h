#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    /**
     * PyTimeSeriesListOutput - Wrapper for list outputs (TSL)
     *
     * Provides element access via view navigation.
     */
    struct PyTimeSeriesListOutput : PyTimeSeriesOutput
    {
        using PyTimeSeriesOutput::PyTimeSeriesOutput;

        PyTimeSeriesListOutput(PyTimeSeriesListOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        PyTimeSeriesListOutput& operator=(PyTimeSeriesListOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesListOutput(const PyTimeSeriesListOutput&) = delete;
        PyTimeSeriesListOutput& operator=(const PyTimeSeriesListOutput&) = delete;

        // Override value() to return tuple of element values (matching Python behavior)
        [[nodiscard]] nb::object value() const override;

        // Element access - __getitem__
        [[nodiscard]] nb::object get_item(const nb::handle &key) const;

        // Iteration
        [[nodiscard]] nb::object iter() const;
        [[nodiscard]] nb::object keys() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object items() const;

        // Valid/modified subset access
        [[nodiscard]] nb::object valid_keys() const;
        [[nodiscard]] nb::object valid_values() const;
        [[nodiscard]] nb::object valid_items() const;
        [[nodiscard]] nb::object modified_keys() const;
        [[nodiscard]] nb::object modified_values() const;
        [[nodiscard]] nb::object modified_items() const;

        [[nodiscard]] nb::int_ len() const;
        [[nodiscard]] bool empty() const;

        void clear();

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    };

    /**
     * PyTimeSeriesListInput - Wrapper for list inputs (TSL)
     *
     * Provides element access via view navigation.
     */
    struct PyTimeSeriesListInput : PyTimeSeriesInput
    {
        using PyTimeSeriesInput::PyTimeSeriesInput;

        PyTimeSeriesListInput(PyTimeSeriesListInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        PyTimeSeriesListInput& operator=(PyTimeSeriesListInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesListInput(const PyTimeSeriesListInput&) = delete;
        PyTimeSeriesListInput& operator=(const PyTimeSeriesListInput&) = delete;

        // Override value() to return tuple of element values (matching Python behavior)
        [[nodiscard]] nb::object value() const override;

        // Element access - __getitem__
        [[nodiscard]] nb::object get_item(const nb::handle &key) const;

        // Iteration
        [[nodiscard]] nb::object iter() const;
        [[nodiscard]] nb::object keys() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object items() const;

        // Valid/modified subset access
        [[nodiscard]] nb::object valid_keys() const;
        [[nodiscard]] nb::object valid_values() const;
        [[nodiscard]] nb::object valid_items() const;
        [[nodiscard]] nb::object modified_keys() const;
        [[nodiscard]] nb::object modified_values() const;
        [[nodiscard]] nb::object modified_items() const;

        [[nodiscard]] nb::int_ len() const;
        [[nodiscard]] bool empty() const;

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    };

    void tsl_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
