#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    /**
     * PyTimeSeriesBundleOutput - Wrapper for bundle outputs (TSB)
     *
     * Provides field access via view navigation.
     * Fields are accessed by name or index, returning wrapped sub-views.
     */
    struct PyTimeSeriesBundleOutput : PyTimeSeriesOutput
    {
        using PyTimeSeriesOutput::PyTimeSeriesOutput;

        PyTimeSeriesBundleOutput(PyTimeSeriesBundleOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        PyTimeSeriesBundleOutput& operator=(PyTimeSeriesBundleOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesBundleOutput(const PyTimeSeriesBundleOutput&) = delete;
        PyTimeSeriesBundleOutput& operator=(const PyTimeSeriesBundleOutput&) = delete;

        // Field access - __getitem__ and __getattr__
        [[nodiscard]] nb::object get_item(const nb::handle &key) const;
        [[nodiscard]] nb::object get_attr(const nb::handle &key) const;
        [[nodiscard]] nb::bool_ contains(const nb::handle &key) const;

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
        [[nodiscard]] nb::bool_ empty() const;

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    };

    /**
     * PyTimeSeriesBundleInput - Wrapper for bundle inputs (TSB)
     *
     * Provides field access via view navigation.
     * Fields are accessed by name or index, returning wrapped sub-views.
     */
    struct PyTimeSeriesBundleInput : PyTimeSeriesInput
    {
        using PyTimeSeriesInput::PyTimeSeriesInput;

        PyTimeSeriesBundleInput(PyTimeSeriesBundleInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        PyTimeSeriesBundleInput& operator=(PyTimeSeriesBundleInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesBundleInput(const PyTimeSeriesBundleInput&) = delete;
        PyTimeSeriesBundleInput& operator=(const PyTimeSeriesBundleInput&) = delete;

        // Field access - __getitem__ and __getattr__
        [[nodiscard]] nb::object get_item(const nb::handle &key) const;
        [[nodiscard]] nb::object get_attr(const nb::handle &key) const;
        [[nodiscard]] nb::bool_ contains(const nb::handle &key) const;

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
        [[nodiscard]] nb::bool_ empty() const;

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    };

    void tsb_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
