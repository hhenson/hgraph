#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    void ref_register_with_nanobind(nb::module_ &m);

    /**
     * PyTimeSeriesReferenceOutput - Wrapper for reference outputs (REF)
     *
     * References wrap another time-series type.
     */
    struct PyTimeSeriesReferenceOutput : PyTimeSeriesOutput
    {
        using PyTimeSeriesOutput::PyTimeSeriesOutput;

        PyTimeSeriesReferenceOutput(PyTimeSeriesReferenceOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        PyTimeSeriesReferenceOutput& operator=(PyTimeSeriesReferenceOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesReferenceOutput(const PyTimeSeriesReferenceOutput&) = delete;
        PyTimeSeriesReferenceOutput& operator=(const PyTimeSeriesReferenceOutput&) = delete;

        [[nodiscard]] nb::str to_string() const;
        [[nodiscard]] nb::str to_repr() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    /**
     * PyTimeSeriesReferenceInput - Wrapper for reference inputs (REF)
     *
     * References wrap another time-series type.
     */
    struct PyTimeSeriesReferenceInput : PyTimeSeriesInput
    {
        using PyTimeSeriesInput::PyTimeSeriesInput;

        PyTimeSeriesReferenceInput(PyTimeSeriesReferenceInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        PyTimeSeriesReferenceInput& operator=(PyTimeSeriesReferenceInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        PyTimeSeriesReferenceInput(const PyTimeSeriesReferenceInput&) = delete;
        PyTimeSeriesReferenceInput& operator=(const PyTimeSeriesReferenceInput&) = delete;

        [[nodiscard]] nb::str to_string() const;
        [[nodiscard]] nb::str to_repr() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    // Specialized reference types - value, list, bundle, dict, set, window
    // These all inherit the base behavior and add type-specific operations

    struct PyTimeSeriesValueReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesValueReferenceInput : PyTimeSeriesReferenceInput
    {
        using PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput;
        [[nodiscard]] nb::int_ size() const;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceInput : PyTimeSeriesReferenceInput
    {
        using PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput;
        [[nodiscard]] size_t size() const;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesBundleReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput;
        [[nodiscard]] nb::int_ size() const;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesBundleReferenceInput : PyTimeSeriesReferenceInput
    {
        using PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput;
        [[nodiscard]] nb::int_ size() const;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesDictReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesDictReferenceInput : PyTimeSeriesReferenceInput
    {
        using PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceInput : PyTimeSeriesReferenceInput
    {
        using PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceInput : PyTimeSeriesReferenceInput
    {
        using PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);
    };

}  // namespace hgraph
