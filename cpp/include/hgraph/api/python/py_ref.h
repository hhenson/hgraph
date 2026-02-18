#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/hgraph_base.h>

namespace hgraph
{

    void ref_register_with_nanobind(nb::module_ &m);

    struct PyTimeSeriesReferenceOutput : PyTimeSeriesOutput
    {
        // View-based constructor
        explicit PyTimeSeriesReferenceOutput(TSOutputView view);

        // Move constructor
        PyTimeSeriesReferenceOutput(PyTimeSeriesReferenceOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesReferenceOutput& operator=(PyTimeSeriesReferenceOutput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesOutput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy
        PyTimeSeriesReferenceOutput(const PyTimeSeriesReferenceOutput&) = delete;
        PyTimeSeriesReferenceOutput& operator=(const PyTimeSeriesReferenceOutput&) = delete;

        [[nodiscard]] nb::str to_string() const;
        [[nodiscard]] nb::str to_repr() const;

        static void register_with_nanobind(nb::module_ &m);

      protected:
        // Allow derived classes to use base constructor
        using PyTimeSeriesOutput::PyTimeSeriesOutput;
    };

    struct PyTimeSeriesReferenceInput : PyTimeSeriesInput
    {
        // View-based constructor
        explicit PyTimeSeriesReferenceInput(TSInputView view);

        // Move constructor
        PyTimeSeriesReferenceInput(PyTimeSeriesReferenceInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}

        // Move assignment
        PyTimeSeriesReferenceInput& operator=(PyTimeSeriesReferenceInput&& other) noexcept {
            if (this != &other) {
                PyTimeSeriesInput::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy
        PyTimeSeriesReferenceInput(const PyTimeSeriesReferenceInput&) = delete;
        PyTimeSeriesReferenceInput& operator=(const PyTimeSeriesReferenceInput&) = delete;

        // Override value to return TimeSeriesReference (matching Python semantics)
        // For non-peered binding (TS→REF): returns BoundTimeSeriesReference wrapping the target output
        // For peered binding (REF→REF): returns the output's TSReference value
        [[nodiscard]] nb::object ref_value() const;
        [[nodiscard]] nb::object modified_items() const;
        [[nodiscard]] nb::object removed_keys() const;

        [[nodiscard]] nb::str to_string() const;
        [[nodiscard]] nb::str to_repr() const;

        static void register_with_nanobind(nb::module_ &m);

      protected:
        // Allow derived classes to use base constructor
        using PyTimeSeriesInput::PyTimeSeriesInput;
    };

    // Specialized Reference Input Classes - kept as empty subclasses for nanobind registration
    struct PyTimeSeriesValueReferenceInput : PyTimeSeriesReferenceInput
    {
        using PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceInput : PyTimeSeriesReferenceInput
    {
        using PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesBundleReferenceInput : PyTimeSeriesReferenceInput
    {
        using PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesDictReferenceInput : PyTimeSeriesReferenceInput
    {
        using PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceInput : PyTimeSeriesReferenceInput
    {
        using PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceInput : PyTimeSeriesReferenceInput
    {
        using PyTimeSeriesReferenceInput::PyTimeSeriesReferenceInput;
        static void register_with_nanobind(nb::module_ &m);
    };

    // Specialized Reference Output Classes - kept as empty subclasses for nanobind registration
    struct PyTimeSeriesValueReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesBundleReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesDictReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using PyTimeSeriesReferenceOutput::PyTimeSeriesReferenceOutput;
        static void register_with_nanobind(nb::module_ &m);
    };

}  // namespace hgraph
