#pragma once

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/hgraph_base.h>
#include <hgraph/types/ref.h>
#include <hgraph/api/python/py_time_series.h>


namespace hgraph
{

    void ref_register_with_nanobind(nb::module_ &m);

    struct PyTimeSeriesReferenceOutput : PyTimeSeriesOutput
    {
        using api_ptr = ApiPtr<TimeSeriesReferenceOutput>;

        // Legacy constructor - uses ApiPtr
        explicit PyTimeSeriesReferenceOutput(api_ptr impl);

        // New view-based constructor
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

        // Delete copy constructor and assignment
        PyTimeSeriesReferenceOutput(const PyTimeSeriesReferenceOutput&) = delete;
        PyTimeSeriesReferenceOutput& operator=(const PyTimeSeriesReferenceOutput&) = delete;

        [[nodiscard]] nb::str to_string() const;
        [[nodiscard]] nb::str to_repr() const;

        static void register_with_nanobind(nb::module_ &m);

    protected:
        // Allow derived classes to use either constructor type
        using PyTimeSeriesOutput::PyTimeSeriesOutput;

    private:
        [[nodiscard]] TimeSeriesReferenceOutput *impl() const;
    };

    struct PyTimeSeriesReferenceInput : PyTimeSeriesInput
    {
        using api_ptr = ApiPtr<TimeSeriesReferenceInput>;

        // Legacy constructor - uses ApiPtr
        explicit PyTimeSeriesReferenceInput(api_ptr impl);

        // New view-based constructor
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

        // Delete copy constructor and assignment
        PyTimeSeriesReferenceInput(const PyTimeSeriesReferenceInput&) = delete;
        PyTimeSeriesReferenceInput& operator=(const PyTimeSeriesReferenceInput&) = delete;

        [[nodiscard]] nb::str to_string() const;
        [[nodiscard]] nb::str to_repr() const;

        static void register_with_nanobind(nb::module_ &m);

    protected:
        // Allow derived classes to use either constructor type
        using PyTimeSeriesInput::PyTimeSeriesInput;

    private:
        [[nodiscard]] TimeSeriesReferenceInput *impl() const;
    };

    // ============================================================
    // Specialized Reference Input Classes
    // ============================================================

    struct PyTimeSeriesValueReferenceInput : PyTimeSeriesReferenceInput
    {
        using api_ptr = ApiPtr<TimeSeriesValueReferenceInput>;
        explicit PyTimeSeriesValueReferenceInput(api_ptr impl);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceInput : PyTimeSeriesReferenceInput
    {
        using api_ptr = ApiPtr<TimeSeriesListReferenceInput>;
        explicit PyTimeSeriesListReferenceInput(api_ptr impl);

        size_t size() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesBundleReferenceInput : PyTimeSeriesReferenceInput
    {
        using api_ptr = ApiPtr<TimeSeriesBundleReferenceInput>;
        explicit PyTimeSeriesBundleReferenceInput(api_ptr impl);

        [[nodiscard]] nb::int_ size() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesDictReferenceInput : PyTimeSeriesReferenceInput
    {
        using api_ptr = ApiPtr<TimeSeriesDictReferenceInput>;
        explicit PyTimeSeriesDictReferenceInput(api_ptr impl);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceInput : PyTimeSeriesReferenceInput
    {
        using api_ptr = ApiPtr<TimeSeriesSetReferenceInput>;
        explicit PyTimeSeriesSetReferenceInput(api_ptr impl);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceInput : PyTimeSeriesReferenceInput
    {
        using api_ptr = ApiPtr<TimeSeriesWindowReferenceInput>;
        explicit PyTimeSeriesWindowReferenceInput(api_ptr impl);

        static void register_with_nanobind(nb::module_ &m);
    };

    // ============================================================
    // Specialized Reference Output Classes
    // ============================================================

    struct PyTimeSeriesValueReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using api_ptr = ApiPtr<TimeSeriesValueReferenceOutput>;
        explicit PyTimeSeriesValueReferenceOutput(api_ptr impl);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using api_ptr = ApiPtr<TimeSeriesListReferenceOutput>;
        explicit PyTimeSeriesListReferenceOutput(api_ptr impl);

        [[nodiscard]] nb::int_ size() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesBundleReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using api_ptr = ApiPtr<TimeSeriesBundleReferenceOutput>;
        explicit PyTimeSeriesBundleReferenceOutput(api_ptr impl);

        [[nodiscard]] nb::int_ size() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesDictReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using api_ptr = ApiPtr<TimeSeriesDictReferenceOutput>;
        explicit PyTimeSeriesDictReferenceOutput(api_ptr impl);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using api_ptr = ApiPtr<TimeSeriesSetReferenceOutput>;
        explicit PyTimeSeriesSetReferenceOutput(api_ptr impl);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using api_ptr = ApiPtr<TimeSeriesWindowReferenceOutput>;
        explicit PyTimeSeriesWindowReferenceOutput(api_ptr impl);

        static void register_with_nanobind(nb::module_ &m);
    };

}  // namespace hgraph
