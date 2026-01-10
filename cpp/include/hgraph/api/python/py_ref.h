#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/hgraph_base.h>
#include <unordered_map>

namespace hgraph
{
    // Forward declaration of TSValue
    struct TSValue;

    // Global cache for REF output values - maps TSValue* to TimeSeriesReference object
    // This is used to store TimeSeriesReference values when set via from_python on REF outputs
    // Declared here so it can be accessed from py_ts.cpp for dereferencing
    extern std::unordered_map<const TSValue*, nb::object> g_ref_output_cache;

    void ref_register_with_nanobind(nb::module_ &m);

    struct PyTimeSeriesReferenceOutput : PyTimeSeriesOutput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesReferenceOutput(TSMutableView view);

        [[nodiscard]] nb::str to_string() const;
        [[nodiscard]] nb::str to_repr() const;

        // Note: value(), delta_value(), set_value(), apply_result() are inherited
        // from PyTimeSeriesOutput. The view layer (TSView/TSMutableView) handles
        // REF-specific behavior via TSTypeKind::REF dispatch.

        static void register_with_nanobind(nb::module_ &m);

        // Move/copy
        PyTimeSeriesReferenceOutput(PyTimeSeriesReferenceOutput&& other) noexcept
            : PyTimeSeriesOutput(std::move(other)) {}
        PyTimeSeriesReferenceOutput& operator=(PyTimeSeriesReferenceOutput&& other) noexcept = default;
        PyTimeSeriesReferenceOutput(const PyTimeSeriesReferenceOutput&) = delete;
        PyTimeSeriesReferenceOutput& operator=(const PyTimeSeriesReferenceOutput&) = delete;
    };

    struct PyTimeSeriesReferenceInput : PyTimeSeriesInput
    {
        // View-based constructor (the only supported mode)
        explicit PyTimeSeriesReferenceInput(TSView view);

        [[nodiscard]] nb::str to_string() const;
        [[nodiscard]] nb::str to_repr() const;

        // Note: value() and delta_value() are inherited from PyTimeSeriesInput.
        // The view layer (TSView::to_python()) handles REF-specific behavior
        // via TSTypeKind::REF dispatch, including link navigation for inputs.

        static void register_with_nanobind(nb::module_ &m);

        // Move/copy
        PyTimeSeriesReferenceInput(PyTimeSeriesReferenceInput&& other) noexcept
            : PyTimeSeriesInput(std::move(other)) {}
        PyTimeSeriesReferenceInput& operator=(PyTimeSeriesReferenceInput&& other) noexcept = default;
        PyTimeSeriesReferenceInput(const PyTimeSeriesReferenceInput&) = delete;
        PyTimeSeriesReferenceInput& operator=(const PyTimeSeriesReferenceInput&) = delete;
    };

    // ============================================================
    // Specialized Reference Input Classes
    // ============================================================

    struct PyTimeSeriesValueReferenceInput : PyTimeSeriesReferenceInput
    {
        explicit PyTimeSeriesValueReferenceInput(TSView view);
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceInput : PyTimeSeriesReferenceInput
    {
        explicit PyTimeSeriesListReferenceInput(TSView view);
        size_t size() const;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesBundleReferenceInput : PyTimeSeriesReferenceInput
    {
        explicit PyTimeSeriesBundleReferenceInput(TSView view);
        [[nodiscard]] nb::int_ size() const;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesDictReferenceInput : PyTimeSeriesReferenceInput
    {
        explicit PyTimeSeriesDictReferenceInput(TSView view);
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceInput : PyTimeSeriesReferenceInput
    {
        explicit PyTimeSeriesSetReferenceInput(TSView view);
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceInput : PyTimeSeriesReferenceInput
    {
        explicit PyTimeSeriesWindowReferenceInput(TSView view);
        static void register_with_nanobind(nb::module_ &m);
    };

    // ============================================================
    // Specialized Reference Output Classes
    // ============================================================

    struct PyTimeSeriesValueReferenceOutput : PyTimeSeriesReferenceOutput
    {
        explicit PyTimeSeriesValueReferenceOutput(TSMutableView view);
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceOutput : PyTimeSeriesReferenceOutput
    {
        explicit PyTimeSeriesListReferenceOutput(TSMutableView view);
        [[nodiscard]] nb::int_ size() const;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesBundleReferenceOutput : PyTimeSeriesReferenceOutput
    {
        explicit PyTimeSeriesBundleReferenceOutput(TSMutableView view);
        [[nodiscard]] nb::int_ size() const;
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesDictReferenceOutput : PyTimeSeriesReferenceOutput
    {
        explicit PyTimeSeriesDictReferenceOutput(TSMutableView view);
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceOutput : PyTimeSeriesReferenceOutput
    {
        explicit PyTimeSeriesSetReferenceOutput(TSMutableView view);
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceOutput : PyTimeSeriesReferenceOutput
    {
        explicit PyTimeSeriesWindowReferenceOutput(TSMutableView view);
        static void register_with_nanobind(nb::module_ &m);
    };

}  // namespace hgraph
