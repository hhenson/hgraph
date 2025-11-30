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

        [[nodiscard]] nb::str to_string() const;
        [[nodiscard]] nb::str to_repr() const;

        static void register_with_nanobind(nb::module_ &m);

    protected:
        using PyTimeSeriesOutput::PyTimeSeriesOutput;

    private:
        [[nodiscard]] TimeSeriesReferenceOutput *impl() const;
    };

    struct PyTimeSeriesReferenceInput : PyTimeSeriesInput
    {
        [[nodiscard]] nb::str to_string() const;
        [[nodiscard]] nb::str to_repr() const;

        static void register_with_nanobind(nb::module_ &m);

    protected:
        using PyTimeSeriesInput::PyTimeSeriesInput;

    private:
        [[nodiscard]] TimeSeriesReferenceInput *impl() const;
    };

    // ============================================================
    // Specialized Reference Input Classes
    // ============================================================

    struct PyTimeSeriesValueReferenceInput : PyTimeSeriesReferenceInput
    {
        explicit PyTimeSeriesValueReferenceInput(TimeSeriesValueReferenceInput* ref);
        explicit PyTimeSeriesValueReferenceInput(TimeSeriesValueReferenceInput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceInput : PyTimeSeriesReferenceInput
    {
        explicit PyTimeSeriesListReferenceInput(TimeSeriesListReferenceInput* ref);
        explicit PyTimeSeriesListReferenceInput(TimeSeriesListReferenceInput* ref, control_block_ptr control_block);

        size_t size() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesBundleReferenceInput : PyTimeSeriesReferenceInput
    {
        explicit PyTimeSeriesBundleReferenceInput(TimeSeriesBundleReferenceInput* ref);
        explicit PyTimeSeriesBundleReferenceInput(TimeSeriesBundleReferenceInput* ref, control_block_ptr control_block);

        [[nodiscard]] nb::int_ size() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesDictReferenceInput : PyTimeSeriesReferenceInput
    {
        explicit PyTimeSeriesDictReferenceInput(TimeSeriesDictReferenceInput* ref);
        explicit PyTimeSeriesDictReferenceInput(TimeSeriesDictReferenceInput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceInput : PyTimeSeriesReferenceInput
    {
        explicit PyTimeSeriesSetReferenceInput(TimeSeriesSetReferenceInput* ref);
        explicit PyTimeSeriesSetReferenceInput(TimeSeriesSetReferenceInput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceInput : PyTimeSeriesReferenceInput
    {
        explicit PyTimeSeriesWindowReferenceInput(TimeSeriesWindowReferenceInput* ref);
        explicit PyTimeSeriesWindowReferenceInput(TimeSeriesWindowReferenceInput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    // ============================================================
    // Specialized Reference Output Classes
    // ============================================================

    struct PyTimeSeriesValueReferenceOutput : PyTimeSeriesReferenceOutput
    {
        explicit PyTimeSeriesValueReferenceOutput(TimeSeriesValueReferenceOutput* ref);
        explicit PyTimeSeriesValueReferenceOutput(TimeSeriesValueReferenceOutput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceOutput : PyTimeSeriesReferenceOutput
    {
        explicit PyTimeSeriesListReferenceOutput(TimeSeriesListReferenceOutput* ref);
        explicit PyTimeSeriesListReferenceOutput(TimeSeriesListReferenceOutput* ref, control_block_ptr control_block);

        [[nodiscard]] nb::int_ size() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesBundleReferenceOutput : PyTimeSeriesReferenceOutput
    {
        explicit PyTimeSeriesBundleReferenceOutput(TimeSeriesBundleReferenceOutput* ref);
        explicit PyTimeSeriesBundleReferenceOutput(TimeSeriesBundleReferenceOutput* ref, control_block_ptr control_block);

        [[nodiscard]] nb::int_ size() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesDictReferenceOutput : PyTimeSeriesReferenceOutput
    {
        explicit PyTimeSeriesDictReferenceOutput(TimeSeriesDictReferenceOutput* ref);
        explicit PyTimeSeriesDictReferenceOutput(TimeSeriesDictReferenceOutput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceOutput : PyTimeSeriesReferenceOutput
    {
        explicit PyTimeSeriesSetReferenceOutput(TimeSeriesSetReferenceOutput* ref);
        explicit PyTimeSeriesSetReferenceOutput(TimeSeriesSetReferenceOutput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceOutput : PyTimeSeriesReferenceOutput
    {
        explicit PyTimeSeriesWindowReferenceOutput(TimeSeriesWindowReferenceOutput* ref);
        explicit PyTimeSeriesWindowReferenceOutput(TimeSeriesWindowReferenceOutput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

}  // namespace hgraph
