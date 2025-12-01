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
        using api_ptr = ApiPtr<TimeSeriesValueReferenceInput>;
        explicit PyTimeSeriesValueReferenceInput(api_ptr impl);
        explicit PyTimeSeriesValueReferenceInput(TimeSeriesValueReferenceInput* ref);
        explicit PyTimeSeriesValueReferenceInput(TimeSeriesValueReferenceInput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceInput : PyTimeSeriesReferenceInput
    {
        using api_ptr = ApiPtr<TimeSeriesListReferenceInput>;
        explicit PyTimeSeriesListReferenceInput(api_ptr impl);
        explicit PyTimeSeriesListReferenceInput(TimeSeriesListReferenceInput* ref);
        explicit PyTimeSeriesListReferenceInput(TimeSeriesListReferenceInput* ref, control_block_ptr control_block);

        size_t size() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesBundleReferenceInput : PyTimeSeriesReferenceInput
    {
        using api_ptr = ApiPtr<TimeSeriesBundleReferenceInput>;
        explicit PyTimeSeriesBundleReferenceInput(api_ptr impl);
        explicit PyTimeSeriesBundleReferenceInput(TimeSeriesBundleReferenceInput* ref);
        explicit PyTimeSeriesBundleReferenceInput(TimeSeriesBundleReferenceInput* ref, control_block_ptr control_block);

        [[nodiscard]] nb::int_ size() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesDictReferenceInput : PyTimeSeriesReferenceInput
    {
        using api_ptr = ApiPtr<TimeSeriesDictReferenceInput>;
        explicit PyTimeSeriesDictReferenceInput(api_ptr impl);
        explicit PyTimeSeriesDictReferenceInput(TimeSeriesDictReferenceInput* ref);
        explicit PyTimeSeriesDictReferenceInput(TimeSeriesDictReferenceInput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceInput : PyTimeSeriesReferenceInput
    {
        using api_ptr = ApiPtr<TimeSeriesSetReferenceInput>;
        explicit PyTimeSeriesSetReferenceInput(api_ptr impl);
        explicit PyTimeSeriesSetReferenceInput(TimeSeriesSetReferenceInput* ref);
        explicit PyTimeSeriesSetReferenceInput(TimeSeriesSetReferenceInput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceInput : PyTimeSeriesReferenceInput
    {
        using api_ptr = ApiPtr<TimeSeriesWindowReferenceInput>;
        explicit PyTimeSeriesWindowReferenceInput(api_ptr impl);
        explicit PyTimeSeriesWindowReferenceInput(TimeSeriesWindowReferenceInput* ref);
        explicit PyTimeSeriesWindowReferenceInput(TimeSeriesWindowReferenceInput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    // ============================================================
    // Specialized Reference Output Classes
    // ============================================================

    struct PyTimeSeriesValueReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using api_ptr = ApiPtr<TimeSeriesValueReferenceOutput>;
        explicit PyTimeSeriesValueReferenceOutput(api_ptr impl);
        explicit PyTimeSeriesValueReferenceOutput(TimeSeriesValueReferenceOutput* ref);
        explicit PyTimeSeriesValueReferenceOutput(TimeSeriesValueReferenceOutput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using api_ptr = ApiPtr<TimeSeriesListReferenceOutput>;
        explicit PyTimeSeriesListReferenceOutput(api_ptr impl);
        explicit PyTimeSeriesListReferenceOutput(TimeSeriesListReferenceOutput* ref);
        explicit PyTimeSeriesListReferenceOutput(TimeSeriesListReferenceOutput* ref, control_block_ptr control_block);

        [[nodiscard]] nb::int_ size() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesBundleReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using api_ptr = ApiPtr<TimeSeriesBundleReferenceOutput>;
        explicit PyTimeSeriesBundleReferenceOutput(api_ptr impl);
        explicit PyTimeSeriesBundleReferenceOutput(TimeSeriesBundleReferenceOutput* ref);
        explicit PyTimeSeriesBundleReferenceOutput(TimeSeriesBundleReferenceOutput* ref, control_block_ptr control_block);

        [[nodiscard]] nb::int_ size() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesDictReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using api_ptr = ApiPtr<TimeSeriesDictReferenceOutput>;
        explicit PyTimeSeriesDictReferenceOutput(api_ptr impl);
        explicit PyTimeSeriesDictReferenceOutput(TimeSeriesDictReferenceOutput* ref);
        explicit PyTimeSeriesDictReferenceOutput(TimeSeriesDictReferenceOutput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using api_ptr = ApiPtr<TimeSeriesSetReferenceOutput>;
        explicit PyTimeSeriesSetReferenceOutput(api_ptr impl);
        explicit PyTimeSeriesSetReferenceOutput(TimeSeriesSetReferenceOutput* ref);
        explicit PyTimeSeriesSetReferenceOutput(TimeSeriesSetReferenceOutput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceOutput : PyTimeSeriesReferenceOutput
    {
        using api_ptr = ApiPtr<TimeSeriesWindowReferenceOutput>;
        explicit PyTimeSeriesWindowReferenceOutput(api_ptr impl);
        explicit PyTimeSeriesWindowReferenceOutput(TimeSeriesWindowReferenceOutput* ref);
        explicit PyTimeSeriesWindowReferenceOutput(TimeSeriesWindowReferenceOutput* ref, control_block_ptr control_block);

        static void register_with_nanobind(nb::module_ &m);
    };

}  // namespace hgraph
