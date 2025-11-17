#pragma once

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/hgraph_base.h>
#include <hgraph/types/ref.h>
#include <hgraph/api/python/py_time_series.h>


namespace hgraph
{

    void register_time_series_reference_with_nanobind(nb::module_ &m);

    struct PyTimeSeriesReferenceOutput : PyTimeSeriesOutput
    {

        nb::object value() const override;

        void set_value(nb::object value) override;

        void apply_result(nb::object value) override;

        bool can_apply_result(nb::object value) override;

        // Clears the reference by setting it to an empty reference
        void clear() override;

        [[nodiscard]] nb::object delta_value() const override;

        void invalidate() override;

        // void copy_from_output(const TimeSeriesOutput &output) override;
        //
        // void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] nb::bool_ is_reference() const override;

       // [[nodiscard]] bool has_reference() const override;

        static void register_with_nanobind(nb::module_ &m);

    };

    struct PyTimeSeriesReferenceInput : PyTimeSeriesInput
    {
        using api_ptr = ApiPtr<PyTimeSeriesReferenceInput>;

        [[nodiscard]] nb::object value() const override;

        [[nodiscard]] nb::object delta_value() const override;

        // Duplicate binding of another input
        void clone_binding(const nb::object &other);

        [[nodiscard]] nb::bool_ bound() const override;

        [[nodiscard]] nb::bool_ modified() const override;

        [[nodiscard]] nb::bool_ valid() const override;

        [[nodiscard]] nb::bool_ all_valid() const override;

        [[nodiscard]] engine_time_t last_modified_time() const override;

        nb::bool_ bind_output(nb::object value) override;

        nb::bool_ un_bind_output(bool unbind_refs) override;

        void make_active() override;

        void make_passive() override;

        [[nodiscard]] nb::object get_input(size_t index) const override;

        [[nodiscard]] nb::object get_ref_input(size_t index);

        static void register_with_nanobind(nb::module_ &m);

        [[nodiscard]] nb::bool_ is_reference() const override;
    };

    // ============================================================
    // Specialized Reference Input Classes
    // ============================================================

    struct PyTimeSeriesValueReferenceInput : PyTimeSeriesReferenceInput
    {
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceInput : PyTimeSeriesReferenceInput
    {

        nb::object get_input(size_t index) const override;
        size_t           size() const;

        static void register_with_nanobind(nb::module_ &m);

    };

    struct PyTimeSeriesBundleReferenceInput : PyTimeSeriesReferenceInput
    {

        nb::int_ size() const;

        static void register_with_nanobind(nb::module_ &m);

    };

    struct PyTimeSeriesDictReferenceInput : PyTimeSeriesReferenceInput
    {
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceInput : PyTimeSeriesReferenceInput
    {
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceInput : PyTimeSeriesReferenceInput
    {
        static void register_with_nanobind(nb::module_ &m);
    };

    // ============================================================
    // Specialized Reference Output Classes
    // ============================================================

    struct PyTimeSeriesValueReferenceOutput : PyTimeSeriesReferenceOutput
    {
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesListReferenceOutput : PyTimeSeriesReferenceOutput
    {

        nb::int_ size() const;

        static void register_with_nanobind(nb::module_ &m);

    };

    struct PyTimeSeriesBundleReferenceOutput : PyTimeSeriesReferenceOutput
    {
        nb::int_ size() const;

        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesDictReferenceOutput : PyTimeSeriesReferenceOutput
    {
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesSetReferenceOutput : PyTimeSeriesReferenceOutput
    {
        static void register_with_nanobind(nb::module_ &m);
    };

    struct PyTimeSeriesWindowReferenceOutput : PyTimeSeriesReferenceOutput
    {
        static void register_with_nanobind(nb::module_ &m);
    };

}  // namespace hgraph
