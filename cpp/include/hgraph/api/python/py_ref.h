#pragma once

#include <hgraph/api/python/py_time_series.h>

#include <hgraph/api/python/api_ptr.h>
#include <hgraph/hgraph_base.h>

namespace hgraph
{

    struct HGRAPH_EXPORT PyTimeSeriesReference
    {
        using api_ptr = ApiPtr<TimeSeriesReference>;
        virtual ~PyTimeSeriesReference() = default;

        void bind_input(nb::object &ts_input) const; //PyTimeSeriesInput

        virtual nb::bool_ has_output() const = 0;

        virtual nb::bool_ is_empty() const = 0;

        virtual nb::bool_ is_valid() const = 0;

        virtual nb::bool_ eq(const nb::handle &other) const = 0;

        virtual std::string to_string() const = 0;

        static nb::object make(nb::object ts, nb::object items); // time_series_output_ptr | std::vector<ptr> | std::vector<nb::ref<TimeSeriesReferenceInput>>

        static void register_with_nanobind(nb::module_ &m);

    protected:
        PyTimeSeriesReference(api_ptr impl);

    private:
        api_ptr _impl;
    };

    struct EmptyTimeSeriesReference final : PyTimeSeriesReference
    {
        void bind_input(nb::object &ts_input) const override;

        nb::bool_ has_output() const override;

        nb::bool_ is_empty() const override;

        nb::bool_ is_valid() const override;

        nb::bool_ eq(const nb::handle &other) const override;

        std::string to_string() const override;
    };

    struct BoundTimeSeriesReference final : PyTimeSeriesReference
    {
        explicit BoundTimeSeriesReference(const time_series_output_ptr &output);

        nb::object output() const;

        void bind_input(TimeSeriesInput &input_) const override;

        bool has_output() const override;

        bool is_empty() const override;

        bool is_valid() const override;

        bool operator==(const TimeSeriesReferenceOutput &other) const override;

        std::string to_string() const override;

      private:
        TimeSeriesOutput::ptr _output;
    };

    struct UnBoundTimeSeriesReference final : TimeSeriesReference
    {
        explicit UnBoundTimeSeriesReference(std::vector<ptr> items);

        const std::vector<ptr> &items() const;

        void bind_input(TimeSeriesInput &input_) const override;

        bool has_output() const override;

        bool is_empty() const override;

        bool is_valid() const override;

        bool operator==(const TimeSeriesReferenceOutput &other) const override;

        const ptr &operator[](size_t ndx);

        std::string to_string() const override;

      private:
        api_ptr
    };

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

        void copy_from_output(const TimeSeriesOutput &output) override;

        void copy_from_input(const TimeSeriesInput &input) override;

        [[nodiscard]] bool is_reference() const override;

        [[nodiscard]] bool has_reference() const override;

        static void register_with_nanobind(nb::module_ &m);

      protected:
        [[nodiscard]] bool has_value() const;

        void reset_value();

      private:
        friend struct TimeSeriesReferenceInput;
        friend struct TimeSeriesReference;
        TimeSeriesReference::ptr _value;
        // Use a raw pointer as we don't have hash implemented on ptr at the moment,
        // So this is a work arround the code managing this also ensures the pointers are incremented
        // and decremented.
        std::unordered_set<TimeSeriesInput::ptr> _reference_observers;
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
        size_t           size() const { return _size; }

        static void register_with_nanobind(nb::module_ &m);

      private:
        size_t _size{0};
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
