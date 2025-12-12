#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    // Forward declarations
    struct TimeSeriesSetOutput;
    struct TimeSeriesSetInput;

    /**
     * Python wrapper for non-templated TimeSeriesSetOutput.
     * All type-specific operations are handled internally by the underlying class.
     */
    struct PyTimeSeriesSetOutput final : PyTimeSeriesOutput
    {
        using api_ptr = ApiPtr<TimeSeriesSetOutput>;
        explicit PyTimeSeriesSetOutput(api_ptr impl);

        [[nodiscard]] bool contains(const nb::object &item) const;
        [[nodiscard]] size_t size() const;
        [[nodiscard]] nb::bool_ empty() const;
        [[nodiscard]] nb::object value() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object added() const;
        [[nodiscard]] nb::object removed() const;
        [[nodiscard]] nb::bool_ was_added(const nb::object &item) const;
        [[nodiscard]] nb::bool_ was_removed(const nb::object &item) const;

        void remove(const nb::object &key) const;
        void add(const nb::object &key) const;

        [[nodiscard]] nb::object get_contains_output(const nb::object &item, const nb::object &requester) const;
        void release_contains_output(const nb::object &item, const nb::object &requester) const;
        [[nodiscard]] nb::object is_empty_output() const;

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;

    protected:
        [[nodiscard]] TimeSeriesSetOutput* impl() const;
    };

    /**
     * Python wrapper for non-templated TimeSeriesSetInput.
     * All type-specific operations are handled internally by the underlying class.
     */
    struct PyTimeSeriesSetInput final : PyTimeSeriesInput
    {
        using api_ptr = ApiPtr<TimeSeriesSetInput>;
        explicit PyTimeSeriesSetInput(api_ptr impl);

        [[nodiscard]] bool contains(const nb::object &item) const;
        [[nodiscard]] size_t size() const;
        [[nodiscard]] nb::bool_ empty() const;
        [[nodiscard]] nb::object value() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object added() const;
        [[nodiscard]] nb::object removed() const;
        [[nodiscard]] nb::bool_ was_added(const nb::object &item) const;
        [[nodiscard]] nb::bool_ was_removed(const nb::object &item) const;

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;

    protected:
        [[nodiscard]] TimeSeriesSetInput* impl() const;
    };

    void tss_register_with_nanobind(nb::module_ &m);
}  // namespace hgraph
