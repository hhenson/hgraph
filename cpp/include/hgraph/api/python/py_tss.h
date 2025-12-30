#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <hgraph/types/tss.h>

namespace hgraph
{
    /**
     * @brief Python wrapper for TimeSeriesSetOutput.
     *
     * Non-templated wrapper that uses Python interop methods for all operations.
     */
    struct PyTimeSeriesSetOutput: PyTimeSeriesOutput
    {
        using api_ptr = ApiPtr<TimeSeriesSetOutput>;
        explicit PyTimeSeriesSetOutput(api_ptr impl) : PyTimeSeriesOutput(std::move(impl)) {}

        [[nodiscard]] bool contains(const nb::object &item) const { return impl()->py_contains(item); }
        [[nodiscard]] size_t size() const { return impl()->size(); }
        [[nodiscard]] nb::bool_ empty() const { return nb::bool_(impl()->empty()); }
        [[nodiscard]] nb::object value() const { return impl()->py_value(); }
        [[nodiscard]] nb::object values() const { return value(); }
        [[nodiscard]] nb::object added() const { return impl()->py_added(); }
        [[nodiscard]] nb::object removed() const { return impl()->py_removed(); }
        [[nodiscard]] nb::bool_ was_added(const nb::object &item) const { return nb::bool_(impl()->py_was_added(item)); }
        [[nodiscard]] nb::bool_ was_removed(const nb::object &item) const { return nb::bool_(impl()->py_was_removed(item)); }
        void add(const nb::object &key) const { impl()->py_add(key); }
        void remove(const nb::object &key) const { impl()->py_remove(key); }
        [[nodiscard]] nb::object get_contains_output(const nb::object &item, const nb::object &requester) const {
            return wrap_output(impl()->get_contains_output(item, requester));
        }
        void release_contains_output(const nb::object &item, const nb::object &requester) const {
            impl()->release_contains_output(item, requester);
        }
        [[nodiscard]] nb::object is_empty_output() const { return wrap_output(impl()->is_empty_output()); }
        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;

    protected:
        [[nodiscard]] TimeSeriesSetOutput *impl() const { return static_cast_impl<TimeSeriesSetOutput>(); }
    };

    /**
     * @brief Python wrapper for TimeSeriesSetInput.
     *
     * Non-templated wrapper that uses Python interop methods for all operations.
     */
    struct PyTimeSeriesSetInput: PyTimeSeriesInput
    {
        using api_ptr = ApiPtr<TimeSeriesSetInput>;
        explicit PyTimeSeriesSetInput(api_ptr impl) : PyTimeSeriesInput(std::move(impl)) {}

        [[nodiscard]] bool contains(const nb::object &item) const { return impl()->py_contains(item); }
        [[nodiscard]] size_t size() const { return impl()->size(); }
        [[nodiscard]] nb::bool_ empty() const { return nb::bool_(impl()->empty()); }
        [[nodiscard]] nb::object value() const { return impl()->py_value(); }
        [[nodiscard]] nb::object values() const { return value(); }
        [[nodiscard]] nb::object added() const { return impl()->py_added(); }
        [[nodiscard]] nb::object removed() const { return impl()->py_removed(); }
        [[nodiscard]] nb::bool_ was_added(const nb::object &item) const { return nb::bool_(impl()->py_was_added(item)); }
        [[nodiscard]] nb::bool_ was_removed(const nb::object &item) const { return nb::bool_(impl()->py_was_removed(item)); }
        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;

    protected:
        [[nodiscard]] TimeSeriesSetInput *impl() const { return static_cast_impl<TimeSeriesSetInput>(); }
    };

    void tss_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
