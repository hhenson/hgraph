#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    template <typename T_TS, typename T_U>
    concept PyTSType = ((std::is_base_of_v<PyTimeSeriesInput, T_TS> || std::is_base_of_v<PyTimeSeriesOutput,T_TS>) &&
                        ((std::is_base_of_v<PyTimeSeriesInput, T_TS> && std::is_base_of_v<TimeSeriesSetInput, T_U>) ||
                         (std::is_base_of_v<PyTimeSeriesOutput,T_TS> && std::is_base_of_v<TimeSeriesSetOutput, T_U>)));

    template <typename T_TS, typename T_U>
        requires PyTSType<T_TS, T_U>
    struct PyTimeSeriesSet : T_TS
    {
        [[nodiscard]] bool contains(const nb::object &item) const {
            return impl()->contains(nb::cast<typename T_U::element_type>(item));
        }

        [[nodiscard]] size_t size() const { return impl()->size(); }

        [[nodiscard]] nb::bool_ empty() const { return nb::bool_(impl()->empty()); }

        [[nodiscard]] nb::object value() const { return nb::frozenset(nb::cast(impl()->value())); }

        [[nodiscard]] nb::object values() const { return value(); }

        [[nodiscard]] nb::object added() const { return nb::frozenset(nb::cast(this->impl()->added())); }

        [[nodiscard]] nb::object removed() const { return nb::frozenset(nb::cast(this->impl()->removed())); }

        [[nodiscard]] nb::bool_ was_added(const nb::object &item) const {
            return nb::bool_(this->impl()->was_added(nb::cast<typename T_U::element_type>(item)));
        }

        [[nodiscard]] nb::bool_ was_removed(const nb::object &item) const {
            return nb::bool_(impl()->was_removed(nb::cast<typename T_U::element_type>(item)));
        }

      protected:
        using T_TS::T_TS;
        [[nodiscard]] T_U *impl() const { return this->template static_cast_impl<T_U>(); };
    };

    struct PyTimeSeriesSetOutput: PyTimeSeriesOutput
    {
        using PyTimeSeriesOutput::PyTimeSeriesOutput;
    };

    template <typename T_U> struct PyTimeSeriesSetOutput_T final : PyTimeSeriesSet<PyTimeSeriesSetOutput, T_U>
    {
        using api_ptr = ApiPtr<T_U>;
        explicit PyTimeSeriesSetOutput_T(api_ptr impl);

        void remove(const nb::object &key) const;

        void add(const nb::object &key) const;

        [[nodiscard]] nb::object get_contains_output(const nb::object &item, const nb::object &requester) const;

        void release_contains_output(const nb::object &item, const nb::object &requester) const;

        [[nodiscard]] nb::object is_empty_output() const;

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    };

    struct PyTimeSeriesSetInput: PyTimeSeriesInput
    {
        using PyTimeSeriesInput::PyTimeSeriesInput;
    };

    template <typename T_U> struct PyTimeSeriesSetInput_T final : PyTimeSeriesSet<PyTimeSeriesSetInput, T_U>
    {
        using api_ptr = ApiPtr<T_U>;
        explicit PyTimeSeriesSetInput_T(api_ptr impl);

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    };

    void tss_register_with_nanobind(nb::module_ &m);
}  // namespace hgraph