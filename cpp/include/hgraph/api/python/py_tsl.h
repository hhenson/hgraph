#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/hgraph_base.h>

namespace hgraph
{

    template <typename T_TS>
    struct PyTimeSeriesList : T_TS
    {
        // View type depends on whether this is input or output
        using view_type = std::conditional_t<std::is_same_v<T_TS, PyTimeSeriesOutput>, TSOutputView, TSInputView>;

        // View-based constructor
        explicit PyTimeSeriesList(view_type view);

        // Move constructor
        PyTimeSeriesList(PyTimeSeriesList&& other) noexcept
            : T_TS(std::move(other)) {}

        // Move assignment
        PyTimeSeriesList& operator=(PyTimeSeriesList&& other) noexcept {
            if (this != &other) {
                T_TS::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy
        PyTimeSeriesList(const PyTimeSeriesList&) = delete;
        PyTimeSeriesList& operator=(const PyTimeSeriesList&) = delete;

        [[nodiscard]] nb::object iter() const;
        [[nodiscard]] nb::object get_item(const nb::handle &key) const;
        [[nodiscard]] nb::object keys() const;
        [[nodiscard]] nb::object values() const;
        [[nodiscard]] nb::object valid_keys() const;
        [[nodiscard]] nb::object modified_keys() const;
        [[nodiscard]] nb::int_ len() const;
        [[nodiscard]] nb::object items() const;
        [[nodiscard]] nb::object valid_values() const;
        [[nodiscard]] nb::object valid_items() const;
        [[nodiscard]] nb::object modified_values() const;
        [[nodiscard]] nb::object modified_items() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] nb::str py_str();
        [[nodiscard]] nb::str py_repr();
    };

    using PyTimeSeriesListOutput = PyTimeSeriesList<PyTimeSeriesOutput>;
    using PyTimeSeriesListInput = PyTimeSeriesList<PyTimeSeriesInput>;

    void tsl_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
