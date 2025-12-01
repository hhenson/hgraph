#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/hgraph_base.h>

namespace hgraph
{

    template <typename T_TS, typename T_U>
    concept is_py_tsl = ((std::is_same_v<T_TS, PyTimeSeriesInput> || std::is_same_v<T_TS, PyTimeSeriesOutput>) &&
                         ((std::is_same_v<T_TS, PyTimeSeriesInput> && std::is_same_v<T_U, TimeSeriesListInput>) ||
                          (std::is_same_v<T_TS, PyTimeSeriesOutput> && std::is_same_v<T_U, TimeSeriesListOutput>)));

    template <typename T_TS, typename T_U>
        requires(is_py_tsl<T_TS, T_U>)
    struct PyTimeSeriesList : T_TS
    {
        using underlying_type = T_U;
        using api_ptr = ApiPtr<underlying_type>;

        explicit PyTimeSeriesList(api_ptr impl);
        explicit PyTimeSeriesList(underlying_type *impl, const control_block_ptr &cb);
        explicit PyTimeSeriesList(underlying_type *impl);

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

        // Delete copy constructor and assignment
        PyTimeSeriesList(const PyTimeSeriesList&) = delete;
        PyTimeSeriesList& operator=(const PyTimeSeriesList&) = delete;

        // Default iterator iterates over keys to keep this more consistent with Python (c.f. dict)
        [[nodiscard]] nb::object iter() const;

        [[nodiscard]] nb::object get_item(const nb::handle &key) const;

        // Retrieves valid keys
        [[nodiscard]] nb::object keys() const;

        [[nodiscard]] nb::object values() const;

        [[nodiscard]] nb::object valid_keys() const;

        [[nodiscard]] nb::object modified_keys() const;

        [[nodiscard]] nb::int_ len() const;

        // Retrieves valid items
        [[nodiscard]] nb::object items() const;

        [[nodiscard]] nb::object valid_values() const;

        [[nodiscard]] nb::object valid_items() const;

        [[nodiscard]] nb::object modified_values() const;

        [[nodiscard]] nb::object modified_items() const;

        [[nodiscard]] bool empty() const;

        [[nodiscard]] nb::str py_str();
        [[nodiscard]] nb::str py_repr();

      private:
        underlying_type *impl() const;
    };


    using PyTimeSeriesListOutput = PyTimeSeriesList<PyTimeSeriesOutput, TimeSeriesListOutput>;

    using PyTimeSeriesListInput = PyTimeSeriesList<PyTimeSeriesInput, TimeSeriesListInput>;

    void tsl_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
