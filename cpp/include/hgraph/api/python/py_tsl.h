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

        explicit PyTimeSeriesList(underlying_type *impl, const control_block_ptr &cb);
        explicit PyTimeSeriesList(underlying_type *impl);

        // Default iterator iterates over keys to keep this more consistent with Python (c.f. dict)
        [[nodiscard]] nb::object iter() const;

        [[nodiscard]] nb::object get_item(const nb::handle &key) const;

        // Retrieves valid keys
        [[nodiscard]] auto keys() const;

        [[nodiscard]] auto values() const;

        [[nodiscard]] auto valid_keys() const;

        [[nodiscard]] auto modified_keys() const;

        [[nodiscard]] nb::int_ len() const;

        // Retrieves valid items
        [[nodiscard]] auto items() const;

        [[nodiscard]] auto valid_values() const;

        [[nodiscard]] auto valid_items() const;

        [[nodiscard]] auto modified_values() const;

        [[nodiscard]] auto modified_items() const;

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
