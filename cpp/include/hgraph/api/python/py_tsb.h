#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/hgraph_base.h>

namespace hgraph
{

    template <typename T_TS, typename T_U>
    concept is_py_tsb = ((std::is_same_v<T_TS, PyTimeSeriesInput> || std::is_same_v<T_TS, PyTimeSeriesOutput>) &&
                         ((std::is_same_v<T_TS, PyTimeSeriesInput> && std::is_same_v<T_U, TimeSeriesBundleInput>) ||
                          (std::is_same_v<T_TS, PyTimeSeriesOutput> && std::is_same_v<T_U, TimeSeriesBundleOutput>)));

    template <typename T_TS, typename T_U>
        requires(is_py_tsb<T_TS, T_U>)
    struct PyTimeSeriesBundle : T_TS
    {
        using underlying_type = T_U;
        using api_ptr = ApiPtr<underlying_type>;

        explicit PyTimeSeriesBundle(api_ptr impl);
        explicit PyTimeSeriesBundle(underlying_type *impl, const control_block_ptr &cb);
        explicit PyTimeSeriesBundle(underlying_type *impl);

        // Move constructor
        PyTimeSeriesBundle(PyTimeSeriesBundle&& other) noexcept
            : T_TS(std::move(other)) {}

        // Move assignment
        PyTimeSeriesBundle& operator=(PyTimeSeriesBundle&& other) noexcept {
            if (this != &other) {
                T_TS::operator=(std::move(other));
            }
            return *this;
        }

        // Delete copy constructor and assignment
        PyTimeSeriesBundle(const PyTimeSeriesBundle&) = delete;
        PyTimeSeriesBundle& operator=(const PyTimeSeriesBundle&) = delete;

        // Default iterator iterates over keys to keep this more consistent with Python (c.f. dict)
        [[nodiscard]] nb::object iter() const;

        [[nodiscard]] nb::object get_item(const nb::handle &key) const;

        [[nodiscard]] nb::object get_attr(const nb::handle &key) const;

        [[nodiscard]] nb::bool_ contains(const nb::handle &key) const;

        [[nodiscard]] const TimeSeriesSchema &schema() const;

        [[nodiscard]] nb::object key_from_value(const nb::handle &value) const;

        // Retrieves valid keys
        [[nodiscard]] nb::object keys() const;

        [[nodiscard]] nb::object values() const;

        [[nodiscard]] nb::object valid_keys() const;

        [[nodiscard]] nb::object valid_values() const;

        [[nodiscard]] nb::object modified_keys() const;

        [[nodiscard]] nb::object modified_values() const;

        [[nodiscard]] nb::int_ len() const;

        [[nodiscard]] nb::bool_ empty() const;

        // Retrieves valid items
        [[nodiscard]] nb::object items() const;

        [[nodiscard]] nb::object valid_items() const;

        [[nodiscard]] nb::object modified_items() const;

        [[nodiscard]] nb::str py_str();
        [[nodiscard]] nb::str py_repr();

      private:
        underlying_type *impl() const;
    };

    using PyTimeSeriesBundleOutput = PyTimeSeriesBundle<PyTimeSeriesOutput, TimeSeriesBundleOutput>;
    using PyTimeSeriesBundleInput  = PyTimeSeriesBundle<PyTimeSeriesInput, TimeSeriesBundleInput>;

    void tsb_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
