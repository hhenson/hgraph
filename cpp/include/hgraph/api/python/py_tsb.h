#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/hgraph_base.h>
#include <type_traits>

namespace hgraph
{

    // Base template for bundle wrappers - now parameterized on I/O type only
    template <typename T_TS>
    struct PyTimeSeriesBundle : T_TS
    {
        // View type depends on whether this is input or output
        using view_type = std::conditional_t<std::is_same_v<T_TS, PyTimeSeriesOutput>, TSOutputView, TSInputView>;

        // View-based constructor
        explicit PyTimeSeriesBundle(view_type view);

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

        // Delete copy
        PyTimeSeriesBundle(const PyTimeSeriesBundle&) = delete;
        PyTimeSeriesBundle& operator=(const PyTimeSeriesBundle&) = delete;

        // Default iterator iterates over keys to keep this more consistent with Python (c.f. dict)
        [[nodiscard]] nb::object iter() const;

        [[nodiscard]] nb::object get_item(const nb::handle &key) const;

        [[nodiscard]] nb::object get_attr(const nb::handle &key) const;

        [[nodiscard]] nb::bool_ contains(const nb::handle &key) const;

        [[nodiscard]] nb::object schema() const;

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
    };

    using PyTimeSeriesBundleOutput = PyTimeSeriesBundle<PyTimeSeriesOutput>;
    using PyTimeSeriesBundleInput  = PyTimeSeriesBundle<PyTimeSeriesInput>;

    void tsb_register_with_nanobind(nb::module_ &m);

}  // namespace hgraph
