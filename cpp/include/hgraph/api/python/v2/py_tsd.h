#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/tsd.h>

namespace hgraph
{
    template <typename T_TS, typename T_U>
    concept is_py_tsd = ((std::is_base_of_v<PyTimeSeriesInput, T_TS> || std::is_base_of_v<PyTimeSeriesOutput, T_TS>) &&
                         ((std::is_base_of_v<PyTimeSeriesInput, T_TS> && std::is_base_of_v<TimeSeriesDictInput, T_U>) ||
                          (std::is_base_of_v<PyTimeSeriesOutput, T_TS> && std::is_base_of_v<TimeSeriesDictOutput, T_U>)));

    template <typename T_TS, typename T_U>
        requires is_py_tsd<T_TS, T_U>
    struct PyTimeSeriesDict : T_TS
    {
        [[nodiscard]] size_t size() const;

        // Create a set of Python-based API, for non-object-based instances there will
        // be typed analogues.
        [[nodiscard]] nb::object get_item(const nb::object &item) const;

        [[nodiscard]] nb::object get(const nb::object &item, const nb::object &default_value) const;

        [[nodiscard]] nb::object get_or_create(const nb::object &key);

        void create(const nb::object &item);

        [[nodiscard]] nb::object iter() const;

        [[nodiscard]] bool contains(const nb::object &item) const;

        [[nodiscard]] nb::object key_set() const;

        [[nodiscard]] nb::object keys() const;

        [[nodiscard]] nb::object values() const;

        [[nodiscard]] nb::object items() const;

        [[nodiscard]] nb::object modified_keys() const;

        [[nodiscard]] nb::object modified_values() const;

        [[nodiscard]] nb::object modified_items() const;

        [[nodiscard]] bool was_modified(const nb::object &key) const;

        [[nodiscard]] nb::object valid_keys() const;

        [[nodiscard]] nb::object valid_values() const;

        [[nodiscard]] nb::object valid_items() const;

        [[nodiscard]] nb::object added_keys() const;

        [[nodiscard]] nb::object added_values() const;

        [[nodiscard]] nb::object added_items() const;

        [[nodiscard]] bool has_added() const;

        [[nodiscard]] bool was_added(const nb::object &key) const;

        [[nodiscard]] nb::object removed_keys() const;

        [[nodiscard]] nb::object removed_values() const;

        [[nodiscard]] nb::object removed_items() const;

        [[nodiscard]] bool has_removed() const;

        [[nodiscard]] bool was_removed(const nb::object &key) const;

        [[nodiscard]] nb::object key_from_value(const nb::object &value) const;

        [[nodiscard]] nb::str py_str();
        [[nodiscard]] nb::str py_repr();
      protected:
        using T_TS::T_TS;
        T_U *impl() const;
    };

    struct PyTimeSeriesDictOutput : PyTimeSeriesOutput
    {
        using PyTimeSeriesOutput::PyTimeSeriesOutput;
    };

    template<typename T_U>
    requires std::is_base_of_v<TimeSeriesDictOutput, T_U>
    struct PyTimeSeriesDictOutput_T : PyTimeSeriesDict<PyTimeSeriesDictOutput, T_U>
    {
        using api_ptr = ApiPtr<T_U>;
        explicit PyTimeSeriesDictOutput_T(api_ptr impl);

        void set_item(const nb::object &key, const nb::object &value);

        void del_item(const nb::object &key);

        nb::object pop(const nb::object &key, const nb::object &default_value);

        nb::object get_ref(const nb::object &key, const nb::object &requester);

        void release_ref(const nb::object &key, const nb::object &requester);
    };

    struct PyTimeSeriesDictInput : PyTimeSeriesInput
    {
        using PyTimeSeriesInput::PyTimeSeriesInput;
    };

    template<typename T_U>
    requires std::is_base_of_v<TimeSeriesDictInput, T_U>
    struct PyTimeSeriesDictInput_T : PyTimeSeriesDict<PyTimeSeriesDictInput, T_U>
    {
        using api_ptr = ApiPtr<T_U>;

        void on_key_added(const nb::object &key);
        void on_key_removed(const nb::object &key);

        explicit PyTimeSeriesDictInput_T(api_ptr impl);
    };

    void tsd_register_with_nanobind(nb::module_ & m);
}  // namespace hgraph
