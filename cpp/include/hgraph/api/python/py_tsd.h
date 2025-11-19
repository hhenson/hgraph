#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/tsd.h>

namespace hgraph
{
    template <typename T_TS, typename T_U>
    concept is_py_tsd = ((std::is_same_v<T_TS, PyTimeSeriesInput> || std::is_same_v<T_TS, PyTimeSeriesOutput>) &&
                         ((std::is_same_v<T_TS, PyTimeSeriesInput> && std::is_base_of_v<TimeSeriesDictInput, T_U>) ||
                          (std::is_same_v<T_TS, PyTimeSeriesOutput> && std::is_base_of_v<TimeSeriesDictOutput, T_U>)));

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

        [[nodiscard]] nb::iterator iter();

        [[nodiscard]] bool contains(const nb::object &item) const;

        [[nodiscard]] nb::object key_set() const;

        [[nodiscard]] nb::iterator keys() const;

        [[nodiscard]] nb::iterator values() const;

        [[nodiscard]] nb::iterator items() const;

        [[nodiscard]] nb::iterator modified_keys() const;

        [[nodiscard]] nb::iterator modified_values() const;

        [[nodiscard]] nb::iterator modified_items() const;

        [[nodiscard]] bool was_modified(const nb::object &key) const;

        [[nodiscard]] nb::iterator valid_keys() const;

        [[nodiscard]] nb::iterator valid_values() const;

        [[nodiscard]] nb::iterator valid_items() const;

        [[nodiscard]] nb::iterator added_keys() const;

        [[nodiscard]] nb::iterator added_values() const;

        [[nodiscard]] nb::iterator added_items() const;

        [[nodiscard]] bool has_added() const;

        [[nodiscard]] bool was_added(const nb::object &key) const;

        [[nodiscard]] nb::iterator removed_keys() const;

        [[nodiscard]] nb::iterator removed_values() const;

        [[nodiscard]] nb::iterator removed_items() const;

        [[nodiscard]] bool has_removed() const;

        [[nodiscard]] bool was_removed(const nb::object &key) const;

        [[nodiscard]] nb::str py_str();
        [[nodiscard]] nb::str py_repr();
      protected:
        using T_TS::T_TS;
        T_U *impl() const;
    };

    template<typename T_U>
    requires std::is_base_of_v<TimeSeriesDictOutput, T_U>
    struct PyTimeSeriesDictOutput : PyTimeSeriesDict<PyTimeSeriesOutput, T_U>
    {
        explicit PyTimeSeriesDictOutput(T_U* o, control_block_ptr cb);
        explicit PyTimeSeriesDictOutput(T_U* o);

        void set_item(const nb::object &key, const nb::object &value);

        void del_item(const nb::object &key);

        nb::object pop(const nb::object &key, const nb::object &default_value);

        nb::object get_ref(const nb::object &key, const nb::object &requester);

        void release_ref(const nb::object &key, const nb::object &requester);
    };

    template<typename T_U>
    requires std::is_base_of_v<TimeSeriesDictInput, T_U>
    struct PyTimeSeriesDictInput : PyTimeSeriesDict<PyTimeSeriesInput, T_U>
    {
        explicit PyTimeSeriesDictInput(T_U* o, control_block_ptr cb);
        explicit PyTimeSeriesDictInput(T_U* o);
    };

    void tsd_register_with_nanobind(nb::module_ & m);
}  // namespace hgraph
