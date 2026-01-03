#pragma once

#include <hgraph/api/python/py_time_series.h>
#include <hgraph/types/tsd.h>

namespace hgraph
{
    struct PyTimeSeriesDictOutput : PyTimeSeriesOutput
    {
        using api_ptr = ApiPtr<TimeSeriesOutput>;
        explicit PyTimeSeriesDictOutput(api_ptr impl) : PyTimeSeriesOutput(std::move(impl)) {}

        [[nodiscard]] size_t size() const;

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

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;

        void set_item(const nb::object &key, const nb::object &value);

        void del_item(const nb::object &key);

        nb::object pop(const nb::object &key, const nb::object &default_value);

        nb::object get_ref(const nb::object &key, const nb::object &requester);

        void release_ref(const nb::object &key, const nb::object &requester);

      protected:
        TimeSeriesDictOutputImpl *impl() const;

        // Convert Python key to Value using TypeMeta
        value::Value<> key_from_python(const nb::object &key) const;
    };

    struct PyTimeSeriesDictInput : PyTimeSeriesInput
    {
        using api_ptr = ApiPtr<TimeSeriesInput>;
        explicit PyTimeSeriesDictInput(api_ptr impl) : PyTimeSeriesInput(std::move(impl)) {}

        [[nodiscard]] size_t size() const;

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

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;

        void on_key_added(const nb::object &key);
        void on_key_removed(const nb::object &key);

      protected:
        TimeSeriesDictInputImpl *impl() const;

        // Convert Python key to Value using TypeMeta
        value::Value<> key_from_python(const nb::object &key) const;
    };

    void tsd_register_with_nanobind(nb::module_ & m);
}  // namespace hgraph
