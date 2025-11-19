#pragma once

#include <hgraph/api/python/py_time_series.h>

namespace hgraph
{
    template <typename T>
    concept PyTSType = std::is_same_v<T, PyTimeSeriesInput> || std::is_same_v<T, PyTimeSeriesOutput>;

    template <typename T_TS>
        requires PyTSType<T_TS>
    struct PyTimeSeriesSet : T_TS
    {
        using ts_type = std::conditional_t<std::is_same_v<PyTimeSeriesInput, T_TS>, TimeSeriesSetInput, TimeSeriesSetOutput>;

        [[nodiscard]] bool contains(const nb::object &item) const { return impl()->py_contains(item); }

        [[nodiscard]] size_t size() const { return impl()->size(); }

        [[nodiscard]] nb::bool_ empty() const { return nb::bool_(impl()->empty()); }

        [[nodiscard]] nb::object values() const { return impl()->py_values(); }

        [[nodiscard]] nb::object added() const { return impl()->py_added(); }

        [[nodiscard]] nb::bool_ was_added(const nb::object &item) const { return nb::bool_(impl()->py_was_added(item)); }

        [[nodiscard]] nb::object removed() const { return impl()->py_removed(); }

        [[nodiscard]] nb::bool_ was_removed(const nb::object &item) const { return nb::bool_(impl()->py_was_removed(item)); }

      protected:
        using T_TS::T_TS;

      private:
        [[nodiscard]] ts_type *impl() const { return this->template static_cast_impl<ts_type>(); };
    };

    struct PyTimeSeriesSetOutput : PyTimeSeriesSet<PyTimeSeriesOutput>
    {
        explicit PyTimeSeriesSetOutput(TimeSeriesSetOutput* o, control_block_ptr cb);
        explicit PyTimeSeriesSetOutput(TimeSeriesSetOutput* o);

        void remove(const nb::object &key) const;

        void add(const nb::object &key) const;

        [[nodiscard]] nb::object get_contains_output(const nb::object &item, const nb::object &requester) const;

        void release_contains_output(const nb::object &item, const nb::object &requester) const;

        [[nodiscard]] nb::object is_empty_output() const;

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    private:
        [[nodiscard]] TimeSeriesSetOutput *impl() const;
    };

    struct PyTimeSeriesSetInput : PyTimeSeriesSet<PyTimeSeriesInput>
    {
        explicit PyTimeSeriesSetInput(TimeSeriesSetInput* o, control_block_ptr cb);
        explicit PyTimeSeriesSetInput(TimeSeriesSetInput* o);

        [[nodiscard]] nb::str py_str() const;
        [[nodiscard]] nb::str py_repr() const;
    private:
        [[nodiscard]] TimeSeriesSetInput *impl() const;
    };

    void tss_register_with_nanobind(nb::module_ & m);
}  // namespace hgraph