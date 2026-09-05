#ifndef HGRAPH_PYTHON_MODULE_INTERNAL_H
#define HGRAPH_PYTHON_MODULE_INTERNAL_H

#include <hgraph/lib/std/operators/comparison.h>
#include <hgraph/lib/std/operators/control.h>
#include <hgraph/python/bridge_state.h>
#include <hgraph/python/chrono.h>
#include <hgraph/types/frame.h>
#include <hgraph/types/series.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value_callable.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value_builder.h>

#include <nanobind/nanobind.h>

#include <cstddef>
#include <functional>
#include <string_view>
#include <unordered_map>

namespace hgraph::python_bridge
{
    namespace nb = nanobind;

    struct PyObj;

    /**
     * Raw-CPython-entry-point boundary (the ``tp_getset`` / ``tp_*`` slot
     * family): run ``f`` and return its result. If ``f`` throws, SET the
     * corresponding Python error — an in-flight ``nb::python_error``
     * restores itself, anything else becomes ``RuntimeError`` — and return
     * ``error_result``, the slot's CPython error sentinel (``nullptr`` for
     * getters, ``-1`` for setters). The python-boundary member of the
     * ``scope.h`` ``fallback_on_exception`` / ``annotate_on_exception``
     * control family: call sites stay expression-shaped instead of
     * hand-rolled try/catch blocks.
     */
    template <typename Result, typename F>
    [[nodiscard]] Result py_error_on_exception(Result error_result, F &&f) noexcept
    {
        try
        {
            return std::forward<F>(f)();
        }
        catch (nb::python_error &error)
        {
            error.restore();
        }
        catch (const std::exception &error)
        {
            PyErr_SetString(PyExc_RuntimeError, error.what());
        }
        catch (...)
        {
            PyErr_SetString(PyExc_RuntimeError,
                            "unhandled C++ exception at the Python boundary");
        }
        return error_result;
    }
}

namespace hgraph::python_bridge
{
    struct PyObj
    {
        PyObject *object{nullptr};

        PyObj() noexcept = default;
        explicit PyObj(nb::object value) noexcept;
        PyObj(const PyObj &other) noexcept;
        PyObj(PyObj &&other) noexcept;
        PyObj &operator=(const PyObj &other) noexcept;
        PyObj &operator=(PyObj &&other) noexcept;
        ~PyObj();

        [[nodiscard]] nb::object get() const;

        friend bool operator==(const PyObj &lhs, const PyObj &rhs) noexcept;
    };
}

namespace hgraph
{
    /** PyObj wraps python objects AS-IS (the object-keyed-TSD lesson: no
        natural conversion may substitute). */
    template <>
    struct python_conversion_traits<python_bridge::PyObj>
    {
        static nanobind::object to_python(const python_bridge::PyObj &value) { return value.get(); }

        static python_bridge::PyObj from_python(nanobind::handle source)
        {
            return python_bridge::PyObj{nanobind::borrow<nanobind::object>(source)};
        }
    };
}

namespace hgraph::python_bridge
{

    struct PyOpaqueRef
    {
        Value value;
        DateTime evaluation_time{MIN_DT};
    };

    struct PyArrowStream
    {
        Frame frame;

        [[nodiscard]] nb::object capsule() const;
    };

    struct PySeriesArray
    {
        Series series;

        /** Export as the arrow C Data Interface pair (schema, array). */
        [[nodiscard]] nb::object arrow_c_array() const;
    };


    [[nodiscard]] Value      py_to_value(nb::handle object);
    [[nodiscard]] nb::object value_to_py(const ValueView &view);
    /** The polars-frames compatibility switch (issue #80): when set, the
        OUTBOUND boundary surfaces Frame as ``polars.DataFrame`` and Series
        as ``polars.Series`` (via ``polars.from_arrow``, zero-copy). Inbound
        already accepts anything exposing ``__arrow_c_stream__``. Arrow stays
        the canonical substrate; record/replay artifacts are unaffected. */
    [[nodiscard]] bool      &polars_frames_enabled();
    /** Canonical Arrow table before Python compatibility presentation. */
    [[nodiscard]] nb::object frame_to_arrow_py(const Frame &frame);
    /** Arrow table for the Python store protocol. This retains the existing
        naive-UTC compatibility surface and schema metadata, but never applies
        the optional Polars user presentation. */
    [[nodiscard]] nb::object frame_to_store_py(const Frame &frame);
    [[nodiscard]] nb::object frame_to_py(const Frame &frame);
    [[nodiscard]] Value      py_arrow_to_frame(nb::handle object);
    [[nodiscard]] nb::object series_to_py(const Series &series);
    [[nodiscard]] Value      py_arrow_to_series(nb::handle object);

    /** meta -> registered python Enum class (backs the core enum ops'
        python conversion; cleared on registry reset). */
    [[nodiscard]] std::unordered_map<const ValueTypeMetaData *, nb::object> &enum_class_registry();
    /** Exact Python Enum class -> nominal native enum schema.  This reverse
        index must be consulted before primitive conversion because IntEnum
        and StrEnum are also instances of int and str respectively. */
    [[nodiscard]] std::unordered_map<PyTypeObject *, const ValueTypeMetaData *> &enum_type_registry();
    [[nodiscard]] std::unordered_map<const ValueTypeMetaData *,
                                     std::unordered_map<long long, nb::object>> &
    enum_to_python_registry();
    [[nodiscard]] std::unordered_map<const ValueTypeMetaData *,
                                     std::unordered_map<std::string, long long>> &
    enum_from_python_registry();

    [[nodiscard]] Value                   py_to_value_as(nb::handle object, const ValueTypeMetaData *meta);
    /** The DSL's schema for a Python annotation or class through the
        registered resolver (``bridge_state.h``), ``nullptr`` when there is no
        resolver or the DSL has no schema for it. */
    [[nodiscard]] const ValueTypeMetaData *python_annotation_schema(nb::handle annotation);
    [[nodiscard]] Value                   py_to_delta(nb::handle object, const TSValueTypeMetaData *ts);
    void                                  install_value_conversion_hooks(nb::module_ &module);
}  // namespace hgraph::python_bridge

template <>
struct std::hash<hgraph::python_bridge::PyObj>
{
    [[nodiscard]] std::size_t operator()(const hgraph::python_bridge::PyObj &value) const noexcept;
};

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<hgraph::python_bridge::PyObj>
    {
        static constexpr std::string_view value{"py_object"};
    };
}  // namespace hgraph::static_schema_detail

#endif  // HGRAPH_PYTHON_MODULE_INTERNAL_H
