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
    [[nodiscard]] nb::object frame_to_py(const Frame &frame);
    [[nodiscard]] Value      py_arrow_to_frame(nb::handle object);
    [[nodiscard]] nb::object series_to_py(const Series &series);
    [[nodiscard]] Value      py_arrow_to_series(nb::handle object);

    /** meta -> registered python Enum class (backs the core enum ops'
        python conversion; cleared on registry reset). */
    [[nodiscard]] std::unordered_map<const ValueTypeMetaData *, nb::object> &enum_class_registry();
    [[nodiscard]] std::unordered_map<const ValueTypeMetaData *,
                                     std::unordered_map<long long, nb::object>> &
    enum_to_python_registry();
    [[nodiscard]] std::unordered_map<const ValueTypeMetaData *,
                                     std::unordered_map<std::string, long long>> &
    enum_from_python_registry();

    [[nodiscard]] ValueTypeRef delta_binding(const ValueTypeMetaData *meta);
    [[nodiscard]] Value                   py_to_value_as(nb::handle object, const ValueTypeMetaData *meta);
    [[nodiscard]] Value                   py_to_delta(nb::handle object, const TSValueTypeMetaData *ts);
    /** Internal TSS delta protocol (py-node frozenset-return shaping):
        explicit added/removed iterables. User objects go through py_to_delta,
        where a dict is NOT a spec (upstream parity: it iterates as its keys). */
    [[nodiscard]] Value py_tss_spec_to_delta(nb::handle add_from, nb::handle remove_from,
                                             const TSValueTypeMetaData *ts);
    void                                  install_value_conversion_hooks();
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
