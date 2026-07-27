#include "module_internal.h"

#include <hgraph/python/native_scalar_registration.h>
#include <hgraph/types/metadata/type_realization.h>

#include <hgraph/lib/std/operators/arithmetic.h>

#include <arrow/array.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <nanobind/stl/string.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::python_bridge
{
    std::unordered_map<const ValueTypeMetaData *, nb::object> &enum_class_registry()
    {
        static auto *registry = new std::unordered_map<const ValueTypeMetaData *, nb::object>{};
        return *registry;
    }

    std::unordered_map<const ValueTypeMetaData *,
                       std::unordered_map<long long, nb::object>> &
    enum_to_python_registry()
    {
        static auto *registry = new std::unordered_map<
            const ValueTypeMetaData *, std::unordered_map<long long, nb::object>>{};
        return *registry;
    }

    std::unordered_map<const ValueTypeMetaData *,
                       std::unordered_map<std::string, long long>> &
    enum_from_python_registry()
    {
        static auto *registry = new std::unordered_map<
            const ValueTypeMetaData *, std::unordered_map<std::string, long long>>{};
        return *registry;
    }

    PyObj::PyObj(nb::object value) noexcept
        : object(value.release().ptr())
    {
    }

    PyObj::PyObj(const PyObj &other) noexcept
        : object(other.object)
    {
        if (object != nullptr)
        {
            nb::gil_scoped_acquire gil;
            Py_INCREF(object);
        }
    }

    PyObj::PyObj(PyObj &&other) noexcept
        : object(other.object)
    {
        other.object = nullptr;
    }

    PyObj &PyObj::operator=(const PyObj &other) noexcept
    {
        if (this == &other) { return *this; }
        PyObj copy{other};
        std::swap(object, copy.object);
        return *this;
    }

    PyObj &PyObj::operator=(PyObj &&other) noexcept
    {
        std::swap(object, other.object);
        return *this;
    }

    PyObj::~PyObj()
    {
        if (object != nullptr)
        {
            nb::gil_scoped_acquire gil;
            Py_DECREF(object);
        }
    }

    nb::object PyObj::get() const { return nb::borrow(nb::handle(object)); }

    bool operator==(const PyObj &lhs, const PyObj &rhs) noexcept
    {
        if (lhs.object == rhs.object) { return true; }
        if (lhs.object == nullptr || rhs.object == nullptr) { return false; }
        nb::gil_scoped_acquire gil;
        const int result = PyObject_RichCompareBool(lhs.object, rhs.object, Py_EQ);
        return result == 1;
    }


    namespace
    {
        [[nodiscard]] const ValueCallableOps &python_value_callable_ops()
        {
            static const ValueCallableOps ops{
                .invoke = [](const void *context, std::span<const ValueCallArg> args,
                             const ValueTypeMetaData *output_schema) {
                    nb::gil_scoped_acquire gil;
                    const auto &callable = *static_cast<const PyObj *>(context);
                    std::size_t positional_count = 0;
                    for (const ValueCallArg &arg : args)
                    {
                        if (arg.name.empty()) { ++positional_count; }
                    }
                    nb::tuple positional = nb::steal<nb::tuple>(
                        PyTuple_New(static_cast<Py_ssize_t>(positional_count)));
                    if (!positional.is_valid()) { throw nb::python_error(); }
                    nb::dict keywords;
                    std::size_t index = 0;
                    for (const ValueCallArg &arg : args)
                    {
                        nb::object value = value_to_py(arg.view());
                        if (arg.name.empty())
                        {
                            if (PyTuple_SetItem(positional.ptr(), static_cast<Py_ssize_t>(index++),
                                                value.release().ptr()) != 0)
                            {
                                throw nb::python_error();
                            }
                        }
                        else { keywords[nb::str(arg.name.data(), arg.name.size())] = std::move(value); }
                    }
                    nb::object result = nb::steal<nb::object>(
                        PyObject_Call(callable.get().ptr(), positional.ptr(), keywords.ptr()));
                    if (!result.is_valid()) { throw nb::python_error(); }
                    if (result.is_none() || output_schema == nullptr) { return Value{}; }
                    return py_to_value_as(result, output_schema);
                },
            };
            return ops;
        }

        [[nodiscard]] ValueCallable python_value_callable(nb::handle source)
        {
            if (PyCallable_Check(source.ptr()) == 0)
            {
                throw nb::type_error("expected a callable value");
            }
            auto record = std::make_shared<PyObj>(nb::borrow<nb::object>(source));
            return ValueCallable{
                .ops      = &python_value_callable_ops(),
                .owner    = record,
                .context  = record.get(),
                .identity = source.ptr(),
                .variadic = true,
            };
        }

        struct PythonDateTimeTypes
        {
            nb::handle datetime;
            nb::handle date;
            nb::handle time;
            nb::handle timedelta;
        };

        [[nodiscard]] const PythonDateTimeTypes &python_datetime_types()
        {
            // The bridge can be built against Python's limited API, where the
            // datetime C API is unavailable. Keep borrowed handles backed by
            // deliberately leaked type references, as nanobind does for its
            // limited-API chrono caster.
            static auto *types = [] {
                nb::module_ module = nb::module_::import_("datetime");
                const auto leak_type = [&](const char *name) {
                    nb::object type = module.attr(name);
                    return type.release();
                };
                auto *result = new PythonDateTimeTypes{
                    .datetime  = leak_type("datetime"),
                    .date      = leak_type("date"),
                    .time      = leak_type("time"),
                    .timedelta = leak_type("timedelta"),
                };
                return result;
            }();
            return *types;
        }

        [[nodiscard]] Value box_any(Value value)
        {
            Value boxed{ValuePlanFactory::instance().type_for(
                TypeRegistry::instance().any())};
            boxed.as_any().begin_mutation().set(std::move(value));
            return boxed;
        }

        [[nodiscard]] Value box_python_object(nb::handle object)
        {
            return box_any(Value{PyObj{nb::borrow<nb::object>(object)}});
        }

        [[nodiscard]] std::size_t python_mro_distance(nb::handle source_class,
                                                      nb::handle registered_class)
        {
            nb::tuple mro = nb::cast<nb::tuple>(nb::getattr(source_class, "__mro__"));
            for (std::size_t index = 0; index < mro.size(); ++index)
            {
                if (mro[index].is(registered_class)) { return index; }
            }
            return std::numeric_limits<std::size_t>::max();
        }

        [[nodiscard]] const ValueTypeMetaData *select_python_bundle_schema(
            nb::handle object, std::span<const ValueTypeMetaData *const> permitted)
        {
            const nb::object source_class = nb::getattr(object, "__class__");
            std::vector<const ValueTypeMetaData *> candidates;
            std::size_t best_distance = std::numeric_limits<std::size_t>::max();
            for (const auto *schema : permitted)
            {
                const auto found = bundle_class_info_registry().find(schema);
                if (found == bundle_class_info_registry().end() ||
                    !found->second.type.is_valid() ||
                    !nb::isinstance(object, found->second.type))
                {
                    continue;
                }
                const auto distance = python_mro_distance(source_class, found->second.type);
                if (distance < best_distance)
                {
                    best_distance = distance;
                    candidates.clear();
                }
                if (distance == best_distance) { candidates.push_back(schema); }
            }
            if (candidates.empty()) { return nullptr; }
            if (candidates.size() == 1) { return candidates.front(); }

            // A non-frozen generic instance normally retains its alias.
            if (nb::hasattr(object, "__orig_class__"))
            {
                nb::object alias = nb::getattr(object, "__orig_class__");
                const ValueTypeMetaData *matched = nullptr;
                for (const auto *candidate : candidates)
                {
                    const auto &info = bundle_class_info_registry().at(candidate);
                    if (!info.specialization.is_valid()) { continue; }
                    const int equal = PyObject_RichCompareBool(
                        alias.ptr(), info.specialization.ptr(), Py_EQ);
                    if (equal < 0) { nb::raise_python_error(); }
                    if (equal == 0) { continue; }
                    if (matched != nullptr)
                    {
                        throw std::invalid_argument(
                            "Python structured scalar specialization is ambiguous");
                    }
                    matched = candidate;
                }
                if (matched != nullptr) { return matched; }
            }

            // Frozen dataclass generics cannot retain ``__orig_class__``.
            // Infer only at this schema-free boundary by comparing declared
            // fields with independently inferred value schemas.
            const ValueTypeMetaData *best = nullptr;
            std::size_t best_score = 0;
            bool ambiguous = false;
            for (const auto *candidate : candidates)
            {
                std::size_t score = 0;
                for (std::size_t index = 0; index < candidate->field_count; ++index)
                {
                    const ValueFieldMetaData &field = candidate->fields[index];
                    if (field.name == nullptr || !nb::hasattr(object, field.name)) { continue; }
                    nb::object field_value = nb::getattr(object, field.name);
                    if (field_value.is_none() || field_value.ptr() == object.ptr()) { continue; }
                    const Value inferred = py_to_value(field_value);
                    if (inferred.schema() == field.type)
                    {
                        score += 2;
                    }
                    else if (inferred.schema() != nullptr && field.type != nullptr &&
                             inferred.schema()->value_kind() == ValueTypeKind::Bundle &&
                             field.type->value_kind() == ValueTypeKind::Bundle &&
                             TypeRegistry::instance().bundle_is_a(inferred.schema(), field.type))
                    {
                        ++score;
                    }
                }
                if (best == nullptr || score > best_score)
                {
                    best = candidate;
                    best_score = score;
                    ambiguous = false;
                }
                else if (score == best_score)
                {
                    ambiguous = true;
                }
            }
            if (ambiguous)
            {
                throw std::invalid_argument(
                    "Python structured scalar schema inference is ambiguous");
            }
            return best;
        }

        [[nodiscard]] std::optional<Value> try_python_bundle_value(nb::handle object)
        {
            std::vector<const ValueTypeMetaData *> registered;
            registered.reserve(bundle_class_info_registry().size());
            for (const auto &[schema, info] : bundle_class_info_registry())
            {
                if (info.type.is_valid())
                {
                    registered.push_back(static_cast<const ValueTypeMetaData *>(schema));
                }
            }
            const auto *selected = select_python_bundle_schema(object, registered);
            return selected != nullptr
                       ? std::optional<Value>{py_to_value_as(object, selected)}
                       : std::nullopt;
        }

        [[nodiscard]] Value py_container_to_value(nb::handle object)
        {
            auto &registry = TypeRegistry::instance();
            const auto build = [&](const ValueTypeMetaData *schema, auto &&fill) {
                const auto type = ValuePlanFactory::instance().type_for(schema);
                Value      value{type};
                fill(value.begin_mutation());
                return value;
            };
            if (nb::isinstance<nb::frozenset>(object) || nb::isinstance<nb::set>(object))
            {
                std::vector<Value> elements;
                for (nb::handle item : object) { elements.push_back(py_to_value(item)); }
                if (elements.empty()) { return box_python_object(object); }
                if (std::ranges::any_of(elements, [&](const Value &element) {
                        return element.schema() != elements.front().schema();
                    }))
                {
                    return box_python_object(object);
                }
                return build(registry.mutable_set(elements.front().schema()), [&](ValueView view) {
                    MutableSetView set{std::move(view)};
                    for (const auto &element : elements) { set.add(element.view()); }
                });
            }
            if (nb::isinstance<nb::dict>(object))
            {
                std::vector<std::pair<Value, Value>> entries;
                for (auto [key, item] : nb::cast<nb::dict>(object))
                {
                    entries.emplace_back(py_to_value(key), py_to_value(item));
                }
                if (entries.empty()) { return box_python_object(object); }
                if (std::ranges::any_of(entries, [&](const auto &entry) {
                        return entry.first.schema() != entries.front().first.schema() ||
                               entry.second.schema() != entries.front().second.schema();
                    }))
                {
                    return box_python_object(object);
                }
                return build(registry.mutable_map(entries.front().first.schema(), entries.front().second.schema()),
                             [&](ValueView view) {
                                 MutableMapView map{std::move(view)};
                                 for (const auto &[key, item] : entries) { map.set_item(key.view(), item.view()); }
                             });
            }
            if (nb::isinstance<nb::tuple>(object))
            {
                std::vector<Value> elements;
                std::vector<const ValueTypeMetaData *> schemas;
                for (nb::handle item : object)
                {
                    elements.push_back(py_to_value(item));
                    schemas.push_back(elements.back().schema());
                }
                if (elements.empty()) { return box_python_object(object); }
                const bool homogeneous = std::ranges::all_of(
                    elements, [&](const Value &element) {
                        return element.schema() == elements.front().schema();
                    });
                if (homogeneous)
                {
                    Value value{ValuePlanFactory::instance().type_for(
                        registry.list(elements.front().schema(), 0, true))};
                    value.view().assign_from_python(object);
                    return value;
                }
                Value value{ValuePlanFactory::instance().type_for(
                    registry.tuple(schemas))};
                value.view().assign_from_python(object);
                return value;
            }
            if (nb::isinstance<nb::list>(object))
            {
                std::vector<Value> elements;
                for (nb::handle item : object) { elements.push_back(py_to_value(item)); }
                if (elements.empty()) { return box_python_object(object); }
                if (std::ranges::any_of(elements, [&](const Value &element) {
                        return element.schema() != elements.front().schema();
                    }))
                {
                    return box_python_object(object);
                }
                return build(registry.mutable_list(elements.front().schema()), [&](ValueView view) {
                    MutableListView list{std::move(view)};
                    for (const auto &element : elements) { list.push_back(element.view()); }
                });
            }
            throw nb::type_error("unsupported Python value for hgraph");
        }

    }  // namespace

    Value py_to_value(nb::handle object)
    {
        if (nb::isinstance<nb::bool_>(object)) { return Value{Bool{nb::cast<bool>(object)}}; }
        if (nb::isinstance<nb::int_>(object)) { return Value{Int{nb::cast<Int>(object)}}; }
        if (nb::isinstance<nb::float_>(object)) { return Value{Float{nb::cast<Float>(object)}}; }
        if (nb::isinstance<nb::str>(object)) { return Value{Str{nb::cast<std::string>(object)}}; }
        if (nb::isinstance<nb::bytes>(object))
        {
            auto raw = nb::cast<nb::bytes>(object);
            return Value{Bytes{std::string{raw.c_str(), raw.size()}}};
        }
        // A Python Enum class is registered when its TS annotation is
        // materialised. Preserve that nominal schema for plain enum values
        // used in operator calls so the C++ auto-const path receives the
        // same type as a connected TS[Enum] input.
        for (const auto &[meta, python_type] : enum_class_registry())
        {
            if (python_type.is_valid() && nb::isinstance(object, python_type))
            {
                return py_to_value_as(object, meta);
            }
        }
        const auto &date_time_types = python_datetime_types();
        if (nb::isinstance(object, date_time_types.datetime))
        {
            DateTime when;
            if (!nb::try_cast<DateTime>(object, when))
            {
                throw nb::type_error("invalid Python datetime value");
            }
            return Value{when};
        }
        if (nb::isinstance(object, date_time_types.date))
        {
            return Value{Date{std::chrono::year{nb::cast<int>(object.attr("year"))},
                              std::chrono::month{nb::cast<unsigned>(object.attr("month"))},
                              std::chrono::day{nb::cast<unsigned>(object.attr("day"))}}};
        }
        if (nb::isinstance(object, date_time_types.time))
        {
            nb::object offset = object.attr("utcoffset")();
            if (!offset.is_none())
            {
                throw nb::type_error(
                    "timezone-aware time values require a zoned time scalar");
            }
            const std::int64_t micros = nb::cast<std::int64_t>(object.attr("hour")) * 3'600'000'000LL +
                                        nb::cast<std::int64_t>(object.attr("minute")) * 60'000'000LL +
                                        nb::cast<std::int64_t>(object.attr("second")) * 1'000'000LL +
                                        nb::cast<std::int64_t>(object.attr("microsecond"));
            return Value{Time{micros}};
        }
        if (nb::isinstance(object, date_time_types.timedelta))
        {
            TimeDelta delta;
            if (!nb::try_cast<TimeDelta>(object, delta))
            {
                throw nb::type_error("invalid Python timedelta value");
            }
            return Value{delta};
        }
        if (const auto *native =
                python_bridge::native_scalar_type_for_value(object))
        {
            return py_to_value_as(object, native);
        }
        // Python classes are callable too, but are scalar data for dispatch,
        // context, and TS[object]. Only unregistered callable instances become
        // runtime callables during schema-free inference.
        if (PyType_Check(object.ptr()) == 0 && PyCallable_Check(object.ptr()) != 0)
        {
            return Value{python_value_callable(object)};
        }
        if (nb::hasattr(object, "__arrow_c_stream__"))
        {
            nb::object arrow_stream = object.attr("__arrow_c_stream__");
            if (PyCallable_Check(arrow_stream.ptr()) != 0) { return py_arrow_to_frame(object); }
        }
        if (nb::isinstance<PyOpaqueRef>(object)) { return Value{nb::cast<PyOpaqueRef &>(object).value.view()}; }
        if (cmp_result_enum_slot().is_valid() && nb::isinstance(object, cmp_result_enum_slot()))
        {
            return Value{static_cast<stdlib::CmpResult>(nb::cast<std::int64_t>(object.attr("value")))};
        }
        if (divide_by_zero_enum_slot().is_valid() && nb::isinstance(object, divide_by_zero_enum_slot()))
        {
            return Value{static_cast<stdlib::DivideByZero>(nb::cast<std::int64_t>(object.attr("value")))};
        }
        if (auto bundle = try_python_bundle_value(object)) { return std::move(*bundle); }
        if (nb::isinstance<nb::frozenset>(object) || nb::isinstance<nb::set>(object) ||
            nb::isinstance<nb::dict>(object) || nb::isinstance<nb::tuple>(object) ||
            nb::isinstance<nb::list>(object))
        {
            return py_container_to_value(object);
        }
        return box_python_object(object);
    }

    nb::object PyArrowStream::capsule() const
    {
        auto reader = std::make_shared<arrow::TableBatchReader>(*frame.table);
        auto *stream = new ArrowArrayStream{};
        const auto status = arrow::ExportRecordBatchReader(std::move(reader), stream);
        if (!status.ok())
        {
            delete stream;
            throw std::runtime_error("arrow export failed: " + status.ToString());
        }
        return nb::steal(PyCapsule_New(stream, "arrow_array_stream", [](PyObject *object) {
            if (!PyCapsule_IsValid(object, "arrow_array_stream")) { return; }
            auto *raw = static_cast<ArrowArrayStream *>(PyCapsule_GetPointer(object, "arrow_array_stream"));
            if (raw != nullptr)
            {
                if (raw->release != nullptr) { raw->release(raw); }
                delete raw;
            }
        }));
    }

    bool &polars_frames_enabled()
    {
        static bool enabled = false;
        return enabled;
    }

    namespace
    {
        /* The user-boundary frame/series presentation lives PYTHON-side
           (hgraph._frame): naive-UTC timestamp columns and the optional
           polars form (issue #80). The bridge stays usable without the
           hgraph package (raw pyarrow passthrough). */
        [[nodiscard]] nb::object present_through(const char *hook, nb::object value)
        {
            nb::object presenter;
            try
            {
                presenter = nb::module_::import_("hgraph._frame").attr(hook);
            }
            catch (nb::python_error &)
            {
                return value;
            }
            return presenter(value);
        }
    }  // namespace

    nb::object frame_to_py(const Frame &frame)
    {
        if (!frame.has_value()) { return nb::none(); }
        nb::object stream = nb::cast(PyArrowStream{frame});
        nb::object table  = nb::module_::import_("pyarrow").attr("table")(stream);
        return present_through("_present_frame", std::move(table));
    }

    Value py_arrow_to_frame(nb::handle object)
    {
        nb::object capsule = object.attr("__arrow_c_stream__")();
        auto *stream = static_cast<ArrowArrayStream *>(PyCapsule_GetPointer(capsule.ptr(), "arrow_array_stream"));
        if (stream == nullptr) { throw nb::type_error("expected an arrow_array_stream capsule"); }
        auto reader = arrow::ImportRecordBatchReader(stream);
        if (!reader.ok()) { throw std::runtime_error("arrow import failed: " + reader.status().ToString()); }
        auto table = arrow::Table::FromRecordBatchReader(reader->get());
        if (!table.ok()) { throw std::runtime_error("arrow read failed: " + table.status().ToString()); }
        return Value{Frame{.table = std::move(*table)}};
    }

    nb::object PySeriesArray::arrow_c_array() const
    {
        auto *c_array  = new ArrowArray{};
        auto *c_schema = new ArrowSchema{};
        const auto status = arrow::ExportArray(*series.array, c_array, c_schema);
        if (!status.ok())
        {
            delete c_array;
            delete c_schema;
            throw std::runtime_error("arrow array export failed: " + status.ToString());
        }
        const auto array_release = [](PyObject *object) {
            if (!PyCapsule_IsValid(object, "arrow_array")) { return; }
            auto *raw = static_cast<ArrowArray *>(PyCapsule_GetPointer(object, "arrow_array"));
            if (raw != nullptr) { if (raw->release != nullptr) { raw->release(raw); } delete raw; }
        };
        const auto schema_release = [](PyObject *object) {
            if (!PyCapsule_IsValid(object, "arrow_schema")) { return; }
            auto *raw = static_cast<ArrowSchema *>(PyCapsule_GetPointer(object, "arrow_schema"));
            if (raw != nullptr) { if (raw->release != nullptr) { raw->release(raw); } delete raw; }
        };
        nb::object schema_capsule = nb::steal(PyCapsule_New(c_schema, "arrow_schema", schema_release));
        nb::object array_capsule  = nb::steal(PyCapsule_New(c_array, "arrow_array", array_release));
        return nb::make_tuple(schema_capsule, array_capsule);
    }

    nb::object series_to_py(const Series &series)
    {
        if (!series.has_value()) { return nb::none(); }
        nb::object holder = nb::cast(PySeriesArray{series});
        nb::object array  = nb::module_::import_("pyarrow").attr("array")(holder);
        return present_through("_present_series", std::move(array));
    }

    Value py_arrow_to_series(nb::handle object)
    {
        nb::object pair = object.attr("__arrow_c_array__")();
        nb::tuple  capsules = nb::cast<nb::tuple>(pair);
        auto *c_schema = static_cast<ArrowSchema *>(
            PyCapsule_GetPointer(capsules[0].ptr(), "arrow_schema"));
        auto *c_array = static_cast<ArrowArray *>(
            PyCapsule_GetPointer(capsules[1].ptr(), "arrow_array"));
        if (c_schema == nullptr || c_array == nullptr)
        {
            throw nb::type_error("expected an (arrow_schema, arrow_array) capsule pair");
        }
        auto array = arrow::ImportArray(c_array, c_schema);
        if (!array.ok()) { throw std::runtime_error("arrow array import failed: " + array.status().ToString()); }
        return Value{Series{.array = std::move(*array)}};
    }

    nb::object value_to_py(const ValueView &view)
    {
        if (!view.valid()) { return nb::none(); }
        // Python conversion dispatches through the type-erased ops. Types
        // requiring module-owned wrappers install python_conversion_traits
        // hooks during module initialization.
        return view.binding().ops_ref().to_python(view.data());
    }

    ValueTypeRef delta_binding(const ValueTypeMetaData *meta)
    {
        if (const auto *snapshot = active_type_realization())
        {
            if (const auto realized = snapshot->type_for(meta)) { return realized; }
        }
        const auto type = ValuePlanFactory::instance().type_for(meta);
        if (!type) { throw nb::type_error("schema has no canonical type"); }
        return type;
    }

    [[nodiscard]] const ValueTypeMetaData *python_bundle_source_schema(
        nb::handle object, const ValueTypeMetaData *declared)
    {
        if (declared == nullptr || !declared->is_named_bundle() ||
            declared->bundle_hierarchy == nullptr || declared->bundle_hierarchy->children.empty())
        {
            return declared;
        }

        const auto alternatives = TypeRegistry::instance().bundle_descendants(declared);
        nb::object source = nb::borrow<nb::object>(object);
        if (nb::isinstance<nb::dict>(source))
        {
            nb::dict map = nb::cast<nb::dict>(source);
            nb::str key{std::string{declared->bundle_discriminator()}.c_str()};
            if (!map.contains(key))
            {
                throw std::invalid_argument(
                    "polymorphic Bundle dictionaries require the configured type discriminator");
            }
            const auto requested = nb::cast<std::string>(map[key]);
            const ValueTypeMetaData *match = nullptr;
            for (const auto *alternative : alternatives)
            {
                if (alternative->name() == requested || alternative->bundle_local_name() == requested)
                {
                    if (match != nullptr)
                    {
                        throw std::invalid_argument("polymorphic Bundle discriminator is ambiguous");
                    }
                    match = alternative;
                }
            }
            if (match == nullptr)
            {
                throw std::invalid_argument("polymorphic Bundle discriminator names no valid alternative");
            }
            return match;
        }

        if (const auto *selected = select_python_bundle_schema(source, alternatives))
        {
            return selected;
        }
        throw std::invalid_argument("value is not an instance of a closed Bundle alternative");
    }

    Value py_to_value_as(nb::handle object, const ValueTypeMetaData *meta)
    {
        if (meta != nullptr && meta->value_kind() == ValueTypeKind::Any)
        {
            Value boxed{ValuePlanFactory::instance().type_for(meta)};
            if (object.is_none()) { return boxed; }
            Value inferred = py_to_value(object);
            if (inferred.schema() == meta) { return inferred; }
            boxed.as_any().begin_mutation().set(std::move(inferred));
            return boxed;
        }

        // A typed Frame owns two schemas in one Arrow representation:
        // element_type describes the columns and key_type describes the
        // field-wise hgraph entries in the Arrow schema metadata. Validate
        // those entries while the target schema is available.
        const bool frame_target = meta != nullptr && TypeRegistry::instance().is_frame(meta);
        // Conversion goes through the binding's from_python operation; no
        // schema-kind switch is needed at this boundary.
        std::shared_ptr<const TypeRealizationSnapshot> conversion_snapshot;
        const auto *snapshot = active_type_realization();
        if (snapshot == nullptr && meta != nullptr &&
            meta->value_kind() != ValueTypeKind::Atomic &&
            meta->value_kind() != ValueTypeKind::Any)
        {
            conversion_snapshot = TypeRealizationSnapshot::capture(TypeRegistry::instance());
            snapshot = conversion_snapshot.get();
        }
        const auto canonical = ValuePlanFactory::instance().type_for(meta);
        const auto realized = snapshot != nullptr ? snapshot->type_for(meta) : ValueTypeRef{};
        const bool uses_realized_storage = realized && realized != canonical;
        const auto *storage_schema = uses_realized_storage
                                         ? meta
                                         : python_bundle_source_schema(object, meta);
        const auto type = uses_realized_storage
                              ? realized
                              : ValuePlanFactory::instance().type_for(storage_schema);
        if (!type) { throw nb::type_error("schema has no canonical type"); }
        TypeRealizationScope realization_scope{snapshot};
        Value result{type};
        result.view().assign_from_python(object);
        if (frame_target)
        {
            const auto frame = result.view().checked_as<Frame>();
            const bool carries_metadata = frame.has_metadata();
            if (meta->key_type == nullptr && carries_metadata)
            {
                throw nb::type_error(
                    "row-only Frame input cannot carry hgraph Arrow metadata; "
                    "call without_frame_metadata() explicitly");
            }
            if (meta->key_type != nullptr && !carries_metadata)
            {
                throw nb::type_error(
                    "typed Frame input requires metadata encoded in the Arrow schema");
            }
            if (carries_metadata)
            {
                try
                {
                    (void)frame_metadata(frame, meta->key_type);
                }
                catch (const std::exception &error)
                {
                    throw nb::type_error(error.what());
                }
            }
        }
        return result;
    }

    void install_value_conversion_hooks()
    {
        python_conversion_traits<ValueCallable>::to_python_hook() = [](const ValueCallable &value) {
            if (value.ops != &python_value_callable_ops() || value.context == nullptr)
            {
                throw nb::type_error("native value callable has no Python callable object");
            }
            return static_cast<const PyObj *>(value.context)->get();
        };
        python_conversion_traits<ValueCallable>::from_python_hook() = &python_value_callable;
        python_conversion_traits<Frame>::to_python_hook()   = &frame_to_py;
        python_conversion_traits<Frame>::from_python_hook() = [](nb::handle o) {
            return py_arrow_to_frame(o).view().checked_as<Frame>();
        };
        python_conversion_traits<Series>::to_python_hook()   = &series_to_py;
        python_conversion_traits<Series>::from_python_hook() = [](nb::handle o) {
            return py_arrow_to_series(o).view().checked_as<Series>();
        };
        python_conversion_traits<TimeSeriesReference>::to_python_hook() = [](const TimeSeriesReference &value) {
            return nb::cast(PyOpaqueRef{Value{value}, MIN_DT});
        };
        python_conversion_traits<TimeSeriesReference>::from_python_hook() = [](nb::handle source) {
            if (!nb::isinstance<PyOpaqueRef>(source))
            {
                throw nb::type_error("expected a TimeSeriesReference value");
            }
            return nb::cast<PyOpaqueRef &>(source).value.view().checked_as<TimeSeriesReference>();
        };
    }

    Value py_tss_spec_to_delta(nb::handle add_from, nb::handle remove_from, const TSValueTypeMetaData *ts)
    {
        // INTERNAL protocol: the py-node frozenset-return shaping supplies
        // explicit added/removed iterables. The public py_to_delta TSS path
        // routes every user object through add_from (a dict iterates as its
        // KEYS — upstream duck parity), with Removed(...) members shaping
        // removals.
        const auto *elem = ts->value_schema->element_type;
        SetBuilder  added{delta_binding(elem)};
        SetBuilder  removed{delta_binding(elem)};
        if (add_from.is_valid())
        {
            for (nb::handle item : add_from)
            {
                // hgraph's set-delta literal: Removed(x) members of a
                // plain set are REMOVALS (TS-schema shaping - this is
                // exactly py_to_delta's job). The registered Removed
                // class decides; the name check only covers the
                // pre-registration import window.
                if (removed_class_slot().is_valid()
                        ? nb::isinstance(item, removed_class_slot())
                        : (nb::hasattr(item, "item") &&
                           nb::cast<std::string>(item.type().attr("__name__")) == "Removed"))
                {
                    (void)removed.insert_copy(py_to_value_as(item.attr("item"), elem).view().data());
                    continue;
                }
                (void)added.insert_copy(py_to_value_as(item, elem).view().data());
            }
        }
        if (remove_from.is_valid())
        {
            for (nb::handle item : remove_from) { (void)removed.insert_copy(py_to_value_as(item, elem).view().data()); }
        }
        BundleBuilder bundle{delta_binding(ts->delta_value_schema)};
        bundle.set("added", added.build());
        bundle.set("removed", removed.build());
        return bundle.build();
    }

    Value py_to_delta(nb::handle object, const TSValueTypeMetaData *ts)
    {
        std::shared_ptr<const TypeRealizationSnapshot> conversion_snapshot;
        const auto *snapshot = active_type_realization();
        if (snapshot == nullptr)
        {
            conversion_snapshot = TypeRealizationSnapshot::capture(TypeRegistry::instance());
            snapshot = conversion_snapshot.get();
        }
        TypeRealizationScope realization_scope{snapshot};

        switch (ts->kind)
        {
            case TSTypeKind::TS:
                return py_to_value_as(object, ts->value_schema);
            case TSTypeKind::TSS: {
                // hgraph parity (ruling 2026-07-17): a dict is NEVER a TSS
                // value or delta spec — the former {removed: [...]} removal
                // convention is gone. Upstream's typed surface rejects dicts
                // too (its auto-const tests pin it); its untyped apply path
                // incidentally iterates dict keys, which this typed runtime
                // rejects LOUDLY instead. Removals are expressed with
                // set_delta(...) or Removed(...) markers. The {added,
                // removed} spec shape survives only as the INTERNAL protocol
                // (py_tss_spec_to_delta), used by the py-node
                // frozenset-return shaping.
                // The EMPTY dict is accepted as an empty tick: upstream's own
                // tests write `{}` because Python has no empty-set literal.
                if (nb::isinstance<nb::dict>(object) && nb::len(object) != 0)
                {
                    throw nb::type_error(
                        "a dict is not a TSS value/delta: use a set, set_delta(...), "
                        "or Removed(...) markers ({} is accepted as the empty set)");
                }
                return py_tss_spec_to_delta(object, nb::handle{}, ts);
            }
            case TSTypeKind::TSD: {
                const auto *key_meta = ts->key_type();
                const auto *child    = ts->element_ts();
                SetBuilder  removed{delta_binding(key_meta)};
                SetBuilder  removed_strict{delta_binding(key_meta)};
                MapBuilder  modified{delta_binding(key_meta), delta_binding(child->delta_value_schema)};
                for (auto [key, item] : nb::cast<nb::dict>(object))
                {
                    Value key_value = py_to_value_as(key, key_meta);
                    // hgraph's removal contract: REMOVE raises when the key is
                    // absent at application; REMOVE_IF_EXISTS and the harness
                    // None convention are lenient.
                    const bool strict_remove =
                        removed_sentinel_slot().is_valid() && item.is(removed_sentinel_slot());
                    const bool lenient_remove =
                        item.is_none() ||
                        (remove_if_exists_sentinel_slot().is_valid() &&
                         item.is(remove_if_exists_sentinel_slot()));
                    if (strict_remove) { (void)removed_strict.insert_copy(key_value.view().data()); }
                    else if (lenient_remove) { (void)removed.insert_copy(key_value.view().data()); }
                    else
                    {
                        Value child_delta = py_to_delta(item, child);
                        modified.set_item_copy(key_value.view().data(), child_delta.view().data());
                    }
                }
                BundleBuilder bundle{delta_binding(ts->delta_value_schema)};
                bundle.set("removed", removed.build());
                bundle.set("modified", modified.build());
                bundle.set("removed_strict", removed_strict.build());
                return bundle.build();
            }
            case TSTypeKind::TSL: {
                const auto *map_meta = ts->delta_value_schema;
                MapBuilder  builder{delta_binding(map_meta->key_type), delta_binding(map_meta->element_type)};
                if (nb::isinstance<nb::dict>(object))
                {
                    for (auto [key, item] : nb::cast<nb::dict>(object))
                    {
                        const auto index = nb::cast<std::int64_t>(key);
                        Value child_delta = py_to_delta(item, ts->element_ts());
                        builder.set_item_copy(std::addressof(index), child_delta.view().data());
                    }
                    return builder.build();
                }
                std::int64_t index = 0;
                for (nb::handle item : object)
                {
                    if (!item.is_none())
                    {
                        Value child_delta = py_to_delta(item, ts->element_ts());
                        builder.set_item_copy(std::addressof(index), child_delta.view().data());
                    }
                    ++index;
                }
                return builder.build();
            }
            case TSTypeKind::TSB: {
                BundleBuilder builder{delta_binding(ts->delta_value_schema)};
                const bool is_mapping = nb::isinstance<nb::dict>(object);
                nb::dict fields;
                if (is_mapping)
                {
                    fields = nb::cast<nb::dict>(object);
                }
                else
                {
                    const auto mapped = tsb_compound_value_registry().find(ts);
                    if (mapped == tsb_compound_value_registry().end())
                    {
                        throw nb::type_error(
                            "a TSB delta must be a dict unless the TSB represents a CompoundScalar");
                    }
                    const auto info = bundle_class_info_registry().find(mapped->second);
                    if (info == bundle_class_info_registry().end() ||
                        !info->second.type.is_valid() ||
                        !nb::isinstance(object, info->second.type))
                    {
                        throw nb::type_error(
                            "a TSB CompoundScalar snapshot must be an instance of its registered class");
                    }
                }
                for (std::size_t index = 0; index < ts->field_count(); ++index)
                {
                    const auto &field = ts->fields()[index];
                    nb::str key{field.name};
                    if (is_mapping && !fields.contains(key)) { continue; }
                    if (!is_mapping && !nb::hasattr(object, key)) { continue; }
                    nb::object item = is_mapping
                                          ? nb::borrow<nb::object>(fields[key])
                                          : nb::getattr(object, key);
                    if (item.is_none()) { continue; }
                    builder.set(index, py_to_delta(item, field.type).view());
                }
                return builder.build();
            }
            default:
                return py_to_value_as(object, ts->delta_value_schema);
        }
    }
}  // namespace hgraph::python_bridge

std::size_t std::hash<hgraph::python_bridge::PyObj>::operator()(
    const hgraph::python_bridge::PyObj &value) const noexcept
{
    if (value.object == nullptr) { return 0; }
    nanobind::gil_scoped_acquire gil;
    const Py_hash_t result = PyObject_Hash(value.object);
    if (result == -1)
    {
        PyErr_Clear();
        return std::hash<const void *>{}(value.object);
    }
    return static_cast<std::size_t>(result);
}
