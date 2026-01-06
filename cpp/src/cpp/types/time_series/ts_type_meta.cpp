//
// Created by Claude on 05/01/2025.
//

#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/python/chrono.h>
#include <nanobind/stl/string.h>
#include <sstream>

namespace hgraph {

    // ============================================================================
    // Helper: TypeMeta kind to string
    // ============================================================================

    static std::string type_meta_kind_str(const value::TypeMeta* meta) {
        if (!meta) return "?";
        switch (meta->kind) {
            case value::TypeKind::Scalar: return "Scalar";
            case value::TypeKind::Tuple: return "Tuple";
            case value::TypeKind::Bundle: return "Bundle";
            case value::TypeKind::List: return "List";
            case value::TypeKind::Set: return "Set";
            case value::TypeKind::Map: return "Map";
            case value::TypeKind::CyclicBuffer: return "CyclicBuffer";
            case value::TypeKind::Queue: return "Queue";
            case value::TypeKind::Ref: return "Ref";
            default: return "Unknown";
        }
    }

    // ============================================================================
    // TSValueMeta Implementation
    // ============================================================================

    std::string TSValueMeta::to_string() const {
        std::ostringstream oss;
        oss << "TS[" << type_meta_kind_str(_scalar_schema) << "]";
        return oss.str();
    }

    // ============================================================================
    // TSBTypeMeta Implementation
    // ============================================================================

    TSBTypeMeta::TSBTypeMeta(std::vector<TSBFieldInfo> fields,
                             const value::TypeMeta* bundle_schema,
                             std::string name)
        : _fields(std::move(fields)),
          _bundle_schema(bundle_schema),
          _name(std::move(name)) {}

    const TSBFieldInfo* TSBTypeMeta::field(const std::string& name) const {
        for (const auto& f : _fields) {
            if (f.name == name) {
                return &f;
            }
        }
        return nullptr;
    }

    std::string TSBTypeMeta::to_string() const {
        std::ostringstream oss;
        if (!_name.empty()) {
            oss << "TSB[" << _name << "]";
        } else {
            oss << "TSB[";
            bool first = true;
            for (const auto& f : _fields) {
                if (!first) oss << ", ";
                first = false;
                oss << f.name << ": " << (f.type ? f.type->to_string() : "?");
            }
            oss << "]";
        }
        return oss.str();
    }

    // ============================================================================
    // TSLTypeMeta Implementation
    // ============================================================================

    std::string TSLTypeMeta::to_string() const {
        std::ostringstream oss;
        oss << "TSL[" << (_element_type ? _element_type->to_string() : "?");
        if (_fixed_size > 0) {
            oss << ", " << _fixed_size;
        }
        oss << "]";
        return oss.str();
    }

    // ============================================================================
    // TSDTypeMeta Implementation
    // ============================================================================

    std::string TSDTypeMeta::to_string() const {
        std::ostringstream oss;
        oss << "TSD[" << type_meta_kind_str(_key_type)
            << ", " << (_value_type ? _value_type->to_string() : "?") << "]";
        return oss.str();
    }

    // ============================================================================
    // TSSTypeMeta Implementation
    // ============================================================================

    std::string TSSTypeMeta::to_string() const {
        std::ostringstream oss;
        oss << "TSS[" << type_meta_kind_str(_element_type) << "]";
        return oss.str();
    }

    // ============================================================================
    // TSWTypeMeta Implementation
    // ============================================================================

    std::string TSWTypeMeta::to_string() const {
        std::ostringstream oss;
        oss << "TSW[" << type_meta_kind_str(_value_type);
        if (_is_time_based) {
            oss << ", duration=" << _time_range.count() << "us";
            if (_min_time_range.count() > 0) {
                oss << ", min_duration=" << _min_time_range.count() << "us";
            }
        } else {
            oss << ", size=" << _size;
            if (_min_size > 0) {
                oss << ", min_size=" << _min_size;
            }
        }
        oss << "]";
        return oss.str();
    }

    // ============================================================================
    // REFTypeMeta Implementation
    // ============================================================================

    const value::TypeMeta* REFTypeMeta::value_schema() const {
        // REF's value schema is the referenced type's value schema
        return _referenced_type ? _referenced_type->value_schema() : nullptr;
    }

    std::string REFTypeMeta::to_string() const {
        std::ostringstream oss;
        oss << "REF[" << (_referenced_type ? _referenced_type->to_string() : "?") << "]";
        return oss.str();
    }

    // ============================================================================
    // Python Bindings
    // ============================================================================

    void register_ts_type_meta_with_nanobind(nb::module_& m) {
        // TSTypeKind enum
        nb::enum_<TSTypeKind>(m, "TSTypeKind")
            .value("TS", TSTypeKind::TS)
            .value("TSB", TSTypeKind::TSB)
            .value("TSL", TSTypeKind::TSL)
            .value("TSD", TSTypeKind::TSD)
            .value("TSS", TSTypeKind::TSS)
            .value("TSW", TSTypeKind::TSW)
            .value("REF", TSTypeKind::REF)
            .value("SIGNAL", TSTypeKind::SIGNAL);

        // TSBFieldInfo
        nb::class_<TSBFieldInfo>(m, "TSBFieldInfo")
            .def_ro("name", &TSBFieldInfo::name)
            .def_ro("index", &TSBFieldInfo::index)
            .def_ro("type", &TSBFieldInfo::type);

        // TSMeta base class
        nb::class_<TSMeta>(m, "TSMeta")
            .def_prop_ro("kind", &TSMeta::kind)
            .def_prop_ro("value_schema", &TSMeta::value_schema,
                         nb::rv_policy::reference)
            .def_prop_ro("is_scalar_ts", &TSMeta::is_scalar_ts)
            .def_prop_ro("is_bundle", &TSMeta::is_bundle)
            .def_prop_ro("is_collection", &TSMeta::is_collection)
            .def_prop_ro("is_reference", &TSMeta::is_reference)
            .def_prop_ro("is_signal", &TSMeta::is_signal)
            .def("__str__", &TSMeta::to_string)
            .def("__repr__", &TSMeta::to_string);

        // TSValueMeta
        nb::class_<TSValueMeta, TSMeta>(m, "TSValueMeta")
            .def_prop_ro("scalar_schema", &TSValueMeta::scalar_schema,
                         nb::rv_policy::reference);

        // TSBTypeMeta
        nb::class_<TSBTypeMeta, TSMeta>(m, "TSBTypeMeta")
            .def_prop_ro("name", &TSBTypeMeta::name)
            .def_prop_ro("field_count", &TSBTypeMeta::field_count)
            .def("field_by_index", [](const TSBTypeMeta& self, size_t index) {
                return &self.field(index);
            }, nb::rv_policy::reference)
            .def("field_by_name", [](const TSBTypeMeta& self, const std::string& name) {
                return self.field(name);
            }, nb::rv_policy::reference)
            .def("field_meta_by_index",
                 static_cast<const TSMeta* (TSBTypeMeta::*)(size_t) const>(
                     &TSBTypeMeta::field_meta),
                 nb::rv_policy::reference)
            .def("field_meta_by_name",
                 static_cast<const TSMeta* (TSBTypeMeta::*)(const std::string&) const>(
                     &TSBTypeMeta::field_meta),
                 nb::rv_policy::reference);

        // TSLTypeMeta
        nb::class_<TSLTypeMeta, TSMeta>(m, "TSLTypeMeta")
            .def_prop_ro("element_type", &TSLTypeMeta::element_type,
                         nb::rv_policy::reference)
            .def_prop_ro("fixed_size", &TSLTypeMeta::fixed_size)
            .def_prop_ro("is_fixed_size", &TSLTypeMeta::is_fixed_size);

        // TSDTypeMeta
        nb::class_<TSDTypeMeta, TSMeta>(m, "TSDTypeMeta")
            .def_prop_ro("key_type", &TSDTypeMeta::key_type,
                         nb::rv_policy::reference)
            .def_prop_ro("value_ts_type", &TSDTypeMeta::value_ts_type,
                         nb::rv_policy::reference);

        // TSSTypeMeta
        nb::class_<TSSTypeMeta, TSMeta>(m, "TSSTypeMeta")
            .def_prop_ro("element_type", &TSSTypeMeta::element_type,
                         nb::rv_policy::reference);

        // TSWTypeMeta
        nb::class_<TSWTypeMeta, TSMeta>(m, "TSWTypeMeta")
            .def_prop_ro("element_type", &TSWTypeMeta::element_type,
                         nb::rv_policy::reference)
            .def_prop_ro("is_time_based", &TSWTypeMeta::is_time_based)
            .def_prop_ro("size", &TSWTypeMeta::size)
            .def_prop_ro("min_size", &TSWTypeMeta::min_size)
            .def_prop_ro("time_range", &TSWTypeMeta::time_range)
            .def_prop_ro("min_time_range", &TSWTypeMeta::min_time_range);

        // REFTypeMeta
        nb::class_<REFTypeMeta, TSMeta>(m, "REFTypeMeta")
            .def_prop_ro("referenced_type", &REFTypeMeta::referenced_type,
                         nb::rv_policy::reference);

        // SignalTypeMeta
        nb::class_<SignalTypeMeta, TSMeta>(m, "SignalTypeMeta");
    }

} // namespace hgraph
