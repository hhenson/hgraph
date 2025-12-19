//
// Created by Howard Henson on 13/12/2025.
//
// Python conversion operations for the hgraph::value type system.
// Uses nanobind for type-safe conversions between C++ values and Python objects.
//
// Design: All type dispatch is resolved at type construction time via templates.
// No runtime type checking is needed during conversion.
//

#ifndef HGRAPH_VALUE_PYTHON_CONVERSION_H
#define HGRAPH_VALUE_PYTHON_CONVERSION_H

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
// Note: chrono casters are provided by hgraph/python/chrono.h via hgraph_base.h
#include <nanobind/ndarray.h>

#include <hgraph/util/date_time.h>
#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <hgraph/types/value/bundle_type.h>
#include <hgraph/types/value/list_type.h>
#include <hgraph/types/value/set_type.h>
#include <hgraph/types/value/dict_type.h>
#include <hgraph/types/value/window_type.h>
#include <hgraph/types/value/ref_type.h>
#include <hgraph/types/value/dynamic_list_type.h>

namespace nb = nanobind;

namespace hgraph::value {

    // ========================================================================
    // Scalar Type Python Conversions - Template-based
    // ========================================================================

    /**
     * ScalarPythonOps - Python conversion for scalar type T
     *
     * These functions are instantiated at compile time for each scalar type.
     * No runtime type checking needed.
     */
    template<typename T>
    struct ScalarPythonOps {
        static void* to_python(const void* v, const TypeMeta*) {
            const T& val = *static_cast<const T*>(v);
            nb::object obj = nb::cast(val);
            return obj.release().ptr();
        }

        static void from_python(void* dest, void* py_obj, const TypeMeta*) {
            nb::handle h(static_cast<PyObject*>(py_obj));
            *static_cast<T*>(dest) = nb::cast<T>(h);
        }
    };

    /**
     * ScalarTypeOpsWithPython - Complete TypeOps with Python support for type T
     */
    template<typename T>
    struct ScalarTypeOpsWithPython {
        static void construct(void* dest, const TypeMeta*) {
            new (dest) T{};
        }

        static void destruct(void* dest, const TypeMeta*) {
            static_cast<T*>(dest)->~T();
        }

        static void copy_construct(void* dest, const void* src, const TypeMeta*) {
            new (dest) T(*static_cast<const T*>(src));
        }

        static void move_construct(void* dest, void* src, const TypeMeta*) {
            new (dest) T(std::move(*static_cast<T*>(src)));
        }

        static void copy_assign(void* dest, const void* src, const TypeMeta*) {
            *static_cast<T*>(dest) = *static_cast<const T*>(src);
        }

        static void move_assign(void* dest, void* src, const TypeMeta*) {
            *static_cast<T*>(dest) = std::move(*static_cast<T*>(src));
        }

        static bool equals(const void* a, const void* b, const TypeMeta*) {
            if constexpr (requires(const T& x, const T& y) { x == y; }) {
                return *static_cast<const T*>(a) == *static_cast<const T*>(b);
            } else {
                return false;
            }
        }

        static bool less_than(const void* a, const void* b, const TypeMeta*) {
            if constexpr (requires(const T& x, const T& y) { x < y; }) {
                return *static_cast<const T*>(a) < *static_cast<const T*>(b);
            } else {
                return false;
            }
        }

        static size_t hash(const void* v, const TypeMeta*) {
            if constexpr (requires(const T& x) { std::hash<T>{}(x); }) {
                return std::hash<T>{}(*static_cast<const T*>(v));
            } else {
                return 0;
            }
        }

        static std::string to_string(const void* v, const TypeMeta*) {
            const T& val = *static_cast<const T*>(v);
            if constexpr (std::is_same_v<T, bool>) {
                return val ? "true" : "false";
            } else if constexpr (std::is_arithmetic_v<T>) {
                return std::to_string(val);
            } else if constexpr (requires { std::to_string(val); }) {
                return std::to_string(val);
            } else {
                return "<value>";
            }
        }

        static std::string type_name(const TypeMeta*) {
            // Return Python-style type names
            if constexpr (std::is_same_v<T, bool>) {
                return "bool";
            } else if constexpr (std::is_same_v<T, int64_t>) {
                return "int";
            } else if constexpr (std::is_same_v<T, double>) {
                return "float";
            } else if constexpr (std::is_same_v<T, engine_date_t>) {
                return "date";
            } else if constexpr (std::is_same_v<T, engine_time_t>) {
                return "datetime";
            } else if constexpr (std::is_same_v<T, engine_time_delta_t>) {
                return "timedelta";
            } else {
                return typeid(T).name();
            }
        }

        static void* to_python(const void* v, const TypeMeta*) {
            const T& val = *static_cast<const T*>(v);
            nb::object obj = nb::cast(val);
            return obj.release().ptr();
        }

        static void from_python(void* dest, void* py_obj, const TypeMeta*) {
            nb::handle h(static_cast<PyObject*>(py_obj));
            *static_cast<T*>(dest) = nb::cast<T>(h);
        }

        // Arithmetic operators - delegate to ScalarTypeOps
        static bool add(void* dest, const void* a, const void* b, const TypeMeta* meta) {
            return ScalarTypeOps<T>::add(dest, a, b, meta);
        }
        static bool subtract(void* dest, const void* a, const void* b, const TypeMeta* meta) {
            return ScalarTypeOps<T>::subtract(dest, a, b, meta);
        }
        static bool multiply(void* dest, const void* a, const void* b, const TypeMeta* meta) {
            return ScalarTypeOps<T>::multiply(dest, a, b, meta);
        }
        static bool divide(void* dest, const void* a, const void* b, const TypeMeta* meta) {
            return ScalarTypeOps<T>::divide(dest, a, b, meta);
        }
        static bool floor_divide(void* dest, const void* a, const void* b, const TypeMeta* meta) {
            return ScalarTypeOps<T>::floor_divide(dest, a, b, meta);
        }
        static bool modulo(void* dest, const void* a, const void* b, const TypeMeta* meta) {
            return ScalarTypeOps<T>::modulo(dest, a, b, meta);
        }
        static bool power(void* dest, const void* a, const void* b, const TypeMeta* meta) {
            return ScalarTypeOps<T>::power(dest, a, b, meta);
        }
        static bool negate(void* dest, const void* src, const TypeMeta* meta) {
            return ScalarTypeOps<T>::negate(dest, src, meta);
        }
        static bool absolute(void* dest, const void* src, const TypeMeta* meta) {
            return ScalarTypeOps<T>::absolute(dest, src, meta);
        }
        static bool invert(void* dest, const void* src, const TypeMeta* meta) {
            return ScalarTypeOps<T>::invert(dest, src, meta);
        }
        static bool to_bool(const void* v, const TypeMeta* meta) {
            return ScalarTypeOps<T>::to_bool(v, meta);
        }

        static constexpr TypeOps ops = {
            .construct = construct,
            .destruct = destruct,
            .copy_construct = copy_construct,
            .move_construct = move_construct,
            .copy_assign = copy_assign,
            .move_assign = move_assign,
            .equals = equals,
            .less_than = less_than,
            .hash = hash,
            .to_string = to_string,
            .type_name = type_name,
            .to_python = to_python,
            .from_python = from_python,
            .add = add,
            .subtract = subtract,
            .multiply = multiply,
            .divide = divide,
            .floor_divide = floor_divide,
            .modulo = modulo,
            .power = power,
            .negate = negate,
            .absolute = absolute,
            .invert = invert,
            .to_bool = to_bool,
            .length = nullptr,
            .contains = nullptr,
        };
    };

    // ========================================================================
    // Specialization for nb::object - already a Python object
    // ========================================================================

    template<>
    struct ScalarTypeOpsWithPython<nb::object> {
        static void construct(void* dest, const TypeMeta*) {
            new (dest) nb::object();
        }

        static void destruct(void* dest, const TypeMeta*) {
            static_cast<nb::object*>(dest)->~object();
        }

        static void copy_construct(void* dest, const void* src, const TypeMeta*) {
            new (dest) nb::object(*static_cast<const nb::object*>(src));
        }

        static void move_construct(void* dest, void* src, const TypeMeta*) {
            new (dest) nb::object(std::move(*static_cast<nb::object*>(src)));
        }

        static void copy_assign(void* dest, const void* src, const TypeMeta*) {
            *static_cast<nb::object*>(dest) = *static_cast<const nb::object*>(src);
        }

        static void move_assign(void* dest, void* src, const TypeMeta*) {
            *static_cast<nb::object*>(dest) = std::move(*static_cast<nb::object*>(src));
        }

        static bool equals(const void* a, const void* b, const TypeMeta*) {
            return static_cast<const nb::object*>(a)->equal(*static_cast<const nb::object*>(b));
        }

        static bool less_than(const void* a, const void* b, const TypeMeta*) {
            return static_cast<const nb::object*>(a)->operator<(*static_cast<const nb::object*>(b));
        }

        static size_t hash(const void* v, const TypeMeta*) {
            try {
                return nb::hash(*static_cast<const nb::object*>(v));
            } catch (...) {
                return 0;
            }
        }

        static std::string to_string(const void* v, const TypeMeta*) {
            const nb::object& obj = *static_cast<const nb::object*>(v);
            try {
                return nb::cast<std::string>(nb::str(obj));
            } catch (...) {
                return "<object>";
            }
        }

        static std::string type_name(const TypeMeta*) {
            return "object";
        }

        static void* to_python(const void* v, const TypeMeta*) {
            // Already a Python object - just increment refcount and return
            const nb::object& obj = *static_cast<const nb::object*>(v);
            return nb::object(obj).release().ptr();
        }

        static void from_python(void* dest, void* py_obj, const TypeMeta*) {
            nb::handle h(static_cast<PyObject*>(py_obj));
            *static_cast<nb::object*>(dest) = nb::borrow(h);
        }

        // =========================================================================
        // Arithmetic operators - delegate to Python
        // =========================================================================

        static bool add(void* dest, const void* a, const void* b, const TypeMeta*) {
            const nb::object& oa = *static_cast<const nb::object*>(a);
            const nb::object& ob = *static_cast<const nb::object*>(b);
            try {
                *static_cast<nb::object*>(dest) = oa + ob;
                return true;
            } catch (...) {
                return false;
            }
        }

        static bool subtract(void* dest, const void* a, const void* b, const TypeMeta*) {
            const nb::object& oa = *static_cast<const nb::object*>(a);
            const nb::object& ob = *static_cast<const nb::object*>(b);
            try {
                *static_cast<nb::object*>(dest) = oa - ob;
                return true;
            } catch (...) {
                return false;
            }
        }

        static bool multiply(void* dest, const void* a, const void* b, const TypeMeta*) {
            const nb::object& oa = *static_cast<const nb::object*>(a);
            const nb::object& ob = *static_cast<const nb::object*>(b);
            try {
                *static_cast<nb::object*>(dest) = oa * ob;
                return true;
            } catch (...) {
                return false;
            }
        }

        static bool divide(void* dest, const void* a, const void* b, const TypeMeta*) {
            const nb::object& oa = *static_cast<const nb::object*>(a);
            const nb::object& ob = *static_cast<const nb::object*>(b);
            try {
                *static_cast<nb::object*>(dest) = oa / ob;
                return true;
            } catch (...) {
                return false;
            }
        }

        static bool floor_divide(void* dest, const void* a, const void* b, const TypeMeta*) {
            const nb::object& oa = *static_cast<const nb::object*>(a);
            const nb::object& ob = *static_cast<const nb::object*>(b);
            try {
                *static_cast<nb::object*>(dest) = oa.attr("__floordiv__")(ob);
                return true;
            } catch (...) {
                return false;
            }
        }

        static bool modulo(void* dest, const void* a, const void* b, const TypeMeta*) {
            const nb::object& oa = *static_cast<const nb::object*>(a);
            const nb::object& ob = *static_cast<const nb::object*>(b);
            try {
                *static_cast<nb::object*>(dest) = oa % ob;
                return true;
            } catch (...) {
                return false;
            }
        }

        static bool power(void* dest, const void* a, const void* b, const TypeMeta*) {
            const nb::object& oa = *static_cast<const nb::object*>(a);
            const nb::object& ob = *static_cast<const nb::object*>(b);
            try {
                *static_cast<nb::object*>(dest) = oa.attr("__pow__")(ob);
                return true;
            } catch (...) {
                return false;
            }
        }

        static bool negate(void* dest, const void* src, const TypeMeta*) {
            const nb::object& o = *static_cast<const nb::object*>(src);
            try {
                *static_cast<nb::object*>(dest) = -o;
                return true;
            } catch (...) {
                return false;
            }
        }

        static bool absolute(void* dest, const void* src, const TypeMeta*) {
            const nb::object& o = *static_cast<const nb::object*>(src);
            try {
                *static_cast<nb::object*>(dest) = o.attr("__abs__")();
                return true;
            } catch (...) {
                return false;
            }
        }

        static bool invert(void* dest, const void* src, const TypeMeta*) {
            const nb::object& o = *static_cast<const nb::object*>(src);
            try {
                *static_cast<nb::object*>(dest) = o.attr("__invert__")();
                return true;
            } catch (...) {
                return false;
            }
        }

        static bool to_bool(const void* v, const TypeMeta*) {
            const nb::object& o = *static_cast<const nb::object*>(v);
            try {
                return nb::cast<bool>(nb::bool_(o));
            } catch (...) {
                return true;  // Non-null objects are truthy by default
            }
        }

        static size_t length(const void* v, const TypeMeta*) {
            const nb::object& o = *static_cast<const nb::object*>(v);
            try {
                return nb::len(o);
            } catch (...) {
                return 0;
            }
        }

        static bool contains(const void* container, const void* element, const TypeMeta*) {
            const nb::object& c = *static_cast<const nb::object*>(container);
            const nb::object& e = *static_cast<const nb::object*>(element);
            try {
                return nb::cast<bool>(c.attr("__contains__")(e));
            } catch (...) {
                return false;
            }
        }

        static const TypeOps ops;
    };

    inline const TypeOps ScalarTypeOpsWithPython<nb::object>::ops = {
        .construct = construct,
        .destruct = destruct,
        .copy_construct = copy_construct,
        .move_construct = move_construct,
        .copy_assign = copy_assign,
        .move_assign = move_assign,
        .equals = equals,
        .less_than = less_than,
        .hash = hash,
        .to_string = to_string,
        .type_name = type_name,
        .to_python = to_python,
        .from_python = from_python,
        // Python object operators - delegate to Python runtime
        .add = add,
        .subtract = subtract,
        .multiply = multiply,
        .divide = divide,
        .floor_divide = floor_divide,
        .modulo = modulo,
        .power = power,
        .negate = negate,
        .absolute = absolute,
        .invert = invert,
        .to_bool = to_bool,
        .length = length,
        .contains = contains,
    };

    /**
     * ScalarTypeMetaWithPython - TypeMeta for scalar types with Python support
     */
    template<typename T>
    struct ScalarTypeMetaWithPython {
        static const TypeMeta instance;
        static const TypeMeta* get() { return &instance; }
    };

    template<typename T>
    const TypeMeta ScalarTypeMetaWithPython<T>::instance = {
        .size = sizeof(T),
        .alignment = alignof(T),
        .flags = compute_flags<T>(),
        .kind = TypeKind::Scalar,
        .ops = &ScalarTypeOpsWithPython<T>::ops,
        .type_info = &typeid(T),
        .name = nullptr,
        .numpy_format = numpy_format_for<T>(),
    };

    // Specialization for nb::object - Python objects can be hashable and equatable
    template<>
    struct ScalarTypeMetaWithPython<nb::object> {
        static const TypeMeta instance;
        static const TypeMeta* get() { return &instance; }
    };

    inline const TypeMeta ScalarTypeMetaWithPython<nb::object>::instance = {
        .size = sizeof(nb::object),
        .alignment = alignof(nb::object),
        // Python objects support all operations via Python runtime delegation
        .flags = TypeFlags::Hashable | TypeFlags::Equatable | TypeFlags::Comparable |
                 TypeFlags::Arithmetic | TypeFlags::Integral | TypeFlags::Container,
        .kind = TypeKind::Scalar,
        .ops = &ScalarTypeOpsWithPython<nb::object>::ops,
        .type_info = &typeid(nb::object),
        .name = nullptr,
        .numpy_format = nullptr,
    };

    /**
     * Helper to get TypeMeta for scalar type with Python support
     */
    template<typename T>
    const TypeMeta* scalar_type_meta_with_python() {
        return ScalarTypeMetaWithPython<T>::get();
    }

    // ========================================================================
    // Composite Type Python Conversions - Recursive via stored element ops
    // ========================================================================

    /**
     * Generic value_to_python - Uses the to_python function pointer from TypeMeta
     * Falls back to type-kind-based dispatch if no custom ops.
     */
    inline nb::object value_to_python(const void* v, const TypeMeta* meta);
    inline void value_from_python(void* dest, nb::handle py_obj, const TypeMeta* meta);

    // ========================================================================
    // Bundle Type Python Conversions
    // ========================================================================

    struct BundlePythonOps {
        static void* to_python(const void* v, const TypeMeta* meta) {
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(meta);
            nb::dict result;

            for (const auto& field : bundle_meta->fields) {
                const void* field_ptr = static_cast<const char*>(v) + field.offset;
                // Use the field's type ops directly - resolved at bundle construction time
                nb::object field_value = value_to_python(field_ptr, field.type);
                result[field.name.c_str()] = field_value;
            }

            return result.release().ptr();
        }

        static void from_python(void* dest, void* py_obj, const TypeMeta* meta) {
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(meta);
            nb::handle h(static_cast<PyObject*>(py_obj));

            if (nb::isinstance<nb::dict>(h)) {
                nb::dict d = nb::cast<nb::dict>(h);
                for (const auto& field : bundle_meta->fields) {
                    void* field_ptr = static_cast<char*>(dest) + field.offset;
                    if (d.contains(field.name.c_str())) {
                        value_from_python(field_ptr, d[field.name.c_str()], field.type);
                    }
                }
            } else {
                // Treat as object with attributes
                for (const auto& field : bundle_meta->fields) {
                    void* field_ptr = static_cast<char*>(dest) + field.offset;
                    if (nb::hasattr(h, field.name.c_str())) {
                        value_from_python(field_ptr, nb::getattr(h, field.name.c_str()), field.type);
                    }
                }
            }
        }

        static std::string to_string(const void* v, const TypeMeta* meta) {
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(meta);
            std::string result = "{";
            bool first = true;
            for (const auto& field : bundle_meta->fields) {
                if (!first) result += ", ";
                first = false;
                const void* field_ptr = static_cast<const char*>(v) + field.offset;
                result += field.name + ": " + field.type->to_string_at(field_ptr);
            }
            result += "}";
            return result;
        }

        static std::string type_name(const TypeMeta* meta) {
            auto* bundle_meta = static_cast<const BundleTypeMeta*>(meta);
            if (bundle_meta->name) return bundle_meta->name;
            // Generate: Bundle[field1: type1, field2: type2, ...]
            std::string result = "Bundle[";
            bool first = true;
            for (const auto& field : bundle_meta->fields) {
                if (!first) result += ", ";
                first = false;
                result += field.name + ": " + field.type->type_name_str();
            }
            result += "]";
            return result;
        }
    };

    inline const TypeOps BundleTypeOpsWithPython = {
        .construct = BundleTypeOps::construct,
        .destruct = BundleTypeOps::destruct,
        .copy_construct = BundleTypeOps::copy_construct,
        .move_construct = BundleTypeOps::move_construct,
        .copy_assign = BundleTypeOps::copy_assign,
        .move_assign = BundleTypeOps::move_assign,
        .equals = BundleTypeOps::equals,
        .less_than = BundleTypeOps::less_than,
        .hash = BundleTypeOps::hash,
        .to_string = BundlePythonOps::to_string,
        .type_name = BundlePythonOps::type_name,
        .to_python = BundlePythonOps::to_python,
        .from_python = BundlePythonOps::from_python,
        // Bundles don't support arithmetic
        .add = nullptr,
        .subtract = nullptr,
        .multiply = nullptr,
        .divide = nullptr,
        .floor_divide = nullptr,
        .modulo = nullptr,
        .power = nullptr,
        .negate = nullptr,
        .absolute = nullptr,
        .invert = nullptr,
        .to_bool = nullptr,
        .length = nullptr,
        .contains = nullptr,
    };

    // ========================================================================
    // List Type Python Conversions
    // ========================================================================

    struct ListPythonOps {
        static void* to_python(const void* v, const TypeMeta* meta) {
            auto* list_meta = static_cast<const ListTypeMeta*>(meta);
            nb::list result;

            const char* ptr = static_cast<const char*>(v);
            for (size_t i = 0; i < list_meta->count; ++i) {
                // Use element type's ops directly
                nb::object elem = value_to_python(ptr, list_meta->element_type);
                result.append(elem);
                ptr += list_meta->element_type->size;
            }

            return result.release().ptr();
        }

        static void from_python(void* dest, void* py_obj, const TypeMeta* meta) {
            auto* list_meta = static_cast<const ListTypeMeta*>(meta);
            nb::handle h(static_cast<PyObject*>(py_obj));
            nb::list py_list = nb::cast<nb::list>(h);

            char* ptr = static_cast<char*>(dest);
            size_t count = std::min(static_cast<size_t>(nb::len(py_list)), list_meta->count);

            for (size_t i = 0; i < count; ++i) {
                value_from_python(ptr, py_list[i], list_meta->element_type);
                ptr += list_meta->element_type->size;
            }
        }

        static std::string to_string(const void* v, const TypeMeta* meta) {
            auto* list_meta = static_cast<const ListTypeMeta*>(meta);
            std::string result = "[";
            const char* ptr = static_cast<const char*>(v);
            for (size_t i = 0; i < list_meta->count; ++i) {
                if (i > 0) result += ", ";
                result += list_meta->element_type->to_string_at(ptr);
                ptr += list_meta->element_type->size;
            }
            result += "]";
            return result;
        }

        static std::string type_name(const TypeMeta* meta) {
            auto* list_meta = static_cast<const ListTypeMeta*>(meta);
            if (list_meta->name) return list_meta->name;
            return "List[" + list_meta->element_type->type_name_str() + ", " +
                   std::to_string(list_meta->count) + "]";
        }
    };

    inline const TypeOps ListTypeOpsWithPython = {
        .construct = ListTypeOps::construct,
        .destruct = ListTypeOps::destruct,
        .copy_construct = ListTypeOps::copy_construct,
        .move_construct = ListTypeOps::move_construct,
        .copy_assign = ListTypeOps::copy_assign,
        .move_assign = ListTypeOps::move_assign,
        .equals = ListTypeOps::equals,
        .less_than = ListTypeOps::less_than,
        .hash = ListTypeOps::hash,
        .to_string = ListPythonOps::to_string,
        .type_name = ListPythonOps::type_name,
        .to_python = ListPythonOps::to_python,
        .from_python = ListPythonOps::from_python,
        // Lists don't support arithmetic
        .add = nullptr,
        .subtract = nullptr,
        .multiply = nullptr,
        .divide = nullptr,
        .floor_divide = nullptr,
        .modulo = nullptr,
        .power = nullptr,
        .negate = nullptr,
        .absolute = nullptr,
        .invert = nullptr,
        .to_bool = nullptr,
        .length = nullptr,  // Fixed-size lists - could add if needed
        .contains = nullptr,
    };

    // ========================================================================
    // Set Type Python Conversions
    // ========================================================================

    struct SetPythonOps {
        static void* to_python(const void* v, const TypeMeta* meta) {
            auto* set_meta = static_cast<const SetTypeMeta*>(meta);
            auto* storage = static_cast<const SetStorage*>(v);
            nb::set result;

            for (auto elem : *storage) {
                nb::object py_elem = value_to_python(elem.ptr, set_meta->element_type);
                result.add(py_elem);
            }

            return result.release().ptr();
        }

        static void from_python(void* dest, void* py_obj, const TypeMeta* meta) {
            auto* set_meta = static_cast<const SetTypeMeta*>(meta);
            auto* storage = static_cast<SetStorage*>(dest);
            nb::handle h(static_cast<PyObject*>(py_obj));

            storage->clear();

            std::vector<char> temp_storage(set_meta->element_type->size);

            nb::iterator it = nb::iter(h);
            while (it != nb::iterator::sentinel()) {
                set_meta->element_type->construct_at(temp_storage.data());
                value_from_python(temp_storage.data(), *it, set_meta->element_type);
                storage->add(temp_storage.data());
                set_meta->element_type->destruct_at(temp_storage.data());
                ++it;
            }
        }

        static std::string to_string(const void* v, const TypeMeta* meta) {
            auto* set_meta = static_cast<const SetTypeMeta*>(meta);
            auto* storage = static_cast<const SetStorage*>(v);
            std::string result = "{";
            bool first = true;
            for (auto elem : *storage) {
                if (!first) result += ", ";
                first = false;
                result += set_meta->element_type->to_string_at(elem.ptr);
            }
            result += "}";
            return result;
        }

        static std::string type_name(const TypeMeta* meta) {
            auto* set_meta = static_cast<const SetTypeMeta*>(meta);
            if (set_meta->name) return set_meta->name;
            return "Set[" + set_meta->element_type->type_name_str() + "]";
        }
    };

    inline const TypeOps SetTypeOpsWithPython = {
        .construct = SetTypeOps::construct,
        .destruct = SetTypeOps::destruct,
        .copy_construct = SetTypeOps::copy_construct,
        .move_construct = SetTypeOps::move_construct,
        .copy_assign = SetTypeOps::copy_assign,
        .move_assign = SetTypeOps::move_assign,
        .equals = SetTypeOps::equals,
        .less_than = SetTypeOps::less_than,
        .hash = SetTypeOps::hash,
        .to_string = SetPythonOps::to_string,
        .type_name = SetPythonOps::type_name,
        .to_python = SetPythonOps::to_python,
        .from_python = SetPythonOps::from_python,
        // Sets don't support arithmetic
        .add = nullptr,
        .subtract = nullptr,
        .multiply = nullptr,
        .divide = nullptr,
        .floor_divide = nullptr,
        .modulo = nullptr,
        .power = nullptr,
        .negate = nullptr,
        .absolute = nullptr,
        .invert = nullptr,
        // Container operations - use SetTypeOps
        .to_bool = SetTypeOps::to_bool,
        .length = SetTypeOps::length,
        .contains = SetTypeOps::contains,
    };

    // ========================================================================
    // Dict Type Python Conversions
    // ========================================================================

    struct DictPythonOps {
        static void* to_python(const void* v, const TypeMeta* meta) {
            auto* dict_meta = static_cast<const DictTypeMeta*>(meta);
            auto* storage = static_cast<const DictStorage*>(v);
            nb::dict result;

            for (auto kv : *storage) {
                nb::object py_key = value_to_python(kv.key.ptr, dict_meta->key_type());
                nb::object py_value = value_to_python(kv.value.ptr, dict_meta->value_type);
                result[py_key] = py_value;
            }

            return result.release().ptr();
        }

        static void from_python(void* dest, void* py_obj, const TypeMeta* meta) {
            auto* dict_meta = static_cast<const DictTypeMeta*>(meta);
            auto* storage = static_cast<DictStorage*>(dest);
            nb::handle h(static_cast<PyObject*>(py_obj));
            nb::dict py_dict = nb::cast<nb::dict>(h);

            storage->clear();

            std::vector<char> key_storage(dict_meta->key_type()->size);
            std::vector<char> value_storage(dict_meta->value_type->size);

            for (auto item : py_dict) {
                dict_meta->key_type()->construct_at(key_storage.data());
                dict_meta->value_type->construct_at(value_storage.data());

                value_from_python(key_storage.data(), item.first, dict_meta->key_type());
                value_from_python(value_storage.data(), item.second, dict_meta->value_type);

                storage->insert(key_storage.data(), value_storage.data());

                dict_meta->key_type()->destruct_at(key_storage.data());
                dict_meta->value_type->destruct_at(value_storage.data());
            }
        }

        static std::string to_string(const void* v, const TypeMeta* meta) {
            auto* dict_meta = static_cast<const DictTypeMeta*>(meta);
            auto* storage = static_cast<const DictStorage*>(v);
            std::string result = "{";
            bool first = true;
            for (auto kv : *storage) {
                if (!first) result += ", ";
                first = false;
                result += dict_meta->key_type()->to_string_at(kv.key.ptr) + ": " +
                         dict_meta->value_type->to_string_at(kv.value.ptr);
            }
            result += "}";
            return result;
        }

        static std::string type_name(const TypeMeta* meta) {
            auto* dict_meta = static_cast<const DictTypeMeta*>(meta);
            if (dict_meta->name) return dict_meta->name;
            return "Dict[" + dict_meta->key_type()->type_name_str() + ", " +
                   dict_meta->value_type->type_name_str() + "]";
        }
    };

    inline const TypeOps DictTypeOpsWithPython = {
        .construct = DictTypeOps::construct,
        .destruct = DictTypeOps::destruct,
        .copy_construct = DictTypeOps::copy_construct,
        .move_construct = DictTypeOps::move_construct,
        .copy_assign = DictTypeOps::copy_assign,
        .move_assign = DictTypeOps::move_assign,
        .equals = DictTypeOps::equals,
        .less_than = DictTypeOps::less_than,
        .hash = DictTypeOps::hash,
        .to_string = DictPythonOps::to_string,
        .type_name = DictPythonOps::type_name,
        .to_python = DictPythonOps::to_python,
        .from_python = DictPythonOps::from_python,
        // Dicts don't support arithmetic
        .add = nullptr,
        .subtract = nullptr,
        .multiply = nullptr,
        .divide = nullptr,
        .floor_divide = nullptr,
        .modulo = nullptr,
        .power = nullptr,
        .negate = nullptr,
        .absolute = nullptr,
        .invert = nullptr,
        // Container operations - use DictTypeOps
        .to_bool = DictTypeOps::to_bool,
        .length = DictTypeOps::length,
        .contains = DictTypeOps::contains,
    };

    // ========================================================================
    // Window Type Python Conversions
    // ========================================================================

    struct WindowPythonOps {
        static void* to_python(const void* v, const TypeMeta* meta) {
            auto* window_meta = static_cast<const WindowTypeMeta*>(meta);
            auto* storage = const_cast<WindowStorage*>(static_cast<const WindowStorage*>(v));

            size_t count = storage->size();
            if (count == 0) {
                // Return empty dict with None arrays
                nb::dict result;
                result["values"] = nb::none();
                result["timestamps"] = nb::none();
                return result.release().ptr();
            }

            // Check if element type is numpy-compatible (resolved at type construction time)
            bool can_use_numpy = window_meta->element_type->is_numpy_compatible() &&
                                 window_meta->element_type->is_buffer_compatible();

            // For fixed windows, compact first to get contiguous buffer
            if (storage->is_fixed_length()) {
                storage->as_fixed().compact();
            }

            if (can_use_numpy && storage->is_buffer_accessible()) {
                // Return dict with values and timestamps lists
                // Python side can convert to numpy arrays using the numpy_format
                nb::dict result;

                nb::list values_list;
                nb::list timestamps_list;
                for (size_t i = 0; i < count; ++i) {
                    values_list.append(value_to_python(storage->get(i), window_meta->element_type));
                    timestamps_list.append(static_cast<int64_t>(storage->timestamp(i).time_since_epoch().count()));
                }
                result["values"] = values_list;
                result["timestamps"] = timestamps_list;

                return result.release().ptr();
            }

            // Fallback to list of tuples format for non-buffer-compatible types
            nb::list result;
            for (size_t i = 0; i < count; ++i) {
                nb::object py_value = value_to_python(storage->get(i), window_meta->element_type);
                engine_time_t ts = storage->timestamp(i);
                nb::tuple entry = nb::make_tuple(static_cast<int64_t>(ts.time_since_epoch().count()), py_value);
                result.append(entry);
            }

            return result.release().ptr();
        }

        static void from_python(void* dest, void* py_obj, const TypeMeta* meta) {
            auto* window_meta = static_cast<const WindowTypeMeta*>(meta);
            auto* storage = static_cast<WindowStorage*>(dest);
            nb::handle h(static_cast<PyObject*>(py_obj));

            storage->clear();

            std::vector<char> elem_storage(window_meta->element_type->size);

            // Handle dict format with 'values' and 'timestamps' keys
            if (nb::isinstance<nb::dict>(h)) {
                nb::dict d = nb::cast<nb::dict>(h);
                if (d.contains("values") && d.contains("timestamps")) {
                    nb::object values = d["values"];
                    nb::object timestamps = d["timestamps"];

                    if (!values.is_none() && !timestamps.is_none()) {
                        nb::list values_list = nb::cast<nb::list>(values);
                        nb::list ts_list = nb::cast<nb::list>(timestamps);

                        size_t count = std::min(nb::len(values_list), nb::len(ts_list));
                        for (size_t i = 0; i < count; ++i) {
                            int64_t ts_nanos = nb::cast<int64_t>(ts_list[i]);
                            engine_time_t ts{engine_time_delta_t{ts_nanos}};

                            window_meta->element_type->construct_at(elem_storage.data());
                            value_from_python(elem_storage.data(), values_list[i], window_meta->element_type);
                            storage->push(elem_storage.data(), ts);
                            window_meta->element_type->destruct_at(elem_storage.data());
                        }
                        return;
                    }
                }
            }

            // Fallback to list of tuples format
            nb::list py_list = nb::cast<nb::list>(h);

            for (size_t i = 0; i < nb::len(py_list); ++i) {
                nb::tuple entry = nb::cast<nb::tuple>(py_list[i]);
                if (nb::len(entry) >= 2) {
                    int64_t ts_nanos = nb::cast<int64_t>(entry[0]);
                    engine_time_t ts{engine_time_delta_t{ts_nanos}};

                    window_meta->element_type->construct_at(elem_storage.data());
                    value_from_python(elem_storage.data(), entry[1], window_meta->element_type);
                    storage->push(elem_storage.data(), ts);
                    window_meta->element_type->destruct_at(elem_storage.data());
                }
            }
        }

        static std::string to_string(const void* v, const TypeMeta* meta) {
            auto* window_meta = static_cast<const WindowTypeMeta*>(meta);
            auto* storage = const_cast<WindowStorage*>(static_cast<const WindowStorage*>(v));
            std::string result = "Window[";
            for (size_t i = 0; i < storage->size(); ++i) {
                if (i > 0) result += ", ";
                result += window_meta->element_type->to_string_at(storage->get(i));
            }
            result += "]";
            return result;
        }

        static std::string type_name(const TypeMeta* meta) {
            auto* window_meta = static_cast<const WindowTypeMeta*>(meta);
            if (window_meta->name) return window_meta->name;
            std::string result = "Window[" + window_meta->element_type->type_name_str();
            if (window_meta->max_count > 0) {
                result += ", count=" + std::to_string(window_meta->max_count);
            } else if (window_meta->window_duration.count() > 0) {
                result += ", duration=" + std::to_string(window_meta->window_duration.count()) + "us";
            }
            result += "]";
            return result;
        }
    };

    inline const TypeOps WindowTypeOpsWithPython = {
        .construct = WindowTypeOps::construct,
        .destruct = WindowTypeOps::destruct,
        .copy_construct = WindowTypeOps::copy_construct,
        .move_construct = WindowTypeOps::move_construct,
        .copy_assign = WindowTypeOps::copy_assign,
        .move_assign = WindowTypeOps::move_assign,
        .equals = WindowTypeOps::equals,
        .less_than = WindowTypeOps::less_than,
        .hash = WindowTypeOps::hash,
        .to_string = WindowPythonOps::to_string,
        .type_name = WindowPythonOps::type_name,
        .to_python = WindowPythonOps::to_python,
        .from_python = WindowPythonOps::from_python,
        // Windows don't support arithmetic
        .add = nullptr,
        .subtract = nullptr,
        .multiply = nullptr,
        .divide = nullptr,
        .floor_divide = nullptr,
        .modulo = nullptr,
        .power = nullptr,
        .negate = nullptr,
        .absolute = nullptr,
        .invert = nullptr,
        .to_bool = nullptr,
        .length = nullptr,
        .contains = nullptr,
    };

    // ========================================================================
    // Ref Type Python Conversions
    // ========================================================================

    struct RefPythonOps {
        static void* to_python(const void* v, const TypeMeta* meta) {
            auto* ref_meta = static_cast<const RefTypeMeta*>(meta);
            auto* storage = static_cast<const RefStorage*>(v);

            if (storage->is_empty()) {
                return nb::none().release().ptr();
            }

            if (storage->is_bound()) {
                const ValueRef& target = storage->target();
                if (target.data) {
                    return value_to_python(target.data, ref_meta->value_type).release().ptr();
                }
                return nb::none().release().ptr();
            }

            // Unbound composite - return list of referenced values
            nb::list result;
            const auto& items = storage->items();
            for (const auto& item : items) {
                if (item.is_bound()) {
                    const ValueRef& target = item.target();
                    if (target.data) {
                        result.append(value_to_python(target.data, ref_meta->value_type));
                    } else {
                        result.append(nb::none());
                    }
                } else {
                    result.append(nb::none());
                }
            }
            return result.release().ptr();
        }

        static void from_python(void* dest, void* py_obj, const TypeMeta* meta) {
            // Refs are non-owning pointers to C++ objects - cannot reconstruct from Python
            (void)dest;
            (void)py_obj;
            (void)meta;
        }

        static std::string to_string(const void* v, const TypeMeta* meta) {
            auto* ref_meta = static_cast<const RefTypeMeta*>(meta);
            auto* storage = static_cast<const RefStorage*>(v);
            if (storage->is_empty()) {
                return "Ref(empty)";
            }
            if (storage->is_bound()) {
                const ValueRef& target = storage->target();
                if (target.data) {
                    return "Ref(" + ref_meta->value_type->to_string_at(target.data) + ")";
                }
                return "Ref(null)";
            }
            return "Ref(unbound)";
        }

        static std::string type_name(const TypeMeta* meta) {
            auto* ref_meta = static_cast<const RefTypeMeta*>(meta);
            if (ref_meta->name) return ref_meta->name;
            return "Ref[" + ref_meta->value_type->type_name_str() + "]";
        }
    };

    inline const TypeOps RefTypeOpsWithPython = {
        .construct = RefTypeOps::construct,
        .destruct = RefTypeOps::destruct,
        .copy_construct = RefTypeOps::copy_construct,
        .move_construct = RefTypeOps::move_construct,
        .copy_assign = RefTypeOps::copy_assign,
        .move_assign = RefTypeOps::move_assign,
        .equals = RefTypeOps::equals,
        .less_than = RefTypeOps::less_than,
        .hash = RefTypeOps::hash,
        .to_string = RefPythonOps::to_string,
        .type_name = RefPythonOps::type_name,
        .to_python = RefPythonOps::to_python,
        .from_python = RefPythonOps::from_python,
        // Refs don't support arithmetic
        .add = nullptr,
        .subtract = nullptr,
        .multiply = nullptr,
        .divide = nullptr,
        .floor_divide = nullptr,
        .modulo = nullptr,
        .power = nullptr,
        .negate = nullptr,
        .absolute = nullptr,
        .invert = nullptr,
        .to_bool = nullptr,
        .length = nullptr,
        .contains = nullptr,
    };

    // ========================================================================
    // DynamicList Type Python Conversions (for tuple[T, ...])
    // ========================================================================

    struct DynamicListPythonOps {
        static void* to_python(const void* v, const TypeMeta* meta) {
            auto* list_meta = static_cast<const DynamicListTypeMeta*>(meta);
            auto* storage = static_cast<const DynamicListStorage*>(v);

            // Build a list first, then convert to tuple
            // (tuple[T, ...] in Python should return a tuple)
            nb::list temp;
            for (size_t i = 0; i < storage->size(); ++i) {
                nb::object elem = value_to_python(storage->get(i), list_meta->element_type);
                temp.append(elem);
            }

            // Convert to tuple
            nb::tuple result = nb::cast<nb::tuple>(nb::module_::import_("builtins").attr("tuple")(temp));
            return result.release().ptr();
        }

        static void from_python(void* dest, void* py_obj, const TypeMeta* meta) {
            auto* list_meta = static_cast<const DynamicListTypeMeta*>(meta);
            auto* storage = static_cast<DynamicListStorage*>(dest);
            nb::handle h(static_cast<PyObject*>(py_obj));

            storage->clear();

            std::vector<char> temp_storage(list_meta->element_type->size);

            nb::iterator it = nb::iter(h);
            while (it != nb::iterator::sentinel()) {
                list_meta->element_type->construct_at(temp_storage.data());
                value_from_python(temp_storage.data(), *it, list_meta->element_type);
                storage->push_back(temp_storage.data());
                list_meta->element_type->destruct_at(temp_storage.data());
                ++it;
            }
        }

        static std::string to_string(const void* v, const TypeMeta* meta) {
            auto* list_meta = static_cast<const DynamicListTypeMeta*>(meta);
            auto* storage = static_cast<const DynamicListStorage*>(v);
            std::string result = "(";
            for (size_t i = 0; i < storage->size(); ++i) {
                if (i > 0) result += ", ";
                result += list_meta->element_type->to_string_at(storage->get(i));
            }
            if (storage->size() == 1) result += ",";  // Python tuple notation for single element
            result += ")";
            return result;
        }

        static std::string type_name(const TypeMeta* meta) {
            auto* list_meta = static_cast<const DynamicListTypeMeta*>(meta);
            if (list_meta->name) return list_meta->name;
            return "tuple[" + list_meta->element_type->type_name_str() + ", ...]";
        }
    };

    inline const TypeOps DynamicListTypeOpsWithPython = {
        .construct = DynamicListTypeOps::construct,
        .destruct = DynamicListTypeOps::destruct,
        .copy_construct = DynamicListTypeOps::copy_construct,
        .move_construct = DynamicListTypeOps::move_construct,
        .copy_assign = DynamicListTypeOps::copy_assign,
        .move_assign = DynamicListTypeOps::move_assign,
        .equals = DynamicListTypeOps::equals,
        .less_than = DynamicListTypeOps::less_than,
        .hash = DynamicListTypeOps::hash,
        .to_string = DynamicListPythonOps::to_string,
        .type_name = DynamicListPythonOps::type_name,
        .to_python = DynamicListPythonOps::to_python,
        .from_python = DynamicListPythonOps::from_python,
        // DynamicLists don't support arithmetic
        .add = nullptr,
        .subtract = nullptr,
        .multiply = nullptr,
        .divide = nullptr,
        .floor_divide = nullptr,
        .modulo = nullptr,
        .power = nullptr,
        .negate = nullptr,
        .absolute = nullptr,
        .invert = nullptr,
        // Container operations - use DynamicListTypeOps
        .to_bool = DynamicListTypeOps::to_bool,
        .length = DynamicListTypeOps::length,
        .contains = DynamicListTypeOps::contains,
    };

    // ========================================================================
    // Generic Dispatcher - Uses stored to_python/from_python function pointers
    // ========================================================================

    /**
     * Convert value to Python using the stored ops.
     * The to_python function was set at type construction time based on element types.
     */
    inline nb::object value_to_python(const void* v, const TypeMeta* meta) {
        if (!v || !meta) {
            return nb::none();
        }

        // Use the type's registered to_python function
        if (meta->ops && meta->ops->to_python) {
            PyObject* result = static_cast<PyObject*>(meta->ops->to_python(v, meta));
            return nb::steal(result);
        }

        return nb::none();
    }

    /**
     * Convert Python object to value using the stored ops.
     */
    inline void value_from_python(void* dest, nb::handle py_obj, const TypeMeta* meta) {
        if (!dest || !meta || py_obj.is_none()) {
            return;
        }

        if (meta->ops && meta->ops->from_python) {
            meta->ops->from_python(dest, py_obj.ptr(), meta);
        }
    }

    // ========================================================================
    // Builder Extensions - Create types with Python support
    // ========================================================================

    /**
     * BundleTypeBuilderWithPython - Builds BundleTypeMeta with Python conversion ops
     */
    class BundleTypeBuilderWithPython {
    public:
        BundleTypeBuilderWithPython() = default;

        template<typename T>
        BundleTypeBuilderWithPython& add_field(const std::string& name) {
            return add_field(name, scalar_type_meta_with_python<T>());
        }

        BundleTypeBuilderWithPython& add_field(const std::string& name, const TypeMeta* field_type) {
            _pending_fields.push_back({name, field_type});
            return *this;
        }

        std::unique_ptr<BundleTypeMeta> build(const char* type_name = nullptr) {
            auto meta = std::make_unique<BundleTypeMeta>();

            size_t current_offset = 0;
            size_t max_alignment = 1;
            TypeFlags combined_flags = TypeFlags::Equatable | TypeFlags::Comparable | TypeFlags::Hashable;
            bool all_trivially_copyable = true;
            bool all_trivially_destructible = true;
            bool all_buffer_compatible = true;

            for (size_t i = 0; i < _pending_fields.size(); ++i) {
                const auto& pf = _pending_fields[i];

                current_offset = align_offset(current_offset, pf.type->alignment);
                max_alignment = std::max(max_alignment, pf.type->alignment);

                meta->fields.push_back({
                    .name = pf.name,
                    .offset = current_offset,
                    .type = pf.type,
                });
                meta->name_to_index[pf.name] = i;

                current_offset += pf.type->size;

                if (!pf.type->is_trivially_copyable()) all_trivially_copyable = false;
                if (!pf.type->is_trivially_destructible()) all_trivially_destructible = false;
                if (!pf.type->is_buffer_compatible()) all_buffer_compatible = false;
                if (!has_flag(pf.type->flags, TypeFlags::Equatable)) {
                    combined_flags = combined_flags & ~TypeFlags::Equatable;
                }
                if (!has_flag(pf.type->flags, TypeFlags::Comparable)) {
                    combined_flags = combined_flags & ~TypeFlags::Comparable;
                }
                if (!has_flag(pf.type->flags, TypeFlags::Hashable)) {
                    combined_flags = combined_flags & ~TypeFlags::Hashable;
                }
            }

            size_t total_size = align_offset(current_offset, max_alignment);

            TypeFlags flags = combined_flags;
            if (all_trivially_copyable) flags = flags | TypeFlags::TriviallyCopyable;
            if (all_trivially_destructible) flags = flags | TypeFlags::TriviallyDestructible;
            if (all_buffer_compatible) flags = flags | TypeFlags::BufferCompatible;

            meta->size = total_size;
            meta->alignment = max_alignment;
            meta->flags = flags;
            meta->kind = TypeKind::Bundle;
            meta->ops = &BundleTypeOpsWithPython;  // With Python support
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->numpy_format = nullptr;

            return meta;
        }

    private:
        struct PendingField {
            std::string name;
            const TypeMeta* type;
        };
        std::vector<PendingField> _pending_fields;
    };

    /**
     * ListTypeBuilderWithPython - Builds ListTypeMeta with Python conversion ops
     */
    class ListTypeBuilderWithPython {
    public:
        ListTypeBuilderWithPython& element_type(const TypeMeta* type) {
            _element_type = type;
            return *this;
        }

        template<typename T>
        ListTypeBuilderWithPython& element() {
            return element_type(scalar_type_meta_with_python<T>());
        }

        ListTypeBuilderWithPython& count(size_t n) {
            _count = n;
            return *this;
        }

        std::unique_ptr<ListTypeMeta> build(const char* type_name = nullptr) {
            assert(_element_type && _count > 0);

            auto meta = std::make_unique<ListTypeMeta>();

            meta->size = _element_type->size * _count;
            meta->alignment = _element_type->alignment;
            meta->flags = _element_type->flags;
            meta->kind = TypeKind::List;
            meta->ops = &ListTypeOpsWithPython;  // With Python support
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->numpy_format = nullptr;
            meta->element_type = _element_type;
            meta->count = _count;

            return meta;
        }

    private:
        const TypeMeta* _element_type{nullptr};
        size_t _count{0};
    };

    /**
     * SetTypeBuilderWithPython - Builds SetTypeMeta with Python conversion ops
     */
    class SetTypeBuilderWithPython {
    public:
        SetTypeBuilderWithPython& element_type(const TypeMeta* type) {
            _element_type = type;
            return *this;
        }

        template<typename T>
        SetTypeBuilderWithPython& element() {
            return element_type(scalar_type_meta_with_python<T>());
        }

        std::unique_ptr<SetTypeMeta> build(const char* type_name = nullptr) {
            assert(_element_type);
            assert(has_flag(_element_type->flags, TypeFlags::Hashable));
            assert(has_flag(_element_type->flags, TypeFlags::Equatable));

            auto meta = std::make_unique<SetTypeMeta>();

            meta->size = sizeof(SetStorage);
            meta->alignment = alignof(SetStorage);
            meta->flags = TypeFlags::Hashable | TypeFlags::Equatable;
            meta->kind = TypeKind::Set;
            meta->ops = &SetTypeOpsWithPython;  // With Python support
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->numpy_format = nullptr;
            meta->element_type = _element_type;

            return meta;
        }

    private:
        const TypeMeta* _element_type{nullptr};
    };

    /**
     * DictTypeBuilderWithPython - Builds DictTypeMeta with Python conversion ops
     */
    class DictTypeBuilderWithPython {
    public:
        DictTypeBuilderWithPython& key_type(const TypeMeta* type) {
            _key_type = type;
            return *this;
        }

        DictTypeBuilderWithPython& value_type(const TypeMeta* type) {
            _value_type = type;
            return *this;
        }

        template<typename K>
        DictTypeBuilderWithPython& key() {
            return key_type(scalar_type_meta_with_python<K>());
        }

        template<typename V>
        DictTypeBuilderWithPython& value() {
            return value_type(scalar_type_meta_with_python<V>());
        }

        std::unique_ptr<DictTypeMeta> build(const char* type_name = nullptr) {
            assert(_key_type && _value_type);
            assert(has_flag(_key_type->flags, TypeFlags::Hashable));
            assert(has_flag(_key_type->flags, TypeFlags::Equatable));

            auto meta = std::make_unique<DictTypeMeta>();

            TypeFlags flags = TypeFlags::Equatable;
            if (has_flag(_value_type->flags, TypeFlags::Hashable)) {
                flags = flags | TypeFlags::Hashable;
            }

            // Initialize the DictTypeMeta
            meta->size = sizeof(DictStorage);
            meta->alignment = alignof(DictStorage);
            meta->flags = flags;
            meta->kind = TypeKind::Dict;
            meta->ops = &DictTypeOpsWithPython;  // With Python support
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->numpy_format = nullptr;
            meta->value_type = _value_type;

            // Initialize the embedded SetTypeMeta for keys
            meta->key_set_meta.size = sizeof(SetStorage);
            meta->key_set_meta.alignment = alignof(SetStorage);
            meta->key_set_meta.flags = TypeFlags::Hashable | TypeFlags::Equatable;
            meta->key_set_meta.kind = TypeKind::Set;
            meta->key_set_meta.ops = &SetTypeOpsWithPython;  // With Python support
            meta->key_set_meta.type_info = nullptr;
            meta->key_set_meta.name = nullptr;  // Anonymous set type for keys
            meta->key_set_meta.numpy_format = nullptr;
            meta->key_set_meta.element_type = _key_type;

            return meta;
        }

    private:
        const TypeMeta* _key_type{nullptr};
        const TypeMeta* _value_type{nullptr};
    };

    /**
     * WindowTypeBuilderWithPython - Builds WindowTypeMeta with Python conversion ops
     */
    class WindowTypeBuilderWithPython {
    public:
        WindowTypeBuilderWithPython& element_type(const TypeMeta* type) {
            _element_type = type;
            return *this;
        }

        template<typename T>
        WindowTypeBuilderWithPython& element() {
            return element_type(scalar_type_meta_with_python<T>());
        }

        WindowTypeBuilderWithPython& fixed_count(size_t count) {
            _max_count = count;
            _window_duration = engine_time_delta_t{0};
            return *this;
        }

        WindowTypeBuilderWithPython& time_duration(engine_time_delta_t duration) {
            _window_duration = duration;
            _max_count = 0;
            return *this;
        }

        template<typename Duration>
        WindowTypeBuilderWithPython& time_duration(Duration duration) {
            _window_duration = std::chrono::duration_cast<engine_time_delta_t>(duration);
            _max_count = 0;
            return *this;
        }

        std::unique_ptr<WindowTypeMeta> build(const char* type_name = nullptr) {
            assert(_element_type);
            assert(_max_count > 0 || _window_duration.count() > 0);
            assert(!(_max_count > 0 && _window_duration.count() > 0));

            auto meta = std::make_unique<WindowTypeMeta>();

            meta->size = sizeof(WindowStorage);
            meta->alignment = alignof(WindowStorage);

            TypeFlags flags = TypeFlags::None;
            if (has_flag(_element_type->flags, TypeFlags::Hashable)) {
                flags = flags | TypeFlags::Hashable;
            }
            if (has_flag(_element_type->flags, TypeFlags::Equatable)) {
                flags = flags | TypeFlags::Equatable;
            }
            meta->flags = flags;

            meta->kind = TypeKind::Window;
            meta->ops = &WindowTypeOpsWithPython;  // With Python support
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->numpy_format = nullptr;  // Windows themselves aren't numpy, but elements may be
            meta->element_type = _element_type;
            meta->max_count = _max_count;
            meta->window_duration = _window_duration;

            return meta;
        }

    private:
        const TypeMeta* _element_type{nullptr};
        size_t _max_count{0};
        engine_time_delta_t _window_duration{0};
    };

    /**
     * DynamicListTypeBuilderWithPython - Builds DynamicListTypeMeta with Python conversion ops
     *
     * Used for variable-length sequences like tuple[T, ...]
     */
    class DynamicListTypeBuilderWithPython {
    public:
        DynamicListTypeBuilderWithPython& element_type(const TypeMeta* type) {
            _element_type = type;
            return *this;
        }

        template<typename T>
        DynamicListTypeBuilderWithPython& element() {
            return element_type(scalar_type_meta_with_python<T>());
        }

        std::unique_ptr<DynamicListTypeMeta> build(const char* type_name = nullptr) {
            assert(_element_type);

            auto meta = std::make_unique<DynamicListTypeMeta>();

            meta->size = sizeof(DynamicListStorage);
            meta->alignment = alignof(DynamicListStorage);

            // Propagate flags from element type
            TypeFlags flags = TypeFlags::None;
            if (has_flag(_element_type->flags, TypeFlags::Hashable)) {
                flags = flags | TypeFlags::Hashable;
            }
            if (has_flag(_element_type->flags, TypeFlags::Equatable)) {
                flags = flags | TypeFlags::Equatable;
            }
            meta->flags = flags;

            meta->kind = TypeKind::List;  // Reuse List kind for dynamic lists
            meta->ops = &DynamicListTypeOpsWithPython;  // With Python support
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->numpy_format = nullptr;  // Dynamic lists are not numpy-compatible
            meta->element_type = _element_type;

            return meta;
        }

    private:
        const TypeMeta* _element_type{nullptr};
    };

    // ========================================================================
    // Named Type Instances - Standard scalar types with Python support
    // ========================================================================

    /**
     * Named type metadata instances for the standard hgraph scalar types.
     * These use the same naming convention as the existing time series types.
     *
     * Usage:
     *   const TypeMeta* int_type = bool_type();  // Gets the bool TypeMeta
     */

    // bool type
    inline const TypeMeta* bool_type() {
        return scalar_type_meta_with_python<bool>();
    }

    // int type (int64_t)
    inline const TypeMeta* int_type() {
        return scalar_type_meta_with_python<int64_t>();
    }

    // float type (double)
    inline const TypeMeta* float_type() {
        return scalar_type_meta_with_python<double>();
    }

    // date type (engine_date_t / year_month_day)
    inline const TypeMeta* date_type() {
        return scalar_type_meta_with_python<engine_date_t>();
    }

    // date_time type (engine_time_t / time_point)
    inline const TypeMeta* date_time_type() {
        return scalar_type_meta_with_python<engine_time_t>();
    }

    // time_delta type (engine_time_delta_t / microseconds)
    inline const TypeMeta* time_delta_type() {
        return scalar_type_meta_with_python<engine_time_delta_t>();
    }

    // object type (nb::object)
    inline const TypeMeta* object_type() {
        return scalar_type_meta_with_python<nb::object>();
    }

    /**
     * TypeRegistry helper - Maps type names to TypeMeta instances
     */
    inline const TypeMeta* scalar_type_by_name(const std::string& name) {
        if (name == "bool") return bool_type();
        if (name == "int") return int_type();
        if (name == "float") return float_type();
        if (name == "date") return date_type();
        if (name == "date_time") return date_time_type();
        if (name == "time_delta") return time_delta_type();
        if (name == "object") return object_type();
        return nullptr;
    }

} // namespace hgraph::value

#endif // HGRAPH_VALUE_PYTHON_CONVERSION_H
