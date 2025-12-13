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

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/scalar_type.h>
#include <hgraph/types/value/bundle_type.h>
#include <hgraph/types/value/list_type.h>
#include <hgraph/types/value/set_type.h>
#include <hgraph/types/value/dict_type.h>
#include <hgraph/types/value/window_type.h>
#include <hgraph/types/value/ref_type.h>

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

        static void* to_python(const void* v, const TypeMeta*) {
            const T& val = *static_cast<const T*>(v);
            nb::object obj = nb::cast(val);
            return obj.release().ptr();
        }

        static void from_python(void* dest, void* py_obj, const TypeMeta*) {
            nb::handle h(static_cast<PyObject*>(py_obj));
            *static_cast<T*>(dest) = nb::cast<T>(h);
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
            .to_python = to_python,
            .from_python = from_python,
        };
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
        .to_python = BundlePythonOps::to_python,
        .from_python = BundlePythonOps::from_python,
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
        .to_python = ListPythonOps::to_python,
        .from_python = ListPythonOps::from_python,
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
        .to_python = SetPythonOps::to_python,
        .from_python = SetPythonOps::from_python,
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
                nb::object py_key = value_to_python(kv.key.ptr, dict_meta->key_type);
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

            std::vector<char> key_storage(dict_meta->key_type->size);
            std::vector<char> value_storage(dict_meta->value_type->size);

            for (auto item : py_dict) {
                dict_meta->key_type->construct_at(key_storage.data());
                dict_meta->value_type->construct_at(value_storage.data());

                value_from_python(key_storage.data(), item.first, dict_meta->key_type);
                value_from_python(value_storage.data(), item.second, dict_meta->value_type);

                storage->insert(key_storage.data(), value_storage.data());

                dict_meta->key_type->destruct_at(key_storage.data());
                dict_meta->value_type->destruct_at(value_storage.data());
            }
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
        .to_python = DictPythonOps::to_python,
        .from_python = DictPythonOps::from_python,
    };

    // ========================================================================
    // Window Type Python Conversions
    // ========================================================================

    struct WindowPythonOps {
        static void* to_python(const void* v, const TypeMeta* meta) {
            auto* window_meta = static_cast<const WindowTypeMeta*>(meta);
            auto* storage = static_cast<const WindowStorage*>(v);
            nb::list result;

            for (size_t i = 0; i < storage->size(); ++i) {
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
            nb::list py_list = nb::cast<nb::list>(h);

            storage->clear();

            std::vector<char> elem_storage(window_meta->element_type->size);

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
        .to_python = WindowPythonOps::to_python,
        .from_python = WindowPythonOps::from_python,
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
        .to_python = RefPythonOps::to_python,
        .from_python = RefPythonOps::from_python,
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

            meta->size = sizeof(DictStorage);
            meta->alignment = alignof(DictStorage);
            meta->flags = flags;
            meta->kind = TypeKind::Dict;
            meta->ops = &DictTypeOpsWithPython;  // With Python support
            meta->type_info = nullptr;
            meta->name = type_name;
            meta->key_type = _key_type;
            meta->value_type = _value_type;

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

} // namespace hgraph::value

#endif // HGRAPH_VALUE_PYTHON_CONVERSION_H
