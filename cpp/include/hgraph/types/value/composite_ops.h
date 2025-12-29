#pragma once

/**
 * @file composite_ops.h
 * @brief TypeOps implementations for composite types (Bundle, Tuple, List, Set, Map).
 *
 * Each composite type needs its own operations implementation that handles
 * construction, destruction, copying, Python interop, and type-specific
 * operations like field access (Bundle) or element access (List).
 */

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/value_view.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include <cstring>
#include <stdexcept>
#include <string>

namespace nb = nanobind;

namespace hgraph::value {

// ============================================================================
// Bundle Operations
// ============================================================================

/**
 * @brief Operations for Bundle types (struct-like named field collections).
 *
 * Bundles store their fields contiguously in memory, laid out according to
 * the field offsets in BundleFieldInfo. Each field can be accessed by name
 * or index.
 */
struct BundleOps {
    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        // Construct each field using its type's ops
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* field_ptr = static_cast<char*>(dst) + field.offset;
            if (field.type && field.type->ops && field.type->ops->construct) {
                field.type->ops->construct(field_ptr, field.type);
            }
        }
    }

    static void destruct(void* obj, const TypeMeta* schema) {
        // Destruct each field using its type's ops
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* field_ptr = static_cast<char*>(obj) + field.offset;
            if (field.type && field.type->ops && field.type->ops->destruct) {
                field.type->ops->destruct(field_ptr, field.type);
            }
        }
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        // Copy each field using its type's ops
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* dst_field = static_cast<char*>(dst) + field.offset;
            const void* src_field = static_cast<const char*>(src) + field.offset;
            if (field.type && field.type->ops && field.type->ops->copy_assign) {
                field.type->ops->copy_assign(dst_field, src_field, field.type);
            }
        }
    }

    static void move_assign(void* dst, void* src, const TypeMeta* schema) {
        // Move each field using its type's ops
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* dst_field = static_cast<char*>(dst) + field.offset;
            void* src_field = static_cast<char*>(src) + field.offset;
            if (field.type && field.type->ops && field.type->ops->move_assign) {
                field.type->ops->move_assign(dst_field, src_field, field.type);
            }
        }
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        // All fields must be equal
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            const void* a_field = static_cast<const char*>(a) + field.offset;
            const void* b_field = static_cast<const char*>(b) + field.offset;
            if (field.type && field.type->ops && field.type->ops->equals) {
                if (!field.type->ops->equals(a_field, b_field, field.type)) {
                    return false;
                }
            }
        }
        return true;
    }

    static std::string to_string(const void* obj, const TypeMeta* schema) {
        std::string result = "{";
        for (size_t i = 0; i < schema->field_count; ++i) {
            if (i > 0) result += ", ";
            const BundleFieldInfo& field = schema->fields[i];
            const void* field_ptr = static_cast<const char*>(obj) + field.offset;
            result += field.name ? field.name : "";
            result += ": ";
            if (field.type && field.type->ops && field.type->ops->to_string) {
                result += field.type->ops->to_string(field_ptr, field.type);
            } else {
                result += "<null>";
            }
        }
        result += "}";
        return result;
    }

    // ========== Python Interop ==========

    static nb::object to_python(const void* obj, const TypeMeta* schema) {
        // Convert to a Python dict with field names as keys
        nb::dict result;
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            const void* field_ptr = static_cast<const char*>(obj) + field.offset;
            if (field.type && field.type->ops && field.type->ops->to_python && field.name) {
                result[field.name] = field.type->ops->to_python(field_ptr, field.type);
            }
        }
        return result;
    }

    static void from_python(void* dst, const nb::object& src, const TypeMeta* schema) {
        // Expect a dict with field names as keys
        if (!nb::isinstance<nb::dict>(src)) {
            throw std::runtime_error("Bundle.from_python expects a dict");
        }
        nb::dict d = nb::cast<nb::dict>(src);

        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* field_ptr = static_cast<char*>(dst) + field.offset;
            if (field.name && d.contains(field.name)) {
                if (field.type && field.type->ops && field.type->ops->from_python) {
                    field.type->ops->from_python(field_ptr, d[field.name], field.type);
                }
            }
        }
    }

    // ========== Hashable Operations ==========

    static size_t hash(const void* obj, const TypeMeta* schema) {
        // Combine hashes of all fields
        size_t result = 0;
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            const void* field_ptr = static_cast<const char*>(obj) + field.offset;
            if (field.type && field.type->ops && field.type->ops->hash) {
                size_t field_hash = field.type->ops->hash(field_ptr, field.type);
                // Combine with XOR and rotation
                result ^= field_hash + 0x9e3779b9 + (result << 6) + (result >> 2);
            }
        }
        return result;
    }

    // ========== Iterable Operations ==========

    static size_t size(const void* /*obj*/, const TypeMeta* schema) {
        return schema->field_count;
    }

    // ========== Indexable Operations ==========

    static const void* get_at(const void* obj, size_t index, const TypeMeta* schema) {
        if (index >= schema->field_count) {
            throw std::out_of_range("Bundle field index out of range");
        }
        return static_cast<const char*>(obj) + schema->fields[index].offset;
    }

    static void set_at(void* obj, size_t index, const void* value, const TypeMeta* schema) {
        if (index >= schema->field_count) {
            throw std::out_of_range("Bundle field index out of range");
        }
        const BundleFieldInfo& field = schema->fields[index];
        void* field_ptr = static_cast<char*>(obj) + field.offset;
        if (field.type && field.type->ops && field.type->ops->copy_assign) {
            field.type->ops->copy_assign(field_ptr, value, field.type);
        }
    }

    // ========== Bundle-specific Operations ==========

    static const void* get_field(const void* obj, const char* name, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            if (schema->fields[i].name && std::strcmp(schema->fields[i].name, name) == 0) {
                return static_cast<const char*>(obj) + schema->fields[i].offset;
            }
        }
        throw std::out_of_range(std::string("Bundle has no field named '") + name + "'");
    }

    static void set_field(void* obj, const char* name, const void* value, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            if (schema->fields[i].name && std::strcmp(schema->fields[i].name, name) == 0) {
                const BundleFieldInfo& field = schema->fields[i];
                void* field_ptr = static_cast<char*>(obj) + field.offset;
                if (field.type && field.type->ops && field.type->ops->copy_assign) {
                    field.type->ops->copy_assign(field_ptr, value, field.type);
                }
                return;
            }
        }
        throw std::out_of_range(std::string("Bundle has no field named '") + name + "'");
    }

    /// Get the operations vtable for bundles
    static const TypeOps* ops() {
        static const TypeOps bundle_ops = {
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            &hash,
            nullptr,   // less_than (bundles not ordered)
            &size,
            &get_at,
            &set_at,
            &get_field,
            &set_field,
            nullptr,   // contains (not a set)
            nullptr,   // insert (not a set)
            nullptr,   // erase (not a set)
            nullptr,   // map_get (not a map)
            nullptr,   // map_set (not a map)
            nullptr,   // resize (not resizable)
            nullptr,   // clear (not clearable)
        };
        return &bundle_ops;
    }
};

// ============================================================================
// Tuple Operations
// ============================================================================

/**
 * @brief Operations for Tuple types (heterogeneous indexed collections).
 *
 * Tuples are like bundles but without field names - access is by index only.
 * The layout is identical to Bundle, using BundleFieldInfo with nullptr names.
 */
struct TupleOps {
    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* field_ptr = static_cast<char*>(dst) + field.offset;
            if (field.type && field.type->ops && field.type->ops->construct) {
                field.type->ops->construct(field_ptr, field.type);
            }
        }
    }

    static void destruct(void* obj, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* field_ptr = static_cast<char*>(obj) + field.offset;
            if (field.type && field.type->ops && field.type->ops->destruct) {
                field.type->ops->destruct(field_ptr, field.type);
            }
        }
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* dst_field = static_cast<char*>(dst) + field.offset;
            const void* src_field = static_cast<const char*>(src) + field.offset;
            if (field.type && field.type->ops && field.type->ops->copy_assign) {
                field.type->ops->copy_assign(dst_field, src_field, field.type);
            }
        }
    }

    static void move_assign(void* dst, void* src, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* dst_field = static_cast<char*>(dst) + field.offset;
            void* src_field = static_cast<char*>(src) + field.offset;
            if (field.type && field.type->ops && field.type->ops->move_assign) {
                field.type->ops->move_assign(dst_field, src_field, field.type);
            }
        }
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            const void* a_field = static_cast<const char*>(a) + field.offset;
            const void* b_field = static_cast<const char*>(b) + field.offset;
            if (field.type && field.type->ops && field.type->ops->equals) {
                if (!field.type->ops->equals(a_field, b_field, field.type)) {
                    return false;
                }
            }
        }
        return true;
    }

    static std::string to_string(const void* obj, const TypeMeta* schema) {
        std::string result = "(";
        for (size_t i = 0; i < schema->field_count; ++i) {
            if (i > 0) result += ", ";
            const BundleFieldInfo& field = schema->fields[i];
            const void* field_ptr = static_cast<const char*>(obj) + field.offset;
            if (field.type && field.type->ops && field.type->ops->to_string) {
                result += field.type->ops->to_string(field_ptr, field.type);
            } else {
                result += "<null>";
            }
        }
        result += ")";
        return result;
    }

    // ========== Python Interop ==========

    static nb::object to_python(const void* obj, const TypeMeta* schema) {
        nb::list result;
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            const void* field_ptr = static_cast<const char*>(obj) + field.offset;
            if (field.type && field.type->ops && field.type->ops->to_python) {
                result.append(field.type->ops->to_python(field_ptr, field.type));
            } else {
                result.append(nb::none());
            }
        }
        return nb::tuple(result);
    }

    static void from_python(void* dst, const nb::object& src, const TypeMeta* schema) {
        if (!nb::isinstance<nb::tuple>(src) && !nb::isinstance<nb::list>(src)) {
            throw std::runtime_error("Tuple.from_python expects a tuple or list");
        }

        nb::sequence seq = nb::cast<nb::sequence>(src);
        size_t src_len = nb::len(seq);

        for (size_t i = 0; i < schema->field_count && i < src_len; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* field_ptr = static_cast<char*>(dst) + field.offset;
            if (field.type && field.type->ops && field.type->ops->from_python) {
                field.type->ops->from_python(field_ptr, seq[i], field.type);
            }
        }
    }

    // ========== Hashable Operations ==========

    static size_t hash(const void* obj, const TypeMeta* schema) {
        size_t result = 0;
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            const void* field_ptr = static_cast<const char*>(obj) + field.offset;
            if (field.type && field.type->ops && field.type->ops->hash) {
                size_t field_hash = field.type->ops->hash(field_ptr, field.type);
                result ^= field_hash + 0x9e3779b9 + (result << 6) + (result >> 2);
            }
        }
        return result;
    }

    // ========== Iterable Operations ==========

    static size_t size(const void* /*obj*/, const TypeMeta* schema) {
        return schema->field_count;
    }

    // ========== Indexable Operations ==========

    static const void* get_at(const void* obj, size_t index, const TypeMeta* schema) {
        if (index >= schema->field_count) {
            throw std::out_of_range("Tuple element index out of range");
        }
        return static_cast<const char*>(obj) + schema->fields[index].offset;
    }

    static void set_at(void* obj, size_t index, const void* value, const TypeMeta* schema) {
        if (index >= schema->field_count) {
            throw std::out_of_range("Tuple element index out of range");
        }
        const BundleFieldInfo& field = schema->fields[index];
        void* field_ptr = static_cast<char*>(obj) + field.offset;
        if (field.type && field.type->ops && field.type->ops->copy_assign) {
            field.type->ops->copy_assign(field_ptr, value, field.type);
        }
    }

    /// Get the operations vtable for tuples
    static const TypeOps* ops() {
        static const TypeOps tuple_ops = {
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            &hash,
            nullptr,   // less_than (tuples not ordered)
            &size,
            &get_at,
            &set_at,
            nullptr,   // get_field (not a bundle)
            nullptr,   // set_field (not a bundle)
            nullptr,   // contains (not a set)
            nullptr,   // insert (not a set)
            nullptr,   // erase (not a set)
            nullptr,   // map_get (not a map)
            nullptr,   // map_set (not a map)
            nullptr,   // resize (not resizable)
            nullptr,   // clear (not clearable)
        };
        return &tuple_ops;
    }
};

// ============================================================================
// List Operations
// ============================================================================

/**
 * @brief Storage structure for dynamic (variable-size) lists.
 *
 * This is the inline storage for dynamic list Values. The actual element
 * data is stored in a separately allocated buffer pointed to by `data`.
 */
struct DynamicListStorage {
    void* data{nullptr};       // Pointer to element array
    size_t size{0};            // Current number of elements
    size_t capacity{0};        // Allocated capacity

    DynamicListStorage() = default;

    DynamicListStorage(const DynamicListStorage&) = delete;
    DynamicListStorage& operator=(const DynamicListStorage&) = delete;
};

/**
 * @brief Operations for List types (homogeneous indexed collections).
 *
 * Lists come in two variants:
 * - Fixed-size: Elements stored inline, size determined at type creation
 * - Dynamic: Elements stored in separately allocated buffer that can grow
 *
 * The schema->fixed_size field determines which variant:
 * - fixed_size > 0: Fixed-size list with inline storage
 * - fixed_size == 0: Dynamic list with DynamicListStorage
 */
struct ListOps {
    // ========== Helper Functions ==========

    static bool is_fixed(const TypeMeta* schema) {
        return schema->fixed_size > 0;
    }

    static size_t get_element_size(const TypeMeta* schema) {
        return schema->element_type ? schema->element_type->size : 0;
    }

    static void* get_element_ptr(void* obj, size_t index, const TypeMeta* schema) {
        if (is_fixed(schema)) {
            // Fixed list: elements stored inline
            size_t elem_size = get_element_size(schema);
            return static_cast<char*>(obj) + index * elem_size;
        } else {
            // Dynamic list: elements in separate buffer
            auto* storage = static_cast<DynamicListStorage*>(obj);
            size_t elem_size = get_element_size(schema);
            return static_cast<char*>(storage->data) + index * elem_size;
        }
    }

    static const void* get_element_ptr_const(const void* obj, size_t index, const TypeMeta* schema) {
        if (is_fixed(schema)) {
            size_t elem_size = get_element_size(schema);
            return static_cast<const char*>(obj) + index * elem_size;
        } else {
            auto* storage = static_cast<const DynamicListStorage*>(obj);
            size_t elem_size = get_element_size(schema);
            return static_cast<const char*>(storage->data) + index * elem_size;
        }
    }

    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        if (is_fixed(schema)) {
            // Fixed list: construct all elements inline
            const TypeMeta* elem_type = schema->element_type;
            for (size_t i = 0; i < schema->fixed_size; ++i) {
                void* elem_ptr = get_element_ptr(dst, i, schema);
                if (elem_type && elem_type->ops && elem_type->ops->construct) {
                    elem_type->ops->construct(elem_ptr, elem_type);
                }
            }
        } else {
            // Dynamic list: initialize empty storage
            new (dst) DynamicListStorage();
        }
    }

    static void destruct(void* obj, const TypeMeta* schema) {
        const TypeMeta* elem_type = schema->element_type;

        if (is_fixed(schema)) {
            // Fixed list: destruct all elements
            for (size_t i = 0; i < schema->fixed_size; ++i) {
                void* elem_ptr = get_element_ptr(obj, i, schema);
                if (elem_type && elem_type->ops && elem_type->ops->destruct) {
                    elem_type->ops->destruct(elem_ptr, elem_type);
                }
            }
        } else {
            // Dynamic list: destruct elements and free buffer
            auto* storage = static_cast<DynamicListStorage*>(obj);
            if (storage->data && elem_type) {
                for (size_t i = 0; i < storage->size; ++i) {
                    void* elem_ptr = static_cast<char*>(storage->data) + i * elem_type->size;
                    if (elem_type->ops && elem_type->ops->destruct) {
                        elem_type->ops->destruct(elem_ptr, elem_type);
                    }
                }
                std::free(storage->data);
            }
            storage->~DynamicListStorage();
        }
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        const TypeMeta* elem_type = schema->element_type;

        if (is_fixed(schema)) {
            // Fixed list: copy all elements
            for (size_t i = 0; i < schema->fixed_size; ++i) {
                void* dst_elem = get_element_ptr(dst, i, schema);
                const void* src_elem = get_element_ptr_const(src, i, schema);
                if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
                    elem_type->ops->copy_assign(dst_elem, src_elem, elem_type);
                }
            }
        } else {
            // Dynamic list: resize and copy
            auto* dst_storage = static_cast<DynamicListStorage*>(dst);
            auto* src_storage = static_cast<const DynamicListStorage*>(src);

            // Resize destination to match source
            do_resize(dst, src_storage->size, schema);

            // Copy elements
            if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
                for (size_t i = 0; i < src_storage->size; ++i) {
                    void* dst_elem = static_cast<char*>(dst_storage->data) + i * elem_type->size;
                    const void* src_elem = static_cast<const char*>(src_storage->data) + i * elem_type->size;
                    elem_type->ops->copy_assign(dst_elem, src_elem, elem_type);
                }
            }
        }
    }

    static void move_assign(void* dst, void* src, const TypeMeta* schema) {
        if (is_fixed(schema)) {
            // Fixed list: move all elements
            const TypeMeta* elem_type = schema->element_type;
            for (size_t i = 0; i < schema->fixed_size; ++i) {
                void* dst_elem = get_element_ptr(dst, i, schema);
                void* src_elem = get_element_ptr(src, i, schema);
                if (elem_type && elem_type->ops && elem_type->ops->move_assign) {
                    elem_type->ops->move_assign(dst_elem, src_elem, elem_type);
                }
            }
        } else {
            // Dynamic list: swap storage
            auto* dst_storage = static_cast<DynamicListStorage*>(dst);
            auto* src_storage = static_cast<DynamicListStorage*>(src);

            // First destruct dst elements
            destruct(dst, schema);

            // Move ownership
            dst_storage->data = src_storage->data;
            dst_storage->size = src_storage->size;
            dst_storage->capacity = src_storage->capacity;

            // Reset source
            src_storage->data = nullptr;
            src_storage->size = 0;
            src_storage->capacity = 0;
        }
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        const TypeMeta* elem_type = schema->element_type;
        size_t size_a = size(a, schema);
        size_t size_b = size(b, schema);

        if (size_a != size_b) return false;

        for (size_t i = 0; i < size_a; ++i) {
            const void* elem_a = get_element_ptr_const(a, i, schema);
            const void* elem_b = get_element_ptr_const(b, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->equals) {
                if (!elem_type->ops->equals(elem_a, elem_b, elem_type)) {
                    return false;
                }
            }
        }
        return true;
    }

    static std::string to_string(const void* obj, const TypeMeta* schema) {
        const TypeMeta* elem_type = schema->element_type;
        std::string result = "[";
        size_t n = size(obj, schema);

        for (size_t i = 0; i < n; ++i) {
            if (i > 0) result += ", ";
            const void* elem_ptr = get_element_ptr_const(obj, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->to_string) {
                result += elem_type->ops->to_string(elem_ptr, elem_type);
            } else {
                result += "<null>";
            }
        }
        result += "]";
        return result;
    }

    // ========== Python Interop ==========

    static nb::object to_python(const void* obj, const TypeMeta* schema) {
        const TypeMeta* elem_type = schema->element_type;
        nb::list result;
        size_t n = size(obj, schema);

        for (size_t i = 0; i < n; ++i) {
            const void* elem_ptr = get_element_ptr_const(obj, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->to_python) {
                result.append(elem_type->ops->to_python(elem_ptr, elem_type));
            } else {
                result.append(nb::none());
            }
        }
        return result;
    }

    static void from_python(void* dst, const nb::object& src, const TypeMeta* schema) {
        if (!nb::isinstance<nb::list>(src) && !nb::isinstance<nb::tuple>(src)) {
            throw std::runtime_error("List.from_python expects a list or tuple");
        }

        const TypeMeta* elem_type = schema->element_type;
        nb::sequence seq = nb::cast<nb::sequence>(src);
        size_t src_len = nb::len(seq);

        if (is_fixed(schema)) {
            // Fixed list: copy up to fixed_size elements
            size_t copy_count = std::min(src_len, schema->fixed_size);
            for (size_t i = 0; i < copy_count; ++i) {
                void* elem_ptr = get_element_ptr(dst, i, schema);
                if (elem_type && elem_type->ops && elem_type->ops->from_python) {
                    elem_type->ops->from_python(elem_ptr, seq[i], elem_type);
                }
            }
        } else {
            // Dynamic list: resize and populate
            do_resize(dst, src_len, schema);
            auto* storage = static_cast<DynamicListStorage*>(dst);
            for (size_t i = 0; i < src_len; ++i) {
                void* elem_ptr = static_cast<char*>(storage->data) + i * elem_type->size;
                if (elem_type && elem_type->ops && elem_type->ops->from_python) {
                    elem_type->ops->from_python(elem_ptr, seq[i], elem_type);
                }
            }
        }
    }

    // ========== Hashable Operations ==========

    static size_t hash(const void* obj, const TypeMeta* schema) {
        const TypeMeta* elem_type = schema->element_type;
        size_t result = 0;
        size_t n = size(obj, schema);

        for (size_t i = 0; i < n; ++i) {
            const void* elem_ptr = get_element_ptr_const(obj, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->hash) {
                size_t elem_hash = elem_type->ops->hash(elem_ptr, elem_type);
                result ^= elem_hash + 0x9e3779b9 + (result << 6) + (result >> 2);
            }
        }
        return result;
    }

    // ========== Iterable Operations ==========

    static size_t size(const void* obj, const TypeMeta* schema) {
        if (is_fixed(schema)) {
            return schema->fixed_size;
        } else {
            auto* storage = static_cast<const DynamicListStorage*>(obj);
            return storage->size;
        }
    }

    // ========== Indexable Operations ==========

    static const void* get_at(const void* obj, size_t index, const TypeMeta* schema) {
        size_t n = size(obj, schema);
        if (index >= n) {
            throw std::out_of_range("List index out of range");
        }
        return get_element_ptr_const(obj, index, schema);
    }

    static void set_at(void* obj, size_t index, const void* value, const TypeMeta* schema) {
        size_t n = size(obj, schema);
        if (index >= n) {
            throw std::out_of_range("List index out of range");
        }
        void* elem_ptr = get_element_ptr(obj, index, schema);
        const TypeMeta* elem_type = schema->element_type;
        if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
            elem_type->ops->copy_assign(elem_ptr, value, elem_type);
        }
    }

    // ========== Dynamic List Operations ==========

    static void do_resize(void* obj, size_t new_size, const TypeMeta* schema) {
        if (is_fixed(schema)) {
            throw std::runtime_error("Cannot resize fixed-size list");
        }

        auto* storage = static_cast<DynamicListStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;

        if (new_size == storage->size) return;

        if (new_size < storage->size) {
            // Shrinking: destruct excess elements
            for (size_t i = new_size; i < storage->size; ++i) {
                void* elem_ptr = static_cast<char*>(storage->data) + i * elem_size;
                if (elem_type && elem_type->ops && elem_type->ops->destruct) {
                    elem_type->ops->destruct(elem_ptr, elem_type);
                }
            }
            storage->size = new_size;
        } else {
            // Growing: may need to reallocate
            if (new_size > storage->capacity) {
                // Calculate new capacity (at least double, or new_size)
                size_t new_capacity = std::max(storage->capacity * 2, new_size);
                void* new_data = std::malloc(new_capacity * elem_size);

                if (!new_data) {
                    throw std::bad_alloc();
                }

                // Move existing elements to new buffer
                if (storage->data && elem_type) {
                    for (size_t i = 0; i < storage->size; ++i) {
                        void* dst_elem = static_cast<char*>(new_data) + i * elem_size;
                        void* src_elem = static_cast<char*>(storage->data) + i * elem_size;
                        if (elem_type->ops && elem_type->ops->construct) {
                            elem_type->ops->construct(dst_elem, elem_type);
                        }
                        if (elem_type->ops && elem_type->ops->move_assign) {
                            elem_type->ops->move_assign(dst_elem, src_elem, elem_type);
                        }
                        if (elem_type->ops && elem_type->ops->destruct) {
                            elem_type->ops->destruct(src_elem, elem_type);
                        }
                    }
                    std::free(storage->data);
                }

                storage->data = new_data;
                storage->capacity = new_capacity;
            }

            // Construct new elements
            for (size_t i = storage->size; i < new_size; ++i) {
                void* elem_ptr = static_cast<char*>(storage->data) + i * elem_size;
                if (elem_type && elem_type->ops && elem_type->ops->construct) {
                    elem_type->ops->construct(elem_ptr, elem_type);
                }
            }
            storage->size = new_size;
        }
    }

    static void resize(void* obj, size_t new_size, const TypeMeta* schema) {
        do_resize(obj, new_size, schema);
    }

    static void clear(void* obj, const TypeMeta* schema) {
        if (is_fixed(schema)) {
            throw std::runtime_error("Cannot clear fixed-size list");
        }
        do_resize(obj, 0, schema);
    }

    /// Get the operations vtable for lists
    static const TypeOps* ops() {
        static const TypeOps list_ops = {
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            &hash,
            nullptr,   // less_than (lists not ordered)
            &size,
            &get_at,
            &set_at,
            nullptr,   // get_field (not a bundle)
            nullptr,   // set_field (not a bundle)
            nullptr,   // contains (not a set)
            nullptr,   // insert (not a set)
            nullptr,   // erase (not a set)
            nullptr,   // map_get (not a map)
            nullptr,   // map_set (not a map)
            &resize,
            &clear,
        };
        return &list_ops;
    }
};

// ============================================================================
// Set Operations
// ============================================================================

/**
 * @brief Storage structure for sets.
 *
 * Sets use a simple dynamic array with linear search for contains/insert/erase.
 * For small sets this is efficient enough. A more sophisticated implementation
 * would use proper hash tables.
 */
struct SetStorage {
    void* data{nullptr};       // Pointer to element array
    size_t size{0};            // Current number of elements
    size_t capacity{0};        // Allocated capacity

    SetStorage() = default;
    SetStorage(const SetStorage&) = delete;
    SetStorage& operator=(const SetStorage&) = delete;
};

/**
 * @brief Operations for Set types (collections of unique elements).
 *
 * Sets store unique elements. Duplicates are not allowed.
 * Elements must be comparable via equals().
 */
struct SetOps {
    // ========== Helper Functions ==========

    static size_t get_element_size(const TypeMeta* schema) {
        return schema->element_type ? schema->element_type->size : 0;
    }

    static void* get_element_ptr(SetStorage* storage, size_t index, const TypeMeta* schema) {
        size_t elem_size = get_element_size(schema);
        return static_cast<char*>(storage->data) + index * elem_size;
    }

    static const void* get_element_ptr_const(const SetStorage* storage, size_t index, const TypeMeta* schema) {
        size_t elem_size = get_element_size(schema);
        return static_cast<const char*>(storage->data) + index * elem_size;
    }

    // Find element in set, returns index or size if not found
    static size_t find_element(const SetStorage* storage, const void* value, const TypeMeta* schema) {
        const TypeMeta* elem_type = schema->element_type;
        if (!elem_type || !elem_type->ops || !elem_type->ops->equals) {
            return storage->size;
        }

        for (size_t i = 0; i < storage->size; ++i) {
            const void* elem = get_element_ptr_const(storage, i, schema);
            if (elem_type->ops->equals(elem, value, elem_type)) {
                return i;
            }
        }
        return storage->size;
    }

    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* /*schema*/) {
        new (dst) SetStorage();
    }

    static void destruct(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<SetStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;

        if (storage->data && elem_type) {
            for (size_t i = 0; i < storage->size; ++i) {
                void* elem_ptr = get_element_ptr(storage, i, schema);
                if (elem_type->ops && elem_type->ops->destruct) {
                    elem_type->ops->destruct(elem_ptr, elem_type);
                }
            }
            std::free(storage->data);
        }
        storage->~SetStorage();
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<SetStorage*>(dst);
        auto* src_storage = static_cast<const SetStorage*>(src);
        const TypeMeta* elem_type = schema->element_type;

        // Clear destination
        do_clear(dst, schema);

        // Copy elements from source
        if (elem_type && elem_type->ops) {
            for (size_t i = 0; i < src_storage->size; ++i) {
                const void* src_elem = get_element_ptr_const(src_storage, i, schema);
                do_insert(dst, src_elem, schema);
            }
        }
    }

    static void move_assign(void* dst, void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<SetStorage*>(dst);
        auto* src_storage = static_cast<SetStorage*>(src);

        // Clear destination
        destruct(dst, schema);

        // Move ownership
        dst_storage->data = src_storage->data;
        dst_storage->size = src_storage->size;
        dst_storage->capacity = src_storage->capacity;

        // Reset source
        src_storage->data = nullptr;
        src_storage->size = 0;
        src_storage->capacity = 0;
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        auto* storage_a = static_cast<const SetStorage*>(a);
        auto* storage_b = static_cast<const SetStorage*>(b);

        if (storage_a->size != storage_b->size) return false;

        // Check that all elements in a are in b
        for (size_t i = 0; i < storage_a->size; ++i) {
            const void* elem = get_element_ptr_const(storage_a, i, schema);
            if (find_element(storage_b, elem, schema) == storage_b->size) {
                return false;
            }
        }
        return true;
    }

    static std::string to_string(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const SetStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        std::string result = "{";

        for (size_t i = 0; i < storage->size; ++i) {
            if (i > 0) result += ", ";
            const void* elem_ptr = get_element_ptr_const(storage, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->to_string) {
                result += elem_type->ops->to_string(elem_ptr, elem_type);
            } else {
                result += "<null>";
            }
        }
        result += "}";
        return result;
    }

    // ========== Python Interop ==========

    static nb::object to_python(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const SetStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        nb::set result;

        for (size_t i = 0; i < storage->size; ++i) {
            const void* elem_ptr = get_element_ptr_const(storage, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->to_python) {
                result.add(elem_type->ops->to_python(elem_ptr, elem_type));
            }
        }
        return result;
    }

    static void from_python(void* dst, const nb::object& src, const TypeMeta* schema) {
        if (!nb::isinstance<nb::set>(src) && !nb::isinstance<nb::frozenset>(src) &&
            !nb::isinstance<nb::list>(src) && !nb::isinstance<nb::tuple>(src)) {
            throw std::runtime_error("Set.from_python expects a set, frozenset, list, or tuple");
        }

        const TypeMeta* elem_type = schema->element_type;

        // Clear destination
        do_clear(dst, schema);

        // Insert elements from Python
        nb::iterator it = nb::iter(src);
        while (it != nb::iterator::sentinel()) {
            // Create temp element
            std::vector<char> temp_storage(elem_type->size);
            void* temp_elem = temp_storage.data();

            if (elem_type->ops && elem_type->ops->construct) {
                elem_type->ops->construct(temp_elem, elem_type);
            }
            if (elem_type->ops && elem_type->ops->from_python) {
                nb::object item = nb::borrow<nb::object>(*it);
                elem_type->ops->from_python(temp_elem, item, elem_type);
            }

            do_insert(dst, temp_elem, schema);

            if (elem_type->ops && elem_type->ops->destruct) {
                elem_type->ops->destruct(temp_elem, elem_type);
            }

            ++it;
        }
    }

    // ========== Hashable Operations ==========

    static size_t hash(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const SetStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        size_t result = 0;

        // XOR all element hashes (order-independent)
        for (size_t i = 0; i < storage->size; ++i) {
            const void* elem_ptr = get_element_ptr_const(storage, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->hash) {
                result ^= elem_type->ops->hash(elem_ptr, elem_type);
            }
        }
        return result;
    }

    // ========== Iterable Operations ==========

    static size_t size(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const SetStorage*>(obj);
        return storage->size;
    }

    // ========== Indexable Operations (for iteration) ==========

    static const void* get_at(const void* obj, size_t index, const TypeMeta* schema) {
        auto* storage = static_cast<const SetStorage*>(obj);
        if (index >= storage->size) {
            throw std::out_of_range("Set index out of range");
        }
        return get_element_ptr_const(storage, index, schema);
    }

    // ========== Set-specific Operations ==========

    static bool do_contains(const void* obj, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<const SetStorage*>(obj);
        return find_element(storage, value, schema) < storage->size;
    }

    static bool contains(const void* obj, const void* value, const TypeMeta* schema) {
        return do_contains(obj, value, schema);
    }

    static bool do_insert(void* obj, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<SetStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;

        // Check if already exists
        if (find_element(storage, value, schema) < storage->size) {
            return false;  // Already exists
        }

        // Grow if needed
        if (storage->size >= storage->capacity) {
            size_t new_capacity = std::max(storage->capacity * 2, size_t(8));
            size_t elem_size = get_element_size(schema);
            void* new_data = std::malloc(new_capacity * elem_size);

            if (!new_data) {
                throw std::bad_alloc();
            }

            // Move existing elements
            if (storage->data && elem_type) {
                for (size_t i = 0; i < storage->size; ++i) {
                    void* dst_elem = static_cast<char*>(new_data) + i * elem_size;
                    void* src_elem = get_element_ptr(storage, i, schema);
                    if (elem_type->ops && elem_type->ops->construct) {
                        elem_type->ops->construct(dst_elem, elem_type);
                    }
                    if (elem_type->ops && elem_type->ops->move_assign) {
                        elem_type->ops->move_assign(dst_elem, src_elem, elem_type);
                    }
                    if (elem_type->ops && elem_type->ops->destruct) {
                        elem_type->ops->destruct(src_elem, elem_type);
                    }
                }
                std::free(storage->data);
            }

            storage->data = new_data;
            storage->capacity = new_capacity;
        }

        // Add new element
        void* new_elem = get_element_ptr(storage, storage->size, schema);
        if (elem_type && elem_type->ops) {
            if (elem_type->ops->construct) {
                elem_type->ops->construct(new_elem, elem_type);
            }
            if (elem_type->ops->copy_assign) {
                elem_type->ops->copy_assign(new_elem, value, elem_type);
            }
        }
        storage->size++;
        return true;
    }

    static void insert(void* obj, const void* value, const TypeMeta* schema) {
        do_insert(obj, value, schema);
    }

    static bool do_erase(void* obj, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<SetStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;

        size_t index = find_element(storage, value, schema);
        if (index >= storage->size) {
            return false;  // Not found
        }

        // Destruct the element
        void* elem = get_element_ptr(storage, index, schema);
        if (elem_type && elem_type->ops && elem_type->ops->destruct) {
            elem_type->ops->destruct(elem, elem_type);
        }

        // Move last element to fill the gap (if not last)
        if (index < storage->size - 1) {
            void* last = get_element_ptr(storage, storage->size - 1, schema);
            if (elem_type && elem_type->ops) {
                if (elem_type->ops->construct) {
                    elem_type->ops->construct(elem, elem_type);
                }
                if (elem_type->ops->move_assign) {
                    elem_type->ops->move_assign(elem, last, elem_type);
                }
                if (elem_type->ops->destruct) {
                    elem_type->ops->destruct(last, elem_type);
                }
            }
        }

        storage->size--;
        return true;
    }

    static void erase(void* obj, const void* value, const TypeMeta* schema) {
        do_erase(obj, value, schema);
    }

    static void do_clear(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<SetStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;

        if (storage->data && elem_type) {
            for (size_t i = 0; i < storage->size; ++i) {
                void* elem = get_element_ptr(storage, i, schema);
                if (elem_type->ops && elem_type->ops->destruct) {
                    elem_type->ops->destruct(elem, elem_type);
                }
            }
        }
        storage->size = 0;
    }

    static void clear(void* obj, const TypeMeta* schema) {
        do_clear(obj, schema);
    }

    /// Get the operations vtable for sets
    static const TypeOps* ops() {
        static const TypeOps set_ops = {
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            &hash,
            nullptr,   // less_than (sets not ordered)
            &size,
            &get_at,
            nullptr,   // set_at (not used for sets)
            nullptr,   // get_field (not a bundle)
            nullptr,   // set_field (not a bundle)
            &contains,
            &insert,
            &erase,
            nullptr,   // map_get (not a map)
            nullptr,   // map_set (not a map)
            nullptr,   // resize (use insert/erase)
            &clear,
        };
        return &set_ops;
    }
};

// ============================================================================
// Map Operations
// ============================================================================

/**
 * @brief Storage structure for maps.
 *
 * Maps use a simple dynamic array with linear search for get/set.
 * Key-value pairs are stored contiguously: [key1, val1, key2, val2, ...]
 */
struct MapStorage {
    void* data{nullptr};       // Pointer to key-value array
    size_t size{0};            // Current number of key-value pairs
    size_t capacity{0};        // Allocated capacity (pairs)

    MapStorage() = default;
    MapStorage(const MapStorage&) = delete;
    MapStorage& operator=(const MapStorage&) = delete;
};

/**
 * @brief Operations for Map types (key-value collections).
 */
struct MapOps {
    // ========== Helper Functions ==========

    static size_t get_key_size(const TypeMeta* schema) {
        return schema->key_type ? schema->key_type->size : 0;
    }

    static size_t get_value_size(const TypeMeta* schema) {
        return schema->element_type ? schema->element_type->size : 0;
    }

    static size_t get_pair_size(const TypeMeta* schema) {
        return get_key_size(schema) + get_value_size(schema);
    }

    static void* get_key_ptr(MapStorage* storage, size_t index, const TypeMeta* schema) {
        size_t pair_size = get_pair_size(schema);
        return static_cast<char*>(storage->data) + index * pair_size;
    }

    static void* get_value_ptr(MapStorage* storage, size_t index, const TypeMeta* schema) {
        size_t pair_size = get_pair_size(schema);
        size_t key_size = get_key_size(schema);
        return static_cast<char*>(storage->data) + index * pair_size + key_size;
    }

    static const void* get_key_ptr_const(const MapStorage* storage, size_t index, const TypeMeta* schema) {
        size_t pair_size = get_pair_size(schema);
        return static_cast<const char*>(storage->data) + index * pair_size;
    }

    static const void* get_value_ptr_const(const MapStorage* storage, size_t index, const TypeMeta* schema) {
        size_t pair_size = get_pair_size(schema);
        size_t key_size = get_key_size(schema);
        return static_cast<const char*>(storage->data) + index * pair_size + key_size;
    }

    // Find key in map, returns index or size if not found
    static size_t find_key(const MapStorage* storage, const void* key, const TypeMeta* schema) {
        const TypeMeta* key_type = schema->key_type;
        if (!key_type || !key_type->ops || !key_type->ops->equals) {
            return storage->size;
        }

        for (size_t i = 0; i < storage->size; ++i) {
            const void* stored_key = get_key_ptr_const(storage, i, schema);
            if (key_type->ops->equals(stored_key, key, key_type)) {
                return i;
            }
        }
        return storage->size;
    }

    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* /*schema*/) {
        new (dst) MapStorage();
    }

    static void destruct(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<MapStorage*>(obj);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;

        if (storage->data) {
            for (size_t i = 0; i < storage->size; ++i) {
                void* key_ptr = get_key_ptr(storage, i, schema);
                void* val_ptr = get_value_ptr(storage, i, schema);
                if (key_type && key_type->ops && key_type->ops->destruct) {
                    key_type->ops->destruct(key_ptr, key_type);
                }
                if (val_type && val_type->ops && val_type->ops->destruct) {
                    val_type->ops->destruct(val_ptr, val_type);
                }
            }
            std::free(storage->data);
        }
        storage->~MapStorage();
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        auto* src_storage = static_cast<const MapStorage*>(src);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;

        // Clear destination
        do_clear(dst, schema);

        // Copy elements from source
        for (size_t i = 0; i < src_storage->size; ++i) {
            const void* src_key = get_key_ptr_const(src_storage, i, schema);
            const void* src_val = get_value_ptr_const(src_storage, i, schema);
            do_map_set(dst, src_key, src_val, schema);
        }
    }

    static void move_assign(void* dst, void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<MapStorage*>(dst);
        auto* src_storage = static_cast<MapStorage*>(src);

        // Clear destination
        destruct(dst, schema);

        // Move ownership
        dst_storage->data = src_storage->data;
        dst_storage->size = src_storage->size;
        dst_storage->capacity = src_storage->capacity;

        // Reset source
        src_storage->data = nullptr;
        src_storage->size = 0;
        src_storage->capacity = 0;
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        auto* storage_a = static_cast<const MapStorage*>(a);
        auto* storage_b = static_cast<const MapStorage*>(b);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;

        if (storage_a->size != storage_b->size) return false;

        // Check that all key-value pairs in a exist in b with same value
        for (size_t i = 0; i < storage_a->size; ++i) {
            const void* key = get_key_ptr_const(storage_a, i, schema);
            const void* val_a = get_value_ptr_const(storage_a, i, schema);

            size_t idx_b = find_key(storage_b, key, schema);
            if (idx_b >= storage_b->size) {
                return false;  // Key not found
            }

            const void* val_b = get_value_ptr_const(storage_b, idx_b, schema);
            if (val_type && val_type->ops && val_type->ops->equals) {
                if (!val_type->ops->equals(val_a, val_b, val_type)) {
                    return false;
                }
            }
        }
        return true;
    }

    static std::string to_string(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const MapStorage*>(obj);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;
        std::string result = "{";

        for (size_t i = 0; i < storage->size; ++i) {
            if (i > 0) result += ", ";
            const void* key_ptr = get_key_ptr_const(storage, i, schema);
            const void* val_ptr = get_value_ptr_const(storage, i, schema);

            if (key_type && key_type->ops && key_type->ops->to_string) {
                result += key_type->ops->to_string(key_ptr, key_type);
            } else {
                result += "<key>";
            }
            result += ": ";
            if (val_type && val_type->ops && val_type->ops->to_string) {
                result += val_type->ops->to_string(val_ptr, val_type);
            } else {
                result += "<value>";
            }
        }
        result += "}";
        return result;
    }

    // ========== Python Interop ==========

    static nb::object to_python(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const MapStorage*>(obj);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;
        nb::dict result;

        for (size_t i = 0; i < storage->size; ++i) {
            const void* key_ptr = get_key_ptr_const(storage, i, schema);
            const void* val_ptr = get_value_ptr_const(storage, i, schema);

            nb::object py_key, py_val;
            if (key_type && key_type->ops && key_type->ops->to_python) {
                py_key = key_type->ops->to_python(key_ptr, key_type);
            } else {
                py_key = nb::none();
            }
            if (val_type && val_type->ops && val_type->ops->to_python) {
                py_val = val_type->ops->to_python(val_ptr, val_type);
            } else {
                py_val = nb::none();
            }

            result[py_key] = py_val;
        }
        return result;
    }

    static void from_python(void* dst, const nb::object& src, const TypeMeta* schema) {
        if (!nb::isinstance<nb::dict>(src)) {
            throw std::runtime_error("Map.from_python expects a dict");
        }

        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;

        // Clear destination
        do_clear(dst, schema);

        // Insert items from Python dict
        nb::dict d = nb::cast<nb::dict>(src);
        for (auto item : d) {
            // Create temp key
            std::vector<char> temp_key_storage(key_type->size);
            void* temp_key = temp_key_storage.data();
            if (key_type->ops && key_type->ops->construct) {
                key_type->ops->construct(temp_key, key_type);
            }
            if (key_type->ops && key_type->ops->from_python) {
                nb::object key_obj = nb::borrow<nb::object>(item.first);
                key_type->ops->from_python(temp_key, key_obj, key_type);
            }

            // Create temp value
            std::vector<char> temp_val_storage(val_type->size);
            void* temp_val = temp_val_storage.data();
            if (val_type->ops && val_type->ops->construct) {
                val_type->ops->construct(temp_val, val_type);
            }
            if (val_type->ops && val_type->ops->from_python) {
                nb::object val_obj = nb::borrow<nb::object>(item.second);
                val_type->ops->from_python(temp_val, val_obj, val_type);
            }

            do_map_set(dst, temp_key, temp_val, schema);

            if (key_type->ops && key_type->ops->destruct) {
                key_type->ops->destruct(temp_key, key_type);
            }
            if (val_type->ops && val_type->ops->destruct) {
                val_type->ops->destruct(temp_val, val_type);
            }
        }
    }

    // ========== Hashable Operations ==========

    static size_t hash(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const MapStorage*>(obj);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;
        size_t result = 0;

        // XOR all key-value pair hashes (order-independent)
        for (size_t i = 0; i < storage->size; ++i) {
            const void* key_ptr = get_key_ptr_const(storage, i, schema);
            const void* val_ptr = get_value_ptr_const(storage, i, schema);
            size_t pair_hash = 0;
            if (key_type && key_type->ops && key_type->ops->hash) {
                pair_hash ^= key_type->ops->hash(key_ptr, key_type);
            }
            if (val_type && val_type->ops && val_type->ops->hash) {
                pair_hash ^= val_type->ops->hash(val_ptr, val_type) << 1;
            }
            result ^= pair_hash;
        }
        return result;
    }

    // ========== Iterable Operations ==========

    static size_t size(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const MapStorage*>(obj);
        return storage->size;
    }

    // ========== Map-specific Operations ==========

    static const void* do_map_get(const void* obj, const void* key, const TypeMeta* schema) {
        auto* storage = static_cast<const MapStorage*>(obj);
        size_t index = find_key(storage, key, schema);
        if (index >= storage->size) {
            throw std::out_of_range("Map key not found");
        }
        return get_value_ptr_const(storage, index, schema);
    }

    static const void* map_get(const void* obj, const void* key, const TypeMeta* schema) {
        return do_map_get(obj, key, schema);
    }

    // Contains for key lookup (used by MapView::contains)
    static bool contains(const void* obj, const void* key, const TypeMeta* schema) {
        auto* storage = static_cast<const MapStorage*>(obj);
        return find_key(storage, key, schema) < storage->size;
    }

    static void do_map_set(void* obj, const void* key, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<MapStorage*>(obj);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;

        // Check if key already exists
        size_t index = find_key(storage, key, schema);
        if (index < storage->size) {
            // Update existing value
            void* val_ptr = get_value_ptr(storage, index, schema);
            if (val_type && val_type->ops && val_type->ops->copy_assign) {
                val_type->ops->copy_assign(val_ptr, value, val_type);
            }
            return;
        }

        // Grow if needed
        if (storage->size >= storage->capacity) {
            size_t new_capacity = std::max(storage->capacity * 2, size_t(8));
            size_t pair_size = get_pair_size(schema);
            void* new_data = std::malloc(new_capacity * pair_size);

            if (!new_data) {
                throw std::bad_alloc();
            }

            // Move existing pairs
            if (storage->data) {
                for (size_t i = 0; i < storage->size; ++i) {
                    void* dst_key = static_cast<char*>(new_data) + i * pair_size;
                    void* dst_val = static_cast<char*>(new_data) + i * pair_size + get_key_size(schema);
                    void* src_key = get_key_ptr(storage, i, schema);
                    void* src_val = get_value_ptr(storage, i, schema);

                    if (key_type && key_type->ops) {
                        if (key_type->ops->construct) key_type->ops->construct(dst_key, key_type);
                        if (key_type->ops->move_assign) key_type->ops->move_assign(dst_key, src_key, key_type);
                        if (key_type->ops->destruct) key_type->ops->destruct(src_key, key_type);
                    }
                    if (val_type && val_type->ops) {
                        if (val_type->ops->construct) val_type->ops->construct(dst_val, val_type);
                        if (val_type->ops->move_assign) val_type->ops->move_assign(dst_val, src_val, val_type);
                        if (val_type->ops->destruct) val_type->ops->destruct(src_val, val_type);
                    }
                }
                std::free(storage->data);
            }

            storage->data = new_data;
            storage->capacity = new_capacity;
        }

        // Add new pair
        void* new_key = get_key_ptr(storage, storage->size, schema);
        void* new_val = get_value_ptr(storage, storage->size, schema);

        if (key_type && key_type->ops) {
            if (key_type->ops->construct) key_type->ops->construct(new_key, key_type);
            if (key_type->ops->copy_assign) key_type->ops->copy_assign(new_key, key, key_type);
        }
        if (val_type && val_type->ops) {
            if (val_type->ops->construct) val_type->ops->construct(new_val, val_type);
            if (val_type->ops->copy_assign) val_type->ops->copy_assign(new_val, value, val_type);
        }
        storage->size++;
    }

    static void map_set(void* obj, const void* key, const void* value, const TypeMeta* schema) {
        do_map_set(obj, key, value, schema);
    }

    static bool do_erase(void* obj, const void* key, const TypeMeta* schema) {
        auto* storage = static_cast<MapStorage*>(obj);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;

        size_t index = find_key(storage, key, schema);
        if (index >= storage->size) {
            return false;  // Key not found
        }

        // Destruct the key-value pair at index
        void* key_ptr = get_key_ptr(storage, index, schema);
        void* val_ptr = get_value_ptr(storage, index, schema);
        if (key_type && key_type->ops && key_type->ops->destruct) {
            key_type->ops->destruct(key_ptr, key_type);
        }
        if (val_type && val_type->ops && val_type->ops->destruct) {
            val_type->ops->destruct(val_ptr, val_type);
        }

        // Move last pair to fill the gap (if not last)
        if (index < storage->size - 1) {
            void* last_key = get_key_ptr(storage, storage->size - 1, schema);
            void* last_val = get_value_ptr(storage, storage->size - 1, schema);

            if (key_type && key_type->ops) {
                if (key_type->ops->construct) key_type->ops->construct(key_ptr, key_type);
                if (key_type->ops->move_assign) key_type->ops->move_assign(key_ptr, last_key, key_type);
                if (key_type->ops->destruct) key_type->ops->destruct(last_key, key_type);
            }
            if (val_type && val_type->ops) {
                if (val_type->ops->construct) val_type->ops->construct(val_ptr, val_type);
                if (val_type->ops->move_assign) val_type->ops->move_assign(val_ptr, last_val, val_type);
                if (val_type->ops->destruct) val_type->ops->destruct(last_val, val_type);
            }
        }

        storage->size--;
        return true;
    }

    static void erase(void* obj, const void* key, const TypeMeta* schema) {
        do_erase(obj, key, schema);
    }

    static void do_clear(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<MapStorage*>(obj);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;

        if (storage->data) {
            for (size_t i = 0; i < storage->size; ++i) {
                void* key_ptr = get_key_ptr(storage, i, schema);
                void* val_ptr = get_value_ptr(storage, i, schema);
                if (key_type && key_type->ops && key_type->ops->destruct) {
                    key_type->ops->destruct(key_ptr, key_type);
                }
                if (val_type && val_type->ops && val_type->ops->destruct) {
                    val_type->ops->destruct(val_ptr, val_type);
                }
            }
        }
        storage->size = 0;
    }

    static void clear(void* obj, const TypeMeta* schema) {
        do_clear(obj, schema);
    }

    /// Get the operations vtable for maps
    static const TypeOps* ops() {
        static const TypeOps map_ops = {
            &construct,
            &destruct,
            &copy_assign,
            &move_assign,
            &equals,
            &to_string,
            &to_python,
            &from_python,
            &hash,
            nullptr,   // less_than (maps not ordered)
            &size,
            nullptr,   // get_at (not indexed)
            nullptr,   // set_at (not indexed)
            nullptr,   // get_field (not a bundle)
            nullptr,   // set_field (not a bundle)
            &contains, // contains (for key lookup)
            nullptr,   // insert (not a set)
            &erase,    // erase (remove by key)
            &map_get,
            &map_set,
            nullptr,   // resize
            &clear,
        };
        return &map_ops;
    }
};

} // namespace hgraph::value
