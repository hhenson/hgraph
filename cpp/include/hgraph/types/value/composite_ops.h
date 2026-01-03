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
#include <ankerl/unordered_dense.h>

#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

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

    static void move_construct(void* dst, void* src, const TypeMeta* schema) {
        // Move-construct each field using its type's ops
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* dst_field = static_cast<char*>(dst) + field.offset;
            void* src_field = static_cast<char*>(src) + field.offset;
            if (field.type && field.type->ops && field.type->ops->move_construct) {
                field.type->ops->move_construct(dst_field, src_field, field.type);
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
        if (nb::isinstance<nb::dict>(src)) {
            // Handle dict: field names as keys
            nb::dict d = nb::cast<nb::dict>(src);

            for (size_t i = 0; i < schema->field_count; ++i) {
                const BundleFieldInfo& field = schema->fields[i];
                void* field_ptr = static_cast<char*>(dst) + field.offset;
                if (field.name && d.contains(field.name)) {
                    if (field.type && field.type->ops && field.type->ops->from_python) {
                        nb::object val = d[field.name];
                        // Skip None values - can't cast None to non-nullable scalar types
                        if (!val.is_none()) {
                            field.type->ops->from_python(field_ptr, val, field.type);
                        }
                    }
                }
            }
        } else if (nb::isinstance<nb::tuple>(src) || nb::isinstance<nb::list>(src)) {
            // Handle tuple/list: map by index position
            // This supports tuples represented as bundles with fields $0, $1, etc.
            nb::sequence seq = nb::cast<nb::sequence>(src);
            size_t seq_len = nb::len(seq);
            size_t n = std::min(seq_len, schema->field_count);

            for (size_t i = 0; i < n; ++i) {
                const BundleFieldInfo& field = schema->fields[i];
                void* field_ptr = static_cast<char*>(dst) + field.offset;
                if (field.type && field.type->ops && field.type->ops->from_python) {
                    nb::object elem = seq[i];
                    // Skip None values - can't cast None to non-nullable scalar types
                    if (!elem.is_none()) {
                        field.type->ops->from_python(field_ptr, elem, field.type);
                    }
                }
            }
        } else {
            // Handle object with attributes (e.g., dataclass, namedtuple, custom objects)
            // Extract attributes by field names using getattr
            for (size_t i = 0; i < schema->field_count; ++i) {
                const BundleFieldInfo& field = schema->fields[i];
                void* field_ptr = static_cast<char*>(dst) + field.offset;
                if (field.name && nb::hasattr(src, field.name)) {
                    if (field.type && field.type->ops && field.type->ops->from_python) {
                        nb::object attr = nb::getattr(src, field.name);
                        // Skip None values - can't cast None to non-nullable scalar types
                        if (!attr.is_none()) {
                            field.type->ops->from_python(field_ptr, attr, field.type);
                        }
                    }
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
            &move_construct,
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

    static void move_construct(void* dst, void* src, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* dst_field = static_cast<char*>(dst) + field.offset;
            void* src_field = static_cast<char*>(src) + field.offset;
            if (field.type && field.type->ops && field.type->ops->move_construct) {
                field.type->ops->move_construct(dst_field, src_field, field.type);
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
                nb::object elem = seq[i];
                // Skip None values - can't cast None to non-nullable scalar types
                if (!elem.is_none()) {
                    field.type->ops->from_python(field_ptr, elem, field.type);
                }
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
            &move_construct,
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
 * data is stored in a std::vector<std::byte> that manages memory automatically.
 */
struct DynamicListStorage {
    std::vector<std::byte> data;  // Element storage (capacity managed by vector)
    size_t size{0};               // Current number of valid elements

    DynamicListStorage() = default;

    // Allow move but not copy (elements need proper construction)
    DynamicListStorage(DynamicListStorage&&) noexcept = default;
    DynamicListStorage& operator=(DynamicListStorage&&) noexcept = default;
    DynamicListStorage(const DynamicListStorage&) = delete;
    DynamicListStorage& operator=(const DynamicListStorage&) = delete;

    /// Get raw pointer to element data
    [[nodiscard]] void* data_ptr() { return data.data(); }
    [[nodiscard]] const void* data_ptr() const { return data.data(); }

    /// Get capacity in bytes
    [[nodiscard]] size_t byte_capacity() const { return data.capacity(); }
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
            // Dynamic list: elements in vector storage
            auto* storage = static_cast<DynamicListStorage*>(obj);
            size_t elem_size = get_element_size(schema);
            return static_cast<char*>(storage->data_ptr()) + index * elem_size;
        }
    }

    static const void* get_element_ptr_const(const void* obj, size_t index, const TypeMeta* schema) {
        if (is_fixed(schema)) {
            size_t elem_size = get_element_size(schema);
            return static_cast<const char*>(obj) + index * elem_size;
        } else {
            auto* storage = static_cast<const DynamicListStorage*>(obj);
            size_t elem_size = get_element_size(schema);
            return static_cast<const char*>(storage->data_ptr()) + index * elem_size;
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
            // Dynamic list: destruct elements (vector handles memory cleanup)
            auto* storage = static_cast<DynamicListStorage*>(obj);
            if (!storage->data.empty() && elem_type) {
                for (size_t i = 0; i < storage->size; ++i) {
                    void* elem_ptr = static_cast<char*>(storage->data_ptr()) + i * elem_type->size;
                    if (elem_type->ops && elem_type->ops->destruct) {
                        elem_type->ops->destruct(elem_ptr, elem_type);
                    }
                }
            }
            storage->~DynamicListStorage();  // Vector destructor frees memory
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
                    void* dst_elem = static_cast<char*>(dst_storage->data_ptr()) + i * elem_type->size;
                    const void* src_elem = static_cast<const char*>(src_storage->data_ptr()) + i * elem_type->size;
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
            // Dynamic list: move storage via vector move
            auto* dst_storage = static_cast<DynamicListStorage*>(dst);
            auto* src_storage = static_cast<DynamicListStorage*>(src);

            // First destruct dst elements
            destruct(dst, schema);

            // Move vector and size
            dst_storage->data = std::move(src_storage->data);
            dst_storage->size = src_storage->size;

            // Reset source
            src_storage->size = 0;
        }
    }

    static void move_construct(void* dst, void* src, const TypeMeta* schema) {
        if (is_fixed(schema)) {
            // Fixed list: move-construct all elements
            const TypeMeta* elem_type = schema->element_type;
            for (size_t i = 0; i < schema->fixed_size; ++i) {
                void* dst_elem = get_element_ptr(dst, i, schema);
                void* src_elem = get_element_ptr(src, i, schema);
                if (elem_type && elem_type->ops && elem_type->ops->move_construct) {
                    elem_type->ops->move_construct(dst_elem, src_elem, elem_type);
                }
            }
        } else {
            // Dynamic list: placement new with move
            auto* src_storage = static_cast<DynamicListStorage*>(src);
            new (dst) DynamicListStorage(std::move(*src_storage));
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
        // Return as tuple if this is a variadic tuple (tuple[T, ...]), otherwise list
        if (schema->is_variadic_tuple()) {
            return nb::tuple(result);
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
                    nb::object elem = seq[i];
                    // Skip None values - can't cast None to non-nullable scalar types
                    if (!elem.is_none()) {
                        elem_type->ops->from_python(elem_ptr, elem, elem_type);
                    }
                }
            }
        } else {
            // Dynamic list: resize and populate
            do_resize(dst, src_len, schema);
            auto* storage = static_cast<DynamicListStorage*>(dst);
            for (size_t i = 0; i < src_len; ++i) {
                void* elem_ptr = static_cast<char*>(storage->data_ptr()) + i * elem_type->size;
                if (elem_type && elem_type->ops && elem_type->ops->from_python) {
                    nb::object elem = seq[i];
                    // Skip None values - can't cast None to non-nullable scalar types
                    if (!elem.is_none()) {
                        elem_type->ops->from_python(elem_ptr, elem, elem_type);
                    }
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
            // Shrinking: destruct excess elements (keep vector capacity)
            for (size_t i = new_size; i < storage->size; ++i) {
                void* elem_ptr = static_cast<char*>(storage->data_ptr()) + i * elem_size;
                if (elem_type && elem_type->ops && elem_type->ops->destruct) {
                    elem_type->ops->destruct(elem_ptr, elem_type);
                }
            }
            storage->size = new_size;
        } else {
            // Growing: resize vector if needed, then construct new elements
            size_t new_byte_size = new_size * elem_size;
            size_t current_byte_capacity = storage->data.capacity();

            if (new_byte_size > current_byte_capacity) {
                // Need more capacity - must properly handle non-trivially-copyable types
                size_t current_elem_capacity = elem_size > 0 ? current_byte_capacity / elem_size : 0;
                size_t new_capacity = std::max(current_elem_capacity * 2, new_size);
                size_t new_capacity_bytes = new_capacity * elem_size;

                // For non-trivially-copyable types with existing elements, we must manually move
                if (elem_type && !elem_type->is_trivially_copyable() && storage->size > 0) {
                    std::vector<std::byte> new_data(new_capacity_bytes);

                    // Move-construct existing elements to new storage
                    for (size_t i = 0; i < storage->size; i++) {
                        void* old_elem = storage->data.data() + i * elem_size;
                        void* new_elem = new_data.data() + i * elem_size;

                        if (elem_type->ops && elem_type->ops->move_construct) {
                            elem_type->ops->move_construct(new_elem, old_elem, elem_type);
                        }
                        if (elem_type->ops && elem_type->ops->destruct) {
                            elem_type->ops->destruct(old_elem, elem_type);
                        }
                    }

                    storage->data = std::move(new_data);
                } else {
                    // Trivially copyable or no existing elements - reserve is safe
                    storage->data.reserve(new_capacity_bytes);
                }
            }

            // Ensure vector has enough bytes for all elements
            if (storage->data.size() < new_byte_size) {
                storage->data.resize(new_byte_size);
            }

            // Construct new elements
            for (size_t i = storage->size; i < new_size; ++i) {
                void* elem_ptr = static_cast<char*>(storage->data_ptr()) + i * elem_size;
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
            &move_construct,
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

// Forward declaration for SetStorage
struct SetStorage;

/**
 * @brief Transparent hash functor for SetStorage.
 *
 * Enables heterogeneous lookup: can hash both indices (existing elements)
 * and raw pointers (lookup keys) without copying.
 * Uses storage->element_type directly for type information.
 */
struct SetIndexHash {
    using is_transparent = void;
    using is_avalanching = void;

    const SetStorage* storage{nullptr};

    SetIndexHash() = default;
    explicit SetIndexHash(const SetStorage* s) : storage(s) {}

    [[nodiscard]] uint64_t operator()(size_t idx) const;
    [[nodiscard]] uint64_t operator()(const void* ptr) const;
};

/**
 * @brief Transparent equality functor for SetStorage.
 *
 * Enables heterogeneous lookup: can compare indices with indices,
 * indices with pointers, and pointers with indices.
 * Uses storage->element_type directly for type information.
 */
struct SetIndexEqual {
    using is_transparent = void;

    const SetStorage* storage{nullptr};

    SetIndexEqual() = default;
    explicit SetIndexEqual(const SetStorage* s) : storage(s) {}

    [[nodiscard]] bool operator()(size_t a, size_t b) const;
    [[nodiscard]] bool operator()(size_t idx, const void* ptr) const;
    [[nodiscard]] bool operator()(const void* ptr, size_t idx) const;
};

/**
 * @brief Storage structure for sets using robin-hood hashing.
 *
 * Elements are stored contiguously in a byte vector for cache efficiency.
 * An index-based hash set provides O(1) lookup/insert/erase.
 * Uses ankerl::unordered_dense with transparent hash/equal for heterogeneous lookup.
 */
struct SetStorage {
    using IndexSet = ankerl::unordered_dense::set<size_t, SetIndexHash, SetIndexEqual>;

    std::vector<std::byte> elements;     // Contiguous element storage
    size_t element_count{0};             // Number of valid elements
    std::unique_ptr<IndexSet> index_set; // Index-based hash set
    const TypeMeta* element_type{nullptr};

    SetStorage() = default;
    SetStorage(const SetStorage&) = delete;
    SetStorage& operator=(const SetStorage&) = delete;

    // Move constructor - need to rebuild index_set with new 'this' pointer
    SetStorage(SetStorage&& other) noexcept
        : elements(std::move(other.elements))
        , element_count(other.element_count)
        , element_type(other.element_type) {
        // Rebuild index set with functors pointing to this storage
        if (other.index_set) {
            index_set = std::make_unique<IndexSet>(
                0, SetIndexHash(this), SetIndexEqual(this));
            for (size_t idx : *other.index_set) {
                index_set->insert(idx);
            }
        }
        other.element_count = 0;
        other.index_set.reset();
    }

    SetStorage& operator=(SetStorage&& other) noexcept {
        if (this != &other) {
            elements = std::move(other.elements);
            element_count = other.element_count;
            element_type = other.element_type;
            if (other.index_set) {
                index_set = std::make_unique<IndexSet>(
                    0, SetIndexHash(this), SetIndexEqual(this));
                for (size_t idx : *other.index_set) {
                    index_set->insert(idx);
                }
            } else {
                index_set.reset();
            }
            other.element_count = 0;
            other.index_set.reset();
        }
        return *this;
    }

    // Helper to get element pointer by index
    [[nodiscard]] const void* get_element_ptr(size_t idx) const {
        if (!element_type) return nullptr;
        return elements.data() + idx * element_type->size;
    }

    [[nodiscard]] void* get_element_ptr(size_t idx) {
        if (!element_type) return nullptr;
        return elements.data() + idx * element_type->size;
    }
};

/**
 * @brief Operations for Set types (collections of unique elements).
 *
 * Sets store unique elements using robin-hood hashing for O(1) operations.
 * Elements must be hashable and equatable.
 */
struct SetOps {
    // ========== Helper Functions ==========

    /**
     * @brief Safely grow the element storage, properly handling non-trivially-copyable types.
     *
     * When std::vector<std::byte> reallocates, it does a raw memcpy of bytes. This is incorrect
     * for non-trivially-copyable types like std::string. This function handles reallocation
     * by properly move-constructing elements to the new buffer.
     */
    static void grow_storage(SetStorage* storage, size_t new_count, const TypeMeta* elem_type) {
        if (!elem_type) return;

        size_t elem_size = elem_type->size;
        size_t required_bytes = new_count * elem_size;

        // Already have enough space?
        if (storage->elements.size() >= required_bytes) {
            return;
        }

        // Have enough capacity (resize won't reallocate)?
        if (storage->elements.capacity() >= required_bytes) {
            storage->elements.resize(required_bytes);
            return;
        }

        // Need to reallocate - must properly move elements for non-trivially-copyable types
        size_t old_count = storage->element_count;

        // For trivially copyable types, vector's memcpy is fine
        if (elem_type->is_trivially_copyable() || old_count == 0) {
            storage->elements.resize(required_bytes);
            return;
        }

        // Non-trivially-copyable with existing elements: manual reallocation required
        size_t new_capacity = std::max(required_bytes, storage->elements.capacity() * 2);
        std::vector<std::byte> new_storage(new_capacity);

        // Move-construct existing elements to new storage
        for (size_t i = 0; i < old_count; i++) {
            void* old_elem = storage->elements.data() + i * elem_size;
            void* new_elem = new_storage.data() + i * elem_size;

            if (elem_type->ops && elem_type->ops->move_construct) {
                elem_type->ops->move_construct(new_elem, old_elem, elem_type);
            }
            if (elem_type->ops && elem_type->ops->destruct) {
                elem_type->ops->destruct(old_elem, elem_type);
            }
        }

        // Replace storage
        storage->elements = std::move(new_storage);
    }

    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        auto* storage = new (dst) SetStorage();
        storage->element_type = schema->element_type;
        storage->index_set = std::make_unique<SetStorage::IndexSet>(
            0, SetIndexHash(storage), SetIndexEqual(storage));
    }

    static void destruct(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<SetStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;

        // Destruct all elements
        if (elem_type && elem_type->ops && elem_type->ops->destruct && storage->index_set) {
            for (size_t idx : *storage->index_set) {
                void* elem_ptr = storage->get_element_ptr(idx);
                elem_type->ops->destruct(elem_ptr, elem_type);
            }
        }
        storage->~SetStorage();
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        auto* src_storage = static_cast<const SetStorage*>(src);

        // Clear destination
        do_clear(dst, schema);

        // Copy elements from source
        if (src_storage->index_set) {
            for (size_t idx : *src_storage->index_set) {
                const void* src_elem = src_storage->get_element_ptr(idx);
                do_insert(dst, src_elem, schema);
            }
        }
    }

    static void move_assign(void* dst, void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<SetStorage*>(dst);
        auto* src_storage = static_cast<SetStorage*>(src);

        // Clear destination
        destruct(dst, schema);

        // Move via move assignment operator
        *dst_storage = std::move(*src_storage);
    }

    static void move_construct(void* dst, void* src, const TypeMeta* /*schema*/) {
        auto* src_storage = static_cast<SetStorage*>(src);
        // Placement new with move constructor
        new (dst) SetStorage(std::move(*src_storage));
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        auto* storage_a = static_cast<const SetStorage*>(a);
        auto* storage_b = static_cast<const SetStorage*>(b);

        size_t size_a = storage_a->index_set ? storage_a->index_set->size() : 0;
        size_t size_b = storage_b->index_set ? storage_b->index_set->size() : 0;

        if (size_a != size_b) return false;

        // Check that all elements in a are in b (O(n) with O(1) lookups)
        if (storage_a->index_set && storage_b->index_set) {
            for (size_t idx : *storage_a->index_set) {
                const void* elem = storage_a->get_element_ptr(idx);
                if (storage_b->index_set->find(elem) == storage_b->index_set->end()) {
                    return false;
                }
            }
        }
        return true;
    }

    static std::string to_string(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const SetStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        std::string result = "{";

        if (storage->index_set) {
            bool first = true;
            for (size_t idx : *storage->index_set) {
                if (!first) result += ", ";
                first = false;
                const void* elem_ptr = storage->get_element_ptr(idx);
                if (elem_type && elem_type->ops && elem_type->ops->to_string) {
                    result += elem_type->ops->to_string(elem_ptr, elem_type);
                } else {
                    result += "<null>";
                }
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

        if (storage->index_set) {
            for (size_t idx : *storage->index_set) {
                const void* elem_ptr = storage->get_element_ptr(idx);
                if (elem_type && elem_type->ops && elem_type->ops->to_python) {
                    result.add(elem_type->ops->to_python(elem_ptr, elem_type));
                }
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
        if (storage->index_set) {
            for (size_t idx : *storage->index_set) {
                const void* elem_ptr = storage->get_element_ptr(idx);
                if (elem_type && elem_type->ops && elem_type->ops->hash) {
                    result ^= elem_type->ops->hash(elem_ptr, elem_type);
                }
            }
        }
        return result;
    }

    // ========== Iterable Operations ==========

    static size_t size(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const SetStorage*>(obj);
        return storage->index_set ? storage->index_set->size() : 0;
    }

    // ========== Indexable Operations (for iteration) ==========

    static const void* get_at(const void* obj, size_t index, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const SetStorage*>(obj);
        if (!storage->index_set || index >= storage->index_set->size()) {
            throw std::out_of_range("Set index out of range");
        }
        // ankerl::unordered_dense::set supports random access
        auto it = storage->index_set->begin();
        std::advance(it, index);
        return storage->get_element_ptr(*it);
    }

    // ========== Set-specific Operations ==========

    static bool do_contains(const void* obj, const void* value, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const SetStorage*>(obj);
        if (!storage->index_set) return false;
        return storage->index_set->find(value) != storage->index_set->end();
    }

    static bool contains(const void* obj, const void* value, const TypeMeta* schema) {
        return do_contains(obj, value, schema);
    }

    static bool do_insert(void* obj, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<SetStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;

        if (!storage->index_set) return false;

        // Check if already exists (O(1))
        if (storage->index_set->find(value) != storage->index_set->end()) {
            return false;  // Already exists
        }

        // Add new element at element_count position
        size_t new_idx = storage->element_count;

        // Grow element storage if needed (safely handles non-trivially-copyable types)
        grow_storage(storage, new_idx + 1, elem_type);

        // Construct and copy new element
        void* new_elem = storage->get_element_ptr(new_idx);
        if (elem_type->ops) {
            if (elem_type->ops->construct) {
                elem_type->ops->construct(new_elem, elem_type);
            }
            if (elem_type->ops->copy_assign) {
                elem_type->ops->copy_assign(new_elem, value, elem_type);
            }
        }

        storage->element_count++;
        storage->index_set->insert(new_idx);
        return true;
    }

    static void insert(void* obj, const void* value, const TypeMeta* schema) {
        do_insert(obj, value, schema);
    }

    static bool do_erase(void* obj, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<SetStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;

        if (!storage->index_set) return false;

        auto it = storage->index_set->find(value);
        if (it == storage->index_set->end()) {
            return false;  // Not found
        }

        size_t idx = *it;
        size_t last_idx = storage->element_count - 1;

        // Remove the found element's index from index_set
        storage->index_set->erase(it);

        if (idx != last_idx) {
            // Swap-with-last: move last element to fill the gap
            void* slot_to_fill = storage->get_element_ptr(idx);
            void* last_elem = storage->get_element_ptr(last_idx);

            // IMPORTANT: Remove last_idx from index_set BEFORE moving data
            // After moving, both indices would have the same element value,
            // causing the equality functor to return true for both
            storage->index_set->erase(last_idx);

            // Move last element to the erased slot (overwrites the erased element)
            if (elem_type && elem_type->ops) {
                if (elem_type->ops->move_assign) {
                    elem_type->ops->move_assign(slot_to_fill, last_elem, elem_type);
                } else if (elem_type->ops->copy_assign) {
                    elem_type->ops->copy_assign(slot_to_fill, last_elem, elem_type);
                }
            }

            // Insert idx for the moved element (now that data is in place)
            storage->index_set->insert(idx);

            // Destruct the moved-from slot (last position)
            if (elem_type && elem_type->ops && elem_type->ops->destruct) {
                elem_type->ops->destruct(last_elem, elem_type);
            }
        } else {
            // Erasing the last element - just destruct it
            void* elem = storage->get_element_ptr(idx);
            if (elem_type && elem_type->ops && elem_type->ops->destruct) {
                elem_type->ops->destruct(elem, elem_type);
            }
        }

        storage->element_count--;
        return true;
    }

    static void erase(void* obj, const void* value, const TypeMeta* schema) {
        do_erase(obj, value, schema);
    }

    static void do_clear(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<SetStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;

        // Destruct all elements
        if (storage->index_set && elem_type && elem_type->ops && elem_type->ops->destruct) {
            for (size_t idx : *storage->index_set) {
                void* elem = storage->get_element_ptr(idx);
                elem_type->ops->destruct(elem, elem_type);
            }
        }

        // Clear storage
        if (storage->index_set) {
            storage->index_set->clear();
        }
        storage->elements.clear();
        storage->element_count = 0;
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
            &move_construct,
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

// ========== SetIndexHash and SetIndexEqual implementations ==========

inline uint64_t SetIndexHash::operator()(size_t idx) const {
    const void* elem = storage->get_element_ptr(idx);
    const TypeMeta* elem_type = storage->element_type;
    if (elem_type && elem_type->ops && elem_type->ops->hash) {
        return elem_type->ops->hash(elem, elem_type);
    }
    return 0;
}

inline uint64_t SetIndexHash::operator()(const void* ptr) const {
    const TypeMeta* elem_type = storage->element_type;
    if (elem_type && elem_type->ops && elem_type->ops->hash) {
        return elem_type->ops->hash(ptr, elem_type);
    }
    return 0;
}

inline bool SetIndexEqual::operator()(size_t a, size_t b) const {
    if (a == b) return true;
    const void* elem_a = storage->get_element_ptr(a);
    const void* elem_b = storage->get_element_ptr(b);
    const TypeMeta* elem_type = storage->element_type;
    if (elem_type && elem_type->ops && elem_type->ops->equals) {
        return elem_type->ops->equals(elem_a, elem_b, elem_type);
    }
    return false;
}

inline bool SetIndexEqual::operator()(size_t idx, const void* ptr) const {
    const void* elem = storage->get_element_ptr(idx);
    const TypeMeta* elem_type = storage->element_type;
    if (elem_type && elem_type->ops && elem_type->ops->equals) {
        return elem_type->ops->equals(elem, ptr, elem_type);
    }
    return false;
}

inline bool SetIndexEqual::operator()(const void* ptr, size_t idx) const {
    return (*this)(idx, ptr);
}

// ============================================================================
// Map Operations
// ============================================================================

/**
 * @brief Storage structure for maps using robin-hood hashing.
 *
 * Embeds a SetStorage for key management (index set + key storage).
 * Values are stored in a parallel contiguous byte vector.
 * This design reuses SetStorage's hash/equality functors for keys.
 */
struct MapStorage {
    SetStorage keys;                   // Key storage with index_set (reuses SetIndexHash/Equal)
    std::vector<std::byte> values;     // Parallel value storage
    const TypeMeta* value_type{nullptr};

    MapStorage() = default;
    MapStorage(const MapStorage&) = delete;
    MapStorage& operator=(const MapStorage&) = delete;

    // Move constructor - SetStorage handles its own move
    MapStorage(MapStorage&& other) noexcept
        : keys(std::move(other.keys))
        , values(std::move(other.values))
        , value_type(other.value_type) {
    }

    MapStorage& operator=(MapStorage&& other) noexcept {
        if (this != &other) {
            keys = std::move(other.keys);
            values = std::move(other.values);
            value_type = other.value_type;
        }
        return *this;
    }

    // Delegate key operations to embedded SetStorage
    [[nodiscard]] const void* get_key_ptr(size_t idx) const {
        return keys.get_element_ptr(idx);
    }

    [[nodiscard]] void* get_key_ptr(size_t idx) {
        return keys.get_element_ptr(idx);
    }

    // Helper to get value pointer by index
    [[nodiscard]] const void* get_value_ptr(size_t idx) const {
        if (!value_type) return nullptr;
        return values.data() + idx * value_type->size;
    }

    [[nodiscard]] void* get_value_ptr(size_t idx) {
        if (!value_type) return nullptr;
        return values.data() + idx * value_type->size;
    }

    // Convenience accessors
    [[nodiscard]] size_t entry_count() const { return keys.element_count; }
    [[nodiscard]] const TypeMeta* key_type() const { return keys.element_type; }
    [[nodiscard]] SetStorage::IndexSet* index_set() { return keys.index_set.get(); }
    [[nodiscard]] const SetStorage::IndexSet* index_set() const { return keys.index_set.get(); }
};

/**
 * @brief Operations for Map types (key-value collections).
 *
 * Maps store key-value pairs using robin-hood hashing for O(1) operations.
 * Keys must be hashable and equatable.
 */
struct MapOps {
    // ========== Helper Functions ==========

    /**
     * @brief Safely grow the key storage, properly handling non-trivially-copyable types.
     */
    static void grow_key_storage(MapStorage* storage, size_t new_count, const TypeMeta* key_type) {
        // Delegate to SetOps::grow_storage since keys use SetStorage
        SetOps::grow_storage(&storage->keys, new_count, key_type);
    }

    /**
     * @brief Safely grow the value storage, properly handling non-trivially-copyable types.
     */
    static void grow_value_storage(MapStorage* storage, size_t new_count, const TypeMeta* val_type) {
        if (!val_type) return;

        size_t val_size = val_type->size;
        size_t required_bytes = new_count * val_size;

        if (storage->values.size() >= required_bytes) {
            return;
        }

        if (storage->values.capacity() >= required_bytes) {
            storage->values.resize(required_bytes);
            return;
        }

        // Need to reallocate
        size_t old_count = storage->entry_count();

        if (val_type->is_trivially_copyable() || old_count == 0) {
            storage->values.resize(required_bytes);
            return;
        }

        // Non-trivially-copyable with existing elements: manual reallocation required
        size_t new_capacity = std::max(required_bytes, storage->values.capacity() * 2);
        std::vector<std::byte> new_storage(new_capacity);

        for (size_t i = 0; i < old_count; i++) {
            void* old_elem = storage->values.data() + i * val_size;
            void* new_elem = new_storage.data() + i * val_size;

            if (val_type->ops && val_type->ops->move_construct) {
                val_type->ops->move_construct(new_elem, old_elem, val_type);
            }
            if (val_type->ops && val_type->ops->destruct) {
                val_type->ops->destruct(old_elem, val_type);
            }
        }

        storage->values = std::move(new_storage);
    }

    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        auto* storage = new (dst) MapStorage();
        // Initialize the embedded SetStorage for keys
        storage->keys.element_type = schema->key_type;
        storage->keys.index_set = std::make_unique<SetStorage::IndexSet>(
            0, SetIndexHash(&storage->keys), SetIndexEqual(&storage->keys));
        storage->value_type = schema->element_type;
    }

    static void destruct(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<MapStorage*>(obj);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;

        // Destruct all key-value pairs
        if (storage->index_set()) {
            for (size_t idx : *storage->index_set()) {
                void* key_ptr = storage->get_key_ptr(idx);
                void* val_ptr = storage->get_value_ptr(idx);
                if (key_type && key_type->ops && key_type->ops->destruct) {
                    key_type->ops->destruct(key_ptr, key_type);
                }
                if (val_type && val_type->ops && val_type->ops->destruct) {
                    val_type->ops->destruct(val_ptr, val_type);
                }
            }
        }
        storage->~MapStorage();
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        auto* src_storage = static_cast<const MapStorage*>(src);

        // Clear destination
        do_clear(dst, schema);

        // Copy entries from source
        if (src_storage->index_set()) {
            for (size_t idx : *src_storage->index_set()) {
                const void* src_key = src_storage->get_key_ptr(idx);
                const void* src_val = src_storage->get_value_ptr(idx);
                do_map_set(dst, src_key, src_val, schema);
            }
        }
    }

    static void move_assign(void* dst, void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<MapStorage*>(dst);
        auto* src_storage = static_cast<MapStorage*>(src);

        // Clear destination
        destruct(dst, schema);

        // Move via move assignment operator
        *dst_storage = std::move(*src_storage);
    }

    static void move_construct(void* dst, void* src, const TypeMeta* /*schema*/) {
        auto* src_storage = static_cast<MapStorage*>(src);
        // Placement new with move constructor
        new (dst) MapStorage(std::move(*src_storage));
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        auto* storage_a = static_cast<const MapStorage*>(a);
        auto* storage_b = static_cast<const MapStorage*>(b);
        const TypeMeta* val_type = schema->element_type;

        size_t size_a = storage_a->index_set() ? storage_a->index_set()->size() : 0;
        size_t size_b = storage_b->index_set() ? storage_b->index_set()->size() : 0;

        if (size_a != size_b) return false;

        // Check that all key-value pairs in a exist in b with same value (O(n) with O(1) lookups)
        if (storage_a->index_set() && storage_b->index_set()) {
            for (size_t idx_a : *storage_a->index_set()) {
                const void* key = storage_a->get_key_ptr(idx_a);
                const void* val_a = storage_a->get_value_ptr(idx_a);

                // O(1) lookup in b
                auto it_b = storage_b->index_set()->find(key);
                if (it_b == storage_b->index_set()->end()) {
                    return false;  // Key not found
                }

                const void* val_b = storage_b->get_value_ptr(*it_b);
                if (val_type && val_type->ops && val_type->ops->equals) {
                    if (!val_type->ops->equals(val_a, val_b, val_type)) {
                        return false;
                    }
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

        if (storage->index_set()) {
            bool first = true;
            for (size_t idx : *storage->index_set()) {
                if (!first) result += ", ";
                first = false;
                const void* key_ptr = storage->get_key_ptr(idx);
                const void* val_ptr = storage->get_value_ptr(idx);

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

        if (storage->index_set()) {
            for (size_t idx : *storage->index_set()) {
                const void* key_ptr = storage->get_key_ptr(idx);
                const void* val_ptr = storage->get_value_ptr(idx);

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
        }
        return result;
    }

    static void from_python(void* dst, const nb::object& src, const TypeMeta* schema) {
        // Handle dict, frozendict, and dict-like objects with items() method
        if (!nb::isinstance<nb::dict>(src) && !nb::hasattr(src, "items")) {
            throw std::runtime_error("Map.from_python expects a dict or dict-like object");
        }

        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;

        // Clear destination
        do_clear(dst, schema);

        // Get items - works for dict, frozendict, and any dict-like object
        nb::object items;
        if (nb::isinstance<nb::dict>(src)) {
            items = nb::cast<nb::dict>(src).attr("items")();
        } else {
            items = src.attr("items")();
        }

        // Iterate over items (key, value) pairs
        for (nb::handle item : items) {
            nb::tuple kv = nb::cast<nb::tuple>(item);

            // Create temp key
            std::vector<char> temp_key_storage(key_type->size);
            void* temp_key = temp_key_storage.data();
            if (key_type->ops && key_type->ops->construct) {
                key_type->ops->construct(temp_key, key_type);
            }
            if (key_type->ops && key_type->ops->from_python) {
                nb::object key_obj = nb::borrow<nb::object>(kv[0]);
                key_type->ops->from_python(temp_key, key_obj, key_type);
            }

            // Create temp value
            std::vector<char> temp_val_storage(val_type->size);
            void* temp_val = temp_val_storage.data();
            if (val_type->ops && val_type->ops->construct) {
                val_type->ops->construct(temp_val, val_type);
            }
            if (val_type->ops && val_type->ops->from_python) {
                nb::object val_obj = nb::borrow<nb::object>(kv[1]);
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
        if (storage->index_set()) {
            for (size_t idx : *storage->index_set()) {
                const void* key_ptr = storage->get_key_ptr(idx);
                const void* val_ptr = storage->get_value_ptr(idx);
                size_t pair_hash = 0;
                if (key_type && key_type->ops && key_type->ops->hash) {
                    pair_hash ^= key_type->ops->hash(key_ptr, key_type);
                }
                if (val_type && val_type->ops && val_type->ops->hash) {
                    pair_hash ^= val_type->ops->hash(val_ptr, val_type) << 1;
                }
                result ^= pair_hash;
            }
        }
        return result;
    }

    // ========== Iterable Operations ==========

    static size_t size(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const MapStorage*>(obj);
        return storage->index_set() ? storage->index_set()->size() : 0;
    }

    // ========== Map-specific Operations ==========

    static const void* do_map_get(const void* obj, const void* key, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const MapStorage*>(obj);
        if (!storage->index_set()) {
            throw std::out_of_range("Map key not found");
        }

        auto it = storage->index_set()->find(key);
        if (it == storage->index_set()->end()) {
            throw std::out_of_range("Map key not found");
        }
        return storage->get_value_ptr(*it);
    }

    static const void* map_get(const void* obj, const void* key, const TypeMeta* schema) {
        return do_map_get(obj, key, schema);
    }

    // Contains for key lookup (O(1))
    static bool do_contains(const void* obj, const void* key, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const MapStorage*>(obj);
        if (!storage->index_set()) return false;
        return storage->index_set()->find(key) != storage->index_set()->end();
    }

    static bool contains(const void* obj, const void* key, const TypeMeta* schema) {
        return do_contains(obj, key, schema);
    }

    static void do_map_set(void* obj, const void* key, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<MapStorage*>(obj);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;

        if (!storage->index_set()) return;

        // Check if key already exists (O(1))
        auto it = storage->index_set()->find(key);
        if (it != storage->index_set()->end()) {
            // Update existing value
            void* val_ptr = storage->get_value_ptr(*it);
            if (val_type && val_type->ops && val_type->ops->copy_assign) {
                val_type->ops->copy_assign(val_ptr, value, val_type);
            }
            return;
        }

        // Add new entry at entry_count position
        size_t new_idx = storage->entry_count();

        // Grow storage if needed (safely handles non-trivially-copyable types)
        grow_key_storage(storage, new_idx + 1, key_type);
        grow_value_storage(storage, new_idx + 1, val_type);

        // Construct and copy new key
        void* new_key = storage->get_key_ptr(new_idx);
        if (key_type->ops) {
            if (key_type->ops->construct) {
                key_type->ops->construct(new_key, key_type);
            }
            if (key_type->ops->copy_assign) {
                key_type->ops->copy_assign(new_key, key, key_type);
            }
        }

        // Construct and copy new value
        void* new_val = storage->get_value_ptr(new_idx);
        if (val_type->ops) {
            if (val_type->ops->construct) {
                val_type->ops->construct(new_val, val_type);
            }
            if (val_type->ops->copy_assign) {
                val_type->ops->copy_assign(new_val, value, val_type);
            }
        }

        storage->keys.element_count++;
        storage->index_set()->insert(new_idx);
    }

    static void map_set(void* obj, const void* key, const void* value, const TypeMeta* schema) {
        do_map_set(obj, key, value, schema);
    }

    static bool do_erase(void* obj, const void* key, const TypeMeta* schema) {
        auto* storage = static_cast<MapStorage*>(obj);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;

        if (!storage->index_set()) return false;

        auto it = storage->index_set()->find(key);
        if (it == storage->index_set()->end()) {
            return false;  // Key not found
        }

        size_t idx = *it;
        size_t last_idx = storage->entry_count() - 1;

        // Remove the found key's index from index_set
        storage->index_set()->erase(it);

        if (idx != last_idx) {
            // Swap-with-last: move last key-value pair to fill the gap
            void* key_slot = storage->get_key_ptr(idx);
            void* val_slot = storage->get_value_ptr(idx);
            void* last_key = storage->get_key_ptr(last_idx);
            void* last_val = storage->get_value_ptr(last_idx);

            // IMPORTANT: Remove last_idx from index_set BEFORE moving data
            // After moving, both indices would have the same key value,
            // causing the equality functor to return true for both
            storage->index_set()->erase(last_idx);

            // Move last key to the erased slot (overwrites the erased key)
            if (key_type && key_type->ops) {
                if (key_type->ops->move_assign) {
                    key_type->ops->move_assign(key_slot, last_key, key_type);
                } else if (key_type->ops->copy_assign) {
                    key_type->ops->copy_assign(key_slot, last_key, key_type);
                }
            }

            // Move last value to the erased slot (overwrites the erased value)
            if (val_type && val_type->ops) {
                if (val_type->ops->move_assign) {
                    val_type->ops->move_assign(val_slot, last_val, val_type);
                } else if (val_type->ops->copy_assign) {
                    val_type->ops->copy_assign(val_slot, last_val, val_type);
                }
            }

            // Insert idx for the moved key (now that data is in place)
            storage->index_set()->insert(idx);

            // Destruct the moved-from slots (last position)
            if (key_type && key_type->ops && key_type->ops->destruct) {
                key_type->ops->destruct(last_key, key_type);
            }
            if (val_type && val_type->ops && val_type->ops->destruct) {
                val_type->ops->destruct(last_val, val_type);
            }
        } else {
            // Erasing the last entry - just destruct it
            void* key_ptr = storage->get_key_ptr(idx);
            void* val_ptr = storage->get_value_ptr(idx);
            if (key_type && key_type->ops && key_type->ops->destruct) {
                key_type->ops->destruct(key_ptr, key_type);
            }
            if (val_type && val_type->ops && val_type->ops->destruct) {
                val_type->ops->destruct(val_ptr, val_type);
            }
        }

        storage->keys.element_count--;
        return true;
    }

    static void erase(void* obj, const void* key, const TypeMeta* schema) {
        do_erase(obj, key, schema);
    }

    static void do_clear(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<MapStorage*>(obj);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;

        // Destruct all key-value pairs
        if (storage->index_set()) {
            for (size_t idx : *storage->index_set()) {
                void* key_ptr = storage->get_key_ptr(idx);
                void* val_ptr = storage->get_value_ptr(idx);
                if (key_type && key_type->ops && key_type->ops->destruct) {
                    key_type->ops->destruct(key_ptr, key_type);
                }
                if (val_type && val_type->ops && val_type->ops->destruct) {
                    val_type->ops->destruct(val_ptr, val_type);
                }
            }
            storage->index_set()->clear();
        }

        // Clear storage
        storage->keys.elements.clear();
        storage->keys.element_count = 0;
        storage->values.clear();
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
            &move_construct,
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
