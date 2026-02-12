#pragma once

/**
 * @file composite_ops.h
 * @brief type_ops implementations for composite types (Bundle, Tuple, List, Set, Map).
 *
 * Each composite type needs its own operations implementation that handles
 * construction, destruction, copying, Python interop, and type-specific
 * operations like field access (Bundle) or element access (List).
 */

#include <hgraph/types/value/type_meta.h>
#include <hgraph/types/value/value_view.h>
#include <hgraph/types/value/set_storage.h>
#include <hgraph/types/value/map_storage.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

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
            if (field.type && field.type->ops().construct) {
                field.type->ops().construct(field_ptr, field.type);
            }
        }
    }

    static void destroy(void* obj, const TypeMeta* schema) {
        // Destruct each field using its type's ops
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* field_ptr = static_cast<char*>(obj) + field.offset;
            if (field.type && field.type->ops().destroy) {
                field.type->ops().destroy(field_ptr, field.type);
            }
        }
    }

    static void copy(void* dst, const void* src, const TypeMeta* schema) {
        // Copy each field using its type's ops
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* dst_field = static_cast<char*>(dst) + field.offset;
            const void* src_field = static_cast<const char*>(src) + field.offset;
            if (field.type && field.type->ops().copy) {
                field.type->ops().copy(dst_field, src_field, field.type);
            }
        }
    }

    static void move(void* dst, void* src, const TypeMeta* schema) {
        // Move each field using its type's ops
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* dst_field = static_cast<char*>(dst) + field.offset;
            void* src_field = static_cast<char*>(src) + field.offset;
            if (field.type && field.type->ops().move) {
                field.type->ops().move(dst_field, src_field, field.type);
            }
        }
    }

    static void move_construct(void* dst, void* src, const TypeMeta* schema) {
        // Move-construct each field using its type's ops
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* dst_field = static_cast<char*>(dst) + field.offset;
            void* src_field = static_cast<char*>(src) + field.offset;
            if (field.type && field.type->ops().move_construct) {
                field.type->ops().move_construct(dst_field, src_field, field.type);
            }
        }
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        // All fields must be equal
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            const void* a_field = static_cast<const char*>(a) + field.offset;
            const void* b_field = static_cast<const char*>(b) + field.offset;
            if (field.type && field.type->ops().equals) {
                if (!field.type->ops().equals(a_field, b_field, field.type)) {
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
            if (field.type && field.type->ops().to_string) {
                result += field.type->ops().to_string(field_ptr, field.type);
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
            if (field.type && field.type->ops().to_python && field.name) {
                result[field.name] = field.type->ops().to_python(field_ptr, field.type);
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
                    if (field.type && field.type->ops().from_python) {
                        nb::object val = d[field.name];
                        // Skip None values - can't cast None to non-nullable scalar types
                        if (!val.is_none()) {
                            field.type->ops().from_python(field_ptr, val, field.type);
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
                if (field.type && field.type->ops().from_python) {
                    nb::object elem = seq[i];
                    // Skip None values - can't cast None to non-nullable scalar types
                    if (!elem.is_none()) {
                        field.type->ops().from_python(field_ptr, elem, field.type);
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
                    if (field.type && field.type->ops().from_python) {
                        nb::object attr = nb::getattr(src, field.name);
                        // Skip None values - can't cast None to non-nullable scalar types
                        if (!attr.is_none()) {
                            field.type->ops().from_python(field_ptr, attr, field.type);
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
            if (field.type && field.type->ops().hash) {
                size_t field_hash = field.type->ops().hash(field_ptr, field.type);
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

    static const void* at(const void* obj, size_t index, const TypeMeta* schema) {
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
        if (field.type && field.type->ops().copy) {
            field.type->ops().copy(field_ptr, value, field.type);
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
                if (field.type && field.type->ops().copy) {
                    field.type->ops().copy(field_ptr, value, field.type);
                }
                return;
            }
        }
        throw std::out_of_range(std::string("Bundle has no field named '") + name + "'");
    }

    /// Build type_ops for bundles
    static type_ops make_ops() {
        type_ops ops{};
        ops.construct = &construct;
        ops.destroy = &destroy;
        ops.copy = &copy;
        ops.move = &move;
        ops.move_construct = &move_construct;
        ops.equals = &equals;
        ops.hash = &hash;
        ops.to_string = &to_string;
        ops.to_python = &to_python;
        ops.from_python = &from_python;
        ops.kind = TypeKind::Bundle;
        ops.specific.bundle = {&size, &at, &set_at, &get_field, &set_field};
        return ops;
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
            if (field.type && field.type->ops().construct) {
                field.type->ops().construct(field_ptr, field.type);
            }
        }
    }

    static void destroy(void* obj, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* field_ptr = static_cast<char*>(obj) + field.offset;
            if (field.type && field.type->ops().destroy) {
                field.type->ops().destroy(field_ptr, field.type);
            }
        }
    }

    static void copy(void* dst, const void* src, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* dst_field = static_cast<char*>(dst) + field.offset;
            const void* src_field = static_cast<const char*>(src) + field.offset;
            if (field.type && field.type->ops().copy) {
                field.type->ops().copy(dst_field, src_field, field.type);
            }
        }
    }

    static void move(void* dst, void* src, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* dst_field = static_cast<char*>(dst) + field.offset;
            void* src_field = static_cast<char*>(src) + field.offset;
            if (field.type && field.type->ops().move) {
                field.type->ops().move(dst_field, src_field, field.type);
            }
        }
    }

    static void move_construct(void* dst, void* src, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* dst_field = static_cast<char*>(dst) + field.offset;
            void* src_field = static_cast<char*>(src) + field.offset;
            if (field.type && field.type->ops().move_construct) {
                field.type->ops().move_construct(dst_field, src_field, field.type);
            }
        }
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            const void* a_field = static_cast<const char*>(a) + field.offset;
            const void* b_field = static_cast<const char*>(b) + field.offset;
            if (field.type && field.type->ops().equals) {
                if (!field.type->ops().equals(a_field, b_field, field.type)) {
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
            if (field.type && field.type->ops().to_string) {
                result += field.type->ops().to_string(field_ptr, field.type);
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
            if (field.type && field.type->ops().to_python) {
                result.append(field.type->ops().to_python(field_ptr, field.type));
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
            if (field.type && field.type->ops().from_python) {
                nb::object elem = seq[i];
                // Skip None values - can't cast None to non-nullable scalar types
                if (!elem.is_none()) {
                    field.type->ops().from_python(field_ptr, elem, field.type);
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
            if (field.type && field.type->ops().hash) {
                size_t field_hash = field.type->ops().hash(field_ptr, field.type);
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

    static const void* at(const void* obj, size_t index, const TypeMeta* schema) {
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
        if (field.type && field.type->ops().copy) {
            field.type->ops().copy(field_ptr, value, field.type);
        }
    }

    /// Build type_ops for tuples
    static type_ops make_ops() {
        type_ops ops{};
        ops.construct = &construct;
        ops.destroy = &destroy;
        ops.copy = &copy;
        ops.move = &move;
        ops.move_construct = &move_construct;
        ops.equals = &equals;
        ops.hash = &hash;
        ops.to_string = &to_string;
        ops.to_python = &to_python;
        ops.from_python = &from_python;
        ops.kind = TypeKind::Tuple;
        ops.specific.tuple = {&size, &at, &set_at};
        return ops;
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
                if (elem_type && elem_type->ops().construct) {
                    elem_type->ops().construct(elem_ptr, elem_type);
                }
            }
        } else {
            // Dynamic list: initialize empty storage
            new (dst) DynamicListStorage();
        }
    }

    static void destroy(void* obj, const TypeMeta* schema) {
        const TypeMeta* elem_type = schema->element_type;

        if (is_fixed(schema)) {
            // Fixed list: destruct all elements
            for (size_t i = 0; i < schema->fixed_size; ++i) {
                void* elem_ptr = get_element_ptr(obj, i, schema);
                if (elem_type && elem_type->ops().destroy) {
                    elem_type->ops().destroy(elem_ptr, elem_type);
                }
            }
        } else {
            // Dynamic list: destruct elements (vector handles memory cleanup)
            auto* storage = static_cast<DynamicListStorage*>(obj);
            if (!storage->data.empty() && elem_type) {
                for (size_t i = 0; i < storage->size; ++i) {
                    void* elem_ptr = static_cast<char*>(storage->data_ptr()) + i * elem_type->size;
                    if (elem_type->ops().destroy) {
                        elem_type->ops().destroy(elem_ptr, elem_type);
                    }
                }
            }
            storage->~DynamicListStorage();  // Vector destructor frees memory
        }
    }

    static void copy(void* dst, const void* src, const TypeMeta* schema) {
        const TypeMeta* elem_type = schema->element_type;

        if (is_fixed(schema)) {
            // Fixed list: copy all elements
            for (size_t i = 0; i < schema->fixed_size; ++i) {
                void* dst_elem = get_element_ptr(dst, i, schema);
                const void* src_elem = get_element_ptr_const(src, i, schema);
                if (elem_type && elem_type->ops().copy) {
                    elem_type->ops().copy(dst_elem, src_elem, elem_type);
                }
            }
        } else {
            // Dynamic list: resize and copy
            auto* dst_storage = static_cast<DynamicListStorage*>(dst);
            auto* src_storage = static_cast<const DynamicListStorage*>(src);

            // Resize destination to match source
            do_resize(dst, src_storage->size, schema);

            // Copy elements
            if (elem_type && elem_type->ops().copy) {
                for (size_t i = 0; i < src_storage->size; ++i) {
                    void* dst_elem = static_cast<char*>(dst_storage->data_ptr()) + i * elem_type->size;
                    const void* src_elem = static_cast<const char*>(src_storage->data_ptr()) + i * elem_type->size;
                    elem_type->ops().copy(dst_elem, src_elem, elem_type);
                }
            }
        }
    }

    static void move(void* dst, void* src, const TypeMeta* schema) {
        if (is_fixed(schema)) {
            // Fixed list: move all elements
            const TypeMeta* elem_type = schema->element_type;
            for (size_t i = 0; i < schema->fixed_size; ++i) {
                void* dst_elem = get_element_ptr(dst, i, schema);
                void* src_elem = get_element_ptr(src, i, schema);
                if (elem_type && elem_type->ops().move) {
                    elem_type->ops().move(dst_elem, src_elem, elem_type);
                }
            }
        } else {
            // Dynamic list: move storage via vector move
            auto* dst_storage = static_cast<DynamicListStorage*>(dst);
            auto* src_storage = static_cast<DynamicListStorage*>(src);

            // First destruct dst elements
            destroy(dst, schema);

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
                if (elem_type && elem_type->ops().move_construct) {
                    elem_type->ops().move_construct(dst_elem, src_elem, elem_type);
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
            if (elem_type && elem_type->ops().equals) {
                if (!elem_type->ops().equals(elem_a, elem_b, elem_type)) {
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
            if (elem_type && elem_type->ops().to_string) {
                result += elem_type->ops().to_string(elem_ptr, elem_type);
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
            if (elem_type && elem_type->ops().to_python) {
                result.append(elem_type->ops().to_python(elem_ptr, elem_type));
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
                if (elem_type && elem_type->ops().from_python) {
                    nb::object elem = seq[i];
                    // Skip None values - can't cast None to non-nullable scalar types
                    if (!elem.is_none()) {
                        elem_type->ops().from_python(elem_ptr, elem, elem_type);
                    }
                }
            }
        } else {
            // Dynamic list: resize and populate
            do_resize(dst, src_len, schema);
            auto* storage = static_cast<DynamicListStorage*>(dst);
            for (size_t i = 0; i < src_len; ++i) {
                void* elem_ptr = static_cast<char*>(storage->data_ptr()) + i * elem_type->size;
                if (elem_type && elem_type->ops().from_python) {
                    nb::object elem = seq[i];
                    // Skip None values - can't cast None to non-nullable scalar types
                    if (!elem.is_none()) {
                        elem_type->ops().from_python(elem_ptr, elem, elem_type);
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
            if (elem_type && elem_type->ops().hash) {
                size_t elem_hash = elem_type->ops().hash(elem_ptr, elem_type);
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

    static const void* at(const void* obj, size_t index, const TypeMeta* schema) {
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
        if (elem_type && elem_type->ops().copy) {
            elem_type->ops().copy(elem_ptr, value, elem_type);
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
                if (elem_type && elem_type->ops().destroy) {
                    elem_type->ops().destroy(elem_ptr, elem_type);
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

                        if (elem_type->ops().move_construct) {
                            elem_type->ops().move_construct(new_elem, old_elem, elem_type);
                        }
                        if (elem_type->ops().destroy) {
                            elem_type->ops().destroy(old_elem, elem_type);
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
                if (elem_type && elem_type->ops().construct) {
                    elem_type->ops().construct(elem_ptr, elem_type);
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

    /// Build type_ops for lists
    static type_ops make_ops() {
        type_ops ops{};
        ops.construct = &construct;
        ops.destroy = &destroy;
        ops.copy = &copy;
        ops.move = &move;
        ops.move_construct = &move_construct;
        ops.equals = &equals;
        ops.hash = &hash;
        ops.to_string = &to_string;
        ops.to_python = &to_python;
        ops.from_python = &from_python;
        ops.kind = TypeKind::List;
        ops.specific.list = {&size, &at, &set_at, &resize, &clear};
        return ops;
    }
};

// ============================================================================
// Set Operations
// ============================================================================

/**
 * @brief Operations for Set types (collections of unique elements).
 *
 * Sets store unique elements using KeySet for O(1) operations with stable slots.
 * Elements must be hashable and equatable.
 */
struct SetOps {
    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        new (dst) SetStorage(schema->element_type);
    }

    static void destroy(void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<SetStorage*>(obj);
        // KeySet destructor handles element cleanup
        storage->~SetStorage();
    }

    static void copy(void* dst, const void* src, const TypeMeta* /*schema*/) {
        auto* dst_storage = static_cast<SetStorage*>(dst);
        auto* src_storage = static_cast<const SetStorage*>(src);

        // Clear destination
        dst_storage->clear();

        // Copy elements from source
        for (auto it = src_storage->begin(); it != src_storage->end(); ++it) {
            dst_storage->add(*it);
        }
    }

    static void move(void* dst, void* src, const TypeMeta* /*schema*/) {
        auto* dst_storage = static_cast<SetStorage*>(dst);
        auto* src_storage = static_cast<SetStorage*>(src);

        // Destroy destination, then move-construct in place
        dst_storage->~SetStorage();
        new (dst) SetStorage(std::move(*src_storage));
    }

    static void move_construct(void* dst, void* src, const TypeMeta* /*schema*/) {
        auto* src_storage = static_cast<SetStorage*>(src);
        new (dst) SetStorage(std::move(*src_storage));
    }

    static bool equals(const void* a, const void* b, const TypeMeta* /*schema*/) {
        auto* storage_a = static_cast<const SetStorage*>(a);
        auto* storage_b = static_cast<const SetStorage*>(b);

        if (storage_a->size() != storage_b->size()) return false;

        // Check that all elements in a are in b
        for (auto it = storage_a->begin(); it != storage_a->end(); ++it) {
            if (!storage_b->contains(*it)) return false;
        }
        return true;
    }

    static std::string to_string(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const SetStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        std::string result = "{";

        bool first = true;
        for (auto it = storage->begin(); it != storage->end(); ++it) {
            if (!first) result += ", ";
            first = false;
            if (elem_type && elem_type->ops().to_string) {
                result += elem_type->ops().to_string(*it, elem_type);
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

        for (auto it = storage->begin(); it != storage->end(); ++it) {
            if (elem_type && elem_type->ops().to_python) {
                result.add(elem_type->ops().to_python(*it, elem_type));
            }
        }
        return result;
    }

    static void from_python(void* dst, const nb::object& src, const TypeMeta* schema) {
        if (!nb::isinstance<nb::set>(src) && !nb::isinstance<nb::frozenset>(src) &&
            !nb::isinstance<nb::list>(src) && !nb::isinstance<nb::tuple>(src)) {
            throw std::runtime_error("Set.from_python expects a set, frozenset, list, or tuple");
        }

        auto* storage = static_cast<SetStorage*>(dst);
        const TypeMeta* elem_type = schema->element_type;

        // Clear destination
        storage->clear();

        // Insert elements from Python
        nb::iterator it = nb::iter(src);
        while (it != nb::iterator::sentinel()) {
            nb::handle item_handle = *it;
            if (item_handle.is_none()) {
                throw std::runtime_error("Set.from_python does not allow None elements");
            }
            nb::object item = nb::borrow<nb::object>(item_handle);

            // Create temp element
            std::vector<char> temp_storage(elem_type->size);
            void* temp_elem = temp_storage.data();

            if (elem_type->ops().construct) {
                elem_type->ops().construct(temp_elem, elem_type);
            }
            if (elem_type->ops().from_python) {
                elem_type->ops().from_python(temp_elem, item, elem_type);
            }

            storage->add(temp_elem);

            if (elem_type->ops().destroy) {
                elem_type->ops().destroy(temp_elem, elem_type);
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
        for (auto it = storage->begin(); it != storage->end(); ++it) {
            if (elem_type && elem_type->ops().hash) {
                result ^= elem_type->ops().hash(*it, elem_type);
            }
        }
        return result;
    }

    // ========== Iterable Operations ==========

    static size_t size(const void* obj, const TypeMeta* /*schema*/) {
        return static_cast<const SetStorage*>(obj)->size();
    }

    // ========== Indexable Operations (for iteration) ==========

    static const void* at(const void* obj, size_t index, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const SetStorage*>(obj);
        if (index >= storage->size()) {
            throw std::out_of_range("Set index out of range");
        }
        // Iterate KeySet alive slots to find n-th element
        auto it = storage->begin();
        std::advance(it, index);
        return *it;
    }

    // ========== Set-specific Operations ==========

    static bool contains(const void* obj, const void* value, const TypeMeta* /*schema*/) {
        return static_cast<const SetStorage*>(obj)->contains(value);
    }

    static void add(void* obj, const void* value, const TypeMeta* /*schema*/) {
        static_cast<SetStorage*>(obj)->add(value);
    }

    static void remove(void* obj, const void* value, const TypeMeta* /*schema*/) {
        static_cast<SetStorage*>(obj)->remove(value);
    }

    static void clear(void* obj, const TypeMeta* /*schema*/) {
        static_cast<SetStorage*>(obj)->clear();
    }

    /// Build type_ops for sets
    static type_ops make_ops() {
        type_ops ops{};
        ops.construct = &construct;
        ops.destroy = &destroy;
        ops.copy = &copy;
        ops.move = &move;
        ops.move_construct = &move_construct;
        ops.equals = &equals;
        ops.hash = &hash;
        ops.to_string = &to_string;
        ops.to_python = &to_python;
        ops.from_python = &from_python;
        ops.kind = TypeKind::Set;
        ops.specific.set = {&size, &at, &contains, &add, &remove, &clear};
        return ops;
    }
};

// ============================================================================
// Map Operations
// ============================================================================

/**
 * @brief Operations for Map types (key-value collections).
 *
 * Maps store key-value pairs using KeySet + ValueArray for O(1) operations
 * with stable slots.
 * Keys must be hashable and equatable.
 */
struct MapOps {
    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        new (dst) MapStorage(schema->key_type, schema->element_type);
    }

    static void destroy(void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<MapStorage*>(obj);
        // clear() destroys values at live slots, then keys via KeySet::clear
        storage->clear();
        // Destructor unregisters observer and frees memory
        storage->~MapStorage();
    }

    static void copy(void* dst, const void* src, const TypeMeta* /*schema*/) {
        auto* dst_storage = static_cast<MapStorage*>(dst);
        auto* src_storage = static_cast<const MapStorage*>(src);

        // Clear destination
        dst_storage->clear();

        // Copy entries from source
        for (auto slot : src_storage->key_set()) {
            const void* src_key = src_storage->key_at_slot(slot);
            const void* src_val = src_storage->value_at_slot(slot);
            dst_storage->set_item(src_key, src_val);
        }
    }

    static void move(void* dst, void* src, const TypeMeta* /*schema*/) {
        auto* dst_storage = static_cast<MapStorage*>(dst);
        auto* src_storage = static_cast<MapStorage*>(src);

        // Clear and destroy destination, then move-construct in place
        dst_storage->clear();
        dst_storage->~MapStorage();
        new (dst) MapStorage(std::move(*src_storage));
    }

    static void move_construct(void* dst, void* src, const TypeMeta* /*schema*/) {
        auto* src_storage = static_cast<MapStorage*>(src);
        new (dst) MapStorage(std::move(*src_storage));
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        auto* storage_a = static_cast<const MapStorage*>(a);
        auto* storage_b = static_cast<const MapStorage*>(b);
        const TypeMeta* val_type = schema->element_type;

        if (storage_a->size() != storage_b->size()) return false;

        // Check that all key-value pairs in a exist in b with same value
        for (auto slot : storage_a->key_set()) {
            const void* key = storage_a->key_at_slot(slot);
            const void* val_a = storage_a->value_at_slot(slot);

            if (!storage_b->contains(key)) return false;

            const void* val_b = storage_b->at(key);
            if (val_type && val_type->ops().equals) {
                if (!val_type->ops().equals(val_a, val_b, val_type)) {
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

        bool first = true;
        for (auto slot : storage->key_set()) {
            if (!first) result += ", ";
            first = false;
            const void* key_ptr = storage->key_at_slot(slot);
            const void* val_ptr = storage->value_at_slot(slot);

            if (key_type && key_type->ops().to_string) {
                result += key_type->ops().to_string(key_ptr, key_type);
            } else {
                result += "<key>";
            }
            result += ": ";
            if (val_type && val_type->ops().to_string) {
                result += val_type->ops().to_string(val_ptr, val_type);
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

        for (auto slot : storage->key_set()) {
            const void* key_ptr = storage->key_at_slot(slot);
            const void* val_ptr = storage->value_at_slot(slot);

            nb::object py_key, py_val;
            if (key_type && key_type->ops().to_python) {
                py_key = key_type->ops().to_python(key_ptr, key_type);
            } else {
                py_key = nb::none();
            }
            if (val_type && val_type->ops().to_python) {
                py_val = val_type->ops().to_python(val_ptr, val_type);
            } else {
                py_val = nb::none();
            }

            result[py_key] = py_val;
        }
        return result;
    }

    static void from_python(void* dst, const nb::object& src, const TypeMeta* schema) {
        // Handle dict, frozendict, and dict-like objects with items() method
        if (!nb::isinstance<nb::dict>(src) && !nb::hasattr(src, "items")) {
            throw std::runtime_error("Map.from_python expects a dict or dict-like object");
        }

        auto* storage = static_cast<MapStorage*>(dst);
        const TypeMeta* key_type = schema->key_type;
        const TypeMeta* val_type = schema->element_type;

        // Clear destination
        storage->clear();

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
            nb::handle key_handle = kv[0];
            if (key_handle.is_none()) {
                throw std::runtime_error("Map.from_python does not allow None keys");
            }
            nb::object key_obj = nb::borrow<nb::object>(key_handle);

            // Create temp key
            std::vector<char> temp_key_storage(key_type->size);
            void* temp_key = temp_key_storage.data();
            if (key_type->ops().construct) {
                key_type->ops().construct(temp_key, key_type);
            }
            if (key_type->ops().from_python) {
                key_type->ops().from_python(temp_key, key_obj, key_type);
            }

            // Create temp value
            std::vector<char> temp_val_storage(val_type->size);
            void* temp_val = temp_val_storage.data();
            if (val_type->ops().construct) {
                val_type->ops().construct(temp_val, val_type);
            }
            if (val_type->ops().from_python) {
                nb::object val_obj = nb::borrow<nb::object>(kv[1]);
                val_type->ops().from_python(temp_val, val_obj, val_type);
            }

            storage->set_item(temp_key, temp_val);

            if (key_type->ops().destroy) {
                key_type->ops().destroy(temp_key, key_type);
            }
            if (val_type->ops().destroy) {
                val_type->ops().destroy(temp_val, val_type);
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
        for (auto slot : storage->key_set()) {
            const void* key_ptr = storage->key_at_slot(slot);
            const void* val_ptr = storage->value_at_slot(slot);
            size_t pair_hash = 0;
            if (key_type && key_type->ops().hash) {
                pair_hash ^= key_type->ops().hash(key_ptr, key_type);
            }
            if (val_type && val_type->ops().hash) {
                pair_hash ^= val_type->ops().hash(val_ptr, val_type) << 1;
            }
            result ^= pair_hash;
        }
        return result;
    }

    // ========== Iterable Operations ==========

    static size_t size(const void* obj, const TypeMeta* /*schema*/) {
        return static_cast<const MapStorage*>(obj)->size();
    }

    // ========== Map-specific Operations ==========

    static const void* at(const void* obj, const void* key, const TypeMeta* /*schema*/) {
        return static_cast<const MapStorage*>(obj)->at(key);
    }

    static bool contains(const void* obj, const void* key, const TypeMeta* /*schema*/) {
        return static_cast<const MapStorage*>(obj)->contains(key);
    }

    static void set_item(void* obj, const void* key, const void* value, const TypeMeta* /*schema*/) {
        static_cast<MapStorage*>(obj)->set_item(key, value);
    }

    static void remove(void* obj, const void* key, const TypeMeta* /*schema*/) {
        static_cast<MapStorage*>(obj)->remove(key);
    }

    static void clear(void* obj, const TypeMeta* /*schema*/) {
        static_cast<MapStorage*>(obj)->clear();
    }

    /// Build type_ops for maps
    static type_ops make_ops() {
        type_ops ops{};
        ops.construct = &construct;
        ops.destroy = &destroy;
        ops.copy = &copy;
        ops.move = &move;
        ops.move_construct = &move_construct;
        ops.equals = &equals;
        ops.hash = &hash;
        ops.to_string = &to_string;
        ops.to_python = &to_python;
        ops.from_python = &from_python;
        ops.kind = TypeKind::Map;
        ops.specific.map = {&size, &contains, &at, &set_item, &remove, &clear};
        return ops;
    }
};

} // namespace hgraph::value
