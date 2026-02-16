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
#include <hgraph/types/value/set_storage.h>
#include <hgraph/types/value/map_storage.h>

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
    // ========== None Mask Helpers ==========
    // The none_mask is stored as a uint64_t at the end of the tuple data
    // (after all fields, at offset schema->size - sizeof(uint64_t))

    static uint64_t* none_mask_ptr(void* obj, const TypeMeta* schema) {
        return reinterpret_cast<uint64_t*>(static_cast<char*>(obj) + schema->size - sizeof(uint64_t));
    }

    static const uint64_t* none_mask_ptr(const void* obj, const TypeMeta* schema) {
        return reinterpret_cast<const uint64_t*>(static_cast<const char*>(obj) + schema->size - sizeof(uint64_t));
    }

    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        for (size_t i = 0; i < schema->field_count; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* field_ptr = static_cast<char*>(dst) + field.offset;
            if (field.type && field.type->ops && field.type->ops->construct) {
                field.type->ops->construct(field_ptr, field.type);
            }
        }
        *none_mask_ptr(dst, schema) = 0;
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
        *none_mask_ptr(dst, schema) = *none_mask_ptr(src, schema);
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
        *none_mask_ptr(dst, schema) = *none_mask_ptr(src, schema);
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
        *none_mask_ptr(dst, schema) = *none_mask_ptr(src, schema);
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        // Compare None masks first
        if (*none_mask_ptr(a, schema) != *none_mask_ptr(b, schema)) return false;

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
        uint64_t mask = *none_mask_ptr(obj, schema);
        for (size_t i = 0; i < schema->field_count; ++i) {
            if (i > 0) result += ", ";
            if (mask & (1ULL << i)) {
                result += "None";
            } else {
                const BundleFieldInfo& field = schema->fields[i];
                const void* field_ptr = static_cast<const char*>(obj) + field.offset;
                if (field.type && field.type->ops && field.type->ops->to_string) {
                    result += field.type->ops->to_string(field_ptr, field.type);
                } else {
                    result += "<null>";
                }
            }
        }
        result += ")";
        return result;
    }

    // ========== Python Interop ==========

    static nb::object to_python(const void* obj, const TypeMeta* schema) {
        nb::list result;
        uint64_t mask = *none_mask_ptr(obj, schema);
        for (size_t i = 0; i < schema->field_count; ++i) {
            if (mask & (1ULL << i)) {
                result.append(nb::none());
            } else {
                const BundleFieldInfo& field = schema->fields[i];
                const void* field_ptr = static_cast<const char*>(obj) + field.offset;
                if (field.type && field.type->ops && field.type->ops->to_python) {
                    result.append(field.type->ops->to_python(field_ptr, field.type));
                } else {
                    result.append(nb::none());
                }
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
        uint64_t mask = 0;

        for (size_t i = 0; i < schema->field_count && i < src_len; ++i) {
            const BundleFieldInfo& field = schema->fields[i];
            void* field_ptr = static_cast<char*>(dst) + field.offset;
            if (field.type && field.type->ops && field.type->ops->from_python) {
                nb::object elem = seq[i];
                if (elem.is_none()) {
                    mask |= (1ULL << i);
                } else {
                    field.type->ops->from_python(field_ptr, elem, field.type);
                }
            }
        }
        *none_mask_ptr(dst, schema) = mask;
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
    bool is_linked{false};        // If true, data contains ViewData array
    std::vector<bool> none_mask;  // Tracks None elements for variadic tuples

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

    /// Check if this list's elements are links (ViewData) rather than local data
    [[nodiscard]] bool linked() const noexcept { return is_linked; }

    /// Set the linked state
    void set_linked(bool linked) noexcept { is_linked = linked; }

    /// Check if element at index is None
    [[nodiscard]] bool is_none(size_t i) const noexcept {
        return i < none_mask.size() && none_mask[i];
    }
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
            // Copy None mask
            dst_storage->none_mask = src_storage->none_mask;
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

            // Move vector, size, and None mask
            dst_storage->data = std::move(src_storage->data);
            dst_storage->size = src_storage->size;
            dst_storage->none_mask = std::move(src_storage->none_mask);

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

        // For dynamic lists, also compare None masks
        if (!is_fixed(schema)) {
            auto* storage_a = static_cast<const DynamicListStorage*>(a);
            auto* storage_b = static_cast<const DynamicListStorage*>(b);
            if (storage_a->none_mask != storage_b->none_mask) return false;
        }

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

        // For dynamic lists, check the None mask for elements that were stored as None
        const DynamicListStorage* storage = is_fixed(schema) ? nullptr : static_cast<const DynamicListStorage*>(obj);

        for (size_t i = 0; i < n; ++i) {
            if (storage && storage->is_none(i)) {
                result.append(nb::none());
            } else {
                const void* elem_ptr = get_element_ptr_const(obj, i, schema);
                if (elem_type && elem_type->ops && elem_type->ops->to_python) {
                    result.append(elem_type->ops->to_python(elem_ptr, elem_type));
                } else {
                    result.append(nb::none());
                }
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
            // Track None elements for round-tripping (needed by variadic tuples like tuple[int, ...])
            storage->none_mask.assign(src_len, false);
            for (size_t i = 0; i < src_len; ++i) {
                void* elem_ptr = static_cast<char*>(storage->data_ptr()) + i * elem_type->size;
                if (elem_type && elem_type->ops && elem_type->ops->from_python) {
                    nb::object elem = seq[i];
                    if (elem.is_none()) {
                        storage->none_mask[i] = true;
                    } else {
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

/**
 * @brief Operations for Set types (collections of unique elements).
 *
 * Sets store unique elements using KeySet for O(1) operations.
 * Elements must be hashable and equatable.
 * Uses SetStorage from set_storage.h which wraps KeySet.
 */
struct SetOps {
    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        // Placement-new SetStorage with the element type
        new (dst) SetStorage(schema->element_type);
    }

    static void destruct(void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<SetStorage*>(obj);
        // SetStorage destructor handles cleanup via KeySet
        storage->~SetStorage();
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<SetStorage*>(dst);
        auto* src_storage = static_cast<const SetStorage*>(src);

        // Clear destination
        dst_storage->clear();

        // Copy elements from source using iterator
        for (auto it = src_storage->begin(); it != src_storage->end(); ++it) {
            dst_storage->add(*it);
        }
        (void)schema;
    }

    static void move_assign(void* dst, void* src, const TypeMeta* /*schema*/) {
        auto* dst_storage = static_cast<SetStorage*>(dst);
        auto* src_storage = static_cast<SetStorage*>(src);
        *dst_storage = std::move(*src_storage);
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
            if (!storage_b->contains(*it)) {
                return false;
            }
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
            if (elem_type && elem_type->ops && elem_type->ops->to_string) {
                result += elem_type->ops->to_string(*it, elem_type);
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
            if (elem_type && elem_type->ops && elem_type->ops->to_python) {
                result.add(elem_type->ops->to_python(*it, elem_type));
            }
        }
        // Return frozenset — immutable representation matching SetStorage's read-only nature.
        // TSS wrappers (PyTimeSeriesSetOutput/Input::value()) return mutable set separately.
        // This ensures TS[frozenset[int]].value returns frozenset, which TSS from_python
        // recognizes as Case 2 (replacement diff).
        return nb::frozenset(result);
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

            storage->add(temp_elem);

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
        for (auto it = storage->begin(); it != storage->end(); ++it) {
            if (elem_type && elem_type->ops && elem_type->ops->hash) {
                result ^= elem_type->ops->hash(*it, elem_type);
            }
        }
        return result;
    }

    // ========== Iterable Operations ==========

    static size_t size(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const SetStorage*>(obj);
        return storage->size();
    }

    // ========== Indexable Operations (for iteration) ==========

    static const void* get_at(const void* obj, size_t index, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const SetStorage*>(obj);
        if (index >= storage->size()) {
            throw std::out_of_range("Set index out of range");
        }
        // Use KeySet's index_set for random access
        auto* index_set = storage->key_set().index_set();
        if (!index_set) {
            throw std::out_of_range("Set index out of range");
        }
        auto it = index_set->begin();
        std::advance(it, index);
        return storage->key_set().key_at_slot(*it);
    }

    // ========== Set-specific Operations ==========

    static bool contains(const void* obj, const void* value, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const SetStorage*>(obj);
        return storage->contains(value);
    }

    static void insert(void* obj, const void* value, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<SetStorage*>(obj);
        storage->add(value);
    }

    static void erase(void* obj, const void* value, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<SetStorage*>(obj);
        storage->remove(value);
    }

    static void clear(void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<SetStorage*>(obj);
        storage->clear();
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

// ============================================================================
// Map Operations
// ============================================================================

/**
 * @brief Operations for Map types (key-value collections).
 *
 * Maps store key-value pairs using KeySet + ValueArray for O(1) operations.
 * Keys must be hashable and equatable.
 * Uses MapStorage from map_storage.h which composes SetStorage + ValueArray.
 */
struct MapOps {
    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        // Placement-new MapStorage with key and value types
        new (dst) MapStorage(schema->key_type, schema->element_type);
    }

    static void destruct(void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<MapStorage*>(obj);
        // MapStorage destructor handles cleanup via KeySet and ValueArray
        storage->~MapStorage();
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<MapStorage*>(dst);
        auto* src_storage = static_cast<const MapStorage*>(src);

        // Clear destination
        dst_storage->clear();

        // Copy entries from source using iteration
        for (auto slot : src_storage->key_set()) {
            const void* src_key = src_storage->key_at_slot(slot);
            const void* src_val = src_storage->value_at_slot(slot);
            dst_storage->set_item(src_key, src_val);
        }
        (void)schema;
    }

    static void move_assign(void* dst, void* src, const TypeMeta* /*schema*/) {
        auto* dst_storage = static_cast<MapStorage*>(dst);
        auto* src_storage = static_cast<MapStorage*>(src);
        *dst_storage = std::move(*src_storage);
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
        for (auto slot_a : storage_a->key_set()) {
            const void* key = storage_a->key_at_slot(slot_a);
            const void* val_a = storage_a->value_at_slot(slot_a);

            if (!storage_b->contains(key)) {
                return false;  // Key not found
            }

            const void* val_b = storage_b->at(key);
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

        bool first = true;
        for (auto slot : storage->key_set()) {
            if (!first) result += ", ";
            first = false;
            const void* key_ptr = storage->key_at_slot(slot);
            const void* val_ptr = storage->value_at_slot(slot);

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

        for (auto slot : storage->key_set()) {
            const void* key_ptr = storage->key_at_slot(slot);
            const void* val_ptr = storage->value_at_slot(slot);

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

            storage->set_item(temp_key, temp_val);

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
        for (auto slot : storage->key_set()) {
            const void* key_ptr = storage->key_at_slot(slot);
            const void* val_ptr = storage->value_at_slot(slot);
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
        return storage->size();
    }

    /**
     * @brief Get the key at a given iteration index.
     *
     * This enables uniform indexed access for SetView when viewing map keys.
     * The index is into the iteration order (slot index set), not a storage slot.
     */
    static const void* get_at(const void* obj, size_t index, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const MapStorage*>(obj);
        auto* index_set = storage->key_set().index_set();
        if (!index_set || index >= index_set->size()) {
            return nullptr;
        }
        // Advance to the index-th element in the iteration order
        auto it = index_set->begin();
        std::advance(it, index);
        size_t slot = *it;
        return storage->key_at_slot(slot);
    }

    // ========== Map-specific Operations ==========

    static const void* map_get(const void* obj, const void* key, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const MapStorage*>(obj);
        return storage->at(key);  // Throws if not found
    }

    static bool contains(const void* obj, const void* key, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const MapStorage*>(obj);
        return storage->contains(key);
    }

    static void map_set(void* obj, const void* key, const void* value, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<MapStorage*>(obj);
        storage->set_item(key, value);
    }

    static void erase(void* obj, const void* key, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<MapStorage*>(obj);
        storage->remove(key);
    }

    static void clear(void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<MapStorage*>(obj);
        storage->clear();
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
            &get_at,   // get_at (returns key at iteration index)
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
