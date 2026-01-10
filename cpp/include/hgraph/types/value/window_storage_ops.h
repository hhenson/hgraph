#pragma once

/**
 * @file window_storage_ops.h
 * @brief TypeOps implementation for Window type (TSW storage).
 *
 * Window is a fixed-size circular buffer that stores both values AND timestamps.
 * It is designed specifically for Time Series Window (TSW) types.
 *
 * Key features:
 * - Two parallel cyclic buffers: one for values, one for timestamps
 * - Capacity+1 allocation to preserve the removed (evicted) value
 * - Push semantics: accepts single value + timestamp pair
 * - Logical index 0 = oldest element, index size-1 = newest element
 */

#include <hgraph/types/value/type_meta.h>
#include <hgraph/util/date_time.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>

namespace nb = nanobind;

namespace hgraph::value {

// ============================================================================
// Window Storage Structure
// ============================================================================

/**
 * @brief Storage structure for TSW window type.
 *
 * Maintains two parallel cyclic buffers:
 * - values_data: stores the actual element values
 * - times_data: stores the engine_time_t timestamps
 *
 * Both buffers have capacity+1 slots to preserve the removed value.
 * The `has_removed` flag indicates if a value was evicted on the last push.
 */
struct WindowStorage {
    void* values_data{nullptr};           // Cyclic buffer for values
    engine_time_t* times_data{nullptr};   // Cyclic buffer for timestamps
    size_t capacity{0};                   // Logical window size (actual allocation is capacity+1)
    size_t min_size{0};                   // Minimum size for window to be "valid" (for all_valid)
    size_t size{0};                       // Current element count (0..capacity)
    size_t head{0};                       // Index of oldest element (rotation pointer)
    bool has_removed{false};              // True if last push evicted a value

    WindowStorage() = default;
    WindowStorage(const WindowStorage&) = delete;
    WindowStorage& operator=(const WindowStorage&) = delete;

    // The removed value is at physical index (head + capacity) % (capacity + 1)
    // which is the slot just before the current head after a rotation
};

// ============================================================================
// Window Operations
// ============================================================================

/**
 * @brief Operations for Window types (TSW storage with dual cyclic buffers).
 *
 * Key behaviors:
 * - Fixed capacity set at type creation (stored in schema->fixed_size)
 * - Allocates capacity+1 slots to preserve removed value
 * - push_back accepts (value, time) and may evict oldest element
 * - Separate accessors for values, times, and removed value/time
 */
struct WindowStorageOps {
    // ========== Helper Functions ==========

    static size_t get_element_size(const TypeMeta* schema) {
        return schema->element_type ? schema->element_type->size : 0;
    }

    /// Convert logical index to physical storage index
    /// Physical buffer has capacity+1 slots
    static size_t to_physical_index(const WindowStorage* storage, size_t logical_index) {
        return (storage->head + logical_index) % (storage->capacity + 1);
    }

    /// Get the physical index of the removed slot (one before head in circular order)
    static size_t removed_slot_index(const WindowStorage* storage) {
        // The removed slot is at (head - 1 + capacity + 1) % (capacity + 1)
        // which simplifies to (head + capacity) % (capacity + 1)
        return (storage->head + storage->capacity) % (storage->capacity + 1);
    }

    static void* get_value_ptr(void* obj, size_t logical_index, const TypeMeta* schema) {
        auto* storage = static_cast<WindowStorage*>(obj);
        size_t elem_size = get_element_size(schema);
        size_t physical = to_physical_index(storage, logical_index);
        return static_cast<char*>(storage->values_data) + physical * elem_size;
    }

    static const void* get_value_ptr_const(const void* obj, size_t logical_index, const TypeMeta* schema) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        size_t elem_size = get_element_size(schema);
        size_t physical = to_physical_index(storage, logical_index);
        return static_cast<const char*>(storage->values_data) + physical * elem_size;
    }

    static engine_time_t* get_time_ptr(void* obj, size_t logical_index) {
        auto* storage = static_cast<WindowStorage*>(obj);
        size_t physical = to_physical_index(storage, logical_index);
        return &storage->times_data[physical];
    }

    static const engine_time_t* get_time_ptr_const(const void* obj, size_t logical_index) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        size_t physical = to_physical_index(storage, logical_index);
        return &storage->times_data[physical];
    }

    // ========== Core Operations ==========

    static void construct(void* dst, const TypeMeta* schema) {
        auto* storage = new (dst) WindowStorage();
        storage->capacity = schema->fixed_size;
        storage->min_size = schema->min_size;
        storage->size = 0;
        storage->head = 0;
        storage->has_removed = false;

        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;
        size_t alloc_slots = storage->capacity + 1;  // +1 for removed value slot

        if (alloc_slots > 0 && elem_size > 0) {
            // Allocate values buffer
            storage->values_data = std::malloc(alloc_slots * elem_size);
            if (!storage->values_data) {
                throw std::bad_alloc();
            }

            // Allocate times buffer
            storage->times_data = static_cast<engine_time_t*>(
                std::malloc(alloc_slots * sizeof(engine_time_t)));
            if (!storage->times_data) {
                std::free(storage->values_data);
                storage->values_data = nullptr;
                throw std::bad_alloc();
            }

            // Construct all value elements
            for (size_t i = 0; i < alloc_slots; ++i) {
                void* elem_ptr = static_cast<char*>(storage->values_data) + i * elem_size;
                if (elem_type->ops && elem_type->ops->construct) {
                    elem_type->ops->construct(elem_ptr, elem_type);
                }
                // Initialize times to MIN_DT
                storage->times_data[i] = MIN_DT;
            }
        }
    }

    static void destruct(void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<WindowStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;
        size_t alloc_slots = storage->capacity + 1;

        // Destruct all value elements
        if (storage->values_data && elem_type) {
            for (size_t i = 0; i < alloc_slots; ++i) {
                void* elem_ptr = static_cast<char*>(storage->values_data) + i * elem_size;
                if (elem_type->ops && elem_type->ops->destruct) {
                    elem_type->ops->destruct(elem_ptr, elem_type);
                }
            }
            std::free(storage->values_data);
        }

        if (storage->times_data) {
            std::free(storage->times_data);
        }

        storage->~WindowStorage();
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<WindowStorage*>(dst);
        auto* src_storage = static_cast<const WindowStorage*>(src);
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;
        size_t alloc_slots = dst_storage->capacity + 1;

        // Copy metadata
        dst_storage->size = src_storage->size;
        dst_storage->head = src_storage->head;
        dst_storage->has_removed = src_storage->has_removed;

        // Copy all elements (physical layout)
        if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
            for (size_t i = 0; i < alloc_slots; ++i) {
                void* dst_elem = static_cast<char*>(dst_storage->values_data) + i * elem_size;
                const void* src_elem = static_cast<const char*>(src_storage->values_data) + i * elem_size;
                elem_type->ops->copy_assign(dst_elem, src_elem, elem_type);
            }
        }

        // Copy times
        std::memcpy(dst_storage->times_data, src_storage->times_data,
                    alloc_slots * sizeof(engine_time_t));
    }

    static void move_assign(void* dst, void* src, const TypeMeta* schema) {
        auto* dst_storage = static_cast<WindowStorage*>(dst);
        auto* src_storage = static_cast<WindowStorage*>(src);
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;
        size_t alloc_slots = dst_storage->capacity + 1;

        // Destruct dst elements
        if (dst_storage->values_data && elem_type) {
            for (size_t i = 0; i < alloc_slots; ++i) {
                void* elem_ptr = static_cast<char*>(dst_storage->values_data) + i * elem_size;
                if (elem_type->ops && elem_type->ops->destruct) {
                    elem_type->ops->destruct(elem_ptr, elem_type);
                }
            }
            std::free(dst_storage->values_data);
        }
        if (dst_storage->times_data) {
            std::free(dst_storage->times_data);
        }

        // Move ownership from src to dst
        dst_storage->values_data = src_storage->values_data;
        dst_storage->times_data = src_storage->times_data;
        dst_storage->capacity = src_storage->capacity;
        dst_storage->min_size = src_storage->min_size;
        dst_storage->size = src_storage->size;
        dst_storage->head = src_storage->head;
        dst_storage->has_removed = src_storage->has_removed;

        // Reset source
        src_storage->values_data = nullptr;
        src_storage->times_data = nullptr;
        src_storage->capacity = 0;
        src_storage->min_size = 0;
        src_storage->size = 0;
        src_storage->head = 0;
        src_storage->has_removed = false;
    }

    static void move_construct(void* dst, void* src, const TypeMeta* /*schema*/) {
        auto* src_storage = static_cast<WindowStorage*>(src);
        auto* dst_storage = new (dst) WindowStorage();

        // Move ownership
        dst_storage->values_data = src_storage->values_data;
        dst_storage->times_data = src_storage->times_data;
        dst_storage->capacity = src_storage->capacity;
        dst_storage->min_size = src_storage->min_size;
        dst_storage->size = src_storage->size;
        dst_storage->head = src_storage->head;
        dst_storage->has_removed = src_storage->has_removed;

        // Reset source
        src_storage->values_data = nullptr;
        src_storage->times_data = nullptr;
        src_storage->capacity = 0;
        src_storage->min_size = 0;
        src_storage->size = 0;
        src_storage->head = 0;
        src_storage->has_removed = false;
    }

    static bool equals(const void* a, const void* b, const TypeMeta* schema) {
        auto* storage_a = static_cast<const WindowStorage*>(a);
        auto* storage_b = static_cast<const WindowStorage*>(b);
        const TypeMeta* elem_type = schema->element_type;

        if (storage_a->size != storage_b->size) return false;

        // Compare elements in logical order
        for (size_t i = 0; i < storage_a->size; ++i) {
            const void* elem_a = get_value_ptr_const(a, i, schema);
            const void* elem_b = get_value_ptr_const(b, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->equals) {
                if (!elem_type->ops->equals(elem_a, elem_b, elem_type)) {
                    return false;
                }
            }
            // Also compare times
            if (*get_time_ptr_const(a, i) != *get_time_ptr_const(b, i)) {
                return false;
            }
        }
        return true;
    }

    static std::string to_string(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        std::string result = "Window[";

        for (size_t i = 0; i < storage->size; ++i) {
            if (i > 0) result += ", ";
            const void* elem_ptr = get_value_ptr_const(obj, i, schema);
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

    /**
     * @brief Convert to Python - returns only the values list.
     *
     * Times are accessed separately via get_times_python().
     */
    static nb::object to_python(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const WindowStorage*>(obj);

        // Return None if window is below min_size (not yet "valid")
        if (storage->size < storage->min_size) {
            return nb::none();
        }

        const TypeMeta* elem_type = schema->element_type;
        nb::list result;

        // Return values in logical order (re-centered)
        for (size_t i = 0; i < storage->size; ++i) {
            const void* elem_ptr = get_value_ptr_const(obj, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->to_python) {
                result.append(elem_type->ops->to_python(elem_ptr, elem_type));
            } else {
                result.append(nb::none());
            }
        }
        return result;
    }

    /**
     * @brief Convert from Python - NOT SUPPORTED for direct from_python.
     *
     * Windows must use push_back with explicit (value, time) pairs.
     * This method throws if called directly.
     */
    static void from_python(void* /*dst*/, const nb::object& /*src*/, const TypeMeta* /*schema*/) {
        throw std::runtime_error(
            "Window.from_python: direct assignment not supported. "
            "Use push_back with (value, time) instead.");
    }

    // ========== Hashable Operations ==========

    static size_t hash(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        size_t result = 0;

        for (size_t i = 0; i < storage->size; ++i) {
            const void* elem_ptr = get_value_ptr_const(obj, i, schema);
            if (elem_type && elem_type->ops && elem_type->ops->hash) {
                size_t elem_hash = elem_type->ops->hash(elem_ptr, elem_type);
                result ^= elem_hash + 0x9e3779b9 + (result << 6) + (result >> 2);
            }
            // Also include time in hash
            auto time_us = get_time_ptr_const(obj, i)->time_since_epoch().count();
            result ^= std::hash<decltype(time_us)>{}(time_us) + 0x9e3779b9 + (result << 6) + (result >> 2);
        }
        return result;
    }

    // ========== Iterable Operations ==========

    static size_t size(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        return storage->size;
    }

    // ========== Indexable Operations ==========

    static const void* get_at(const void* obj, size_t index, const TypeMeta* schema) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        if (index >= storage->size) {
            throw std::out_of_range("Window index out of range");
        }
        return get_value_ptr_const(obj, index, schema);
    }

    static void set_at(void* obj, size_t index, const void* value, const TypeMeta* schema) {
        auto* storage = static_cast<WindowStorage*>(obj);
        if (index >= storage->size) {
            throw std::out_of_range("Window index out of range");
        }
        void* elem_ptr = get_value_ptr(obj, index, schema);
        const TypeMeta* elem_type = schema->element_type;
        if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
            elem_type->ops->copy_assign(elem_ptr, value, elem_type);
        }
    }

    // ========== Window-Specific Operations ==========

    /**
     * @brief Push a value with timestamp to the window.
     *
     * If buffer is not full, increments size and adds at the end.
     * If buffer is full, the oldest value is moved to the removed slot,
     * then overwritten with the new value, and head advances.
     *
     * @param obj Pointer to WindowStorage
     * @param value The value to push (must match element_type)
     * @param time The timestamp associated with this value
     * @param schema The TypeMeta for this Window type
     */
    static void push_back(void* obj, const void* value, engine_time_t time, const TypeMeta* schema) {
        auto* storage = static_cast<WindowStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;
        size_t elem_size = elem_type ? elem_type->size : 0;
        size_t alloc_slots = storage->capacity + 1;

        if (storage->size < storage->capacity) {
            // Buffer not full: add at end
            size_t physical = to_physical_index(storage, storage->size);
            void* elem_ptr = static_cast<char*>(storage->values_data) + physical * elem_size;
            if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
                elem_type->ops->copy_assign(elem_ptr, value, elem_type);
            }
            storage->times_data[physical] = time;
            storage->size++;
            storage->has_removed = false;
        } else {
            // Buffer full: current head becomes the removed slot
            // The value at head is already in place - it becomes the removed value
            // We advance head, which makes the old head accessible as the removed slot

            // Advance head (wraps in alloc_slots space)
            storage->head = (storage->head + 1) % alloc_slots;

            // Now write new value at the new "last" position
            // The new last position is (new_head + capacity - 1) % alloc_slots
            // Example: capacity=3, alloc_slots=4, old_head=0, new_head=1
            //   new_last = (1 + 3 - 1) % 4 = 3
            size_t new_last_physical = (storage->head + storage->capacity - 1) % alloc_slots;
            void* new_elem_ptr = static_cast<char*>(storage->values_data) + new_last_physical * elem_size;
            if (elem_type && elem_type->ops && elem_type->ops->copy_assign) {
                elem_type->ops->copy_assign(new_elem_ptr, value, elem_type);
            }
            storage->times_data[new_last_physical] = time;
            storage->has_removed = true;
        }
    }

    /**
     * @brief Push a value from Python with timestamp.
     *
     * Convenience method that converts the Python object to C++ value.
     */
    static void push_back_python(void* obj, const nb::object& py_value, engine_time_t time, const TypeMeta* schema) {
        auto* storage = static_cast<WindowStorage*>(obj);
        const TypeMeta* elem_type = schema->element_type;

        if (!elem_type || !elem_type->ops || !elem_type->ops->from_python) {
            throw std::runtime_error("Window element type does not support from_python");
        }

        // Use a temporary buffer to convert Python value
        size_t elem_size = elem_type->size;
        alignas(std::max_align_t) char temp_buffer[256];
        void* temp = (elem_size <= sizeof(temp_buffer)) ? temp_buffer : std::malloc(elem_size);

        try {
            // Construct temporary
            if (elem_type->ops->construct) {
                elem_type->ops->construct(temp, elem_type);
            }

            // Convert from Python
            elem_type->ops->from_python(temp, py_value, elem_type);

            // Push the converted value
            push_back(obj, temp, time, schema);

            // Destruct temporary
            if (elem_type->ops->destruct) {
                elem_type->ops->destruct(temp, elem_type);
            }
        } catch (...) {
            if (temp != temp_buffer && temp != nullptr) {
                std::free(temp);
            }
            throw;
        }

        if (temp != temp_buffer && temp != nullptr) {
            std::free(temp);
        }
    }

    /**
     * @brief Clear all elements from the window.
     */
    static void clear(void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<WindowStorage*>(obj);
        storage->size = 0;
        storage->head = 0;
        storage->has_removed = false;
    }

    /**
     * @brief Get the capacity (max size) of the window.
     */
    static size_t capacity(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        return storage->capacity;
    }

    /**
     * @brief Check if window is full (size == capacity).
     */
    static bool full(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        return storage->size == storage->capacity;
    }

    /**
     * @brief Check if a value was removed on the last push.
     */
    static bool has_removed_value(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        return storage->has_removed;
    }

    /**
     * @brief Get the removed value (only valid if has_removed_value() is true).
     *
     * The removed value is stored at the slot just before head.
     */
    static const void* get_removed_value(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        if (!storage->has_removed) {
            return nullptr;
        }
        size_t elem_size = get_element_size(schema);
        size_t removed_physical = removed_slot_index(storage);
        return static_cast<const char*>(storage->values_data) + removed_physical * elem_size;
    }

    /**
     * @brief Get the timestamp of the removed value.
     */
    static engine_time_t get_removed_time(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        if (!storage->has_removed) {
            return MIN_DT;
        }
        size_t removed_physical = removed_slot_index(storage);
        return storage->times_data[removed_physical];
    }

    /**
     * @brief Get removed value as Python object.
     */
    static nb::object get_removed_value_python(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        if (!storage->has_removed) {
            return nb::none();
        }
        const TypeMeta* elem_type = schema->element_type;
        const void* removed_ptr = get_removed_value(obj, schema);
        if (elem_type && elem_type->ops && elem_type->ops->to_python && removed_ptr) {
            return elem_type->ops->to_python(removed_ptr, elem_type);
        }
        return nb::none();
    }

    /**
     * @brief Get timestamp at logical index.
     */
    static engine_time_t get_time_at(const void* obj, size_t index, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        if (index >= storage->size) {
            throw std::out_of_range("Window time index out of range");
        }
        return *get_time_ptr_const(obj, index);
    }

    /**
     * @brief Get all timestamps as Python tuple.
     */
    static nb::object get_times_python(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        nb::list result;
        for (size_t i = 0; i < storage->size; ++i) {
            result.append(nb::cast(*get_time_ptr_const(obj, i)));
        }
        return result;
    }

    /**
     * @brief Get the most recent value (delta_value).
     *
     * Returns the newest element in the window (logical index size-1).
     */
    static const void* get_newest_value(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        if (storage->size == 0) {
            return nullptr;
        }
        return get_value_ptr_const(obj, storage->size - 1, schema);
    }

    /**
     * @brief Get the newest timestamp.
     */
    static engine_time_t get_newest_time(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        if (storage->size == 0) {
            return MIN_DT;
        }
        return *get_time_ptr_const(obj, storage->size - 1);
    }

    /**
     * @brief Get newest value as Python object.
     */
    static nb::object get_newest_value_python(const void* obj, const TypeMeta* schema) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        if (storage->size == 0) {
            return nb::none();
        }
        const TypeMeta* elem_type = schema->element_type;
        const void* newest_ptr = get_newest_value(obj, schema);
        if (elem_type && elem_type->ops && elem_type->ops->to_python && newest_ptr) {
            return elem_type->ops->to_python(newest_ptr, elem_type);
        }
        return nb::none();
    }

    /**
     * @brief Get the oldest timestamp (first_modified_time).
     */
    static engine_time_t get_oldest_time(const void* obj, const TypeMeta* /*schema*/) {
        auto* storage = static_cast<const WindowStorage*>(obj);
        if (storage->size == 0) {
            return MIN_DT;
        }
        return *get_time_ptr_const(obj, 0);
    }

    /// Get the operations vtable for Window types
    static const TypeOps* ops() {
        static const TypeOps window_ops = {
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
            nullptr,   // less_than (windows not ordered)
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
            nullptr,   // resize (fixed capacity)
            &clear,
        };
        return &window_ops;
    }
};

} // namespace hgraph::value
