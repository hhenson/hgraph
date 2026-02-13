#pragma once

/**
 * @file ts_reference_ops.h
 * @brief ScalarOps specialization for TSReference.
 *
 * This file provides the type operations needed to store TSReference
 * as a scalar value in the Value type system. Include this file
 * before calling TypeRegistry::register_scalar<TSReference>().
 */

#include <hgraph/types/time_series/ts_reference.h>
#include <hgraph/types/value/type_meta.h>

#include <nanobind/nanobind.h>

namespace nb = nanobind;

namespace hgraph::value {

/**
 * @brief ScalarOps specialization for TSReference.
 *
 * TSReference is a value type representing a reference to a time-series.
 * It can be stored in TSValue as the value for REF[TS[X]] types.
 *
 * Note on Python interop:
 * - to_python converts TSReference to Python TimeSeriesReference
 * - from_python converts Python TimeSeriesReference to TSReference
 * - These operations require access to runtime context (Graph, current_time)
 *   which is obtained from thread-local or passed explicitly.
 */
template<>
struct ScalarOps<TSReference> {
    static void construct(void* dst, const TypeMeta*) {
        new (dst) TSReference{};  // Default constructs as EMPTY
    }

    static void destruct(void* obj, const TypeMeta*) {
        static_cast<TSReference*>(obj)->~TSReference();
    }

    static void copy_assign(void* dst, const void* src, const TypeMeta*) {
        *static_cast<TSReference*>(dst) = *static_cast<const TSReference*>(src);
    }

    static void move_assign(void* dst, void* src, const TypeMeta*) {
        *static_cast<TSReference*>(dst) = std::move(*static_cast<TSReference*>(src));
    }

    static void move_construct(void* dst, void* src, const TypeMeta*) {
        new (dst) TSReference(std::move(*static_cast<TSReference*>(src)));
    }

    static bool equals(const void* a, const void* b, const TypeMeta*) {
        return *static_cast<const TSReference*>(a) == *static_cast<const TSReference*>(b);
    }

    static size_t hash(const void* obj, const TypeMeta*) {
        // TSReference is not hashable in the traditional sense
        // We provide a simple implementation based on kind and path validity
        const auto& ref = *static_cast<const TSReference*>(obj);
        size_t h = static_cast<size_t>(ref.kind());
        if (ref.is_peered() && ref.path().valid()) {
            // Hash based on node pointer and indices
            h ^= std::hash<void*>{}(static_cast<void*>(ref.path().node())) + 0x9e3779b9 + (h << 6) + (h >> 2);
            for (size_t idx : ref.path().indices()) {
                h ^= std::hash<size_t>{}(idx) + 0x9e3779b9 + (h << 6) + (h >> 2);
            }
        } else if (ref.is_non_peered()) {
            h ^= std::hash<size_t>{}(ref.size()) + 0x9e3779b9 + (h << 6) + (h >> 2);
        }
        return h;
    }

    static bool less_than(const void* a, const void* b, const TypeMeta*) {
        // TSReference doesn't have a natural ordering
        // Compare by kind first, then by hash for deterministic ordering
        const auto& ref_a = *static_cast<const TSReference*>(a);
        const auto& ref_b = *static_cast<const TSReference*>(b);
        if (ref_a.kind() != ref_b.kind()) {
            return static_cast<uint8_t>(ref_a.kind()) < static_cast<uint8_t>(ref_b.kind());
        }
        return hash(a, nullptr) < hash(b, nullptr);
    }

    static std::string to_string(const void* obj, const TypeMeta*) {
        return static_cast<const TSReference*>(obj)->to_string();
    }

    /**
     * @brief Convert TSReference to Python TimeSeriesReference.
     *
     * Converts:
     * - EMPTY → EmptyTimeSeriesReference
     * - PEERED → BoundTimeSeriesReference(resolved_output)
     * - NON_PEERED → UnBoundTimeSeriesReference(items)
     *
     * Note: This requires resolving paths to actual outputs, which needs
     * access to the current engine time. For now, we convert to FQReference
     * which can be reconstructed on the Python side.
     */
    static nb::object to_python(const void* obj, const TypeMeta*);

    /**
     * @brief Convert Python TimeSeriesReference to TSReference.
     *
     * Converts:
     * - EmptyTimeSeriesReference → EMPTY
     * - BoundTimeSeriesReference → PEERED (extracts path from output)
     * - UnBoundTimeSeriesReference → NON_PEERED (converts items)
     *
     * Note: Converting BoundTimeSeriesReference requires extracting the
     * ShortPath from the output, which needs the output to be a valid
     * TSOutput with a known path.
     */
    static void from_python(void* dst, const nb::object& src, const TypeMeta*);

    /// Get the operations vtable for TSReference
    static TypeOps make_ops() {
        return TypeOps{
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
            &less_than,
            nullptr,  // size (not iterable)
            nullptr,  // get_at (not indexable)
            nullptr,  // set_at (not indexable)
            nullptr,  // get_field (not bundle)
            nullptr,  // set_field (not bundle)
            nullptr,  // contains (not set)
            nullptr,  // insert (not set)
            nullptr,  // erase (not set)
            nullptr,  // map_get (not map)
            nullptr,  // map_set (not map)
            nullptr,  // resize (not resizable)
            nullptr,  // clear (not clearable)
        };
    }
};

/**
 * @brief Compute type flags for TSReference.
 *
 * TSReference is:
 * - NOT trivially constructible (has union with non-trivial members)
 * - NOT trivially destructible (has union with non-trivial members)
 * - NOT trivially copyable (has union with non-trivial members)
 * - Hashable (with caveats - see hash implementation)
 * - NOT truly comparable (less_than is synthetic for ordering)
 * - Equatable
 */
template<>
constexpr TypeFlags compute_scalar_flags<TSReference>() {
    return TypeFlags::Hashable | TypeFlags::Equatable;
}

} // namespace hgraph::value
