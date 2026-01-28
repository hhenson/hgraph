#pragma once

/**
 * @file ts_meta.h
 * @brief Time-series type metadata structures.
 *
 * TSMeta describes the schema of a time-series type: its kind, value type,
 * nested time-series types, and for TSB, field information. These structures
 * are immutable after creation and managed by TSTypeRegistry.
 *
 * Thread Safety: TSMeta structures are immutable after creation.
 * TSTypeRegistry handles thread-safe creation and caching.
 */

#include <hgraph/types/value/type_meta.h>
#include <hgraph/util/date_time.h>

#include <nanobind/nanobind.h>

#include <cstddef>
#include <cstdint>

namespace nb = nanobind;

namespace hgraph {

// Forward declarations for self-referential types
struct TSMeta;
struct TSBFieldInfo;

// ============================================================================
// TSKind Enumeration
// ============================================================================

/**
 * @brief Categories of time-series types.
 *
 * Each time-series type falls into one of these categories, which determines
 * what properties are valid in TSMeta and how the time-series behaves.
 */
enum class TSKind : uint8_t {
    TSValue,     ///< TS[T] - scalar time-series
    TSS,         ///< TSS[T] - time-series set
    TSD,         ///< TSD[K, V] - time-series dict
    TSL,         ///< TSL[TS, Size] - time-series list
    TSW,         ///< TSW[T, size, min_size] - time-series window
    TSB,         ///< TSB[Schema] - time-series bundle
    REF,         ///< REF[TS] - reference to time-series
    SIGNAL       ///< SIGNAL - presence/absence marker
};

// ============================================================================
// TSB Field Information
// ============================================================================

/**
 * @brief Metadata for a single field in a TSB (time-series bundle).
 *
 * Each field has a name, index (position), and a pointer to the field's
 * time-series schema. Field names and TSMeta pointers are owned by TSTypeRegistry
 * and remain stable for the process lifetime.
 */
struct TSBFieldInfo {
    const char* name;        ///< Field name (owned by registry)
    size_t index;            ///< 0-based field index
    const TSMeta* ts_type;   ///< Field's time-series schema
};

// ============================================================================
// Time-Series Metadata
// ============================================================================

/**
 * @brief Complete metadata describing a time-series type.
 *
 * TSMeta is the schema for a time-series type. It uses a tagged union approach
 * where the `kind` field determines which members are valid:
 *
 * - TSValue: value_type is valid
 * - TSS: value_type is valid (set element type)
 * - TSD: key_type, element_ts are valid
 * - TSL: element_ts, fixed_size are valid
 * - TSW: value_type, is_duration_based, window union are valid
 * - TSB: fields, field_count, bundle_name, python_type are valid
 * - REF: element_ts is valid (referenced time-series)
 * - SIGNAL: no additional fields
 */
struct TSMeta {
    TSKind kind;

    // ========== Value/Key Types ==========
    // Valid for: TSValue (value), TSS (element), TSW (value), TSD (key)

    /// Value type - valid for: TSValue, TSS, TSW
    const value::TypeMeta* value_type = nullptr;

    /// Key type - valid for: TSD
    const value::TypeMeta* key_type = nullptr;

    // ========== Nested Time-Series ==========
    // Valid for: TSD (value TS), TSL (element TS), REF (referenced TS)

    /// Element time-series - valid for: TSD (value), TSL (element), REF (referenced)
    const TSMeta* element_ts = nullptr;

    // ========== Size Information ==========

    /// Fixed size - valid for: TSL (0 = dynamic SIZE)
    size_t fixed_size = 0;

    // ========== Window Parameters ==========
    // Valid for: TSW

    /// True if duration-based window, false if tick-based
    bool is_duration_based = false;

    /// Window parameters union - saves space since only one is used
    union WindowParams {
        struct {
            size_t period;
            size_t min_period;
        } tick;
        struct {
            engine_time_delta_t time_range;
            engine_time_delta_t min_time_range;
        } duration;

        // Default constructor - initialize tick params
        WindowParams() : tick{0, 0} {}
    } window;

    // ========== Bundle Fields ==========
    // Valid for: TSB

    /// Field metadata array - valid for: TSB
    const TSBFieldInfo* fields = nullptr;

    /// Number of fields - valid for: TSB
    size_t field_count = 0;

    /// Bundle schema name - valid for: TSB
    const char* bundle_name = nullptr;

    /// Python type for reconstruction - valid for: TSB (optional)
    /// When set, to_python conversion returns an instance of this class.
    /// When not set (None), returns a dict.
    nb::object python_type;

    // ========== Helper Methods ==========

    /**
     * @brief Check if this is a collection time-series.
     * @return true if TSS, TSD, TSL, or TSB
     */
    [[nodiscard]] bool is_collection() const noexcept {
        return kind == TSKind::TSS || kind == TSKind::TSD ||
               kind == TSKind::TSL || kind == TSKind::TSB;
    }

    /**
     * @brief Check if this is a scalar-like time-series.
     * @return true if TS, TSW, or SIGNAL
     */
    [[nodiscard]] bool is_scalar_ts() const noexcept {
        return kind == TSKind::TSValue || kind == TSKind::TSW ||
               kind == TSKind::SIGNAL;
    }
};

} // namespace hgraph
