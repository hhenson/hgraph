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
#include <utility>

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
 * TSMeta is a compact tagged structure:
 * - `kind` + `value_type` are always present
 * - kind-specific data is stored in a union to minimize per-instance footprint.
 */
struct TSMeta {
    union WindowParams {
        struct {
            size_t period;
            size_t min_period;
        } tick;
        struct {
            engine_time_delta_t time_range;
            engine_time_delta_t min_time_range;
        } duration;

        constexpr WindowParams() : tick{0, 0} {}
    };

    struct EmptyData {};
    struct TSDData {
        const value::TypeMeta* key_type = nullptr;
        const TSMeta* value_ts = nullptr;
    };
    struct TSLData {
        const TSMeta* element_ts = nullptr;
        size_t fixed_size = 0;
    };
    struct TSWData {
        bool is_duration_based = false;
        WindowParams window{};
    };
    struct TSBData {
        const TSBFieldInfo* fields = nullptr;
        size_t field_count = 0;
        const char* bundle_name = nullptr;
        const nb::object* python_type = nullptr;
    };
    struct REFData {
        const TSMeta* referenced_ts = nullptr;
    };

    union KindData {
        EmptyData empty;
        TSDData tsd;
        TSLData tsl;
        TSWData tsw;
        TSBData tsb;
        REFData ref;

        constexpr KindData() : empty{} {}
    };

    TSKind kind = TSKind::SIGNAL;
    const value::TypeMeta* value_type = nullptr;
    KindData data{};

    void set_tsd(const value::TypeMeta* key_type, const TSMeta* value_ts) noexcept {
        data.tsd = TSDData{key_type, value_ts};
    }
    void set_tsl(const TSMeta* element_ts, size_t fixed_size) noexcept {
        data.tsl = TSLData{element_ts, fixed_size};
    }
    void set_tsw_tick(size_t period, size_t min_period) noexcept {
        data.tsw = TSWData{false, WindowParams{}};
        data.tsw.window.tick.period = period;
        data.tsw.window.tick.min_period = min_period;
    }
    void set_tsw_duration(engine_time_delta_t time_range, engine_time_delta_t min_time_range) noexcept {
        data.tsw = TSWData{true, WindowParams{}};
        data.tsw.window.duration.time_range = time_range;
        data.tsw.window.duration.min_time_range = min_time_range;
    }
    void set_tsb(const TSBFieldInfo* fields, size_t field_count, const char* bundle_name,
                 const nb::object* python_type) noexcept {
        data.tsb = TSBData{fields, field_count, bundle_name, python_type};
    }
    void set_ref(const TSMeta* referenced_ts) noexcept {
        data.ref = REFData{referenced_ts};
    }

    [[nodiscard]] const value::TypeMeta* key_type() const noexcept {
        return kind == TSKind::TSD ? data.tsd.key_type : nullptr;
    }

    [[nodiscard]] const TSMeta* element_ts() const noexcept {
        switch (kind) {
            case TSKind::TSD: return data.tsd.value_ts;
            case TSKind::TSL: return data.tsl.element_ts;
            case TSKind::REF: return data.ref.referenced_ts;
            default: return nullptr;
        }
    }

    [[nodiscard]] size_t fixed_size() const noexcept {
        return kind == TSKind::TSL ? data.tsl.fixed_size : 0;
    }

    [[nodiscard]] bool is_duration_based() const noexcept {
        return kind == TSKind::TSW && data.tsw.is_duration_based;
    }

    [[nodiscard]] size_t period() const noexcept {
        return (kind == TSKind::TSW && !data.tsw.is_duration_based) ? data.tsw.window.tick.period : 0;
    }

    [[nodiscard]] size_t min_period() const noexcept {
        return (kind == TSKind::TSW && !data.tsw.is_duration_based) ? data.tsw.window.tick.min_period : 0;
    }

    [[nodiscard]] engine_time_delta_t time_range() const noexcept {
        return (kind == TSKind::TSW && data.tsw.is_duration_based)
                   ? data.tsw.window.duration.time_range
                   : engine_time_delta_t{0};
    }

    [[nodiscard]] engine_time_delta_t min_time_range() const noexcept {
        return (kind == TSKind::TSW && data.tsw.is_duration_based)
                   ? data.tsw.window.duration.min_time_range
                   : engine_time_delta_t{0};
    }

    [[nodiscard]] const TSBFieldInfo* fields() const noexcept {
        return kind == TSKind::TSB ? data.tsb.fields : nullptr;
    }

    [[nodiscard]] size_t field_count() const noexcept {
        return kind == TSKind::TSB ? data.tsb.field_count : 0;
    }

    [[nodiscard]] const char* bundle_name() const noexcept {
        return kind == TSKind::TSB ? data.tsb.bundle_name : nullptr;
    }

    [[nodiscard]] const nb::object* python_type_ptr() const noexcept {
        return kind == TSKind::TSB ? data.tsb.python_type : nullptr;
    }

    [[nodiscard]] nb::object python_type() const {
        const nb::object* obj = python_type_ptr();
        return obj != nullptr ? *obj : nb::none();
    }

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
