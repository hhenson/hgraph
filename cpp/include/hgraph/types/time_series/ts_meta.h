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
#include <cstring>
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
        nb::object python_type{};
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

        KindData() : empty{} {}
        ~KindData() {}  // Owner (TSMeta) manages active member destruction
    };

    TSKind kind = TSKind::SIGNAL;
    const value::TypeMeta* value_type = nullptr;
    KindData data{};

    // TSMeta is always heap-allocated and pointer-referenced; non-copyable due to nb::object in union.
    TSMeta() = default;
    ~TSMeta() { destroy_active_member(); }

    TSMeta(const TSMeta&) = delete;
    TSMeta& operator=(const TSMeta&) = delete;

    TSMeta(TSMeta&& other) noexcept : kind(other.kind), value_type(other.value_type) {
        move_data_from(std::move(other));
    }
    TSMeta& operator=(TSMeta&& other) noexcept {
        if (this != &other) {
            destroy_active_member();
            kind = other.kind;
            value_type = other.value_type;
            move_data_from(std::move(other));
        }
        return *this;
    }

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
                 nb::object python_type) {
        new (&data.tsb) TSBData{fields, field_count, bundle_name, std::move(python_type)};
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

    [[nodiscard]] const nb::object& python_type() const noexcept {
        static const nb::object none_obj{};
        return kind == TSKind::TSB ? data.tsb.python_type : none_obj;
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

private:
    void destroy_active_member() {
        if (kind == TSKind::TSB) {
            data.tsb.~TSBData();
        }
    }

    void move_data_from(TSMeta&& other) noexcept {
        switch (other.kind) {
            case TSKind::TSValue:
            case TSKind::SIGNAL:
                new (&data.empty) EmptyData{};
                break;

            case TSKind::TSS:
                new (&data.empty) EmptyData{};
                break;

            case TSKind::TSD:
                new (&data.tsd) TSDData{other.data.tsd};
                break;

            case TSKind::TSL:
                new (&data.tsl) TSLData{other.data.tsl};
                break;

            case TSKind::TSW:
                new (&data.tsw) TSWData{other.data.tsw};
                break;

            case TSKind::TSB:
                new (&data.tsb) TSBData{std::move(other.data.tsb)};
                break;

            case TSKind::REF:
                new (&data.ref) REFData{other.data.ref};
                break;
        }

        reset_moved_from(other);
    }

    static void reset_moved_from(TSMeta& other) noexcept {
        if (other.kind == TSKind::TSB) {
            other.data.tsb.~TSBData();
        }
        new (&other.data.empty) EmptyData{};
        other.kind = TSKind::SIGNAL;
        other.value_type = nullptr;
    }
};

[[nodiscard]] inline bool equivalent_ts_schema(const TSMeta* lhs, const TSMeta* rhs) noexcept
{
    if (lhs == rhs) { return true; }
    if (lhs == nullptr || rhs == nullptr) { return false; }
    if (lhs->kind != rhs->kind) { return false; }

    switch (lhs->kind) {
        case TSKind::TSValue:
        case TSKind::TSS:
            return lhs->value_type == rhs->value_type;

        case TSKind::SIGNAL:
            return true;

        case TSKind::TSD:
            return lhs->key_type() == rhs->key_type() && equivalent_ts_schema(lhs->element_ts(), rhs->element_ts());

        case TSKind::TSL:
            return lhs->fixed_size() == rhs->fixed_size() && equivalent_ts_schema(lhs->element_ts(), rhs->element_ts());

        case TSKind::TSW:
            if (lhs->value_type != rhs->value_type) { return false; }
            if (lhs->is_duration_based() != rhs->is_duration_based()) { return false; }
            if (lhs->is_duration_based()) {
                return lhs->time_range() == rhs->time_range() && lhs->min_time_range() == rhs->min_time_range();
            }
            return lhs->period() == rhs->period() && lhs->min_period() == rhs->min_period();

        case TSKind::TSB:
            if (lhs->field_count() != rhs->field_count()) { return false; }
            for (size_t i = 0; i < lhs->field_count(); ++i) {
                const TSBFieldInfo &lhs_field = lhs->fields()[i];
                const TSBFieldInfo &rhs_field = rhs->fields()[i];
                if (lhs_field.index != rhs_field.index) { return false; }
                if ((lhs_field.name == nullptr) != (rhs_field.name == nullptr)) { return false; }
                if (lhs_field.name != nullptr && std::strcmp(lhs_field.name, rhs_field.name) != 0) { return false; }
                if (!equivalent_ts_schema(lhs_field.ts_type, rhs_field.ts_type)) { return false; }
            }
            return true;

        case TSKind::REF:
            return equivalent_ts_schema(lhs->element_ts(), rhs->element_ts());
    }

    return false;
}

[[nodiscard]] inline bool binding_compatible_ts_schema(const TSMeta* lhs, const TSMeta* rhs) noexcept
{
    if (equivalent_ts_schema(lhs, rhs)) { return true; }
    if (lhs == nullptr || rhs == nullptr) { return false; }

    const auto compatible_element = [](const TSMeta* left, const TSMeta* right, auto&& self) noexcept -> bool {
        if (equivalent_ts_schema(left, right)) { return true; }
        if (left == nullptr || right == nullptr) { return false; }
        if (left->kind == TSKind::REF) { return self(left->element_ts(), right, self); }
        if (right->kind == TSKind::REF) { return self(left, right->element_ts(), self); }
        return false;
    };

    if (lhs->kind != rhs->kind) { return false; }

    switch (lhs->kind) {
        case TSKind::TSD:
            return lhs->key_type() == rhs->key_type() &&
                   compatible_element(lhs->element_ts(), rhs->element_ts(), compatible_element);

        case TSKind::TSL:
            return lhs->fixed_size() == rhs->fixed_size() &&
                   compatible_element(lhs->element_ts(), rhs->element_ts(), compatible_element);

        case TSKind::TSB:
            if (lhs->field_count() != rhs->field_count()) { return false; }
            for (size_t i = 0; i < lhs->field_count(); ++i) {
                const TSBFieldInfo &lhs_field = lhs->fields()[i];
                const TSBFieldInfo &rhs_field = rhs->fields()[i];
                if (lhs_field.index != rhs_field.index) { return false; }
                if ((lhs_field.name == nullptr) != (rhs_field.name == nullptr)) { return false; }
                if (lhs_field.name != nullptr && std::strcmp(lhs_field.name, rhs_field.name) != 0) { return false; }
                if (!compatible_element(lhs_field.ts_type, rhs_field.ts_type, compatible_element)) { return false; }
            }
            return true;

        default:
            return false;
    }
}

} // namespace hgraph
