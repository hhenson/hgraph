//
// Created by Claude on 15/12/2025.
//
// TimeSeriesTypeMeta - Type metadata for time-series types (TS, TSS, TSD, TSL, TSB, TSW)
//
// This is separate from value::TypeMeta which handles value types.
// Time-series types are wrappers/descriptors for type introspection,
// not value storage, so they don't need construct/destruct/copy operations.
//

#ifndef HGRAPH_TS_TYPE_META_H
#define HGRAPH_TS_TYPE_META_H

#include <hgraph/types/value/type_meta.h>
#include <string>
#include <vector>
#include <memory>

namespace hgraph {

/**
 * TimeSeriesKind - Classification of time-series types
 */
enum class TimeSeriesKind : uint8_t {
    TS,      // Single scalar time-series: TS[T]
    TSS,     // Time-series set: TSS[T]
    TSD,     // Time-series dict: TSD[K, V]
    TSL,     // Time-series list: TSL[V, Size]
    TSB,     // Time-series bundle: TSB[Schema]
    TSW,     // Time-series window: TSW[T, Size]
};

/**
 * TimeSeriesTypeMeta - Base structure for all time-series type metadata
 *
 * Unlike value::TypeMeta, this only provides type introspection capabilities.
 * Time-series objects are created and managed by the runtime, not by TypeMeta ops.
 */
struct TimeSeriesTypeMeta {
    TimeSeriesKind ts_kind;
    const char* name{nullptr};

    virtual ~TimeSeriesTypeMeta() = default;

    /**
     * Generate a type name string (e.g., "TS[int]", "TSD[str, TS[float]]")
     */
    [[nodiscard]] virtual std::string type_name_str() const = 0;
};

/**
 * TSTypeMeta - TS[T] single scalar time-series
 *
 * Represents a time-series that holds a single scalar value of type T.
 */
struct TSTypeMeta : TimeSeriesTypeMeta {
    const value::TypeMeta* scalar_type;

    [[nodiscard]] std::string type_name_str() const override;
};

/**
 * TSSTypeMeta - TSS[T] time-series set
 *
 * Represents a time-series that tracks additions and removals of elements of type T.
 */
struct TSSTypeMeta : TimeSeriesTypeMeta {
    const value::TypeMeta* element_type;

    [[nodiscard]] std::string type_name_str() const override;
};

/**
 * TSDTypeMeta - TSD[K, V] time-series dict
 *
 * Represents a time-series dictionary with scalar keys K and time-series values V.
 * The value type is itself a time-series (typically TS[T]).
 */
struct TSDTypeMeta : TimeSeriesTypeMeta {
    const value::TypeMeta* key_type;
    const TimeSeriesTypeMeta* value_ts_type;

    [[nodiscard]] std::string type_name_str() const override;
};

/**
 * TSLTypeMeta - TSL[V, Size] time-series list
 *
 * Represents a fixed-size list of time-series elements.
 * Size is -1 for unresolved/dynamic size.
 */
struct TSLTypeMeta : TimeSeriesTypeMeta {
    const TimeSeriesTypeMeta* element_ts_type;
    int64_t size;  // -1 = dynamic/unresolved

    [[nodiscard]] std::string type_name_str() const override;
};

/**
 * TSBTypeMeta - TSB[Schema] time-series bundle
 *
 * Represents a structured bundle of named time-series fields.
 */
struct TSBTypeMeta : TimeSeriesTypeMeta {
    struct Field {
        std::string name;
        const TimeSeriesTypeMeta* type;
    };
    std::vector<Field> fields;

    [[nodiscard]] std::string type_name_str() const override;
};

/**
 * TSWTypeMeta - TSW[T, Size] time-series window
 *
 * Represents a windowed view of scalar values, either by count or by time duration.
 */
struct TSWTypeMeta : TimeSeriesTypeMeta {
    const value::TypeMeta* scalar_type;
    int64_t size;       // count for fixed, -1 for time-based
    int64_t min_size;   // min count, -1 for unspecified

    [[nodiscard]] std::string type_name_str() const override;
};

} // namespace hgraph

#endif // HGRAPH_TS_TYPE_META_H
