//
// Created by Claude on 15/12/2025.
//
// TimeSeriesTypeMeta - Type metadata for time-series types (TS, TSS, TSD, TSL, TSB, TSW)
//
// This is separate from value::TypeMeta which handles value types.
// The type meta itself acts as a builder - it can efficiently construct instances
// of the time-series type it represents.
//

#ifndef HGRAPH_TS_TYPE_META_H
#define HGRAPH_TS_TYPE_META_H

#include <hgraph/hgraph_forward_declarations.h>
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
    REF,     // Time-series reference: REF[TS_TYPE]
};

/**
 * TimeSeriesTypeMeta - Base structure for all time-series type metadata
 *
 * The type meta acts as both a type descriptor and a builder.
 * Each concrete type implements make_output/make_input to efficiently
 * construct instances of the time-series type.
 */
struct TimeSeriesTypeMeta {
    TimeSeriesKind ts_kind;
    const char* name{nullptr};

    virtual ~TimeSeriesTypeMeta() = default;

    /**
     * Generate a type name string (e.g., "TS[int]", "TSD[str, TS[float]]")
     */
    [[nodiscard]] virtual std::string type_name_str() const = 0;

    /**
     * Create an output time-series instance owned by a node.
     * The type meta knows how to efficiently construct the appropriate type.
     */
    [[nodiscard]] virtual time_series_output_s_ptr make_output(node_ptr owning_node) const = 0;

    /**
     * Create an output time-series instance owned by a parent time-series (for nested types).
     * Default implementation delegates to the node-based version using parent's node.
     */
    [[nodiscard]] virtual time_series_output_s_ptr make_output(time_series_output_ptr owning_output) const;

    /**
     * Create an input time-series instance owned by a node.
     * The type meta knows how to efficiently construct the appropriate type.
     */
    [[nodiscard]] virtual time_series_input_s_ptr make_input(node_ptr owning_node) const = 0;

    /**
     * Create an input time-series instance owned by a parent time-series (for nested types).
     * Default implementation delegates to the node-based version using parent's node.
     */
    [[nodiscard]] virtual time_series_input_s_ptr make_input(time_series_input_ptr owning_input) const;

    /**
     * Returns true if this type represents a reference type (REF[...])
     */
    [[nodiscard]] bool is_reference() const { return ts_kind == TimeSeriesKind::REF; }
};

/**
 * TSTypeMeta - TS[T] single scalar time-series
 *
 * Represents a time-series that holds a single scalar value of type T.
 */
struct TSTypeMeta : TimeSeriesTypeMeta {
    const value::TypeMeta* scalar_type;

    [[nodiscard]] std::string type_name_str() const override;
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr owning_node) const override;
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr owning_node) const override;
};

/**
 * TSSTypeMeta - TSS[T] time-series set
 *
 * Represents a time-series that tracks additions and removals of elements of type T.
 */
struct TSSTypeMeta : TimeSeriesTypeMeta {
    const value::TypeMeta* element_type;

    [[nodiscard]] std::string type_name_str() const override;
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr owning_node) const override;
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr owning_node) const override;
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
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr owning_node) const override;
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr owning_node) const override;
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
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr owning_node) const override;
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr owning_node) const override;
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
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr owning_node) const override;
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr owning_node) const override;
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
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr owning_node) const override;
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr owning_node) const override;
};

/**
 * REFTypeMeta - REF[TS_TYPE] time-series reference
 *
 * Represents a reference to another time-series type.
 * Used for lazy/deferred evaluation patterns.
 */
struct REFTypeMeta : TimeSeriesTypeMeta {
    const TimeSeriesTypeMeta* value_ts_type;

    [[nodiscard]] std::string type_name_str() const override;
    [[nodiscard]] time_series_output_s_ptr make_output(node_ptr owning_node) const override;
    [[nodiscard]] time_series_input_s_ptr make_input(node_ptr owning_node) const override;
};

} // namespace hgraph

#endif // HGRAPH_TS_TYPE_META_H
