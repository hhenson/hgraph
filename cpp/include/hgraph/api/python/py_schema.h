//
// Created by Claude on 15/12/2025.
//
// PyTimeSeriesSchema - Python API adapter for time-series schema
//
// This class provides the schema interface required by PyTimeSeriesBundle.
// It can delegate to either:
// - Owned data (vector of keys + scalar_type) for legacy bundle types
// - TSBTypeMeta* for value-based bundle types (delegation pattern)
//

#pragma once

#include <hgraph/types/schema_type.h>
#include <vector>

namespace hgraph {

// Forward declaration
struct TSBTypeMeta;

/**
 * PyTimeSeriesSchema - Python API adapter for TimeSeriesSchema
 *
 * In Python, this extends AbstractSchema and provides schema information
 * for TSB (TimeSeriesBundle) types. It can be created from:
 * 1. A list of keys (property names) - legacy bundles
 * 2. A TSBTypeMeta* pointer - value-based bundles (delegation)
 */
struct PyTimeSeriesSchema : AbstractSchema {
    using ptr = nb::ref<PyTimeSeriesSchema>;

    // Legacy constructors: own the data
    explicit PyTimeSeriesSchema(std::vector<std::string> keys);
    explicit PyTimeSeriesSchema(std::vector<std::string> keys, nb::object type);

    // Value-based constructor: delegate to TSBTypeMeta
    explicit PyTimeSeriesSchema(const TSBTypeMeta* meta, nb::object scalar_type = nb::none());

    // Override from AbstractSchema
    [[nodiscard]] const std::vector<std::string>& keys() const override;
    [[nodiscard]] nb::object get_value(const std::string& key) const override;

    // TimeSeriesSchema-specific method
    [[nodiscard]] const nb::object& scalar_type() const;

    static void register_with_nanobind(nb::module_& m);

private:
    const TSBTypeMeta* _meta{nullptr};       // Value-based: delegate to this (may be null)
    mutable std::vector<std::string> _keys;  // Legacy: owned keys, or cached from meta
    nb::object _scalar_type;
    mutable bool _keys_cached{false};
};

// Note: TimeSeriesSchema alias is defined in hgraph_forward_declarations.h for backward compatibility

} // namespace hgraph
