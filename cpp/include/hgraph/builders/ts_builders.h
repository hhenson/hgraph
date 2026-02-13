#ifndef TS_BUILDERS_H
#define TS_BUILDERS_H

#include <hgraph/builders/output_builder.h>
#include <hgraph/builders/input_builder.h>
#include <hgraph/types/time_series/ts_meta.h>

namespace hgraph {

// Forward declarations
class TSInput;
class TSOutput;

/// Output port identifiers
constexpr int OUTPUT_MAIN = 0;
constexpr int ERROR_PATH = 1;
constexpr int STATE_PATH = 2;

/**
 * @brief Builder for creating TSOutput instances.
 *
 * This is a thin wrapper that holds TSMeta and provides it to the Node
 * for TSOutput construction. It provides the bridge between the builder
 * system and the new TSValue infrastructure.
 *
 * Inherits from OutputBuilder so it can be used with node builders that expect OutputBuilder.
 * The legacy make_instance methods throw - the new TSMeta-based path should be used instead.
 *
 * Usage:
 * @code
 * const TSMeta* ts_meta = ...;  // From TSTypeRegistry
 * CppTimeSeriesOutputBuilder builder(ts_meta);
 * // Node will use ts_meta() to create TSOutput internally
 * @endcode
 */
struct HGRAPH_EXPORT CppTimeSeriesOutputBuilder : OutputBuilder {
    using ptr = nb::ref<CppTimeSeriesOutputBuilder>;

    /**
     * @brief Construct a builder for the given time-series type.
     * @param meta The time-series type metadata
     */
    explicit CppTimeSeriesOutputBuilder(const TSMeta* meta)
        : _ts_meta(meta) {}

    /**
     * @brief Get the time-series type metadata.
     */
    [[nodiscard]] const TSMeta* ts_meta() const noexcept override { return _ts_meta; }

    // ========== Builder Interface ==========

    [[nodiscard]] size_t memory_size() const override;

    [[nodiscard]] size_t type_alignment() const override;

    [[nodiscard]] bool is_same_type(const Builder& other) const override;

    /**
     * @brief Legacy interface - throws.
     * @deprecated Use ts_meta() with Node's TSMeta constructor instead.
     */
    [[nodiscard]] time_series_output_s_ptr make_instance(node_ptr owning_node) const override;

    /**
     * @brief Legacy interface - throws.
     * @deprecated Use ts_meta() with Node's TSMeta constructor instead.
     */
    [[nodiscard]] time_series_output_s_ptr make_instance(time_series_output_ptr owning_output) const override;

    static void register_with_nanobind(nb::module_& m);

private:
    const TSMeta* _ts_meta;
};

/**
 * @brief Builder for creating TSInput instances.
 *
 * Similar to CppTimeSeriesOutputBuilder but specifically for inputs.
 * Inherits from InputBuilder so it can be used with node builders that expect InputBuilder.
 * The legacy make_instance methods throw - the new TSMeta-based path should be used instead.
 */
struct HGRAPH_EXPORT CppTimeSeriesInputBuilder : InputBuilder {
    using ptr = nb::ref<CppTimeSeriesInputBuilder>;

    /**
     * @brief Construct a builder for the given time-series type.
     * @param meta The time-series type metadata (must be TSB for inputs)
     */
    explicit CppTimeSeriesInputBuilder(const TSMeta* meta)
        : _ts_meta(meta) {}

    /**
     * @brief Get the time-series type metadata.
     */
    [[nodiscard]] const TSMeta* ts_meta() const noexcept override { return _ts_meta; }

    // ========== Builder Interface ==========

    [[nodiscard]] size_t memory_size() const override;

    [[nodiscard]] size_t type_alignment() const override;

    [[nodiscard]] bool is_same_type(const Builder& other) const override;

    /**
     * @brief Legacy interface - throws.
     * @deprecated Use ts_meta() with Node's TSMeta constructor instead.
     */
    [[nodiscard]] time_series_input_s_ptr make_instance(node_ptr owning_node) const override;

    /**
     * @brief Legacy interface - throws.
     * @deprecated Use ts_meta() with Node's TSMeta constructor instead.
     */
    [[nodiscard]] time_series_input_s_ptr make_instance(time_series_input_ptr owning_input) const override;

    static void register_with_nanobind(nb::module_& m);

private:
    const TSMeta* _ts_meta;
};

} // namespace hgraph

#endif // TS_BUILDERS_H
