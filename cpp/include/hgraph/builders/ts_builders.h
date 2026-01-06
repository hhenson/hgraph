#ifndef TS_BUILDERS_H
#define TS_BUILDERS_H

#include <hgraph/builders/builder.h>
#include <hgraph/types/time_series/ts_type_meta.h>
#include <hgraph/types/time_series/ts_value.h>

namespace hgraph {

/**
 * @brief Builder for creating TSValue instances for outputs.
 *
 * This is a thin wrapper that delegates to TSMeta's factory methods.
 * It provides the bridge between the builder system and the new TSValue infrastructure.
 *
 * Usage:
 * @code
 * const TSMeta* ts_meta = TSTypeRegistry::instance().ts(value::int_type());
 * CppTimeSeriesOutputBuilder builder(ts_meta);
 * TSValue output = builder.make_ts_value(owning_node, OUTPUT_MAIN);
 * @endcode
 */
struct HGRAPH_EXPORT CppTimeSeriesOutputBuilder : Builder {
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
    [[nodiscard]] const TSMeta* ts_meta() const noexcept { return _ts_meta; }

    /**
     * @brief Create a TSValue for use as an output.
     * @param owner The owning node
     * @param output_id The output identifier (OUTPUT_MAIN, ERROR_PATH, STATE_PATH)
     * @return A new TSValue configured for this time-series type
     */
    [[nodiscard]] TSValue make_ts_value(Node* owner, int output_id = OUTPUT_MAIN) const {
        return hgraph::make_ts_value(_ts_meta, owner, output_id);
    }

    // ========== Builder Interface ==========

    [[nodiscard]] size_t memory_size() const override {
        return sizeof(TSValue);
    }

    [[nodiscard]] size_t type_alignment() const override {
        return alignof(TSValue);
    }

    [[nodiscard]] bool is_same_type(const Builder& other) const override {
        if (typeid(*this) != typeid(other)) {
            return false;
        }
        auto& other_builder = static_cast<const CppTimeSeriesOutputBuilder&>(other);
        return _ts_meta == other_builder._ts_meta;
    }

    static void register_with_nanobind(nb::module_& m);

private:
    const TSMeta* _ts_meta;
};

/**
 * @brief Builder for creating TSValue instances for inputs.
 *
 * Similar to CppTimeSeriesOutputBuilder but specifically for inputs.
 * For inputs that are always TSB, this provides bundle-specific views.
 */
struct HGRAPH_EXPORT CppTimeSeriesInputBuilder : Builder {
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
    [[nodiscard]] const TSMeta* ts_meta() const noexcept { return _ts_meta; }

    /**
     * @brief Create a TSValue for use as an input.
     * @param owner The owning node
     * @return A new TSValue configured for this time-series type
     */
    [[nodiscard]] TSValue make_ts_value(Node* owner) const {
        return hgraph::make_ts_value(_ts_meta, owner);
    }

    // ========== Builder Interface ==========

    [[nodiscard]] size_t memory_size() const override {
        return sizeof(TSValue);
    }

    [[nodiscard]] size_t type_alignment() const override {
        return alignof(TSValue);
    }

    [[nodiscard]] bool is_same_type(const Builder& other) const override {
        if (typeid(*this) != typeid(other)) {
            return false;
        }
        auto& other_builder = static_cast<const CppTimeSeriesInputBuilder&>(other);
        return _ts_meta == other_builder._ts_meta;
    }

    static void register_with_nanobind(nb::module_& m);

private:
    const TSMeta* _ts_meta;
};

} // namespace hgraph

#endif // TS_BUILDERS_H
