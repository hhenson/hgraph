#ifndef FEATURE_EXTENSION_H
#define FEATURE_EXTENSION_H

#include <any>
#include <hgraph/hgraph_base.h>
#include <hgraph/types/value/value.h>
#include <utility>

namespace hgraph {

    /**
     * @brief Hash functor for Value with transparent lookup support.
     * Enables heterogeneous lookup with View keys.
     */
    struct ValueHash {
        using is_transparent = void;  // Enable heterogeneous lookup

        size_t operator()(const value::Value& v) const {
            return v.has_value() ? v.hash() : 0u;
        }

        size_t operator()(const value::View& v) const {
            return v.hash();
        }
    };

    /**
     * @brief Equality functor for Value with transparent lookup support.
     * Enables heterogeneous comparison with View keys.
     */
    struct ValueEqual {
        using is_transparent = void;  // Enable heterogeneous lookup

        bool operator()(const value::Value& a, const value::Value& b) const {
            return a.equals(b);
        }

        bool operator()(const value::Value& a, const value::View& b) const {
            return a.equals(b);
        }

        bool operator()(const value::View& a, const value::Value& b) const {
            return b.equals(a);
        }

        bool operator()(const value::View& a, const value::View& b) const {
            return a.equals(b);
        }
    };

    /**
     * @brief Tracks a feature output and its requesters.
     *
     * This struct is registered as a scalar type with ScalarOps to enable
     * storage in Value-based Map containers.
     */
    struct FeatureOutputRequestTracker {
        explicit FeatureOutputRequestTracker(time_series_output_s_ptr output_);

        // Default constructor needed for Value storage
        FeatureOutputRequestTracker() = default;

        time_series_output_s_ptr output;
        std::unordered_set<const void *> requesters;

        // Operators required for ScalarOps
        bool operator==(const FeatureOutputRequestTracker& other) const {
            return output == other.output && requesters == other.requesters;
        }

        bool operator<(const FeatureOutputRequestTracker& other) const {
            return output.get() < other.output.get();
        }
    };

} // namespace hgraph

// std::hash specialization for FeatureOutputRequestTracker.
namespace std {
    template<>
    struct hash<hgraph::FeatureOutputRequestTracker> {
        size_t operator()(const hgraph::FeatureOutputRequestTracker& tracker) const {
            // Hash based on the output pointer address
            return std::hash<hgraph::TimeSeriesOutput*>{}(tracker.output.get());
        }
    };
} // namespace std

namespace hgraph {

    // ========== Value-based FeatureOutputExtension ==========

    /**
     * @brief Non-templated FeatureOutputExtension using type-erased key storage.
     *
     * This class manages feature outputs keyed by type-erased Value keys.
     * Uses std::unordered_map with Value keys (NOT the Value Map type) to
     * properly handle non-trivially-copyable FeatureOutputRequestTracker objects.
     *
     * Usage:
     * - Create with key_type to specify the key schema
     * - Call create_or_increment/release with View keys
     * - Call update with View keys when values change
     */
    struct FeatureOutputExtensionValue {
        using feature_fn = std::function<void(const TimeSeriesOutput &, TimeSeriesOutput &, const value::View &)>;
        using outputs_map_type = std::unordered_map<value::Value, FeatureOutputRequestTracker,
                                                     ValueHash, ValueEqual>;

        FeatureOutputExtensionValue(time_series_output_ptr owning_output_,
                                     output_builder_s_ptr output_builder_,
                                     const value::TypeMeta* key_type_,
                                     feature_fn value_getter_,
                                     std::optional<feature_fn> initial_value_getter_);

        /**
         * @brief Get or create a feature output for the given key.
         *
         * @param key The key as a View
         * @param requester Opaque pointer to track the requester
         * @return Reference to the output shared_ptr
         */
        time_series_output_s_ptr& create_or_increment(const value::View& key, const void *requester);

        /**
         * @brief Update the feature output for a key.
         *
         * @param key The key as a View
         */
        void update(const value::View& key);

        /**
         * @brief Update from Python object key.
         *
         * @param key The Python object key
         */
        void update(const nb::handle& key);

        /**
         * @brief Release a requester's interest in a key.
         *
         * @param key The key as a View
         * @param requester The requester to release
         */
        void release(const value::View& key, const void *requester);

        /**
         * @brief Check if there are any outputs.
         */
        [[nodiscard]] bool empty() const;

        explicit operator bool() const { return !empty(); }

        /**
         * @brief Get the key type schema.
         */
        [[nodiscard]] const value::TypeMeta* key_type() const { return _key_type; }

    private:
        time_series_output_ptr _owning_output;
        output_builder_s_ptr _output_builder;
        const value::TypeMeta* _key_type;
        feature_fn _value_getter;
        std::optional<feature_fn> _initial_value_getter;
        outputs_map_type _outputs;  // Standard unordered_map for proper object handling
    };

} // namespace hgraph

#endif  // FEATURE_EXTENSION_H
