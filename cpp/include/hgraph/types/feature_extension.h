#ifndef FEATURE_EXTENSION_H
#define FEATURE_EXTENSION_H

#include <any>
#include <hgraph/hgraph_base.h>
#include <hgraph/types/value/value.h>
#include <utility>

namespace hgraph {

    /**
     * @brief Hash functor for PlainValue with transparent lookup support.
     * Enables heterogeneous lookup with ConstValueView keys.
     */
    struct PlainValueHash {
        using is_transparent = void;  // Enable heterogeneous lookup

        size_t operator()(const value::PlainValue& v) const {
            return v.hash();
        }

        size_t operator()(const value::ConstValueView& v) const {
            return v.hash();
        }
    };

    /**
     * @brief Equality functor for PlainValue with transparent lookup support.
     * Enables heterogeneous comparison with ConstValueView keys.
     */
    struct PlainValueEqual {
        using is_transparent = void;  // Enable heterogeneous lookup

        bool operator()(const value::PlainValue& a, const value::PlainValue& b) const {
            return a.equals(b.const_view());
        }

        bool operator()(const value::PlainValue& a, const value::ConstValueView& b) const {
            return a.equals(b);
        }

        bool operator()(const value::ConstValueView& a, const value::PlainValue& b) const {
            return b.equals(a);
        }

        bool operator()(const value::ConstValueView& a, const value::ConstValueView& b) const {
            if (!a.valid() || !b.valid()) return false;
            return a.hash() == b.hash() && a.schema() == b.schema();
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

// std::hash specialization for FeatureOutputRequestTracker (required for ScalarOps)
namespace std {
    template<>
    struct hash<hgraph::FeatureOutputRequestTracker> {
        size_t operator()(const hgraph::FeatureOutputRequestTracker& tracker) const {
            // Hash based on the output pointer address
            return std::hash<hgraph::TimeSeriesOutput*>{}(tracker.output.get());
        }
    };
} // namespace std

namespace hgraph::value {

    /**
     * @brief ScalarOps specialization for FeatureOutputRequestTracker.
     *
     * FeatureOutputRequestTracker contains non-trivially-copyable members
     * (shared_ptr and unordered_set), so we need custom ops that properly
     * call constructors, destructors, and copy/move operations.
     */
    template<>
    struct ScalarOps<hgraph::FeatureOutputRequestTracker> {
        using Tracker = hgraph::FeatureOutputRequestTracker;

        static void construct(void* dst, const TypeMeta*) {
            new (dst) Tracker{};
        }

        static void destruct(void* obj, const TypeMeta*) {
            static_cast<Tracker*>(obj)->~Tracker();
        }

        static void copy_assign(void* dst, const void* src, const TypeMeta*) {
            *static_cast<Tracker*>(dst) = *static_cast<const Tracker*>(src);
        }

        static void move_assign(void* dst, void* src, const TypeMeta*) {
            *static_cast<Tracker*>(dst) = std::move(*static_cast<Tracker*>(src));
        }

        static void move_construct(void* dst, void* src, const TypeMeta*) {
            new (dst) Tracker(std::move(*static_cast<Tracker*>(src)));
        }

        static bool equals(const void* a, const void* b, const TypeMeta*) {
            return *static_cast<const Tracker*>(a) == *static_cast<const Tracker*>(b);
        }

        static size_t hash(const void* obj, const TypeMeta*) {
            return std::hash<Tracker>{}(*static_cast<const Tracker*>(obj));
        }

        static bool less_than(const void* a, const void* b, const TypeMeta*) {
            return *static_cast<const Tracker*>(a) < *static_cast<const Tracker*>(b);
        }

        static std::string to_string(const void* obj, const TypeMeta*) {
            const auto& tracker = *static_cast<const Tracker*>(obj);
            return std::format("FeatureOutputRequestTracker(output={}, requesters={})",
                static_cast<void*>(tracker.output.get()), tracker.requesters.size());
        }

        static nb::object to_python(const void* /*obj*/, const TypeMeta*) {
            // FeatureOutputRequestTracker is internal, no Python conversion needed
            return nb::none();
        }

        static void from_python(void* /*dst*/, const nb::object& /*src*/, const TypeMeta*) {
            throw std::runtime_error("Cannot construct FeatureOutputRequestTracker from Python");
        }

        static constexpr TypeOps make_ops() {
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
                nullptr,  // size
                nullptr,  // get_at
                nullptr,  // set_at
                nullptr,  // get_field
                nullptr,  // set_field
                nullptr,  // contains
                nullptr,  // insert
                nullptr,  // erase
                nullptr,  // map_get
                nullptr,  // map_set
                nullptr,  // resize
                nullptr,  // clear
            };
        }
    };

    // FeatureOutputRequestTracker is not trivially copyable
    template<>
    constexpr TypeFlags compute_scalar_flags<hgraph::FeatureOutputRequestTracker>() {
        return TypeFlags::Hashable | TypeFlags::Equatable | TypeFlags::Comparable;
    }

} // namespace hgraph::value

namespace hgraph {

    // ========== Value-based FeatureOutputExtension ==========

    /**
     * @brief Non-templated FeatureOutputExtension using type-erased key storage.
     *
     * This class manages feature outputs keyed by type-erased PlainValue keys.
     * Uses std::unordered_map with PlainValue keys (NOT the Value Map type) to
     * properly handle non-trivially-copyable FeatureOutputRequestTracker objects.
     *
     * Usage:
     * - Create with key_type to specify the key schema
     * - Call create_or_increment/release with ConstValueView keys
     * - Call update with ConstValueView keys when values change
     */
    struct FeatureOutputExtensionValue {
        using feature_fn = std::function<void(const TimeSeriesOutput &, TimeSeriesOutput &, const value::ConstValueView &)>;
        using outputs_map_type = std::unordered_map<value::PlainValue, FeatureOutputRequestTracker,
                                                     PlainValueHash, PlainValueEqual>;

        FeatureOutputExtensionValue(time_series_output_ptr owning_output_,
                                     output_builder_s_ptr output_builder_,
                                     const value::TypeMeta* key_type_,
                                     feature_fn value_getter_,
                                     std::optional<feature_fn> initial_value_getter_);

        /**
         * @brief Get or create a feature output for the given key.
         *
         * @param key The key as a ConstValueView
         * @param requester Opaque pointer to track the requester
         * @return Reference to the output shared_ptr
         */
        time_series_output_s_ptr& create_or_increment(const value::ConstValueView& key, const void *requester);

        /**
         * @brief Update the feature output for a key.
         *
         * @param key The key as a ConstValueView
         */
        void update(const value::ConstValueView& key);

        /**
         * @brief Update from Python object key.
         *
         * @param key The Python object key
         */
        void update(const nb::handle& key);

        /**
         * @brief Release a requester's interest in a key.
         *
         * @param key The key as a ConstValueView
         * @param requester The requester to release
         */
        void release(const value::ConstValueView& key, const void *requester);

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