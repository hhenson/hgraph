#ifndef FEATURE_EXTENSION_H
#define FEATURE_EXTENSION_H

#include <any>
#include <hgraph/hgraph_base.h>
#include <utility>

namespace hgraph {
    struct FeatureOutputRequestTracker {
        explicit FeatureOutputRequestTracker(time_series_output_ptr output_);

        time_series_output_ptr output;
        std::unordered_set<const void *> requesters;
    };

    template<typename T>
    struct FeatureOutputExtension {
        using feature_fn = std::function<void(const TimeSeriesOutput &, TimeSeriesOutput &, const T &)>;

        FeatureOutputExtension(time_series_output_ptr owning_output_, output_builder_ptr output_builder_,
                               feature_fn value_getter_,
                               std::optional<feature_fn> initial_value_getter_);

        time_series_output_ptr create_or_increment(const T &key, const void *requester);

        void update(const T &key) {
            if (auto it{_outputs.find(key)}; it != _outputs.end()) {
                value_getter(*owning_output, *(it->second.output), key);
            }
        }

        void update(const nb::handle &key) { update(nb::cast<T>(key)); }

        void release(const T &key, const void *requester);

        template<typename It>
        void update_all(It begin, It end) {
            if (!_outputs.empty()) {
                for (auto it = begin; it != end; ++it) { update(*it); }
            }
        }

        explicit operator bool() const { return !_outputs.empty(); }

    private:
        time_series_output_ptr owning_output;
        output_builder_ptr output_builder;
        feature_fn value_getter;
        std::optional<feature_fn> initial_value_getter;

        std::unordered_map<T, FeatureOutputRequestTracker> _outputs;
    };

    using FeatureOutputExtensionBool = FeatureOutputExtension<bool>;
    using FeatureOutputExtensionInt = FeatureOutputExtension<int64_t>;
    using FeatureOutputExtensionFloat = FeatureOutputExtension<double>;
    using FeatureOutputExtensionDate = FeatureOutputExtension<engine_date_t>;
    using FeatureOutputExtensionDateTime = FeatureOutputExtension<engine_time_t>;
    using FeatureOutputExtensionTimeDelta = FeatureOutputExtension<engine_time_delta_t>;

    using FeatureOutputExtensionObject = FeatureOutputExtension<nb::object>;
} // namespace hgraph

#endif  // FEATURE_EXTENSION_H