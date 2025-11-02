#include <hgraph/types/time_series_type.h>

#include <hgraph/builders/output_builder.h>
#include <hgraph/types/feature_extension.h>

namespace hgraph {
    FeatureOutputRequestTracker::FeatureOutputRequestTracker(TimeSeriesOutput::ptr output_) : output(
        std::move(output_)) {
    }

    template<typename T>
    FeatureOutputExtension<T>::FeatureOutputExtension(TimeSeriesOutput::ptr owning_output_,
                                                      output_builder_ptr output_builder_,
                                                      feature_fn value_getter_,
                                                      std::optional<feature_fn> initial_value_getter_)
        : owning_output(std::move(owning_output_)), output_builder(std::move(output_builder_)),
          value_getter(std::move(value_getter_)), initial_value_getter(std::move(initial_value_getter_)) {
    }

    template<typename T>
    TimeSeriesOutput::ptr FeatureOutputExtension<T>::create_or_increment(const T &key, const void *requester) {
        auto it = _outputs.find(key);
        if (it == _outputs.end()) {
            auto new_output{output_builder->make_instance(owning_output->owning_node())};

            auto [inserted_it, success] = _outputs.emplace(key, FeatureOutputRequestTracker(new_output));

            (initial_value_getter ? *initial_value_getter : value_getter)(*owning_output, *new_output, key);

            it = inserted_it;
        }

        it->second.requesters.insert(requester);
        return it->second.output;
    }

    template<typename T>
    void FeatureOutputExtension<T>::release(const T &key, const void *requester) {
        if (auto it{_outputs.find(key)}; it != _outputs.end()) {
            it->second.requesters.erase(requester);
            if (it->second.requesters.empty()) { _outputs.erase(it); }
        }
    }

    template struct FeatureOutputExtension<bool>;
    template struct FeatureOutputExtension<int64_t>;
    template struct FeatureOutputExtension<double>;
    template struct FeatureOutputExtension<engine_date_t>;
    template struct FeatureOutputExtension<engine_time_t>;
    template struct FeatureOutputExtension<engine_time_delta_t>;
    template struct FeatureOutputExtension<nb::object>;
} // namespace hgraph