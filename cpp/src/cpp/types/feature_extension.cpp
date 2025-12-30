#include <hgraph/types/time_series_type.h>

#include <hgraph/builders/output_builder.h>
#include <hgraph/types/feature_extension.h>

namespace hgraph {
    FeatureOutputRequestTracker::FeatureOutputRequestTracker(time_series_output_s_ptr output_) : output(
        std::move(output_)) {
    }

    // ========== FeatureOutputExtensionValue Implementation ==========

    namespace {
        // Get the TypeMeta for FeatureOutputRequestTracker (registers on first call)
        const value::TypeMeta* tracker_type_meta() {
            return value::scalar_type_meta<FeatureOutputRequestTracker>();
        }

        // Create a Map schema for key_type -> FeatureOutputRequestTracker
        const value::TypeMeta* make_map_schema(const value::TypeMeta* key_type) {
            auto& registry = value::TypeRegistry::instance();
            return registry.map(key_type, tracker_type_meta()).build();
        }
    }

    FeatureOutputExtensionValue::FeatureOutputExtensionValue(
        time_series_output_ptr owning_output_,
        output_builder_s_ptr output_builder_,
        const value::TypeMeta* key_type_,
        feature_fn value_getter_,
        std::optional<feature_fn> initial_value_getter_)
        : _owning_output(owning_output_),
          _output_builder(std::move(output_builder_)),
          _key_type(key_type_),
          _map_schema(make_map_schema(key_type_)),
          _value_getter(std::move(value_getter_)),
          _initial_value_getter(std::move(initial_value_getter_)),
          _outputs(_map_schema) {  // Initialize PlainValue with Map schema
    }

    time_series_output_s_ptr& FeatureOutputExtensionValue::create_or_increment(
        const value::ConstValueView& key, const void *requester) {

        // Get a MapView for the outputs
        value::MapView map_view = _outputs.view().as_map();

        if (!map_view.contains(key)) {
            // Create new output
            auto new_output{_output_builder->make_instance(_owning_output->owning_node())};

            // Create a new tracker with the output
            FeatureOutputRequestTracker tracker(new_output);

            // Insert into the map
            value::Value<> tracker_val(tracker);
            map_view.set(key, tracker_val.const_view());

            // Call the value getter to initialize
            (_initial_value_getter ? *_initial_value_getter : _value_getter)(*_owning_output, *new_output, key);
        }

        // Get mutable reference to the tracker and add the requester
        value::ValueView tracker_view = map_view.at(key);
        FeatureOutputRequestTracker& tracker = tracker_view.as<FeatureOutputRequestTracker>();
        tracker.requesters.insert(requester);

        return tracker.output;
    }

    void FeatureOutputExtensionValue::update(const value::ConstValueView& key) {
        value::MapView map_view = _outputs.view().as_map();

        if (map_view.contains(key)) {
            value::ValueView tracker_view = map_view.at(key);
            FeatureOutputRequestTracker& tracker = tracker_view.as<FeatureOutputRequestTracker>();
            _value_getter(*_owning_output, *(tracker.output), key);
        }
    }

    void FeatureOutputExtensionValue::update(const nb::handle& key) {
        if (!_key_type) return;
        value::PlainValue key_val(_key_type);
        key_val.from_python(nb::cast<nb::object>(key));
        update(key_val.const_view());
    }

    void FeatureOutputExtensionValue::release(const value::ConstValueView& key, const void *requester) {
        value::MapView map_view = _outputs.view().as_map();

        if (map_view.contains(key)) {
            value::ValueView tracker_view = map_view.at(key);
            FeatureOutputRequestTracker& tracker = tracker_view.as<FeatureOutputRequestTracker>();
            tracker.requesters.erase(requester);

            if (tracker.requesters.empty()) {
                map_view.erase(key);
            }
        }
    }

    bool FeatureOutputExtensionValue::empty() const {
        value::ConstMapView map_view = _outputs.const_view().as_map();
        return map_view.empty();
    }

    // ========== Legacy FeatureOutputExtension Implementation ==========

    template<typename T>
    FeatureOutputExtension<T>::FeatureOutputExtension(time_series_output_ptr owning_output_,
                                                      output_builder_s_ptr output_builder_,
                                                      feature_fn value_getter_,
                                                      std::optional<feature_fn> initial_value_getter_)
        : owning_output(owning_output_), output_builder(std::move(output_builder_)),
          value_getter(std::move(value_getter_)), initial_value_getter(std::move(initial_value_getter_)) {
    }

    template<typename T>
    time_series_output_s_ptr& FeatureOutputExtension<T>::create_or_increment(const T &key, const void *requester) {
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