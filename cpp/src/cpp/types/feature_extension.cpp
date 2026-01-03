#include <hgraph/types/time_series_type.h>

#include <hgraph/builders/output_builder.h>
#include <hgraph/types/feature_extension.h>

namespace hgraph {
    FeatureOutputRequestTracker::FeatureOutputRequestTracker(time_series_output_s_ptr output_) : output(
        std::move(output_)) {
    }

    // ========== FeatureOutputExtensionValue Implementation ==========

    FeatureOutputExtensionValue::FeatureOutputExtensionValue(
        time_series_output_ptr owning_output_,
        output_builder_s_ptr output_builder_,
        const value::TypeMeta* key_type_,
        feature_fn value_getter_,
        std::optional<feature_fn> initial_value_getter_)
        : _owning_output(owning_output_),
          _output_builder(std::move(output_builder_)),
          _key_type(key_type_),
          _value_getter(std::move(value_getter_)),
          _initial_value_getter(std::move(initial_value_getter_)) {
    }

    time_series_output_s_ptr& FeatureOutputExtensionValue::create_or_increment(
        const value::ConstValueView& key, const void *requester) {

        // Use heterogeneous lookup - find returns iterator to existing or end()
        auto it = _outputs.find(key);

        if (it == _outputs.end()) {
            // Create new output
            auto new_output{_output_builder->make_instance(_owning_output->owning_node())};

            // Create a new tracker with the output
            FeatureOutputRequestTracker tracker(new_output);

            // Insert into the map - clone the key for storage
            auto [inserted_it, success] = _outputs.emplace(key.clone(), std::move(tracker));

            // Call the value getter to initialize
            (_initial_value_getter ? *_initial_value_getter : _value_getter)(*_owning_output, *new_output, key);

            it = inserted_it;
        }

        // Add the requester
        it->second.requesters.insert(requester);

        return it->second.output;
    }

    void FeatureOutputExtensionValue::update(const value::ConstValueView& key) {
        auto it = _outputs.find(key);

        if (it != _outputs.end()) {
            _value_getter(*_owning_output, *(it->second.output), key);
        }
    }

    void FeatureOutputExtensionValue::update(const nb::handle& key) {
        if (!_key_type) return;
        value::PlainValue key_val(_key_type);
        key_val.from_python(nb::cast<nb::object>(key));
        update(key_val.const_view());
    }

    void FeatureOutputExtensionValue::release(const value::ConstValueView& key, const void *requester) {
        auto it = _outputs.find(key);

        if (it != _outputs.end()) {
            it->second.requesters.erase(requester);

            if (it->second.requesters.empty()) {
                _outputs.erase(it);
            }
        }
    }

    bool FeatureOutputExtensionValue::empty() const {
        return _outputs.empty();
    }

} // namespace hgraph