#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/time_series/ts_python_helpers.h>
#include <hgraph/types/value/python_conversion.h>

namespace hgraph {
    void LastValuePullNode::do_start() {
        _setup_combine_function();

        // If there's a default value in scalars, apply it and notify
        if (scalars().contains("default")) {
            _delta_value = scalars()["default"];
            notify();
        }
    }

    void LastValuePullNode::do_stop() {
        // Nothing to clean up
    }

    void LastValuePullNode::initialise() {
        // No special initialisation beyond do_start
    }

    void LastValuePullNode::dispose() {
        // No special disposal required
    }

    void LastValuePullNode::_setup_combine_function() {
        // Determine which combine function to use based on the output type
        if (!has_output()) {
            // Default to simple replacement
            _delta_combine_fn = [](const nb::object &old_delta, const nb::object &new_delta) {
                return new_delta;
            };
            return;
        }

        auto* output_obj = output();
        auto* meta = output_obj->meta();

        if (!meta) {
            // Default to simple replacement if no meta
            _delta_combine_fn = [](const nb::object &old_delta, const nb::object &new_delta) {
                return new_delta;
            };
            return;
        }

        // Use ts_kind from meta to determine combine function
        switch (meta->ts_kind) {
            case TimeSeriesKind::TSS:
                _delta_combine_fn = _combine_tss_delta;
                break;
            case TimeSeriesKind::TSD:
                _delta_combine_fn = _combine_tsd_delta;
                break;
            case TimeSeriesKind::TSB:
                _delta_combine_fn = _combine_tsb_delta;
                break;
            case TimeSeriesKind::TSL:
                _delta_combine_fn = _combine_tsl_delta_value;
                break;
            default:
                // Simple replacement for TS, REF, etc.
                _delta_combine_fn = [](const nb::object &old_delta, const nb::object &new_delta) {
                    return new_delta;
                };
                break;
        }
    }

    void LastValuePullNode::copy_from_input(const ts::TSInput &input) {
        // Get value from input view and convert to Python
        auto view = input.view();
        if (!view.valid() || !view.has_value()) {
            return;  // Nothing to copy
        }

        // Convert value to Python object
        auto& value_view = view.value_view();
        auto delta = value::value_to_python(value_view.data(), value_view.schema());

        if (_delta_value.has_value()) {
            _delta_value = _delta_combine_fn(_delta_value.value(), delta);
        } else {
            _delta_value = delta;
        }

        // Notify for the next cycle since we're copying the value now
        notify_next_cycle();
    }

    void LastValuePullNode::copy_from_output(const ts::TSOutput &output) {
        // Get value from output view and convert to Python
        auto view = const_cast<ts::TSOutput&>(output).view();
        if (!view.valid() || !view.has_value()) {
            return;  // Nothing to copy
        }

        // Convert value to Python object
        auto ts_value_view = view.value_view();
        auto vv = ts_value_view.value_view();
        auto delta = value::value_to_python(vv.data(), ts_value_view.schema());

        if (_delta_value.has_value()) {
            _delta_value = _delta_combine_fn(_delta_value.value(), delta);
        } else {
            _delta_value = delta;
        }

        // Notify for the next cycle since we're copying the value now
        notify_next_cycle();
    }

    void LastValuePullNode::apply_value(const nb::object &new_value) {
        try {
            if (_delta_value.has_value()) {
                _delta_value = _delta_combine_fn(_delta_value.value(), new_value);
            } else {
                _delta_value = new_value;
            }
        } catch (const std::exception &e) {
            std::string type_name = nb::cast<std::string>(nb::str(nb::handle(new_value.type().attr("__name__"))));
            throw std::runtime_error(
                std::string("Cannot apply value of type ") + type_name +
                " to " + repr() + ": " + e.what()
            );
        }

        notify_next_cycle();
    }

    void LastValuePullNode::do_eval() {
        if (_delta_value.has_value()) {
            ts::apply_python_result(output(), _delta_value.value(), graph()->evaluation_time());
            _delta_value.reset();
        }
    }

    nb::object LastValuePullNode::combine_delta_values(const nb::object &old_delta, const nb::object &new_delta) {
        return _delta_combine_fn(old_delta, new_delta);
    }

    nb::object LastValuePullNode::_combine_tss_delta(const nb::object &old_delta, const nb::object &new_delta) {
        // For TimeSeriesSet, we need to combine SetDelta objects
        // Handle cases where deltas might be plain sets or SetDelta objects

        nb::object py_set_delta_class = nb::module_::import_("hgraph").attr("PythonSetDelta");
        nb::object py_removed_class = nb::module_::import_("hgraph").attr("Removed");

        // Helper to convert a set to a SetDelta
        auto to_set_delta = [&](const nb::object &delta) -> nb::object {
            if (nb::isinstance(delta, py_set_delta_class)) {
                return delta;
            }
            // It's a plain set - convert it to SetDelta
            nb::set added;
            nb::set removed;
            for (auto item: nb::cast<nb::set>(delta)) {
                if (nb::isinstance(item, py_removed_class)) {
                    removed.add(item);
                } else {
                    added.add(item);
                }
            }
            return py_set_delta_class(nb::arg("added") = added, nb::arg("removed") = removed);
        };

        // Check if both are plain sets
        if (nb::isinstance<nb::set>(old_delta) && nb::isinstance<nb::set>(new_delta)) {
            return nb::cast<nb::set>(new_delta) | nb::cast<nb::set>(old_delta);
        }

        nb::object old_sd = to_set_delta(old_delta);
        nb::object new_sd = to_set_delta(new_delta);

        nb::set old_added = nb::cast<nb::set>(old_sd.attr("added"));
        nb::set old_removed = nb::cast<nb::set>(old_sd.attr("removed"));
        nb::set new_added = nb::cast<nb::set>(new_sd.attr("added"));
        nb::set new_removed = nb::cast<nb::set>(new_sd.attr("removed"));

        // Combine with explicit set operations to avoid implicit nb::object -> nb::set conversions
        nb::set combined_added = nb::steal<nb::set>(PySet_New(nullptr));
        // Start with old_added minus new_removed
        for (auto item: old_added) {
            if (!new_removed.contains(item)) {
                combined_added.add(item);
            }
        }
        // Union with (new_added - old_removed)
        for (auto item: new_added) {
            if (!old_removed.contains(item)) {
                combined_added.add(item);
            }
        }

        // Only remove elements that haven't been recently added and don't remove old removes that have been re-added
        nb::set combined_removed = nb::steal<nb::set>(PySet_New(nullptr));
        // (old_removed - new_added)
        for (auto item: old_removed) {
            if (!new_added.contains(item)) {
                combined_removed.add(item);
            }
        }
        // | (new_removed - old_added)
        for (auto item: new_removed) {
            if (!old_added.contains(item)) {
                combined_removed.add(item);
            }
        }

        return py_set_delta_class(nb::arg("added") = combined_added, nb::arg("removed") = combined_removed);
    }

    nb::object LastValuePullNode::_combine_tsd_delta(const nb::object &old_delta, const nb::object &new_delta) {
        // For TimeSeriesDict, combine mappings
        // REMOVES are tracked inside the dict, so union operator handles everything
        nb::dict old_dict = nb::cast<nb::dict>(old_delta);
        nb::dict new_dict = nb::cast<nb::dict>(new_delta);

        // Python dict union: new_dict values override old_dict values
        nb::dict result;
        for (auto [key, value]: old_dict) {
            result[key] = value;
        }
        for (auto [key, value]: new_dict) {
            result[key] = value;
        }

        return result;
    }

    nb::object LastValuePullNode::_combine_tsb_delta(const nb::object &old_delta, const nb::object &new_delta) {
        // For TimeSeriesBundle, combine mappings (same as TSD)
        nb::dict old_dict = nb::cast<nb::dict>(old_delta);
        nb::dict new_dict = nb::cast<nb::dict>(new_delta);

        nb::dict result;
        for (auto [key, value]: old_dict) {
            result[key] = value;
        }
        for (auto [key, value]: new_dict) {
            result[key] = value;
        }

        return result;
    }

    nb::object LastValuePullNode::_combine_tsl_delta_value(const nb::object &old_delta, const nb::object &new_delta) {
        // For TimeSeriesList, combine mappings (same as TSD/TSB)
        nb::dict old_dict = nb::cast<nb::dict>(old_delta);
        nb::dict new_dict = nb::cast<nb::dict>(new_delta);

        nb::dict result;
        for (auto [key, value]: old_dict) {
            result[key] = value;
        }
        for (auto [key, value]: new_dict) {
            result[key] = value;
        }

        return result;
    }

} // namespace hgraph