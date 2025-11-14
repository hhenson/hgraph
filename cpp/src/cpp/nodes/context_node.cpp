#include <fmt/format.h>
#include <hgraph/nodes/context_node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>
#include <hgraph/python/global_keys.h>
#include <hgraph/api/python/wrapper_factory.h>
#include <nanobind/nanobind.h>

namespace hgraph {
    void ContextStubSourceNode::do_start() {
        _subscribed_output = nullptr;
        notify();
    }

    void ContextStubSourceNode::do_stop() {
        if (_subscribed_output.get() != nullptr) {
            _subscribed_output->un_subscribe(this);
            _subscribed_output.reset();
        }
    }

    void ContextStubSourceNode::do_eval() {
        std::string path_str;
        try {
            path_str = nb::cast<std::string>(scalars()["path"]);
        } catch (...) { throw std::runtime_error("ContextStubSourceNode: missing 'path' scalar"); }

        // Slice owning_graph_id by depth and build a Python tuple string to exactly match Python formatting
        const auto &og = owning_graph_id();
        // Python semantics: owning_graph_id[:depth]
        // - If depth is None: use full length
        // - If depth >= 0: use min(depth, len)
        // - If depth < 0: use max(0, len + depth)
        int use;
        nb::object depth_obj;
        try {
            depth_obj = scalars()["depth"];
        } catch (...) {
            depth_obj = nb::none();
        }
        if (!depth_obj.is_valid() || depth_obj.is_none()) {
            use = static_cast<int>(og.size());
        } else {
            int d = nb::cast<int>(depth_obj);
            if (d >= 0) {
                use = std::min<int>(d, static_cast<int>(og.size()));
            } else {
                use = static_cast<int>(og.size()) + d;
                if (use < 0) use = 0;
            }
        }

        // Build the context key using centralized key builder with the sliced owning_graph_id
        std::vector<int64_t> og_prefix;
        og_prefix.reserve(static_cast<size_t>(use));
        for (int i = 0; i < use; ++i) { og_prefix.push_back(og[static_cast<size_t>(i)]); }
        auto key = keys::context_output_key(og_prefix, path_str);

        // Lookup in GlobalState
        nb::object shared = GlobalState::get(key, nb::none());
        if (!shared.is_valid() || shared.is_none()) {
            std::string diag;
            try {
                nb::object gs = GlobalState::instance();
                nb::object keys_obj = gs.attr("keys")();
                std::vector<std::string> ctx_keys;
                for (auto item: nb::iter(keys_obj)) {
                    std::string s = nb::cast<std::string>(nb::str(item));
                    if (s.rfind("context-", 0) == 0) { ctx_keys.push_back(s); }
                }
                if (!ctx_keys.empty()) {
                    diag = fmt::format(" Available context keys: [{}]", fmt::join(ctx_keys, ", "));
                }
            } catch (...) {
                // ignore diagnostics failures
            }
            throw std::runtime_error(fmt::format("Missing shared output for path: {}{}", key, diag));
        }

        // We will capture the reference value and subscribe to the producing output when available
        TimeSeriesReference::ptr value_ref = nullptr;
        time_series_reference_output_ptr output_ts = nullptr;

        // First, try Python isinstance check for Python TimeSeriesReferenceOutput/Input classes
        // (Python code stores Python objects in GlobalState, not C++ objects)
        try {
            auto hgraph_types = nb::module_::import_("hgraph._types._ref_type");
            auto TimeSeriesReferenceOutput_py = hgraph_types.attr("TimeSeriesReferenceOutput");
            auto TimeSeriesReferenceInput_py = hgraph_types.attr("TimeSeriesReferenceInput");
            auto builtins = nb::module_::import_("builtins");
            auto isinstance = builtins.attr("isinstance");
            
            if (nb::cast<bool>(isinstance(shared, TimeSeriesReferenceOutput_py))) {
                // Python TimeSeriesReferenceOutput - access value and output attributes
                // The output is the shared object itself (matches Python: output = shared)
                // Try to unwrap if it's a wrapped C++ object, otherwise try to cast directly
                if (auto* unwrapped = api::unwrap_output(shared)) {
                    if (auto* ts_ref = dynamic_cast<TimeSeriesReferenceOutput*>(unwrapped)) {
                        output_ts = time_series_reference_output_ptr(ts_ref);
                        value_ref = ts_ref->value();
                    }
                } else if (nb::isinstance<TimeSeriesReferenceOutput>(shared)) {
                    output_ts = nb::cast<time_series_reference_output_ptr>(shared);
                    value_ref = output_ts->value();
                } else {
                    // Pure Python object - try to get value attribute and cast it
                    try {
                        auto value_attr = shared.attr("value");
                        value_ref = nb::cast<TimeSeriesReference::ptr>(value_attr);
                    } catch (const nb::cast_error&) {
                        // If cast fails, value_ref remains nullptr
                        value_ref = nullptr;
                    }
                }
            } else if (nb::cast<bool>(isinstance(shared, TimeSeriesReferenceInput_py))) {
                // Python TimeSeriesReferenceInput - access value and output attributes
                auto has_peer = nb::cast<bool>(shared.attr("has_peer"));
                if (has_peer) {
                    auto output_attr = shared.attr("output");
                    if (!output_attr.is_none()) {
                        // Try to unwrap if it's a wrapped C++ object, otherwise use as-is
                        if (auto* unwrapped = api::unwrap_output(output_attr)) {
                            if (auto* ts_ref = dynamic_cast<TimeSeriesReferenceOutput*>(unwrapped)) {
                                output_ts = time_series_reference_output_ptr(ts_ref);
                            }
                        } else if (nb::isinstance<TimeSeriesReferenceOutput>(output_attr)) {
                            output_ts = nb::cast<time_series_reference_output_ptr>(output_attr);
                        }
                    }
                }
                // Always get the value from the REF input (matches Python: value = shared.value)
                try {
                    auto value_attr = shared.attr("value");
                    value_ref = nb::cast<TimeSeriesReference::ptr>(value_attr);
                } catch (const nb::cast_error&) {
                    // If cast fails, value_ref remains nullptr
                    value_ref = nullptr;
                }
            } else {
                // Not a Python class, try C++ checks
                // Case 1: direct TimeSeriesReferenceOutput stored in GlobalState (check direct C++ objects first)
                if (nb::isinstance<TimeSeriesReferenceOutput>(shared)) {
                    output_ts = nb::cast<time_series_reference_output_ptr>(shared);
                    // Always get the value, even if not valid (matches Python behavior)
                    value_ref = output_ts->value();
                }
                // Case 2: direct TimeSeriesReferenceInput stored in GlobalState
                else if (nb::isinstance<TimeSeriesReferenceInput>(shared)) {
                    auto ref = nb::cast<time_series_reference_input_ptr>(shared);
                    if (ref->has_peer()) {
                        // Use the bound peer output (stub remains a reference node)
                        output_ts = dynamic_cast<TimeSeriesReferenceOutput *>(ref->output().get());
                    }
                    // Always use the value from the REF input (may be empty). Python sets value regardless of peer.
                    value_ref = ref->value();
                }
                // Case 3: new Python API wrapper storing TimeSeriesReferenceOutput
                else if (auto* unwrapped_output = api::unwrap_output(shared)) {
                    if (auto* ts_ref = dynamic_cast<TimeSeriesReferenceOutput*>(unwrapped_output)) {
                        output_ts = time_series_reference_output_ptr(ts_ref);
                        // Always get the value, even if not valid (matches Python behavior)
                        value_ref = ts_ref->value();
                    }
                }
                // Case 4: new Python API wrapper storing TimeSeriesReferenceInput
                else if (auto* unwrapped_input = api::unwrap_input(shared)) {
                    if (auto* ts_ref = dynamic_cast<TimeSeriesReferenceInput*>(unwrapped_input)) {
                        if (ts_ref->has_peer()) {
                            // Use the bound peer output (stub remains a reference node)
                            output_ts = dynamic_cast<TimeSeriesReferenceOutput *>(ts_ref->output().get());
                        }
                        // Always use the value from the REF input (may be empty). Python sets value regardless of peer.
                        value_ref = ts_ref->value();
                    }
                } else {
                    throw std::runtime_error(
                        fmt::format("Context found an unknown output type bound to {}: {}", key,
                                    nb::str(shared.type()).c_str()));
                }
            }
        } catch (const std::exception& e) {
            // Fallback to C++ checks if Python isinstance fails
            // Case 1: direct TimeSeriesReferenceOutput stored in GlobalState (check direct C++ objects first)
            if (nb::isinstance<TimeSeriesReferenceOutput>(shared)) {
                output_ts = nb::cast<time_series_reference_output_ptr>(shared);
                // Always get the value, even if not valid (matches Python behavior)
                value_ref = output_ts->value();
            }
            // Case 2: direct TimeSeriesReferenceInput stored in GlobalState
            else if (nb::isinstance<TimeSeriesReferenceInput>(shared)) {
                auto ref = nb::cast<time_series_reference_input_ptr>(shared);
                if (ref->has_peer()) {
                    // Use the bound peer output (stub remains a reference node)
                    output_ts = dynamic_cast<TimeSeriesReferenceOutput *>(ref->output().get());
                }
                // Always use the value from the REF input (may be empty). Python sets value regardless of peer.
                value_ref = ref->value();
            }
            // Case 3: new Python API wrapper storing TimeSeriesReferenceOutput
            else if (auto* unwrapped_output = api::unwrap_output(shared)) {
                if (auto* ts_ref = dynamic_cast<TimeSeriesReferenceOutput*>(unwrapped_output)) {
                    output_ts = time_series_reference_output_ptr(ts_ref);
                    // Always get the value, even if not valid (matches Python behavior)
                    value_ref = ts_ref->value();
                }
            }
            // Case 4: new Python API wrapper storing TimeSeriesReferenceInput
            else if (auto* unwrapped_input = api::unwrap_input(shared)) {
                if (auto* ts_ref = dynamic_cast<TimeSeriesReferenceInput*>(unwrapped_input)) {
                    if (ts_ref->has_peer()) {
                        // Use the bound peer output (stub remains a reference node)
                        output_ts = dynamic_cast<TimeSeriesReferenceOutput *>(ts_ref->output().get());
                    }
                    // Always use the value from the REF input (may be empty). Python sets value regardless of peer.
                    value_ref = ts_ref->value();
                }
            } else {
                throw std::runtime_error(
                    fmt::format("Context found an unknown output type bound to {}: {} (error: {})", key,
                                nb::str(shared.type()).c_str(), e.what()));
            }
        }

        // Manage subscription if we have a producing output
        if (output_ts.get() != nullptr) {
            bool is_same{_subscribed_output.get() == output_ts.get()};
            if (!is_same) {
                output_ts->subscribe(this);
                if (_subscribed_output != nullptr) { _subscribed_output->un_subscribe(this); }
                _subscribed_output = output_ts;
            }
        }

        // Finally, set this node's own REF output to the captured value (may be None)
        auto my_output = dynamic_cast<TimeSeriesReferenceOutput *>(output().get());
        if (!my_output) {
            throw std::runtime_error("ContextStubSourceNode: output is not a TimeSeriesReferenceOutput");
        }
        my_output->set_value(value_ref);
    }

    void register_context_node_with_nanobind(nb::module_ &m) {
        nb::class_<ContextStubSourceNode, Node>(m, "ContextStubSourceNode");
    }
} // namespace hgraph