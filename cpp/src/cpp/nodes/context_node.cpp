#include <fmt/format.h>
#include <hgraph/nodes/context_node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>
#include <hgraph/python/global_keys.h>
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
        std::optional<TimeSeriesReference> value_ref;
        time_series_reference_output_ptr output_ts = nullptr;

        // Case 1: direct TimeSeriesReferenceOutput stored in GlobalState
        // Use nb::isinstance to handle both base and specialized reference types
        if (nb::isinstance<TimeSeriesReferenceOutput>(shared)) {
            output_ts = nb::cast<time_series_reference_output_ptr>(shared);
            if (output_ts->valid() && output_ts->has_value()) {
                value_ref = output_ts->value();
            }
        }
        // Case 2: TimeSeriesReferenceInput stored in GlobalState
        else if (nb::isinstance<TimeSeriesReferenceInput>(shared)) {
            auto ref = nb::cast<time_series_reference_input_ptr>(shared);
            if (ref->has_peer()) {
                // Use the bound peer output (stub remains a reference node)
                output_ts = dynamic_cast<TimeSeriesReferenceOutput *>(ref->output().get());
            }
            // Always use the value from the REF input (may be empty). Python sets value regardless of peer.
            value_ref = ref->value();
        } else {
            throw std::runtime_error(
                fmt::format("Context found an unknown output type bound to {}: {}", key,
                            nb::str(shared.type()).c_str()));
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
        if (value_ref.has_value()) {
            my_output->set_value(*value_ref);
        } else {
            my_output->set_value(TimeSeriesReference::make());
        }
    }

    void register_context_node_with_nanobind(nb::module_ &m) {
        nb::class_<ContextStubSourceNode, Node>(m, "ContextStubSourceNode");
    }
} // namespace hgraph