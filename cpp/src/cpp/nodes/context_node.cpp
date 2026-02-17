#include <hgraph/api/python/wrapper_factory.h>

#include <fmt/format.h>
#include <hgraph/api/python/py_ref.h>
#include <hgraph/nodes/context_node.h>
#include <hgraph/python/global_keys.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/time_series/ts_ops.h>
#include <hgraph/types/time_series_type.h>
#include <hgraph/types/tsb.h>
#include <nanobind/nanobind.h>

namespace hgraph {
    namespace {
        bool view_data_equals(const ViewData &lhs, const ViewData &rhs) {
            return lhs.path.indices == rhs.path.indices &&
                   lhs.value_data == rhs.value_data &&
                   lhs.time_data == rhs.time_data &&
                   lhs.observer_data == rhs.observer_data &&
                   lhs.delta_data == rhs.delta_data &&
                   lhs.link_data == rhs.link_data &&
                   lhs.projection == rhs.projection &&
                   lhs.ops == rhs.ops &&
                   lhs.meta == rhs.meta;
        }

        engine_time_t node_time(const Node &node) {
            if (auto *et = node.cached_evaluation_time_ptr(); et != nullptr) {
                return *et;
            }
            auto g = node.graph();
            return g != nullptr ? g->evaluation_time() : MIN_DT;
        }
    }  // namespace

    void ContextStubSourceNode::do_start() {
        if (_subscribed_link.is_linked) {
            unregister_ts_link_observer(_subscribed_link);
            _subscribed_link.unbind();
        }
        _owner_time = node_time(*this);
        _subscribed_link.active_notifier = this;
        _subscribed_link.owner_time_ptr = &_owner_time;
        _subscribed_link.parent_link = nullptr;
        notify();
    }

    void ContextStubSourceNode::do_stop() {
        if (_subscribed_link.is_linked) {
            unregister_ts_link_observer(_subscribed_link);
            _subscribed_link.unbind();
        }
        _subscribed_link.active_notifier = nullptr;
        _subscribed_link.owner_time_ptr = nullptr;
        _subscribed_link.parent_link = nullptr;
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

        // Capture the reference value and track the producing output via TS link observer registration.
        nb::object value_ref_obj = nb::none();
        std::optional<ViewData> subscribed_target;

        // Case 1: direct TimeSeriesReferenceOutput stored in GlobalState
        if (nb::isinstance<PyTimeSeriesReferenceOutput>(shared)) {
            auto &output = nb::cast<PyTimeSeriesReferenceOutput &>(shared);
            value_ref_obj = output.value();
            if (output.output_view()) {
                subscribed_target = output.output_view().as_ts_view().view_data();
            }
        }
        // Case 2: TimeSeriesReferenceInput stored in GlobalState
        else if (nb::isinstance<PyTimeSeriesReferenceInput>(shared)) {
            auto &ref = nb::cast<PyTimeSeriesReferenceInput &>(shared);
            if (static_cast<bool>(ref.has_peer())) {
                nb::object output_obj = ref.output();
                if (nb::isinstance<PyTimeSeriesOutput>(output_obj)) {
                    auto &output = nb::cast<PyTimeSeriesOutput &>(output_obj);
                    if (output.output_view()) {
                        subscribed_target = output.output_view().as_ts_view().view_data();
                    }
                }
            }
            value_ref_obj = ref.ref_value();
        } else {
            throw std::runtime_error(
                fmt::format("Context found an unknown output type bound to {}: {}", key,
                            nb::str(shared.type()).c_str()));
        }

        // Manage subscription against output TS link-observer registries.
        if (subscribed_target.has_value()) {
            const bool same_target =
                _subscribed_link.is_linked &&
                view_data_equals(_subscribed_link.as_view_data(false), *subscribed_target);

            if (!same_target) {
                if (_subscribed_link.is_linked) {
                    unregister_ts_link_observer(_subscribed_link);
                    _subscribed_link.unbind();
                }
                _owner_time = node_time(*this);
                _subscribed_link.active_notifier = this;
                _subscribed_link.owner_time_ptr = &_owner_time;
                _subscribed_link.parent_link = nullptr;
                _subscribed_link.bind(*subscribed_target, node_time(*this));
                register_ts_link_observer(_subscribed_link);
            }
        } else if (_subscribed_link.is_linked) {
            unregister_ts_link_observer(_subscribed_link);
            _subscribed_link.unbind();
        }

        // Finally, set this node's own REF output to the captured value (may be None)
        auto out_port = output(node_time(*this));
        if (!out_port) {
            throw std::runtime_error("ContextStubSourceNode: missing TS output");
        }
        if (value_ref_obj.is_none()) {
            out_port.from_python(nb::cast(TimeSeriesReference::make()));
        } else {
            out_port.from_python(value_ref_obj);
        }
    }

    void register_context_node_with_nanobind(nb::module_ &m) {
        nb::class_<ContextStubSourceNode, Node>(m, "ContextStubSourceNode");
    }
} // namespace hgraph
