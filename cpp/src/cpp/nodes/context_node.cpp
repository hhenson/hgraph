#include <hgraph/nodes/context_node.h>
#include <hgraph/api/python/py_time_series.h>
#include <hgraph/python/global_keys.h>
#include <hgraph/types/graph.h>
#include <nanobind/nanobind.h>
#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>

namespace hgraph {
    namespace {
        std::vector<int64_t> owning_graph_prefix(const std::vector<int64_t> &owning_graph_id, int64_t depth) {
            const auto size = static_cast<int64_t>(owning_graph_id.size());
            int64_t stop = depth >= 0 ? depth : size + depth;
            stop = std::clamp<int64_t>(stop, 0, size);
            return {owning_graph_id.begin(), owning_graph_id.begin() + static_cast<size_t>(stop)};
        }

        TSOutput *extract_output_ptr(const nb::object &output_obj) {
            if (!output_obj.is_valid() || output_obj.is_none()) {
                return nullptr;
            }
            try {
                auto &py_output = nb::cast<PyTimeSeriesOutput &>(output_obj);
                return py_output.output_view().output();
            } catch (const nb::cast_error &) {
                return nullptr;
            }
        }
    } // namespace

    void ContextStubSourceNode::do_start() {
        _subscribed_output = nullptr;
        notify();
    }

    void ContextStubSourceNode::do_stop() {
        if (_subscribed_output) {
            _subscribed_output->view(graph()->evaluation_time()).unsubscribe(this);
        }
        _subscribed_output = nullptr;
    }

    void ContextStubSourceNode::do_eval() {
        const auto path = nb::cast<std::string>(scalars()["path"]);
        const auto depth = nb::cast<int64_t>(scalars()["depth"]);
        const auto key = keys::context_output_key(owning_graph_prefix(owning_graph_id(), depth), path);

        auto shared = GlobalState::get(key, nb::none());
        if (shared.is_none()) {
            throw std::runtime_error("Missing shared output for path: " + key);
        }

        nb::object value;
        TSOutput *new_subscribed_output = nullptr;

        // Match Python ContextStub behavior using structural checks because the
        // C++ wrapper classes may not be registered as virtual subclasses of
        // the Python protocol types.
        if (nb::hasattr(shared, "has_peer")) {
            value = shared.attr("value");
            if (nb::hasattr(shared, "output")) {
                new_subscribed_output = extract_output_ptr(shared.attr("output"));
            }
        } else if (nb::hasattr(shared, "value")) {
            value = shared.attr("value");
            new_subscribed_output = extract_output_ptr(shared);
        } else {
            throw std::runtime_error(
                "Unexpected shared output type for path " + key + ": " +
                nb::cast<std::string>(nb::str(shared.type())));
        }

        if (_subscribed_output && _subscribed_output.get() != new_subscribed_output) {
            _subscribed_output->view(graph()->evaluation_time()).unsubscribe(this);
            _subscribed_output = nullptr;
        }

        if (new_subscribed_output && (!_subscribed_output || _subscribed_output.get() != new_subscribed_output)) {
            auto subscribed_output =
                std::shared_ptr<TSOutput>(new_subscribed_output, [](TSOutput *) {});
            subscribed_output->view(graph()->evaluation_time()).subscribe(this);
            _subscribed_output = std::move(subscribed_output);
        }

        if (ts_output()) {
            ts_output()->view(graph()->evaluation_time()).from_python(value);
        }
    }

    void register_context_node_with_nanobind(nb::module_ &m) {
        nb::class_<ContextStubSourceNode, Node>(m, "ContextStubSourceNode");
    }
} // namespace hgraph
