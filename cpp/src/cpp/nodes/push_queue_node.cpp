#include "hgraph/api/python/py_tsd.h"
#include "hgraph/api/python/wrapper_factory.h"

#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_output_view.h>

namespace hgraph {
    void PushQueueNode::do_eval() {
    }

    void PushQueueNode::enqueue_message(nb::object message) {
        ++_messages_queued;
        _receiver->enqueue({node_ndx(), std::move(message)});
    }

    bool PushQueueNode::apply_message(nb::object message) {
        // TODO: Convert to TSOutput-based approach
        // This method needs significant rework for TSOutput:
        // - For batch mode: accumulate messages, handle TSD special case
        // - For non-batch: check can_apply_result and apply
        // The _is_tsd flag and TSD-specific handling needs TSMeta-based detection
        // and TSOutputView navigation
        if (!ts_output()) {
            return false;
        }

        auto output_view = ts_output()->view(graph()->evaluation_time());

        // Non-batch simple case: just apply the message
        if (!_batch) {
            // TODO: Need can_apply_result equivalent for TSOutputView
            output_view.from_python(std::move(message));
            ++_messages_dequeued;
            return true;
        }

        // TODO: Batch mode with tuple accumulation needs implementation
        throw std::runtime_error("PushQueueNode::apply_message batch mode needs TSOutput conversion - TODO");
    }

    int64_t PushQueueNode::messages_in_queue() const { return _messages_queued - _messages_dequeued; }

    void PushQueueNode::set_receiver(sender_receiver_state_ptr value) { _receiver = value; }

    void PushQueueNode::do_start() {
        _receiver = &graph()->receiver();
        _elide = scalars().contains("elide") ? nb::cast<bool>(scalars()["elide"]) : false;
        _batch = scalars().contains("batch") ? nb::cast<bool>(scalars()["batch"]) : false;
        // Determine if output is TSD using TSMeta
        _is_tsd = ts_output() && ts_output()->ts_meta() && ts_output()->ts_meta()->kind == TSKind::TSD;

        // If an eval function was provided (from push_queue decorator), call it with a sender and scalar kwargs
        if (_eval_fn.is_valid() && !_eval_fn.is_none()) {
            // Create a Python-callable sender that enqueues messages into this node
            // The sender will be called from a Python thread, so it needs to acquire the GIL
            nb::object sender = nb::cpp_function([this](nb::object m) {
                // Acquire GIL in case we're being called from a thread that doesn't have it
                nb::gil_scoped_acquire gil;
                this->enqueue_message(std::move(m));
            });
            // Call eval_fn(sender, **scalars)
            try {
                _eval_fn(sender, **scalars());
            } catch (nb::python_error &e) { throw NodeException::capture_error(e, *this, "During push-queue start"); }
        }
    }
} // namespace hgraph
