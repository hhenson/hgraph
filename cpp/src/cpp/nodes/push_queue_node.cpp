#include "hgraph/api/python/py_tsd.h"
#include "hgraph/api/python/wrapper_factory.h"

#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_python_helpers.h>

namespace hgraph {
    PushQueueNode::PushQueueNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, node_signature_s_ptr signature,
                                 nb::dict scalars, nb::callable eval_fn,
                                 const TSMeta* input_meta, const TSMeta* output_meta,
                                 const TSMeta* error_output_meta, const TSMeta* recordable_state_meta)
        : Node(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
               input_meta, output_meta, error_output_meta, recordable_state_meta),
          _eval_fn{std::move(eval_fn)} {}

    void PushQueueNode::do_eval() {
    }

    void PushQueueNode::enqueue_message(nb::object message) {
        ++_messages_queued;
        _receiver->enqueue({node_ndx(), std::move(message)});
    }

    bool PushQueueNode::apply_message(nb::object message) {
        if (_batch) {
            // Batch mode: accumulate messages into a tuple
            auto output_ptr = output();
            auto eval_time = graph()->evaluation_time();

            if (_is_tsd) {
                // TODO: TSD batching not yet implemented
                // For now, fall through to non-batch handling
            } else {
                // For non-TSD outputs, accumulate messages into a tuple
                auto eval_time = graph()->evaluation_time();
                if (output_ptr->modified_at(eval_time)) {
                    // Append to existing tuple
                    auto existing = ts::get_python_value(output_ptr);
                    size_t existing_len = PyTuple_Size(existing.ptr());
                    nb::tuple new_tuple = nb::steal<nb::tuple>(PyTuple_New(existing_len + 1));
                    for (size_t i = 0; i < existing_len; ++i) {
                        PyTuple_SET_ITEM(new_tuple.ptr(), i, nb::borrow(existing[i]).release().ptr());
                    }
                    PyTuple_SET_ITEM(new_tuple.ptr(), existing_len, message.release().ptr());
                    ts::set_python_value(output_ptr, new_tuple, eval_time);
                } else {
                    // Create new tuple with single element
                    nb::tuple new_tuple = nb::make_tuple(message);
                    ts::set_python_value(output_ptr, new_tuple, eval_time);
                }
            }

            ++_messages_dequeued;
            return true;
        }

        if (_elide || ts::can_apply_python_result(output(), message)) {
            ts::apply_python_result(output(), std::move(message), graph()->evaluation_time());
            ++_messages_dequeued;
            return true;
        }
        return false;
    }

    int64_t PushQueueNode::messages_in_queue() const { return _messages_queued - _messages_dequeued; }

    void PushQueueNode::set_receiver(sender_receiver_state_ptr value) { _receiver = value; }

    void PushQueueNode::do_start() {
        _receiver = &graph()->receiver();
        _elide = scalars().contains("elide") ? nb::cast<bool>(scalars()["elide"]) : false;
        _batch = scalars().contains("batch") ? nb::cast<bool>(scalars()["batch"]) : false;
        // TODO: Need a way to check if output is TSD
        _is_tsd = false; // dynamic_cast<TimeSeriesDictOutput *>(output()) != nullptr;

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
