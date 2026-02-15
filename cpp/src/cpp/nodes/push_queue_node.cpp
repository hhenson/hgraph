#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>

namespace hgraph {
    namespace {
        engine_time_t node_time(const Node &node) {
            if (auto *et = node.cached_evaluation_time_ptr(); et != nullptr) {
                return *et;
            }
            auto g = node.graph();
            return g != nullptr ? g->evaluation_time() : MIN_DT;
        }

        value::Value key_from_python(const value::TypeMeta *key_type, const nb::handle &key_obj) {
            value::Value key_value(key_type);
            key_type->ops().from_python(key_value.data(), nb::borrow(key_obj), key_type);
            return key_value;
        }

        nb::tuple append_tuple(const nb::object &existing, const nb::object &value) {
            size_t existing_len = PyTuple_Size(existing.ptr());
            nb::tuple new_tuple = nb::steal<nb::tuple>(PyTuple_New(existing_len + 1));
            for (size_t i = 0; i < existing_len; ++i) {
                PyTuple_SET_ITEM(new_tuple.ptr(), i, nb::borrow(existing[i]).release().ptr());
            }
            PyTuple_SET_ITEM(new_tuple.ptr(), existing_len, nb::borrow(value).release().ptr());
            return new_tuple;
        }
    }  // namespace

    void PushQueueNode::do_eval() {
    }

    void PushQueueNode::enqueue_message(nb::object message) {
        ++_messages_queued;
        _receiver->enqueue({node_ndx(), std::move(message)});
    }

    bool PushQueueNode::apply_message(nb::object message) {
        auto out_view = output(node_time(*this));
        if (!out_view) {
            return false;
        }

        if (_batch) {
            // Batch mode: accumulate messages into a tuple

            if (_is_tsd) {
                if (!out_view.valid()) {
                    out_view.from_python(nb::dict{});
                }

                auto remove = get_remove();
                auto remove_if_exist = get_remove_if_exists();
                auto ts_meta = out_view.ts_meta();
                if (ts_meta == nullptr || ts_meta->kind != TSKind::TSD || ts_meta->key_type() == nullptr) {
                    return false;
                }
                auto key_type = ts_meta->key_type();
                auto tsd_view = out_view.as_dict();

                // For TSD outputs, iterate over message dict
                auto msg_dict = nb::cast<nb::dict>(message);
                for (auto [key, val]: msg_dict) {
                    if (val.is(remove) || val.is(remove_if_exist)) {
                        auto key_value = key_from_python(key_type, key);
                        auto child_output = tsd_view.at_key(key_value.view());
                        if (child_output && child_output.modified()) {
                            return false; // reject message because cannot remove when there is unprocessed data
                        }
                    }
                }
                for (auto [key, val]: msg_dict) {
                    auto key_value = key_from_python(key_type, key);
                    if (!val.is(remove) && !val.is(remove_if_exist)) {
                        auto child_output = tsd_view.at_key(key_value.view());
                        if (!child_output) {
                            child_output = tsd_view.create(key_value.view());
                        }

                        if (child_output && child_output.modified()) {
                            // Append to existing tuple
                            auto existing = child_output.to_python();
                            child_output.from_python(append_tuple(existing, nb::borrow(val)));
                        } else {
                            // Create new tuple with single element
                            nb::tuple new_tuple = nb::make_tuple(val);
                            child_output.from_python(new_tuple);
                        }
                    } else {
                        tsd_view.remove(key_value.view());
                    }
                }
            } else {
                // For non-TSD outputs, accumulate messages into a tuple
                if (out_view.modified()) {
                    // Append to existing tuple
                    auto existing = out_view.to_python();
                    out_view.from_python(append_tuple(existing, message));
                } else {
                    // Create new tuple with single element
                    nb::tuple new_tuple = nb::make_tuple(message);
                    out_view.from_python(new_tuple);
                }
            }

            ++_messages_dequeued;
            return true;
        }

        if (_elide) {
            out_view.from_python(std::move(message));
            ++_messages_dequeued;
            return true;
        }
        try {
            out_view.from_python(std::move(message));
            ++_messages_dequeued;
            return true;
        } catch (...) {}
        return false;
    }

    int64_t PushQueueNode::messages_in_queue() const { return _messages_queued - _messages_dequeued; }

    void PushQueueNode::set_receiver(sender_receiver_state_ptr value) { _receiver = value; }

    void PushQueueNode::do_start() {
        _receiver = &graph()->receiver();
        _elide = scalars().contains("elide") ? nb::cast<bool>(scalars()["elide"]) : false;
        _batch = scalars().contains("batch") ? nb::cast<bool>(scalars()["batch"]) : false;
        auto out_view = output(node_time(*this));
        _is_tsd = out_view && out_view.ts_meta() != nullptr && out_view.ts_meta()->kind == TSKind::TSD;

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
