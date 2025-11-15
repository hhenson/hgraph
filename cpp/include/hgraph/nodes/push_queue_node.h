//
// Created by Howard Henson on 24/10/2025.
//

#ifndef HGRAPH_CPP_ENGINE_PUSH_QUEUE_NODE_H
#define HGRAPH_CPP_ENGINE_PUSH_QUEUE_NODE_H

#include <hgraph/types/node.h>

namespace hgraph {
    /**
     * PushQueueNode - Node that receives messages from external sources
     *
     * This node type is used with the @push_queue decorator in Python.
     * It maintains a queue of messages that can be pushed from external
     * sources (via a sender callable) and processes them through the
     * node evaluation cycle.
     *
     * Features:
     * - Elide mode: applies messages immediately if output can accept them
     * - Batch mode: controls message batching behavior
     * - Message queuing: tracks queued vs dequeued messages
     * - Custom eval function: optional callable that receives a sender
     */
    struct PushQueueNode : Node {
        using Node::Node;

        void set_eval_fn(nb::callable fn) { _eval_fn = std::move(fn); }

        void enqueue_message(nb::object message);

        [[nodiscard]] bool apply_message(nb::object message);

        int64_t messages_in_queue() const;

        void set_receiver(sender_receiver_state_ptr value);

    protected:
        void do_eval() override;

        void do_start() override;

        void do_stop() override {
        }

        void initialise() override {
        }

        void dispose() override {
        }

    private:
        sender_receiver_state_ptr _receiver;
        int64_t _messages_queued{0};
        int64_t _messages_dequeued{0};
        bool _elide{false};
        bool _batch{false};
        nb::callable _eval_fn;

        bool _is_tsd = false;
    };
} // namespace hgraph

#endif  // HGRAPH_CPP_ENGINE_PUSH_QUEUE_NODE_H