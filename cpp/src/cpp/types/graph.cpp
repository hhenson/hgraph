#include <hgraph/builders/graph_builder.h>
#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/traits.h>

#include <utility>

namespace hgraph {
    Graph::Graph(std::vector<int64_t> graph_id_, std::vector<Node::ptr> nodes_, std::optional<Node::ptr> parent_node_,
                 std::string label_, traits_ptr traits_)
        : ComponentLifeCycle(), _graph_id{std::move(graph_id_)}, _nodes{std::move(nodes_)},
          _parent_node{parent_node_.has_value() ? std::move(*parent_node_) : nullptr}, _label{std::move(label_)},
          _traits{std::move(traits_)} {
        auto it{
            std::find_if(_nodes.begin(), _nodes.end(),
                         [](const Node *v) { return v->signature().node_type != NodeTypeEnum::PUSH_SOURCE_NODE; })
        };
        _push_source_nodes_end = std::distance(_nodes.begin(), it);
        _schedule.resize(_nodes.size(), MIN_DT);
    }

    const std::vector<int64_t> &Graph::graph_id() const { return _graph_id; }

    const std::vector<node_ptr> &Graph::nodes() const { return _nodes; }

    node_ptr Graph::parent_node() const { return _parent_node; }

    std::optional<std::string> Graph::label() const { return _label; }

    EvaluationEngineApi::ptr Graph::evaluation_engine_api() { return _evaluation_engine.get(); }

    EvaluationClock::ptr Graph::evaluation_clock() { return _evaluation_engine->evaluation_clock(); }

    EvaluationClock::ptr Graph::evaluation_clock() const { return _evaluation_engine->evaluation_clock(); }

    EngineEvaluationClock::ptr Graph::evaluation_engine_clock() {
        return _evaluation_engine->engine_evaluation_clock();
    }

    EvaluationEngine::ptr Graph::evaluation_engine() { return _evaluation_engine.get(); }

    void Graph::set_evaluation_engine(EvaluationEngine::ptr value) {
        if (_evaluation_engine.get() != nullptr && value.get() != nullptr) {
            throw std::runtime_error("Duplicate attempt to set evaluation engine");
        }
        _evaluation_engine = std::move(value);

        // Cache the clock pointer and evaluation time pointer once at initialization for performance
        _cached_engine_clock = _evaluation_engine->engine_evaluation_clock().get();
        _cached_evaluation_time_ptr = _cached_engine_clock->evaluation_time_ptr();

        if (_push_source_nodes_end > 0) { _receiver.set_evaluation_clock(evaluation_engine_clock()); }
    }

    int64_t Graph::push_source_nodes_end() const { return _push_source_nodes_end; }

    void Graph::schedule_node(int64_t node_ndx, engine_time_t when) { schedule_node(node_ndx, when, false); }

    void Graph::schedule_node(int64_t node_ndx, engine_time_t when, bool force_set) {
        // Use cached evaluation time pointer (set at initialization) - direct memory access
        auto et = *_cached_evaluation_time_ptr;

        // Match Python: just throw if scheduling in the past
        if (when < et) {
            auto graph_id{this->graph_id()};
            auto msg{
                fmt::format(
                    "Graph[{}] Trying to schedule node: {}[{}] for {:%Y-%m-%d %H:%M:%S} but current time is {:%Y-%m-%d %H:%M:%S}",
                    fmt::join(graph_id, ","), this->nodes()[node_ndx]->signature().signature(), node_ndx, when, et)
            };
            throw std::runtime_error(msg);
        }

        auto &st = this->_schedule[node_ndx];
        if (force_set || st <= et || st > when) { st = when; }
        _cached_engine_clock->update_next_scheduled_evaluation_time(when);
    }

    std::vector<engine_time_t> &Graph::schedule() { return _schedule; }

    void Graph::evaluate_graph() {
        NotifyGraphEvaluation nge{evaluation_engine(), graph_ptr{this}};

        // Use cached pointers (set at initialization) for direct memory access
        auto clock = _cached_engine_clock;
        engine_time_t now = *_cached_evaluation_time_ptr;
        auto &nodes = _nodes;
        auto &schedule = _schedule;

        _last_evaluation_time = now;

        // Handle push source nodes scheduling if necessary
        if (push_source_nodes_end() > 0 && clock->push_node_requires_scheduling()) {
            clock->reset_push_node_requires_scheduling();

            while (auto value = receiver().dequeue()) {
                auto [i, message] = *value; // Use the already dequeued value
                auto node = nodes[i];
                auto &node_ref = *node;
                try {
                    NotifyNodeEvaluation nne{evaluation_engine(), node};
                    bool success = dynamic_cast<PushQueueNode &>(node_ref).apply_message(message);
                    if (!success) {
                        receiver().enqueue_front({i, message});
                        clock->mark_push_node_requires_scheduling();
                        break;
                    }
                } catch (const NodeException &e) {
                    throw; // already enriched
                } catch (const std::exception &e) {
                    throw NodeException::capture_error(e, node_ref, "During push node message application");
                } catch (...) {
                    throw NodeException::capture_error(std::current_exception(), node_ref,
                                                       "Unknown error during push node message application");
                }
            }
            try {
                evaluation_engine()->notify_after_push_nodes_evaluation(graph_ptr{this});
            } catch (const NodeException &e) {
                throw; // already enriched
            } catch (const std::exception &e) {
                throw std::runtime_error(std::string("Error in notify_after_push_nodes_evaluation: ") + e.what());
            } catch (...) {
                throw std::runtime_error("Unknown error in notify_after_push_nodes_evaluation");
            }
        }

        for (size_t i = push_source_nodes_end(); i < nodes.size(); ++i) {
            auto scheduled_time = schedule[i];
            auto nodep = nodes[i];
            auto &node = *nodep;

            if (scheduled_time == now) {
                try {
                    NotifyNodeEvaluation nne{evaluation_engine(), nodep};
                    node.eval();
                } catch (const NodeException &e) { throw e; } catch (const std::exception &e) {
                    throw NodeException::capture_error(e, node, "During evaluation");
                } catch (...) {
                    throw NodeException::capture_error(std::current_exception(), node,
                                                       "Unknown error during node evaluation");
                }
            } else if (scheduled_time > now) {
                clock->update_next_scheduled_evaluation_time(scheduled_time);
            }
        }
    }

    Graph::ptr Graph::copy_with(std::vector<Node::ptr> nodes) {
        //This is a copy, need to make sure we copy the graph contents
        return ptr{new Graph(_graph_id, std::move(nodes), _parent_node, _label, _traits->copy())};
    }

    const Traits &Graph::traits() const { return *_traits; }

    SenderReceiverState &Graph::receiver() { return _receiver; }

    void Graph::extend_graph(const GraphBuilder &graph_builder, bool delay_start) {
        auto first_node_index{_nodes.size()};
        auto sz{graph_builder.node_builders.size()};
        auto nodes{graph_builder.make_and_connect_nodes(_graph_id, first_node_index)};
        auto capacity{first_node_index + sz};
        _nodes.reserve(capacity);
        _schedule.reserve(capacity);
        for (auto node: nodes) {
            _nodes.emplace_back(node);
            _schedule.emplace_back(MIN_DT);
        }
        initialise_subgraph(first_node_index, capacity);
        if (!delay_start && is_started()) { start_subgraph(first_node_index, capacity); }
    }

    void Graph::reduce_graph(int64_t start_node) {
        auto end{_nodes.size()};
        if (is_started()) { stop_subgraph(start_node, end); }
        dispose_subgraph(start_node, end);

        _nodes.erase(_nodes.begin() + start_node, _nodes.end());
        _schedule.erase(_schedule.begin() + start_node, _schedule.end());
    }

    void Graph::initialise_subgraph(int64_t start, int64_t end) {
        // Need to ensure that the graph is set prior to initialising the nodes
        // In case of interaction between nodes.
        for (auto i = start; i < end; ++i) {
            auto node{_nodes[i]};
            node->set_graph(this);
        }
        for (auto i = start; i < end; ++i) {
            auto node{_nodes[i]};
            initialise_component(*node);
        }
    }

    void Graph::start_subgraph(int64_t start, int64_t end) {
        for (auto i = start; i < end; ++i) {
            auto node{_nodes[i]};
            try {
                evaluation_engine()->notify_before_start_node(node);
                start_component(*node);
                evaluation_engine()->notify_after_start_node(node);
            } catch (const NodeException &e) {
                throw; // already enriched
            } catch (const std::exception &e) {
                throw NodeException::capture_error(e, *node, "During node start");
            } catch (...) {
                throw NodeException::capture_error(std::current_exception(), *node, "Unknown error during node start");
            }
        }
    }

    void Graph::stop_subgraph(int64_t start, int64_t end) {
        for (auto i = start; i < end; ++i) {
            auto node{_nodes[i]};
            try {
                evaluation_engine()->notify_before_stop_node(node);
                stop_component(*node);
                evaluation_engine()->notify_after_stop_node(node);
            } catch (const NodeException &e) {
                throw; // already enriched
            } catch (const std::exception &e) {
                throw NodeException::capture_error(e, *node, "During node stop");
            } catch (...) {
                throw NodeException::capture_error(std::current_exception(), *node, "Unknown error during node stop");
            }
        }
    }

    void Graph::dispose_subgraph(int64_t start, int64_t end) {
        for (auto i = start; i < end; ++i) {
            auto node{_nodes[i]};
            dispose_component(*node);
        }
    }

    void Graph::register_with_nanobind(nb::module_ &m) {
        nb::class_ < Graph, ComponentLifeCycle > (m, "Graph")
                .def(nb::init<std::vector<int64_t>, std::vector<node_ptr>, std::optional<node_ptr>, std::string,
                         traits_ptr>(),
                     "graph_id"_a, "nodes"_a, "parent_node"_a, "label"_a, "traits"_a)
                .def_prop_ro("graph_id", [](const Graph& self){ return nb::tuple(nb::cast(self.graph_id())); })
                .def_prop_ro("nodes", &Graph::nodes)
                .def_prop_ro("parent_node", &Graph::parent_node)
                .def_prop_ro("label", &Graph::label)
                .def_prop_ro("evaluation_engine_api", &Graph::evaluation_engine_api)
                .def_prop_ro("evaluation_clock",
                             static_cast<EvaluationClock::ptr (Graph::*)() const>(&Graph::evaluation_clock))
                .def_prop_ro("engine_evaluation_clock", &Graph::evaluation_engine_clock)
                .def_prop_rw("evaluation_engine", &Graph::evaluation_engine, &Graph::set_evaluation_engine)
                .def_prop_ro("push_source_nodes_end", &Graph::push_source_nodes_end)
                .def("schedule_node", static_cast<void (Graph::*)(int64_t, engine_time_t, bool)>(&Graph::schedule_node),
                     "node_ndx"_a,
                     "when"_a, "force_set"_a = false)
                .def_prop_ro("schedule", &Graph::schedule)
                .def("evaluate_graph", &Graph::evaluate_graph)
                .def("copy_with", &Graph::copy_with, "nodes"_a)
                .def_prop_ro("traits", &Graph::traits)
                .def("__str__", [](const Graph &self) {
                    return fmt::format("Graph@{:p}[id={}, nodes={}]",
                                       static_cast<const void *>(&self),
                                       fmt::join(self.graph_id(), ","),
                                       self.nodes().size());
                })
                .def("__repr__", [](const Graph &self) {
                    return fmt::format("Graph@{:p}[id={}, nodes={}]",
                                       static_cast<const void *>(&self),
                                       fmt::join(self.graph_id(), ","),
                                       self.nodes().size());
                });;
    }

    void Graph::initialise() {
        // Need to ensure that the graph is set prior to initialising the nodes
        // In case of interaction between nodes.
        for (auto &node: _nodes) { node->set_graph(this); }
        for (auto &node: _nodes) { node->initialise(); }
    }

    void Graph::start() {
        auto &engine = *_evaluation_engine;
        engine.notify_before_start_graph(graph_ptr{this});
        for (auto &node: _nodes) {
            engine.notify_before_start_node(node);
            start_component(*node);
            engine.notify_after_start_node(node);
        }
        engine.notify_after_start_graph(graph_ptr{this});
    }

    void Graph::stop() {
        auto &engine = *_evaluation_engine;
        engine.notify_before_stop_graph(graph_ptr{this});
        std::exception_ptr first_exc;
        for (auto &node: _nodes) {
            try {
                engine.notify_before_stop_node(node);
            } catch (...) {
                if (!first_exc) first_exc = std::current_exception();
            }
            try {
                stop_component(*node);
            } catch (...) {
                if (!first_exc) first_exc = std::current_exception();
            }
            try {
                engine.notify_after_stop_node(node);
            } catch (...) {
                if (!first_exc) first_exc = std::current_exception();
            }
        }
        try {
            engine.notify_after_stop_graph(graph_ptr{this});
        } catch (...) {
            if (!first_exc) first_exc = std::current_exception();
        }
        if (first_exc) std::rethrow_exception(first_exc);
    }

    void Graph::dispose() {
        // Since we initialise nodes from within the graph, we need to dispose them here.
        for (auto &node: _nodes) { node->dispose(); }
    }
} // namespace hgraph