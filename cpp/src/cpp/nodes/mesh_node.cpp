#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/python/global_keys.h>
#include <hgraph/python/global_state.h>
#include <hgraph/types/graph.h>

namespace hgraph {
    MeshNestedEngineEvaluationClock::MeshNestedEngineEvaluationClock(
        EngineEvaluationClock::ptr engine_evaluation_clock,
        value::Value key,
        mesh_node_ptr nested_node)
        : NestedEngineEvaluationClock(std::move(engine_evaluation_clock), static_cast<NestedNode*>(nested_node)),
          _key(std::move(key)) {}

    nb::object MeshNestedEngineEvaluationClock::py_key() const {
        auto* node_ = static_cast<MeshNode*>(node());
        if (const auto* key_schema = node_->key_type_meta(); key_schema != nullptr) {
            return key_schema->ops().to_python(_key.data(), key_schema);
        }
        return nb::none();
    }

    void MeshNestedEngineEvaluationClock::update_next_scheduled_evaluation_time(engine_time_t next_time) {
        NestedEngineEvaluationClock::update_next_scheduled_evaluation_time(next_time);
        auto* node_ = static_cast<MeshNode*>(_nested_node);
        if (!node_) {
            return;
        }
        auto rank_it = node_->active_graphs_rank_.find(_key.view());
        if (rank_it == node_->active_graphs_rank_.end()) {
            return;
        }
        node_->schedule_graph(_key.view(), next_time);
    }

    MeshNode::MeshNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                       nb::dict scalars, const TSMeta* input_meta, const TSMeta* output_meta,
                       const TSMeta* error_output_meta, const TSMeta* recordable_state_meta,
                       graph_builder_s_ptr nested_graph_builder,
                       const std::unordered_map<std::string, int64_t> &input_node_ids,
                       int64_t output_node_id, const std::unordered_set<std::string> &multiplexed_args,
                       const std::string &key_arg, const std::string &context_path)
        : TsdMapNode(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
                     input_meta, output_meta, error_output_meta, recordable_state_meta,
                     std::move(nested_graph_builder), input_node_ids, output_node_id, multiplexed_args, key_arg) {
        full_context_path_ = keys::context_output_key(this->owning_graph_id(), context_path);
    }

    bool MeshNode::_add_graph_dependency(const nb::object &key, const nb::object &depends_on) {
        if (key_type_meta_ == nullptr) {
            return false;
        }
        value::Value key_val(key_type_meta_);
        key_val.emplace();
        key_type_meta_->ops().from_python(key_val.data(), key, key_type_meta_);

        value::Value depends_on_val(key_type_meta_);
        depends_on_val.emplace();
        key_type_meta_->ops().from_python(depends_on_val.data(), depends_on, key_type_meta_);
        return add_graph_dependency(key_val.view(), depends_on_val.view());
    }

    void MeshNode::_remove_graph_dependency(const nb::object &key, const nb::object &depends_on) {
        if (key_type_meta_ == nullptr) {
            return;
        }
        value::Value key_val(key_type_meta_);
        key_val.emplace();
        key_type_meta_->ops().from_python(key_val.data(), key, key_type_meta_);

        value::Value depends_on_val(key_type_meta_);
        depends_on_val.emplace();
        key_type_meta_->ops().from_python(depends_on_val.data(), depends_on, key_type_meta_);
        remove_graph_dependency(key_val.view(), depends_on_val.view());
    }

    void MeshNode::do_start() {
        TsdMapNode::do_start();
        if (GlobalState::has_instance()) {
            GlobalState::set(full_context_path_, nb::none());
        }
    }

    void MeshNode::do_stop() {
        if (GlobalState::has_instance()) {
            GlobalState::remove(full_context_path_);
        }
        TsdMapNode::do_stop();
    }

    void MeshNode::eval() {
        mark_evaluated();
        throw std::runtime_error("MeshNode TS migration pending: legacy TimeSeriesInput/Output path removed");
    }

    TimeSeriesDictOutputImpl &MeshNode::tsd_output() {
        throw std::runtime_error("MeshNode::tsd_output unavailable in TS runtime cutover");
    }

    void MeshNode::create_new_graph(const value::View &, int) {
        throw std::runtime_error("MeshNode::create_new_graph pending TS migration");
    }

    void MeshNode::remove_graph(const value::View &key) {
        TsdMapNode::remove_graph(key);
    }

    void MeshNode::schedule_graph(const value::View &key, engine_time_t tm) {
        auto rank_it = active_graphs_rank_.find(key);
        if (rank_it == active_graphs_rank_.end()) {
            return;
        }

        auto rank = rank_it->second;
        scheduled_keys_by_rank_[rank].insert_or_assign(key.clone(), tm);
        auto eval_time = graph()->evaluation_time();
        auto current_it = scheduled_ranks_.find(rank);
        auto current = current_it == scheduled_ranks_.end() ? MAX_DT : current_it->second;
        scheduled_ranks_[rank] = std::min(std::max(current, eval_time), tm);
        graph()->schedule_node(node_ndx(), tm);
    }

    bool MeshNode::add_graph_dependency(const value::View &key, const value::View &depends_on) {
        active_graphs_dependencies_[depends_on.clone()].insert(key.clone());
        return true;
    }

    void MeshNode::remove_graph_dependency(const value::View &key, const value::View &depends_on) {
        auto deps_it = active_graphs_dependencies_.find(depends_on);
        if (deps_it == active_graphs_dependencies_.end()) {
            return;
        }
        if (auto key_it = deps_it->second.find(key); key_it != deps_it->second.end()) {
            deps_it->second.erase(key_it);
        }
    }

    bool MeshNode::request_re_rank(const value::View &, const value::View &) {
        return true;
    }

    void MeshNode::re_rank(const value::View &, const value::View &, std::vector<value::Value>) {}

    void register_mesh_node_with_nanobind(nb::module_ &m) {
        nb::class_<MeshNestedEngineEvaluationClock, NestedEngineEvaluationClock>(
            m, "MeshNestedEngineEvaluationClock")
            .def_prop_ro("key", &MeshNestedEngineEvaluationClock::py_key);

        nb::class_<MeshNode, TsdMapNode>(m, "MeshNode")
            .def("_add_graph_dependency", &MeshNode::_add_graph_dependency, "key"_a, "depends_on"_a)
            .def("_remove_graph_dependency", &MeshNode::_remove_graph_dependency, "key"_a, "depends_on"_a);
    }
}  // namespace hgraph
