#include <hgraph/types/v2/graph.h>
#include <hgraph/types/v2/node_impl.h>
#include <hgraph/types/v2/node.h>
#include <hgraph/util/scope.h>

#include <cassert>
#include <stdexcept>

namespace hgraph::v2
{
    namespace
    {
        void activate_active_inputs(Node &node, engine_time_t evaluation_time)
        {
            if (!node.has_input()) { return; }

            const TSMeta *schema = node.input_schema();
            if (schema == nullptr || schema->kind != TSKind::TSB) {
                throw std::logic_error("v2 top-level input selectors require a TSB root input schema");
            }

            size_t activated_inputs_end = 0;
            auto rollback_activation = UnwindCleanupGuard([&] {
                auto view = node.input_view(evaluation_time).as_bundle();
                for (size_t i = activated_inputs_end; i > 0; --i) { view[node.spec().active_inputs[i - 1]].make_passive(); }
            });

            // For now selector activation is limited to top-level TSB fields.
            // REF / alternative-view activation comes later.
            auto view = node.input_view(evaluation_time).as_bundle();
            for (const size_t slot : node.spec().active_inputs) {
                if (slot >= schema->field_count()) { throw std::out_of_range("v2 input selector is out of range"); }
                view[slot].make_active();
                ++activated_inputs_end;
            }
        }

        void deactivate_active_inputs(Node &node, engine_time_t evaluation_time)
        {
            if (!node.has_input()) { return; }

            const TSMeta *schema = node.input_schema();
            if (schema == nullptr || schema->kind != TSKind::TSB) {
                throw std::logic_error("v2 top-level input selectors require a TSB root input schema");
            }

            auto view = node.input_view(evaluation_time).as_bundle();
            for (const size_t slot : node.spec().active_inputs) {
                if (slot >= schema->field_count()) { throw std::out_of_range("v2 input selector is out of range"); }
                view[slot].make_passive();
            }
        }
    }  // namespace

    Graph &detail::arg_provider<Graph>::get(Node &node, engine_time_t)
    {
        if (node.graph() == nullptr) { throw std::logic_error("Node implementation requested Graph but node is not attached"); }
        return *node.graph();
    }

    Graph *detail::arg_provider<Graph *>::get(Node &node, engine_time_t)
    {
        return node.graph();
    }

    EvaluationClock detail::arg_provider<EvaluationClock>::get(Node &node, engine_time_t)
    {
        if (node.graph() == nullptr) { throw std::logic_error("Node implementation requested EvaluationClock but node is not attached"); }
        return node.graph()->evaluation_clock();
    }

    TSInputView detail::arg_provider<TSInputView>::get(Node &node, engine_time_t evaluation_time)
    {
        return node.input_view(evaluation_time);
    }

    TSOutputView detail::arg_provider<TSOutputView>::get(Node &node, engine_time_t evaluation_time)
    {
        return node.output_view(evaluation_time);
    }

    TSInputView input_view_for(Node &node, engine_time_t evaluation_time)
    {
        return node.input_view(evaluation_time);
    }

    TSOutputView output_view_for(Node &node, engine_time_t evaluation_time)
    {
        return node.output_view(evaluation_time);
    }

    Node::Node(int64_t node_index, const BuiltNodeSpec *spec) noexcept
        : m_spec(spec), m_node_index(node_index)
    {
        assert(m_spec != nullptr);
        assert(m_spec->runtime_ops != nullptr);
        assert(m_spec->destruct != nullptr);
    }

    void Node::set_graph(Graph *graph) noexcept
    {
        m_graph = graph;
    }

    Graph *Node::graph() const noexcept
    {
        return m_graph;
    }

    int64_t Node::node_index() const noexcept
    {
        return m_node_index;
    }

    std::string_view Node::label() const noexcept
    {
        return spec().label;
    }

    std::string Node::runtime_label() const
    {
        return spec().runtime_ops->runtime_label(*this);
    }

    NodeTypeEnum Node::node_type() const noexcept
    {
        return spec().node_type;
    }

    bool Node::is_push_source_node() const noexcept
    {
        return node_type() == NodeTypeEnum::PUSH_SOURCE_NODE;
    }

    bool Node::is_pull_source_node() const noexcept
    {
        return node_type() == NodeTypeEnum::PULL_SOURCE_NODE;
    }

    const TSMeta *Node::input_schema() const noexcept
    {
        return spec().input_schema;
    }

    const TSMeta *Node::output_schema() const noexcept
    {
        return spec().output_schema;
    }

    bool Node::has_input() const noexcept
    {
        return spec().runtime_ops->has_input(*this);
    }

    bool Node::has_output() const noexcept
    {
        return spec().runtime_ops->has_output(*this);
    }

    bool Node::started() const noexcept
    {
        return m_started;
    }

    engine_time_t Node::evaluation_time() const noexcept
    {
        return m_graph != nullptr ? m_graph->evaluation_time() : MIN_DT;
    }

    void Node::set_started(bool value) noexcept
    {
        m_started = value;
    }

    void *Node::data() noexcept
    {
        return reinterpret_cast<std::byte *>(this) + spec().runtime_data_offset;
    }

    const void *Node::data() const noexcept
    {
        return reinterpret_cast<const std::byte *>(this) + spec().runtime_data_offset;
    }

    TSInputView Node::input_view(engine_time_t evaluation_time)
    {
        return spec().runtime_ops->input_view(*this, evaluation_time);
    }

    TSOutputView Node::output_view(engine_time_t evaluation_time)
    {
        return spec().runtime_ops->output_view(*this, evaluation_time);
    }

    const BuiltNodeSpec &Node::spec() const noexcept
    {
        assert(m_spec != nullptr);
        return *m_spec;
    }

    bool Node::ready_to_eval(engine_time_t evaluation_time)
    {
        if (!has_input()) { return true; }

        // The selector metadata currently addresses top-level TSB slots only.
        for (const size_t slot : spec().valid_inputs) {
            if (!resolve_input_slot(slot, evaluation_time).valid()) { return false; }
        }

        for (const size_t slot : spec().all_valid_inputs) {
            if (!resolve_input_slot(slot, evaluation_time).all_valid()) { return false; }
        }

        return true;
    }

    void Node::start(engine_time_t evaluation_time)
    {
        if (m_started) { return; }

        activate_active_inputs(*this, evaluation_time);
        auto rollback_start = UnwindCleanupGuard([&] { deactivate_active_inputs(*this, evaluation_time); });
        spec().runtime_ops->start(*this, evaluation_time);
        m_started = true;
    }

    void Node::stop(engine_time_t evaluation_time)
    {
        if (!m_started) { return; }

        auto mark_stopped = hgraph::make_scope_exit([&] { m_started = false; });
        auto deactivate_inputs = UnwindCleanupGuard([&] { deactivate_active_inputs(*this, evaluation_time); });
        spec().runtime_ops->stop(*this, evaluation_time);
        deactivate_inputs.complete();
    }

    void Node::eval(engine_time_t evaluation_time)
    {
        if (!ready_to_eval(evaluation_time)) { return; }
        spec().runtime_ops->eval(*this, evaluation_time);
    }

    void Node::notify(engine_time_t et)
    {
        if (m_graph != nullptr) { m_graph->schedule_node(m_node_index, et); }
    }

    void Node::destruct() noexcept
    {
        spec().destruct(*this);
        this->~Node();
    }

    TSInputView Node::resolve_input_slot(size_t slot, engine_time_t evaluation_time)
    {
        const TSMeta *schema = input_schema();
        if (schema == nullptr || schema->kind != TSKind::TSB) {
            throw std::logic_error("v2 top-level input selectors require a TSB root input schema");
        }
        if (slot >= schema->field_count()) { throw std::out_of_range("v2 input selector is out of range"); }
        return input_view(evaluation_time).as_bundle()[slot];
    }

}  // namespace hgraph::v2
