#include <hgraph/types/v2/graph.h>
#include <hgraph/types/v2/node_impl.h>
#include <hgraph/types/v2/node.h>

#include <stdexcept>

namespace hgraph::v2
{
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
        return m_spec != nullptr ? m_spec->label : std::string_view{};
    }

    std::string Node::runtime_label() const
    {
        if (m_spec != nullptr && m_spec->runtime_ops != nullptr && m_spec->runtime_ops->runtime_label != nullptr) {
            return m_spec->runtime_ops->runtime_label(*this);
        }
        return {};
    }

    const TSMeta *Node::input_schema() const noexcept
    {
        return m_spec != nullptr ? m_spec->input_schema : nullptr;
    }

    const TSMeta *Node::output_schema() const noexcept
    {
        return m_spec != nullptr ? m_spec->output_schema : nullptr;
    }

    bool Node::has_input() const noexcept
    {
        return m_spec != nullptr && m_spec->runtime_ops != nullptr && m_spec->runtime_ops->has_input != nullptr &&
               m_spec->runtime_ops->has_input(*this);
    }

    bool Node::has_output() const noexcept
    {
        return m_spec != nullptr && m_spec->runtime_ops != nullptr && m_spec->runtime_ops->has_output != nullptr &&
               m_spec->runtime_ops->has_output(*this);
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
        return m_spec != nullptr ? reinterpret_cast<std::byte *>(this) + m_spec->runtime_data_offset : nullptr;
    }

    const void *Node::data() const noexcept
    {
        return m_spec != nullptr ? reinterpret_cast<const std::byte *>(this) + m_spec->runtime_data_offset : nullptr;
    }

    TSInputView Node::input_view(engine_time_t evaluation_time)
    {
        if (m_spec == nullptr || m_spec->runtime_ops == nullptr || m_spec->runtime_ops->input_view == nullptr) {
            return detail::invalid_input_view(evaluation_time);
        }
        return m_spec->runtime_ops->input_view(*this, evaluation_time);
    }

    TSOutputView Node::output_view(engine_time_t evaluation_time)
    {
        if (m_spec == nullptr || m_spec->runtime_ops == nullptr || m_spec->runtime_ops->output_view == nullptr) {
            return detail::invalid_output_view(evaluation_time);
        }
        return m_spec->runtime_ops->output_view(*this, evaluation_time);
    }

    const BuiltNodeSpec &Node::spec() const noexcept
    {
        return *m_spec;
    }

    bool Node::ready_to_eval(engine_time_t evaluation_time)
    {
        if (!has_input()) { return true; }

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
        if (m_spec == nullptr || m_spec->runtime_ops == nullptr || m_spec->runtime_ops->start == nullptr) { return; }
        m_spec->runtime_ops->start(*this, evaluation_time);
    }

    void Node::stop(engine_time_t evaluation_time)
    {
        if (m_spec == nullptr || m_spec->runtime_ops == nullptr || m_spec->runtime_ops->stop == nullptr) { return; }
        m_spec->runtime_ops->stop(*this, evaluation_time);
    }

    void Node::eval(engine_time_t evaluation_time)
    {
        if (m_spec == nullptr || m_spec->runtime_ops == nullptr || m_spec->runtime_ops->eval == nullptr) { return; }
        m_spec->runtime_ops->eval(*this, evaluation_time);
    }

    void Node::notify(engine_time_t et)
    {
        if (m_graph != nullptr) { m_graph->schedule_node(m_node_index, et); }
    }

    void Node::destruct() noexcept
    {
        if (m_spec != nullptr && m_spec->destruct != nullptr) { m_spec->destruct(*this); }
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
