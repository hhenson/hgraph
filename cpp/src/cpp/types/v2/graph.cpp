#include <hgraph/types/v2/graph.h>

#include <new>
#include <stdexcept>

namespace hgraph::v2
{
    Graph::~Graph()
    {
        clear_storage();
    }

    Graph::Graph(Graph &&other) noexcept
        : m_node_count(other.m_node_count),
          m_started(other.m_started),
          m_storage_alignment(other.m_storage_alignment),
          m_evaluation_runtime(other.m_evaluation_runtime),
          m_storage(other.m_storage)
    {
        other.m_node_count = 0;
        other.m_started = false;
        other.m_storage_alignment = alignof(std::max_align_t);
        other.m_evaluation_runtime = {};
        other.m_storage = nullptr;
        attach_nodes();
    }

    Graph &Graph::operator=(Graph &&other) noexcept
    {
        if (this != &other) {
            clear_storage();

            m_node_count = other.m_node_count;
            m_started = other.m_started;
            m_storage_alignment = other.m_storage_alignment;
            m_evaluation_runtime = other.m_evaluation_runtime;
            m_storage = other.m_storage;

            other.m_node_count = 0;
            other.m_started = false;
            other.m_storage_alignment = alignof(std::max_align_t);
            other.m_evaluation_runtime = {};
            other.m_storage = nullptr;

            attach_nodes();
        }
        return *this;
    }

    EvaluationEngineApi Graph::evaluation_engine_api() const noexcept
    {
        return m_evaluation_runtime ? m_evaluation_runtime.evaluation_engine_api() : EvaluationEngineApi{};
    }

    EvaluationClock Graph::evaluation_clock() const noexcept
    {
        const auto api = evaluation_engine_api();
        return api ? api.evaluation_clock() : EvaluationClock{};
    }

    EngineEvaluationClock Graph::engine_evaluation_clock() const noexcept
    {
        return m_evaluation_runtime ? m_evaluation_runtime.engine_evaluation_clock() : EngineEvaluationClock{};
    }

    engine_time_t Graph::evaluation_time() const noexcept
    {
        const auto clock = engine_evaluation_clock();
        return clock ? clock.evaluation_time() : MIN_DT;
    }

    engine_time_t Graph::scheduled_time(size_t index) const
    {
        if (index >= m_node_count) { throw std::out_of_range("v2 graph schedule index is out of range"); }
        return entry_storage()[index].scheduled;
    }

    void Graph::set_evaluation_runtime(EvaluationRuntime evaluation_runtime)
    {
        if (m_evaluation_runtime && evaluation_runtime) {
            throw std::runtime_error("Duplicate attempt to set evaluation runtime");
        }
        m_evaluation_runtime = evaluation_runtime;
    }

    void Graph::adopt_storage(void *storage, size_t storage_alignment, size_t node_count) noexcept
    {
        clear_storage();
        m_node_count = node_count;
        m_started = false;
        m_storage_alignment = storage_alignment;
        m_storage = storage;
        attach_nodes();
    }

    void Graph::clear_storage() noexcept
    {
        if (m_storage == nullptr) { return; }

        if (m_started) {
            try {
                stop();
            } catch (...) {
            }
        }

        if (entry_storage() != nullptr) {
            for (size_t i = m_node_count; i > 0; --i) {
                if (entry_storage()[i - 1].node != nullptr) { entry_storage()[i - 1].node->destruct(); }
            }
        }

        ::operator delete(m_storage, std::align_val_t(m_storage_alignment));
        m_node_count = 0;
        m_started = false;
        m_storage_alignment = alignof(std::max_align_t);
        m_evaluation_runtime = {};
        m_storage = nullptr;
    }

    void Graph::attach_nodes() noexcept
    {
        if (entry_storage() == nullptr) { return; }
        for (size_t i = 0; i < m_node_count; ++i) {
            if (entry_storage()[i].node != nullptr) { entry_storage()[i].node->set_graph(this); }
        }
    }

    NodeEntry *Graph::entry_storage() noexcept
    {
        return m_storage != nullptr ? reinterpret_cast<NodeEntry *>(m_storage) : nullptr;
    }

    const NodeEntry *Graph::entry_storage() const noexcept
    {
        return m_storage != nullptr ? reinterpret_cast<const NodeEntry *>(m_storage) : nullptr;
    }

    Node &Graph::node_at(size_t index)
    {
        if (index >= m_node_count) { throw std::out_of_range("v2 graph node index is out of range"); }
        return *entry_storage()[index].node;
    }

    const Node &Graph::node_at(size_t index) const
    {
        if (index >= m_node_count) { throw std::out_of_range("v2 graph node index is out of range"); }
        return *entry_storage()[index].node;
    }

    void Graph::start()
    {
        if (m_started) { return; }

        if (m_evaluation_runtime) { m_evaluation_runtime.notify_before_start_graph(*this); }
        for (size_t i = 0; i < m_node_count; ++i) {
            auto &node = *entry_storage()[i].node;
            if (m_evaluation_runtime) { m_evaluation_runtime.notify_before_start_node(node); }
            node.start(evaluation_time());
            if (m_evaluation_runtime) { m_evaluation_runtime.notify_after_start_node(node); }
        }
        if (m_evaluation_runtime) { m_evaluation_runtime.notify_after_start_graph(*this); }
        m_started = true;
    }

    void Graph::stop()
    {
        if (!m_started) { return; }

        if (m_evaluation_runtime) { m_evaluation_runtime.notify_before_stop_graph(*this); }
        for (size_t i = 0; i < m_node_count; ++i) {
            auto &node = *entry_storage()[i].node;
            if (m_evaluation_runtime) { m_evaluation_runtime.notify_before_stop_node(node); }
            node.stop(evaluation_time());
            if (m_evaluation_runtime) { m_evaluation_runtime.notify_after_stop_node(node); }
        }
        if (m_evaluation_runtime) { m_evaluation_runtime.notify_after_stop_graph(*this); }
        m_started = false;
    }

    void Graph::evaluate(engine_time_t when)
    {
        const auto clock = engine_evaluation_clock();
        if (!clock) { throw std::logic_error("v2 graph evaluation requires an attached evaluation engine"); }

        clock.set_evaluation_time(when);
        m_evaluation_runtime.notify_before_graph_evaluation(*this);

        for (size_t index = 0; index < m_node_count; ++index) {
            auto &entry = entry_storage()[index];
            if (entry.scheduled == when) {
                m_evaluation_runtime.notify_before_node_evaluation(*entry.node);
                entry.scheduled = MIN_DT;
                entry.node->eval(when);
                m_evaluation_runtime.notify_after_node_evaluation(*entry.node);
            } else if (entry.scheduled > when) {
                clock.update_next_scheduled_evaluation_time(entry.scheduled);
            }
        }

        m_evaluation_runtime.notify_after_graph_evaluation(*this);
    }

    void Graph::schedule_node(int64_t node_index, engine_time_t when)
    {
        if (node_index < 0 || static_cast<size_t>(node_index) >= m_node_count) {
            throw std::out_of_range("v2 graph schedule index is out of range");
        }

        const auto clock = engine_evaluation_clock();
        if (!clock) { throw std::logic_error("v2 graph scheduling requires an attached evaluation engine"); }

        const engine_time_t current_time = clock.evaluation_time();
        if (current_time != MIN_DT && when < current_time) {
            throw std::runtime_error("v2 graph cannot schedule a node in the past");
        }

        engine_time_t &scheduled_time = entry_storage()[node_index].scheduled;
        if (scheduled_time == MIN_DT || when < scheduled_time) { scheduled_time = when; }
        clock.update_next_scheduled_evaluation_time(when);
    }
}  // namespace hgraph::v2
