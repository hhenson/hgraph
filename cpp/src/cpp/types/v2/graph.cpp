#include <hgraph/types/v2/graph.h>
#include <hgraph/util/scope.h>

#include <new>
#include <stdexcept>
#include <utility>

namespace hgraph::v2
{
    Graph::~Graph()
    {
        clear_storage();
    }

    Graph::Graph(GraphEvaluationEngine evaluation_engine) noexcept : m_evaluation_engine(evaluation_engine)
    {
    }

    Graph::Graph(Graph &&other) noexcept
        : m_node_count(other.m_node_count),
          m_push_source_nodes_end(other.m_push_source_nodes_end),
          m_started(other.m_started),
          m_storage_alignment(other.m_storage_alignment),
          m_last_evaluation_time(other.m_last_evaluation_time),
          m_evaluation_engine(other.m_evaluation_engine),
          m_storage(other.m_storage)
    {
        other.m_node_count = 0;
        other.m_push_source_nodes_end = 0;
        other.m_started = false;
        other.m_storage_alignment = alignof(std::max_align_t);
        other.m_last_evaluation_time = MIN_DT;
        other.m_evaluation_engine = {};
        other.m_storage = nullptr;
        attach_nodes();
    }

    Graph &Graph::operator=(Graph &&other) noexcept
    {
        if (this != &other) {
            clear_storage();

            m_node_count = other.m_node_count;
            m_push_source_nodes_end = other.m_push_source_nodes_end;
            m_started = other.m_started;
            m_storage_alignment = other.m_storage_alignment;
            m_last_evaluation_time = other.m_last_evaluation_time;
            m_evaluation_engine = other.m_evaluation_engine;
            m_storage = other.m_storage;

            other.m_node_count = 0;
            other.m_push_source_nodes_end = 0;
            other.m_started = false;
            other.m_storage_alignment = alignof(std::max_align_t);
            other.m_last_evaluation_time = MIN_DT;
            other.m_evaluation_engine = {};
            other.m_storage = nullptr;

            attach_nodes();
        }
        return *this;
    }

    EvaluationEngineApi Graph::evaluation_engine_api() const noexcept
    {
        return m_evaluation_engine.evaluation_engine_api();
    }

    EvaluationClock Graph::evaluation_clock() const noexcept
    {
        return evaluation_engine_api().evaluation_clock();
    }

    EngineEvaluationClock Graph::engine_evaluation_clock() const noexcept
    {
        return m_evaluation_engine.engine_evaluation_clock();
    }

    engine_time_t Graph::evaluation_time() const noexcept
    {
        return engine_evaluation_clock().evaluation_time();
    }

    engine_time_t Graph::last_evaluation_time() const noexcept
    {
        return m_last_evaluation_time;
    }

    int64_t Graph::push_source_nodes_end() const noexcept
    {
        return m_push_source_nodes_end;
    }

    engine_time_t Graph::scheduled_time(size_t index) const
    {
        if (index >= m_node_count) { throw std::out_of_range("v2 graph schedule index is out of range"); }
        return entry_storage()[index].scheduled;
    }

    void Graph::adopt_storage(void *storage, size_t storage_alignment, size_t node_count, int64_t push_source_nodes_end) noexcept
    {
        clear_storage();
        m_node_count = node_count;
        m_push_source_nodes_end = push_source_nodes_end;
        m_started = false;
        m_storage_alignment = storage_alignment;
        m_last_evaluation_time = MIN_DT;
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
        m_push_source_nodes_end = 0;
        m_started = false;
        m_storage_alignment = alignof(std::max_align_t);
        m_last_evaluation_time = MIN_DT;
        m_evaluation_engine = {};
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

        m_evaluation_engine.notify_before_start_graph(*this);
        size_t rollback_nodes_end = 0;
        auto rollback_started_nodes = UnwindCleanupGuard([&] { stop_nodes(rollback_nodes_end); });

        for (size_t i = 0; i < m_node_count; ++i) {
            auto &node = *entry_storage()[i].node;
            m_evaluation_engine.notify_before_start_node(node);
            node.start(evaluation_time());
            rollback_nodes_end = i + 1;
            m_evaluation_engine.notify_after_start_node(node);
        }
        m_evaluation_engine.notify_after_start_graph(*this);
        m_started = true;
    }

    void Graph::stop()
    {
        if (!m_started) { return; }

        auto mark_stopped = hgraph::make_scope_exit([&] { m_started = false; });
        stop_nodes(m_node_count);
    }

    void Graph::stop_nodes(size_t nodes_end)
    {
        m_evaluation_engine.notify_before_stop_graph(*this);
        FirstExceptionRecorder exceptions;
        for (size_t i = 0; i < nodes_end; ++i) {
            auto &node = *entry_storage()[i].node;
            const bool was_started = node.started();
            exceptions.capture([&] {
                if (was_started) { m_evaluation_engine.notify_before_stop_node(node); }
            });
            exceptions.capture([&] { node.stop(evaluation_time()); });
            exceptions.capture([&] {
                if (was_started) { m_evaluation_engine.notify_after_stop_node(node); }
            });
        }
        exceptions.capture([&] { m_evaluation_engine.notify_after_stop_graph(*this); });
        exceptions.rethrow_if_any();
    }

    void Graph::evaluate(engine_time_t when)
    {
        const auto clock = engine_evaluation_clock();

        clock.set_evaluation_time(when);
        m_last_evaluation_time = when;
        m_evaluation_engine.notify_before_graph_evaluation(*this);
        auto after_graph_evaluation = UnwindCleanupGuard([&] { m_evaluation_engine.notify_after_graph_evaluation(*this); });

        m_evaluation_engine.evaluate_push_source_nodes(*this, when);

        for (size_t index = static_cast<size_t>(m_push_source_nodes_end); index < m_node_count; ++index) {
            auto &entry = entry_storage()[index];
            if (entry.scheduled == when) {
                m_evaluation_engine.notify_before_node_evaluation(*entry.node);
                auto after_node_evaluation =
                    UnwindCleanupGuard([&] { m_evaluation_engine.notify_after_node_evaluation(*entry.node); });
                entry.node->eval(when);
                after_node_evaluation.complete();
            } else if (entry.scheduled > when) {
                clock.update_next_scheduled_evaluation_time(entry.scheduled);
            }
        }

        after_graph_evaluation.complete();
    }

    void Graph::schedule_node(int64_t node_index, engine_time_t when, bool force_set)
    {
        if (node_index < 0 || static_cast<size_t>(node_index) >= m_node_count) {
            throw std::out_of_range("v2 graph schedule index is out of range");
        }

        const auto clock = engine_evaluation_clock();
        const engine_time_t current_time = clock.evaluation_time();
        if (current_time != MIN_DT && when < current_time) {
            throw std::runtime_error("v2 graph cannot schedule a node in the past");
        }

        engine_time_t &scheduled_time = entry_storage()[node_index].scheduled;
        if (force_set || scheduled_time <= current_time || when < scheduled_time) { scheduled_time = when; }
        clock.update_next_scheduled_evaluation_time(when);
    }
}  // namespace hgraph::v2
