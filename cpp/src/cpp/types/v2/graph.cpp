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
          m_evaluation_time(other.m_evaluation_time),
          m_started(other.m_started),
          m_storage_alignment(other.m_storage_alignment),
          m_storage(other.m_storage)
    {
        other.m_node_count = 0;
        other.m_evaluation_time = MIN_DT;
        other.m_started = false;
        other.m_storage_alignment = alignof(std::max_align_t);
        other.m_storage = nullptr;
        attach_nodes();
    }

    Graph &Graph::operator=(Graph &&other) noexcept
    {
        if (this != &other) {
            clear_storage();

            m_node_count = other.m_node_count;
            m_evaluation_time = other.m_evaluation_time;
            m_started = other.m_started;
            m_storage_alignment = other.m_storage_alignment;
            m_storage = other.m_storage;

            other.m_node_count = 0;
            other.m_evaluation_time = MIN_DT;
            other.m_started = false;
            other.m_storage_alignment = alignof(std::max_align_t);
            other.m_storage = nullptr;

            attach_nodes();
        }
        return *this;
    }

    engine_time_t Graph::scheduled_time(size_t index) const
    {
        if (index >= m_node_count) { throw std::out_of_range("v2 graph schedule index is out of range"); }
        return entry_storage()[index].scheduled;
    }

    void Graph::adopt_storage(void *storage, size_t storage_alignment, size_t node_count) noexcept
    {
        clear_storage();
        m_node_count = node_count;
        m_evaluation_time = MIN_DT;
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
        m_evaluation_time = MIN_DT;
        m_started = false;
        m_storage_alignment = alignof(std::max_align_t);
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
        for (size_t i = 0; i < m_node_count; ++i) { entry_storage()[i].node->start(m_evaluation_time); }
        m_started = true;
    }

    void Graph::stop()
    {
        if (!m_started) { return; }
        for (size_t i = 0; i < m_node_count; ++i) { entry_storage()[i].node->stop(m_evaluation_time); }
        m_started = false;
    }

    void Graph::evaluate(engine_time_t when)
    {
        m_evaluation_time = when;

        for (size_t index = 0; index < m_node_count; ++index) {
            if (entry_storage()[index].scheduled != when) { continue; }

            entry_storage()[index].scheduled = MIN_DT;
            entry_storage()[index].node->eval(when);
        }
    }

    void Graph::schedule_node(int64_t node_index, engine_time_t when)
    {
        if (node_index < 0 || static_cast<size_t>(node_index) >= m_node_count) {
            throw std::out_of_range("v2 graph schedule index is out of range");
        }
        if (m_evaluation_time != MIN_DT && when < m_evaluation_time) {
            throw std::runtime_error("v2 graph cannot schedule a node in the past");
        }

        engine_time_t &scheduled_time = entry_storage()[node_index].scheduled;
        if (scheduled_time == MIN_DT || when < scheduled_time) { scheduled_time = when; }
    }
}  // namespace hgraph::v2
