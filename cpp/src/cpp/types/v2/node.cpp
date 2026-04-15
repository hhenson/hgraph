#include <hgraph/types/v2/graph.h>
#include <hgraph/types/v2/node_impl.h>
#include <hgraph/types/v2/node.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/util/scope.h>

#include <cassert>
#include <new>
#include <stdexcept>

namespace hgraph::v2
{
    namespace
    {
        [[nodiscard]] bool has_modified_active_input(Node &node, engine_time_t evaluation_time)
        {
            if (!node.has_input()) { return false; }

            const TSMeta *schema = node.input_schema();
            if (schema == nullptr || schema->kind != TSKind::TSB) {
                return node.input_view(evaluation_time).modified();
            }

            auto view = node.input_view(evaluation_time).as_bundle();
            for (size_t slot = 0; slot < schema->field_count(); ++slot) {
                if (view[slot].modified() && view[slot].active()) { return true; }
            }
            return false;
        }

        void activate_active_inputs(Node &node, engine_time_t evaluation_time)
        {
            if (!node.has_input()) { return; }

            const TSMeta *schema = node.input_schema();
            if (schema == nullptr || schema->kind != TSKind::TSB) {
                throw std::logic_error("v2 top-level input selectors require a TSB root input schema");
            }

            const auto input_slots = [&]() -> std::span<const size_t> {
                return (node.spec().has_explicit_active_inputs || !node.spec().active_inputs.empty())
                           ? node.spec().active_inputs
                           : std::span<const size_t>{};
            }();
            size_t activated_inputs_end = 0;
            auto rollback_activation = UnwindCleanupGuard([&] {
                auto view = node.input_view(evaluation_time).as_bundle();
                if (!input_slots.empty()) {
                    for (size_t i = activated_inputs_end; i > 0; --i) { view[input_slots[i - 1]].make_passive(); }
                } else {
                    for (size_t i = activated_inputs_end; i > 0; --i) { view[i - 1].make_passive(); }
                }
            });

            auto view = node.input_view(evaluation_time).as_bundle();
            if (!input_slots.empty()) {
                // For now selector activation is limited to top-level TSB fields.
                // REF / alternative-view activation comes later.
                for (const size_t slot : input_slots) {
                    if (slot >= schema->field_count()) { throw std::out_of_range("v2 input selector is out of range"); }
                    view[slot].make_active();
                    ++activated_inputs_end;
                }
            } else {
                for (size_t slot = 0; slot < schema->field_count(); ++slot) {
                    view[slot].make_active();
                    ++activated_inputs_end;
                }
            }

            rollback_activation.release();
        }

        void deactivate_active_inputs(Node &node, engine_time_t evaluation_time)
        {
            if (!node.has_input()) { return; }

            const TSMeta *schema = node.input_schema();
            if (schema == nullptr || schema->kind != TSKind::TSB) {
                throw std::logic_error("v2 top-level input selectors require a TSB root input schema");
            }

            auto view = node.input_view(evaluation_time).as_bundle();
            if (node.spec().has_explicit_active_inputs || !node.spec().active_inputs.empty()) {
                for (const size_t slot : node.spec().active_inputs) {
                    if (slot >= schema->field_count()) { throw std::out_of_range("v2 input selector is out of range"); }
                    view[slot].make_passive();
                }
            } else {
                for (size_t slot = 0; slot < schema->field_count(); ++slot) { view[slot].make_passive(); }
            }
        }

        [[nodiscard]] NodeErrorInfo fallback_node_error(const Node &node,
                                                        std::string error_msg,
                                                        std::string additional_context = {})
        {
            return NodeErrorInfo{
                node.runtime_label(),
                std::string{node.label()},
                {},
                std::move(error_msg),
                {},
                {},
                std::move(additional_context),
            };
        }

        void publish_error_output(Node &node, engine_time_t evaluation_time, const NodeErrorInfo &error)
        {
            if (!node.has_error_output()) { return; }
            TSOutputView error_view = node.error_output_view(evaluation_time);
            nb::gil_scoped_acquire guard;
            nb::object py_error = nb::module_::import_("hgraph").attr("NodeError")(
                error.signature_name,
                error.label,
                error.wiring_path,
                error.error_msg,
                error.stack_trace,
                error.activation_back_trace,
                error.additional_context.empty() ? nb::none() : nb::cast(error.additional_context));
            error_view.from_python(py_error);
        }
    }  // namespace

    Graph &detail::arg_provider<Graph>::get(Node &node, engine_time_t)
    {
        assert(node.graph() != nullptr);
        return *node.graph();
    }

    Graph *detail::arg_provider<Graph *>::get(Node &node, engine_time_t)
    {
        return node.graph();
    }

    EvaluationClock detail::arg_provider<EvaluationClock>::get(Node &node, engine_time_t)
    {
        assert(node.graph() != nullptr);
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

    engine_time_t NodeScheduler::next_scheduled_time() const noexcept
    {
        return !m_scheduled_events.empty() ? m_scheduled_events.begin()->first : MIN_DT;
    }

    bool NodeScheduler::requires_scheduling() const noexcept
    {
        return !m_scheduled_events.empty();
    }

    bool NodeScheduler::is_scheduled() const noexcept
    {
        return !m_scheduled_events.empty();
    }

    bool NodeScheduler::is_scheduled_now() const noexcept
    {
        return m_node != nullptr && m_node->graph() != nullptr && !m_scheduled_events.empty() &&
               m_scheduled_events.begin()->first == m_node->graph()->evaluation_time();
    }

    bool NodeScheduler::has_tag(std::string_view tag) const
    {
        return m_tags.contains(std::string{tag});
    }

    engine_time_t NodeScheduler::pop_tag(std::string_view tag, engine_time_t default_time)
    {
        auto it = m_tags.find(std::string{tag});
        if (it == m_tags.end()) { return default_time; }

        const auto when = it->second;
        m_scheduled_events.erase({when, it->first});
        m_tags.erase(it);
        return when;
    }

    void NodeScheduler::schedule(engine_time_t when, std::optional<std::string> tag, bool on_wall_clock)
    {
        if (on_wall_clock) { throw std::invalid_argument("v2 node schedulers do not yet support wall-clock alarms"); }

        assert(m_node != nullptr);
        assert(m_node->graph() != nullptr);

        std::optional<engine_time_t> original_time;
        const std::string tag_value = tag.value_or("");
        if (tag.has_value()) {
            auto it = m_tags.find(tag_value);
            if (it != m_tags.end() && !m_scheduled_events.empty()) {
                original_time = next_scheduled_time();
                m_scheduled_events.erase({it->second, tag_value});
            }
        }

        const bool is_started = m_node->started();
        const engine_time_t now = is_started ? m_node->graph()->evaluation_time() : MIN_DT;
        if (when <= now) { return; }

        m_tags[tag_value] = when;
        const engine_time_t current_first = !m_scheduled_events.empty() ? m_scheduled_events.begin()->first : MAX_DT;
        m_scheduled_events.insert({when, tag_value});
        const engine_time_t next = next_scheduled_time();
        if (is_started && current_first > next) {
            const bool force_set = original_time.has_value() && *original_time < when;
            m_node->graph()->schedule_node(m_node->node_index(), next, force_set);
        }
    }

    void NodeScheduler::schedule(engine_time_delta_t when, std::optional<std::string> tag, bool on_wall_clock)
    {
        assert(m_node != nullptr);
        assert(m_node->graph() != nullptr);
        const engine_time_t base = on_wall_clock ? m_node->graph()->evaluation_clock().now() : m_node->graph()->evaluation_time();
        schedule(base + when, std::move(tag), on_wall_clock);
    }

    void NodeScheduler::un_schedule(const std::string &tag)
    {
        if (auto it = m_tags.find(tag); it != m_tags.end()) {
            m_scheduled_events.erase({it->second, it->first});
            m_tags.erase(it);
        }
    }

    void NodeScheduler::un_schedule()
    {
        if (m_scheduled_events.empty()) { return; }
        const auto ev = *m_scheduled_events.begin();
        m_scheduled_events.erase(m_scheduled_events.begin());
        m_tags.erase(ev.second);
    }

    void NodeScheduler::reset()
    {
        m_scheduled_events.clear();
        m_tags.clear();
    }

    void NodeScheduler::advance()
    {
        if (m_node == nullptr || m_node->graph() == nullptr) { return; }

        const engine_time_t until = m_node->graph()->evaluation_time();
        while (!m_scheduled_events.empty() && m_scheduled_events.begin()->first <= until) {
            m_tags.erase(m_scheduled_events.begin()->second);
            m_scheduled_events.erase(m_scheduled_events.begin());
        }

        if (!m_scheduled_events.empty()) {
            m_node->graph()->schedule_node(m_node->node_index(), m_scheduled_events.begin()->first);
        }
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

    const TSMeta *Node::error_output_schema() const noexcept
    {
        return spec().error_output_schema;
    }

    const TSMeta *Node::recordable_state_schema() const noexcept
    {
        return spec().recordable_state_schema;
    }

    bool Node::has_input() const noexcept
    {
        return spec().runtime_ops->has_input(*this);
    }

    bool Node::has_output() const noexcept
    {
        return spec().runtime_ops->has_output(*this);
    }

    bool Node::has_error_output() const noexcept
    {
        return spec().runtime_ops->has_error_output(*this);
    }

    bool Node::has_recordable_state() const noexcept
    {
        return spec().runtime_ops->has_recordable_state(*this);
    }

    bool Node::started() const noexcept
    {
        return m_started;
    }

    bool Node::uses_scheduler() const noexcept
    {
        return spec().uses_scheduler;
    }

    bool Node::has_scheduler() const noexcept
    {
        return uses_scheduler();
    }

    engine_time_t Node::evaluation_time() const noexcept
    {
        return m_graph != nullptr ? m_graph->evaluation_time() : MIN_DT;
    }

    void Node::set_started(bool value) noexcept
    {
        m_started = value;
    }

    NodeScheduler &Node::scheduler()
    {
        auto *scheduler_state = scheduler_if_present();
        assert(scheduler_state != nullptr);
        return *scheduler_state;
    }

    NodeScheduler *Node::scheduler_if_present() noexcept
    {
        if (!uses_scheduler()) { return nullptr; }
        return std::launder(reinterpret_cast<NodeScheduler *>(reinterpret_cast<std::byte *>(this) + spec().scheduler_offset));
    }

    const NodeScheduler *Node::scheduler_if_present() const noexcept
    {
        if (!uses_scheduler()) { return nullptr; }
        return std::launder(
            reinterpret_cast<const NodeScheduler *>(reinterpret_cast<const std::byte *>(this) + spec().scheduler_offset));
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

    TSOutputView Node::error_output_view(engine_time_t evaluation_time)
    {
        return spec().runtime_ops->error_output_view(*this, evaluation_time);
    }

    TSOutputView Node::recordable_state_view(engine_time_t evaluation_time)
    {
        return spec().runtime_ops->recordable_state_view(*this, evaluation_time);
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
        auto rollback_started = hgraph::make_scope_exit([&] {
            m_started = false;
            if (uses_scheduler()) { scheduler().reset(); }
        });

        if (uses_scheduler() && scheduler().is_scheduled()) {
            if (scheduler().pop_tag("start", MIN_DT) != MIN_DT) {
                notify(evaluation_time);
            } else {
                scheduler().advance();
            }
        }
        rollback_start.release();
        rollback_started.release();
    }

    void Node::stop(engine_time_t evaluation_time)
    {
        if (!m_started) { return; }

        auto mark_stopped = hgraph::make_scope_exit([&] { m_started = false; });
        auto reset_scheduler = hgraph::make_scope_exit([&] {
            if (uses_scheduler()) { scheduler().reset(); }
        });
        auto deactivate_inputs = UnwindCleanupGuard([&] { deactivate_active_inputs(*this, evaluation_time); });
        spec().runtime_ops->stop(*this, evaluation_time);
        deactivate_inputs.complete();
    }

    void Node::eval(engine_time_t evaluation_time)
    {
        const bool ready = ready_to_eval(evaluation_time);
        const bool active_modified = has_input() && has_modified_active_input(*this, evaluation_time);
        if (!ready) { return; }
        const bool scheduled = uses_scheduler() && scheduler().is_scheduled_now();
        if (uses_scheduler() && has_input() && !scheduled && !active_modified) { return; }
        if (has_error_output()) {
            try {
                spec().runtime_ops->eval(*this, evaluation_time);
            } catch (const NodeException &e) {
                publish_error_output(*this, evaluation_time, e.error());
            } catch (const std::exception &e) {
                publish_error_output(*this, evaluation_time, fallback_node_error(*this, e.what(), "During evaluation"));
            } catch (...) {
                publish_error_output(
                    *this,
                    evaluation_time,
                    fallback_node_error(*this, "Unknown non-standard exception during node evaluation", "During evaluation"));
            }
        } else {
            spec().runtime_ops->eval(*this, evaluation_time);
        }

        if (!uses_scheduler()) { return; }
        if (scheduled) {
            scheduler().advance();
        } else if (scheduler().requires_scheduling() && m_graph != nullptr) {
            m_graph->schedule_node(m_node_index, scheduler().next_scheduled_time());
        }
    }

    void Node::notify(engine_time_t et)
    {
        if (m_graph == nullptr) { return; }
        if (m_started) {
            const engine_time_t when = std::max(et, m_graph->evaluation_time());
            m_graph->schedule_node(m_node_index, when);
        } else if (uses_scheduler()) {
            scheduler().schedule(MIN_ST, std::string{"start"});
        } else {
            // Undeclared startup notifications only support "evaluate me in the
            // startup cycle". Any real future/tagged scheduling requires the
            // node to opt into NodeScheduler explicitly.
            m_graph->schedule_node(m_node_index, m_graph->evaluation_time());
        }
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
