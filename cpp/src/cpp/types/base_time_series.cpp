#include <fmt/format.h>
#include <hgraph/types/base_time_series.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/node.h>
#include <hgraph/types/ref.h>
#include <hgraph/types/tsb.h>

namespace hgraph {

    namespace {
        std::string describe_time_series_kind(TimeSeriesKind kind) {
            switch (kind) {
                case TimeSeriesKind::Value | TimeSeriesKind::Input: return "ValueInput";
                case TimeSeriesKind::Value | TimeSeriesKind::Output: return "ValueOutput";
                case TimeSeriesKind::Dict | TimeSeriesKind::Input: return "DictInput";
                case TimeSeriesKind::Dict | TimeSeriesKind::Output: return "DictOutput";
                case TimeSeriesKind::Set | TimeSeriesKind::Input: return "SetInput";
                case TimeSeriesKind::Set | TimeSeriesKind::Output: return "SetOutput";
                case TimeSeriesKind::Bundle | TimeSeriesKind::Input: return "BundleInput";
                case TimeSeriesKind::Bundle | TimeSeriesKind::Output: return "BundleOutput";
                case TimeSeriesKind::List | TimeSeriesKind::Input: return "ListInput";
                case TimeSeriesKind::List | TimeSeriesKind::Output: return "ListOutput";
                case TimeSeriesKind::Reference | TimeSeriesKind::Input: return "ReferenceInput";
                case TimeSeriesKind::Reference | TimeSeriesKind::Output: return "ReferenceOutput";
                default: return fmt::format("kind={}", static_cast<uint32_t>(kind));
            }
        }

        std::string describe_subscriber(Notifiable *subscriber) {
            if (auto *input = dynamic_cast<TimeSeriesInput *>(subscriber)) {
                auto binding = fmt::format("kind={} output@{:p} ref_output@{:p} active={} bound={}",
                                           describe_time_series_kind(input->kind()),
                                           static_cast<void *>(input->output()),
                                           static_cast<void *>(input->reference_output().get()),
                                           input->active(),
                                           input->bound());
                if (input->has_owning_node()) {
                    auto *node = input->owning_node();
                    auto node_id = node != nullptr ? fmt::format("{}", fmt::join(node->node_id(), ", ")) : std::string("?");
                    std::string arg_name = "<unknown>";
                    if (node != nullptr && node->has_input()) {
                        const auto &schema = node->input()->schema().keys();
                        for (size_t i = 0; i < schema.size(); ++i) {
                            if ((*node->input())[i].get() == input) {
                                arg_name = schema[i];
                                break;
                            }
                        }
                    }
                    return fmt::format("TimeSeriesInput@{:p} {} node={} node_id=<{}> arg={}",
                                       static_cast<void *>(input),
                                       binding,
                                       node != nullptr ? node->signature().signature() : std::string("<unknown>"),
                                       node_id,
                                       arg_name);
                }
                return fmt::format("TimeSeriesInput@{:p} {} node=<unknown>", static_cast<void *>(input), binding);
            }

            if (auto *node = dynamic_cast<Node *>(subscriber)) {
                return fmt::format("Node@{:p} node={} node_id=<{}>",
                                   static_cast<void *>(node),
                                   node->signature().signature(),
                                   fmt::join(node->node_id(), ", "));
            }

            return fmt::format("Notifiable@{:p}", static_cast<void *>(subscriber));
        }
    }

    // ============================================================================
    // BaseTimeSeriesOutput Implementation
    // ============================================================================

    // Implement TimeSeriesType pure virtuals for Output
    Node* BaseTimeSeriesOutput::owning_node() { return _owning_node(); }
    Node* BaseTimeSeriesOutput::owning_node() const { return _owning_node(); }

    Graph* BaseTimeSeriesOutput::owning_graph() {
        return has_owning_node() ? owning_node()->graph() : nullptr;
    }

    Graph* BaseTimeSeriesOutput::owning_graph() const {
        return has_owning_node() ? owning_node()->graph() : nullptr;
    }

    bool BaseTimeSeriesOutput::is_reference() const { return false; }
    bool BaseTimeSeriesOutput::has_reference() const { return false; }

    void BaseTimeSeriesOutput::reset_parent_or_node() { _parent_ts_or_node.reset(); }
    
    // Implement re_parent methods
    void BaseTimeSeriesOutput::re_parent(node_ptr parent) {
        _parent_ts_or_node = parent;
    }
    void BaseTimeSeriesOutput::re_parent(const time_series_type_ptr parent) {
        _parent_ts_or_node = static_cast<time_series_output_ptr>(parent);
    }

    // TimeSeriesType helper access methods
    time_series_output_ptr BaseTimeSeriesOutput::_parent_output() const {
        return const_cast<BaseTimeSeriesOutput *>(this)->_parent_output();
    }

    time_series_output_ptr BaseTimeSeriesOutput::_parent_output() {
        if (_parent_ts_or_node.has_value() && std::holds_alternative<time_series_output_ptr>(*_parent_ts_or_node)) {
            return std::get<time_series_output_ptr>(*_parent_ts_or_node);
        } else {
            return nullptr;
        }
    }

    bool BaseTimeSeriesOutput::_has_parent_output() const {
        return _parent_ts_or_node.has_value() && std::holds_alternative<time_series_output_ptr>(*_parent_ts_or_node);
    }

    void BaseTimeSeriesOutput::_set_parent_output(time_series_output_ptr ts) {
        if (_parent_ts_or_node.has_value() && std::holds_alternative<time_series_output_ptr>(*_parent_ts_or_node)) {
            std::get<time_series_output_ptr>(*_parent_ts_or_node) = ts;
        } else {
            _parent_ts_or_node = ts;
        }
    }

    bool BaseTimeSeriesOutput::has_parent_or_node() const { return _parent_ts_or_node.has_value(); }

    bool BaseTimeSeriesOutput::has_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            if (std::holds_alternative<node_ptr>(*_parent_ts_or_node)) {
                return std::get<node_ptr>(*_parent_ts_or_node) != nullptr;
            }
            return std::get<time_series_output_ptr>(*_parent_ts_or_node)->has_owning_node();
        } else {
            return false;
        }
    }

    Node* BaseTimeSeriesOutput::_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            return std::visit(
                []<typename T_>(T_ &&value) -> Node* {
                    using T = std::decay_t<T_>;
                    if constexpr (std::is_same_v<T, time_series_output_ptr>) {
                        return value->owning_node();
                    } else if constexpr (std::is_same_v<T, node_ptr>) {
                        return value;
                    } else {
                        throw std::runtime_error("Unknown type");
                    }
                },
                _parent_ts_or_node.value());
        } else {
            throw std::runtime_error("No node is accessible");
        }
    }

    void BaseTimeSeriesOutput::clear() {
    }

    void BaseTimeSeriesOutput::invalidate() { mark_invalid(); }

    TimeSeriesOutput::s_ptr BaseTimeSeriesOutput::parent_output() const {
        if (_has_parent_output()) {
            auto p = std::get<time_series_output_ptr>(*_parent_ts_or_node);
            return p ? p->shared_from_this() : time_series_output_s_ptr{};
        }
        return {};
    }

    TimeSeriesOutput::s_ptr BaseTimeSeriesOutput::parent_output() {
        return const_cast<const BaseTimeSeriesOutput *>(this)->parent_output();
    }

    bool BaseTimeSeriesOutput::has_parent_output() const { return _has_parent_output(); }

    bool BaseTimeSeriesOutput::can_apply_result(const nb::object& value) {
        return !modified();
    }

    void BaseTimeSeriesOutput::builder_release_cleanup() {
        if (!_subscribers.empty()) {
            std::vector<std::string> subscriber_descriptions;
            subscriber_descriptions.reserve(_subscribers.size());
            for (auto *subscriber : _subscribers) {
                subscriber_descriptions.push_back(describe_subscriber(subscriber));
            }

            // Match the Python runtime invariant check as closely as the C++ runtime can.
            // Unlike Python we cannot suppress this during exception unwinding, so it is commented out to be used in debugging scenarios
            // if (has_owning_node()) {
            //     auto *node = owning_node();
            //     fmt::print(stderr,
            //                "Output instance still has subscribers when released, this is a bug.\n"
            //                "output belongs to node {} node_id=<{}>\n"
            //                "subscriber_count={}\n"
            //                "subscriber_details=[{}]\n"
            //                "output@{:p}\n",
            //                node != nullptr ? node->signature().signature() : std::string("<unknown>"),
            //                node != nullptr ? fmt::format("{}", fmt::join(node->node_id(), ", ")) : std::string("?"),
            //                _subscribers.size(),
            //                fmt::join(subscriber_descriptions, ", "),
            //                static_cast<const void *>(this));
            // } else {
            //     fmt::print(stderr,
            //                "Output instance still has subscribers when released, this is a bug.\n"
            //                "output belongs to node <unknown>\n"
            //                "subscriber_count={}\n"
            //                "subscriber_details=[{}]\n"
            //                "output@{:p}\n",
            //                _subscribers.size(),
            //                fmt::join(subscriber_descriptions, ", "),
            //                static_cast<const void *>(this));
            // }
        }
        // Clear subscribers safely without notifications
        _subscribers.clear();
        // Reset modification state to a neutral value without touching evaluation_clock
        _reset_last_modified_time();
    }

    bool BaseTimeSeriesOutput::modified() const {
        auto n = owning_node();
        if (n == nullptr) { return false; }
        // Use cached evaluation time pointer from node for performance
        return *n->cached_evaluation_time_ptr() == _last_modified_time;
    }

    bool BaseTimeSeriesOutput::valid() const { return _last_modified_time > MIN_DT; }

    bool BaseTimeSeriesOutput::all_valid() const {
        return valid(); // By default, all valid is the same as valid
    }

    engine_time_t BaseTimeSeriesOutput::last_modified_time() const { return _last_modified_time; }

    void BaseTimeSeriesOutput::mark_invalid() {
        if (_last_modified_time > MIN_DT) {
            _last_modified_time = MIN_DT;
            auto n = owning_node();
            if (n != nullptr) {
                // Use cached evaluation time pointer from node for performance
                _notify(*n->cached_evaluation_time_ptr());
            } else {
                // Owning node not yet attached; skip notify to avoid dereferencing null during start/recover
            }
        }
    }

    void BaseTimeSeriesOutput::mark_modified() {
        if (has_parent_or_node()) {
            auto n = owning_node();
            if (n != nullptr) {
                // Skip modifications during graph teardown to prevent notification cascades
                // that could access partially stopped nodes
                auto g = n->graph();
                if (g != nullptr && g->is_stopping()) {
                    return;
                }
                // Use cached evaluation time pointer from node for performance
                mark_modified(*n->cached_evaluation_time_ptr());
            } else {
                // Owning node not yet attached; mark with a maximal time to preserve monotonicity without dereferencing
                // This is a bad situation, I would probably prefer to find out why,
                // TODO: find the root cause of why this could be called without a bound node.
            }
        } else {
            mark_modified(MAX_ET);
        }
    }

    void BaseTimeSeriesOutput::mark_modified(engine_time_t modified_time) { // NOLINT(*-no-recursion)
        if (_last_modified_time < modified_time) {
            _last_modified_time = modified_time;
            if (has_parent_output()) { parent_output()->mark_child_modified(*this, modified_time); }
            _notify(modified_time);
        }
    }

    void BaseTimeSeriesOutput::mark_child_modified(TimeSeriesOutput &child, engine_time_t modified_time) {
        mark_modified(modified_time);
    } // NOLINT(*-no-recursion)

    void BaseTimeSeriesOutput::subscribe(Notifiable *notifiable) { _subscribers.insert(notifiable); }

    void BaseTimeSeriesOutput::un_subscribe(Notifiable *notifiable) { _subscribers.erase(notifiable); }

    void BaseTimeSeriesOutput::_notify(engine_time_t modified_time) {
        for (auto *subscriber: _subscribers) { subscriber->notify(modified_time); }
    }

    void BaseTimeSeriesOutput::_reset_last_modified_time() { _last_modified_time = MIN_DT; }

    // ============================================================================
    // BaseTimeSeriesInput Implementation
    // ============================================================================

    // Implement TimeSeriesType pure virtuals for Input
    Node* BaseTimeSeriesInput::owning_node() { return _owning_node(); }
    Node* BaseTimeSeriesInput::owning_node() const { return _owning_node(); }

    Graph* BaseTimeSeriesInput::owning_graph() {
        return has_owning_node() ? owning_node()->graph() : nullptr;
    }

    Graph* BaseTimeSeriesInput::owning_graph() const {
        return has_owning_node() ? owning_node()->graph() : nullptr;
    }

    bool BaseTimeSeriesInput::is_reference() const { return false; }
    bool BaseTimeSeriesInput::has_reference() const { return false; }

    void BaseTimeSeriesInput::reset_parent_or_node() { _parent_ts_or_node.reset(); }

    // Implement re_parent methods
    void BaseTimeSeriesInput::re_parent(node_ptr parent) {
        _parent_ts_or_node = parent;
    }
    void BaseTimeSeriesInput::re_parent(const time_series_type_ptr parent) {
        _parent_ts_or_node = static_cast<time_series_input_ptr>(parent);
    }

    // TimeSeriesType helper methods
    time_series_input_ptr BaseTimeSeriesInput::_parent_input() const {
        return const_cast<BaseTimeSeriesInput *>(this)->_parent_input();
    }

    time_series_input_ptr BaseTimeSeriesInput::_parent_input() {
        if (_parent_ts_or_node.has_value() && std::holds_alternative<time_series_input_ptr>(*_parent_ts_or_node)) {
            return std::get<time_series_input_ptr>(*_parent_ts_or_node);
        } else {
            return nullptr;
        }
    }

    bool BaseTimeSeriesInput::_has_parent_input() const {
        return _parent_ts_or_node.has_value() && std::holds_alternative<time_series_input_ptr>(*_parent_ts_or_node);
    }

    void BaseTimeSeriesInput::_set_parent_input(time_series_input_ptr ts) {
        if (_parent_ts_or_node.has_value() && std::holds_alternative<time_series_input_ptr>(*_parent_ts_or_node)) {
            std::get<time_series_input_ptr>(*_parent_ts_or_node) = ts;
        } else {
            _parent_ts_or_node = ts;
        }
    }

    bool BaseTimeSeriesInput::has_parent_or_node() const { return _parent_ts_or_node.has_value(); }

    bool BaseTimeSeriesInput::has_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            if (std::holds_alternative<node_ptr>(*_parent_ts_or_node)) {
                return std::get<node_ptr>(*_parent_ts_or_node) != nullptr;
            }
            return std::get<time_series_input_ptr>(*_parent_ts_or_node)->has_owning_node();
        } else {
            return false;
        }
    }

    Node* BaseTimeSeriesInput::_owning_node() const {
        if (_parent_ts_or_node.has_value()) {
            return std::visit(
                []<typename T_>(T_ &&value) -> node_ptr {
                    using T = std::decay_t<T_>;
                    if constexpr (std::is_same_v<T, time_series_input_ptr>) {
                        return value->owning_node();
                    } else if constexpr (std::is_same_v<T, node_ptr>) {
                        return value;
                    } else {
                        throw std::runtime_error("Unknown type");
                    }
                },
                _parent_ts_or_node.value());
        } else {
            throw std::runtime_error("No node is accessible");
        }
    }

    TimeSeriesInput::ptr BaseTimeSeriesInput::parent_input() const {
        if (_has_parent_input()) {
            auto p = std::get<time_series_input_ptr>(*_parent_ts_or_node);
            return p;
        }
        return nullptr;
    }

    bool BaseTimeSeriesInput::has_parent_input() const { return _has_parent_input(); }

    bool BaseTimeSeriesInput::bound() const { return _output != nullptr; }

    bool BaseTimeSeriesInput::has_peer() const {
        // By default, we assume that if there is an output, then we are peered.
        // This is not always True but is a good general assumption.
        return _output != nullptr;
    }

    time_series_output_ptr BaseTimeSeriesInput::output() const { return _output.get(); }

    bool BaseTimeSeriesInput::has_output() const { return _output != nullptr; }

    bool BaseTimeSeriesInput::bind_output(time_series_output_s_ptr output_) {
        bool peer;
        bool was_bound = bound(); // Track if input was previously bound (matches Python behavior)

        if (auto ref_output = std::dynamic_pointer_cast<TimeSeriesReferenceOutput>(output_)) {
            // A non-reference input can only observe one reference output at a time.
            // If dynamic graph rewiring rebinds us to a different REF producer, make sure
            // we unregister from the old producer before observing the new one.
            if (_reference_output != nullptr && _reference_output.get() != ref_output.get()) {
                _reference_output->stop_observing_reference(this);
            }
            if (ref_output->valid() && ref_output->has_value()) { ref_output->value().bind_input(*this); }
            ref_output->observe_reference(this);
            _reference_output = ref_output;
            peer = false;
        } else {
            if (output_.get() == _output.get()) { return has_peer(); }
            peer = do_bind_output(output_);
        }

        // Notify if the node is started/starting and either:
        // - The input was previously bound (rebinding case), OR
        // - The new output is valid
        // This matches the Python implementation: (was_bound or self._output.valid)
        auto n = owning_node();
        if ((n->is_started() || n->is_starting()) && _output && (was_bound || _output->valid())) {
            // Use cached evaluation time pointer from node for performance
            _sample_time = *n->cached_evaluation_time_ptr();
            if (active()) {
                notify(_sample_time);
                // TODO: This might belong to make_active, or not? There is a race with setting sample_time too.
            }
        }

        return peer;
    }

    void BaseTimeSeriesInput::un_bind_output(bool unbind_refs) {
        bool was_valid = valid();

        // Handle reference output unbinding conditionally based on unbind_refs parameter
        if (unbind_refs && _reference_output != nullptr) {
            _reference_output->stop_observing_reference(this);
            _reference_output = nullptr;
        }

        if (bound()) {
            do_un_bind_output(unbind_refs);

            auto n = owning_node();
            if (n->is_started() && was_valid) {
                // Use cached evaluation time pointer from node for performance
                _sample_time = *n->cached_evaluation_time_ptr();
                if (active()) {
                    // Notify as the state of the node has changed from bound to un_bound
                    n->notify(_sample_time);
                }
            }
        }
    }

    bool BaseTimeSeriesInput::active() const { return _active; }

    void BaseTimeSeriesInput::make_active() {
        if (!_active) {
            _active = true;
            if (_output != nullptr) {
                output()->subscribe(this);
                if (output()->valid() && output()->modified()) {
                    notify(output()->last_modified_time());
                    return; // If the output is modified, we do not need to check if sampled
                }
            }

            if (sampled()) { notify(_sample_time); }
        }
    }

    void BaseTimeSeriesInput::make_passive() {
        if (_active) {
            _active = false;
            if (_output != nullptr) { output()->un_subscribe(this); }
        }
    }

    nb::object BaseTimeSeriesInput::py_value() const {
        if (_output != nullptr) {
            return _output->py_value();
        } else {
            return nb::none();
        }
    }

    nb::object BaseTimeSeriesInput::py_delta_value() const {
        if (_output != nullptr) {
            return _output->py_delta_value();
        } else {
            return nb::none();
        }
    }

    bool BaseTimeSeriesInput::do_bind_output(time_series_output_s_ptr output_) {
        auto active_{active()};
        make_passive(); // Ensure we are unsubscribed from the old output.
        // Get shared_ptr from output to keep it alive while bound (mirrors original nb::ref behavior)
        _output = std::move(output_);
        if (active_) {
            make_active(); // If we were active now subscribe to the new output,
            // this is important even if we were not bound previously as this will ensure the new output gets
            // subscribed to
        }
        return true;
    }

    auto BaseTimeSeriesInput::notify(engine_time_t modified_time) -> void { // NOLINT(*-no-recursion)
        if (_notify_time != modified_time) {
            _notify_time = modified_time;
            if (has_parent_input()) {
                // Cast to BaseTimeSeriesInput to access protected notify_parent
                auto parent = static_cast<BaseTimeSeriesInput*>(parent_input());
                parent->notify_parent(this, modified_time);
            } else {
                auto node = owning_node();
                if (node == nullptr) {
                    return;
                }
                // Skip notifications when the graph is stopping to avoid accessing
                // partially stopped nodes or inconsistent state during teardown
                if (node->graph() && node->graph()->is_stopping()) {
                    return;
                }
                node->notify(modified_time);
            }
        }
    }

    void BaseTimeSeriesInput::do_un_bind_output(bool unbind_refs) {
        if (_active) { output()->un_subscribe(this); }
        _output = nullptr;
    }

    // Minimal-teardown helper: avoid consulting owning_node/graph
    void BaseTimeSeriesInput::builder_release_cleanup() {
        if (_output != nullptr && _active) {
            // Unsubscribe from output without triggering any node notifications
            _output->un_subscribe(this);
        }
        _active = false;
        if (_reference_output != nullptr) {
            _reference_output->stop_observing_reference(this);
            _reference_output = nullptr;
        }
        _output = nullptr;
    }

    void BaseTimeSeriesInput::notify_parent(TimeSeriesInput *child, engine_time_t modified_time) {
        notify(modified_time);
    } // NOLINT(*-no-recursion)

    void BaseTimeSeriesInput::set_sample_time(engine_time_t sample_time) { _sample_time = sample_time; }

    engine_time_t BaseTimeSeriesInput::sample_time() const { return _sample_time; }

    bool BaseTimeSeriesInput::sampled() const {
        auto n = owning_node();
        if (n == nullptr) { return false; }
        // Use cached evaluation time pointer from node for performance
        return _sample_time != MIN_DT && _sample_time == *n->cached_evaluation_time_ptr();
    }

    time_series_reference_output_s_ptr BaseTimeSeriesInput::reference_output() const { return _reference_output; }

    TimeSeriesInput::s_ptr BaseTimeSeriesInput::get_input(size_t index) { throw std::runtime_error("BaseTimeSeriesInput [] not supported"); }

    void BaseTimeSeriesInput::reset_output() { _output = nullptr; }

    void BaseTimeSeriesInput::set_output(const time_series_output_s_ptr& output) { _output = output; }

    void BaseTimeSeriesInput::set_active(bool active) { _active = active; }

    bool BaseTimeSeriesInput::modified() const { return _output != nullptr && (_output->modified() || sampled()); }

    bool BaseTimeSeriesInput::valid() const { return bound() && _output != nullptr && _output->valid(); }

    bool BaseTimeSeriesInput::all_valid() const { return bound() && _output != nullptr && _output->all_valid(); }

    engine_time_t BaseTimeSeriesInput::last_modified_time() const {
        return bound() ? std::max(_output->last_modified_time(), _sample_time) : MIN_DT;
    }
} // namespace hgraph
