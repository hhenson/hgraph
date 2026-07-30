#include <hgraph/types/time_series/ts_input/base_view.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/types/time_series/ts_input/view_common.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] detail::TSInputTargetActiveNode *target_root_marker() noexcept
        {
            static detail::TSInputTargetActiveNode marker{};
            return &marker;
        }
    }

    TSInputView::TSInputView() noexcept = default;

    TSInputView::InputDataCursor::InputDataCursor(TSDataView value_data_,
                                                  TSDataView raw_data_,
                                                  detail::TSInputTargetActiveNode *target_node_,
                                                  Classification classification) noexcept
        : value_data(std::move(value_data_)),
          raw_data(raw_data_.valid() ? std::move(raw_data_) : value_data.borrowed_ref()),
          target_node(target_node_ != nullptr
                          ? target_node_
                          : (classification == Classification::Detect &&
                                     detail::is_target_link_view(raw_data)
                                 ? target_root_marker()
                                 : nullptr))
    {
    }

    TSInputView::InputDataCursor TSInputView::InputDataCursor::borrowed_ref() const noexcept
    {
        InputDataCursor cursor{value_data.borrowed_ref(), raw_data.borrowed_ref(), target_node,
                               Classification::Known};
        cursor.route_node = route_node;
        return cursor;
    }

    bool TSInputView::InputDataCursor::has_storage() const noexcept
    {
        return value_data.valid() || raw_data.valid();
    }

    bool TSInputView::InputDataCursor::is_target_position() const noexcept
    {
        return target_node != nullptr;
    }

    bool TSInputView::InputDataCursor::is_target_root() const noexcept
    {
        return target_node == target_root_marker();
    }

    detail::TSInputTargetActiveNode *TSInputView::InputDataCursor::target_path_node() const noexcept
    {
        return is_target_root() ? nullptr : target_node;
    }

    bool TSInputView::InputDataCursor::target_bound() const noexcept
    {
        return detail::target_link_bound(raw_data);
    }

    TSRoleTypeRef TSInputView::InputDataCursor::storage_type() const noexcept
    {
        return is_target_position() ? raw_data.storage_type() : value_data.storage_type();
    }

    const TSValueTypeMetaData *TSInputView::InputDataCursor::schema() const noexcept
    {
        if (is_target_position()) { return target_path_schema(); }
        return value_data.schema();
    }

    const TSValueTypeMetaData *TSInputView::InputDataCursor::target_path_schema() const noexcept
    {
        if (!is_target_position()) { return nullptr; }
        return detail::target_path_schema(raw_data, target_path_node());
    }

    const TSDataView &TSInputView::InputDataCursor::resolved_value_data() const noexcept
    {
        // Prepared route (RFC 0008 stage 5): the trie node's handle is
        // replaced in place on every topology event, so a TRUSTED handle IS
        // the current target. Trust means the same three conditions
        // ``resolved_target_at_path`` requires — locally active, value-kind
        // observation, bound — because a runtime ``make_structural_active()``
        // swaps in a bound STRUCTURAL handle on this same node. Anything
        // else falls through to the full resolution (which also serves
        // rebuilding after source invalidation).
        if (route_node != nullptr && route_node->locally_active &&
            route_node->observation_kind == detail::TSInputObservationKind::Value &&
            route_node->observed.bound())
        {
            value_data = route_node->observed.data_view();
            return value_data;
        }
        if (is_target_position()) { value_data = detail::target_link_resolve(raw_data, target_path_node()); }
        return value_data;
    }

    bool TSInputView::InputDataCursor::value_live() const noexcept
    {
        return resolved_value_data().valid();
    }

    DateTime TSInputView::InputDataCursor::last_modified_time() const
    {
        const auto &data = resolved_value_data();
        if (is_target_position())
        {
            if (!data.valid()) { return raw_data.last_modified_time(); }
            // BLEND the link's own tracking (the sampled-runtime contract): a
            // from-REF retarget records on the LINK - the position is modified
            // even though the (already-valid) target did not tick. Applies
            // only to positions with their OWN storage (the target root);
            // descents within a root link tree share the root's tracking,
            // which must not leak into per-child modified state.
            if (is_target_root())
            {
                return std::max(raw_data.last_modified_time(), data.last_modified_time());
            }
            return data.last_modified_time();
        }
        return data.last_modified_time();
    }

    bool TSInputView::InputDataCursor::modified(DateTime evaluation_time) const
    {
        if (evaluation_time == MIN_DT) { return false; }
        const auto &data = resolved_value_data();
        if (is_target_position())
        {
            if (!data.valid()) { return raw_data.modified(evaluation_time); }
            if (is_target_root() && raw_data.modified(evaluation_time)) { return true; }   // rebind (sampled)
            const auto *link = detail::target_link_storage(raw_data);
            if (link != nullptr && link->sampled_structural_transition() &&
                link->structural_transition_time() == evaluation_time)
            {
                return true;
            }
            return data.modified(evaluation_time);
        }
        return data.valid() && data.modified(evaluation_time);
    }

    TSDataView &TSInputView::InputDataCursor::checked_value_data(const char *what) const
    {
        if (value_live()) { return value_data; }
        throw std::logic_error(std::string{what} + " requires a bound peered input view");
    }

    TSInputView::InputDataCursor TSInputView::InputDataCursor::target_child(TSDataView child,
                                                                            std::size_t index) const
    {
        auto *child_node = detail::target_link_child_node(raw_data, target_path_node(), index);
        return InputDataCursor{std::move(child), raw_data.borrowed_ref(), child_node,
                               Classification::Known};
    }

    void TSInputView::InputDataCursor::bind_target(const TSOutputView &output)
    {
        detail::bind_target_link(raw_data, output);
        value_data = detail::target_link_resolve(raw_data, target_path_node());
    }

    void TSInputView::InputDataCursor::bind_target_sampled(const TSOutputView &output,
                                                            DateTime modified_time)
    {
        if (!is_target_root())
        {
            throw std::logic_error("Sampled TSInput binding requires a target-link root");
        }
        detail::bind_target_link_sampled(raw_data, output, modified_time);
        value_data = detail::target_link_resolve(raw_data, target_path_node());
    }

    void TSInputView::InputDataCursor::unbind_target()
    {
        detail::unbind_target_link(raw_data);
        value_data = {};
    }

    void TSInputView::InputDataCursor::make_active(TSInput *input,
                                                   Notifiable *scheduling_notifier) const
    {
        if (is_target_position())
        {
            detail::make_target_link_active(raw_data,
                                            target_path_node(),
                                            value_live() ? value_data.borrowed_ref() : TSDataView{},
                                            detail::TSInputObservationKind::Value,
                                            scheduling_notifier);
        }
        else if (input != nullptr && value_data.valid())
        {
            input->make_active(value_data.path_from_root(), value_data.borrowed_ref(), scheduling_notifier);
        }
    }

    void TSInputView::InputDataCursor::make_structural_active(
        TSInput *input, Notifiable *scheduling_notifier) const
    {
        const auto *value_schema = schema();
        const auto &ops = detail::input_endpoint_ops_for(value_schema);
        if (ops.structural_observation == nullptr)
        {
            throw std::invalid_argument("Structural input activity is not supported for this time-series shape");
        }

        TSDataView observed{};
        if (value_live()) { observed = ops.structural_observation(value_data); }
        if (is_target_position())
        {
            detail::make_target_link_active(raw_data, target_path_node(), observed,
                                            detail::TSInputObservationKind::Structural,
                                            scheduling_notifier);
        }
        else if (input != nullptr && observed.valid())
        {
            input->make_active(value_data.path_from_root(), std::move(observed), scheduling_notifier);
        }
    }

    void TSInputView::InputDataCursor::make_passive(TSInput *input) const
    {
        if (is_target_position())
        {
            detail::make_target_link_passive(raw_data, target_path_node());
            return;
        }
        if (input != nullptr && value_data.valid()) { input->make_passive(value_data.path_from_root()); }
    }

    bool TSInputView::InputDataCursor::active(const TSInput *input) const
    {
        if (is_target_position()) { return detail::target_link_active(raw_data, target_path_node()); }
        return input != nullptr && value_data.valid() && input->active(value_data.path_from_root());
    }

    TSInputView::TSInputView(TSInput                  *input,
                             TSDataView               value_data,
                             TSDataView               raw_data,
                             detail::TSInputTargetActiveNode *target_node,
                             Notifiable              *scheduling_notifier,
                             DateTime            evaluation_time,
                             InputDataCursor::Classification classification) noexcept
        : input_(input),
          data_(std::move(value_data), std::move(raw_data), target_node, classification),
          scheduling_notifier_(scheduling_notifier),
          evaluation_time_(evaluation_time)
    {
    }

    TSInputView TSInputView::borrowed_ref() const noexcept
    {
        return borrowed_ref(evaluation_time_);
    }

    TSInputView TSInputView::borrowed_ref(DateTime evaluation_time) const noexcept
    {
        auto cursor = data_.borrowed_ref();
        return TSInputView{input_,
                           std::move(cursor.value_data),
                           std::move(cursor.raw_data),
                           cursor.target_node,
                           scheduling_notifier_,
                           evaluation_time,
                           InputDataCursor::Classification::Known};
    }

    namespace detail
    {
        void TSInputViewOps::make_active(TSInputView &view) const
        {
            view.data_.make_active(view.input_, view.scheduling_notifier_);
        }

        void TSInputViewOps::make_structural_active(TSInputView &view) const
        {
            view.data_.make_structural_active(view.input_, view.scheduling_notifier_);
        }

        void TSInputViewOps::make_passive(TSInputView &view) const
        {
            view.data_.make_passive(view.input_);
        }

        bool TSInputViewOps::active(const TSInputView &view) const
        {
            return view.data_.active(view.input_);
        }

        const TSInputViewOps &input_view_ops() noexcept
        {
            static const TSInputViewOps ops{};
            return ops;
        }
    }  // namespace detail

    DateTime TSInputView::evaluation_time() const noexcept
    {
        return evaluation_time_;
    }

    NodeView TSInputView::consumer_node() const
    {
        return input_ != nullptr ? input_->owner_node() : NodeView{};
    }

    GraphView TSInputView::consumer_graph() const
    {
        return input_ != nullptr ? input_->owner_graph() : GraphView{};
    }

    TSEndpointOwnerPort TSInputView::owner_port() const noexcept
    {
        if (input_ == nullptr || !input_->has_value()) { return TSEndpointOwnerPort::Input; }
        const auto owner = input_->data_.view().root_endpoint_owner();
        return owner.node_owned() ? owner.port() : TSEndpointOwnerPort::Input;
    }

    TSInputTypeRef TSInputView::type_ref() const
    {
        const auto type = data_.storage_type();
        // Descents through one peered root share the root TargetLink storage
        // and therefore its root record. That record must not describe a
        // child at a different semantic path; callers fall back through the
        // child's schema until target-path records are introduced.
        if (type && type.schema() != data_.schema()) { return {}; }
        // A migrated composed from-REF alternative is published through its
        // Output record and may be the resolved storage of a non-target input
        // view. Do not reinterpret that record as an Input type.
        if (type && type.role() != TypeRole::Input) { return {}; }
        return type ? TSInputTypeRef::checked(type) : TSInputTypeRef{};
    }

    const TSValueTypeMetaData *TSInputView::schema() const noexcept
    {
        return data_.schema();
    }

    const TSDataView &TSInputView::data_view() const noexcept
    {
        return data_.resolved_value_data();
    }

    bool TSInputView::bound() const noexcept
    {
        if (!data_.has_storage()) { return false; }
        if (!is_bindable()) { return true; }
        return data_.target_bound();
    }

    bool TSInputView::is_bindable() const noexcept
    {
        return is_target_position();
    }

    TSOutputView TSInputView::bound_output() const
    {
        if (!is_target_position()) { return {}; }

        const auto *schema = detail::target_link_schema(data_.raw_data);
        const auto *link = detail::target_link_storage(data_.raw_data);
        if (schema == nullptr || link == nullptr) { return {}; }
        return link->resolved_target_at_path(*schema, data_.target_path_node()).view(evaluation_time_);
    }

    bool TSInputView::valid() const
    {
        const auto &data = data_view();
        return data.valid() && data.has_current_value();
    }

    bool TSInputView::all_valid() const
    {
        const auto &data = data_view();
        return data.valid() && data.all_valid();
    }

    DateTime TSInputView::last_modified_time() const
    {
        return data_.last_modified_time();
    }

    bool TSInputView::modified() const
    {
        return data_.modified(evaluation_time_);
    }

    ValueView TSInputView::value() const
    {
        const auto &data = data_view();
        if (data.valid()) { return data.value(); }
        throw std::logic_error("TSInputView::value requires a live input view");
    }

    ValueView TSInputView::delta_value() const
    {
        const auto &data = data_view();
        if (data.valid())
        {
            // Sampled rebind (the sampled-runtime contract): when the input's
            // LINK was modified after the target's own last tick (a from-REF
            // retarget bound an already-valid output), the delta IS the
            // current value - the target's delta storage belongs to an older
            // cycle.
            if (is_target_position())
            {
                const auto *link = detail::target_link_storage(data_.raw_data);
                if (link != nullptr && link->tracking.last_modified_time > data.last_modified_time())
                {
                    return data.value();
                }
            }
            ValueView delta = data.delta_value(evaluation_time_);
            if (delta.has_value()) { return delta; }

            // A migrated from-REF alternative can carry the sampled
            // modification on its endpoint facade while resolving reads to
            // an older, already-valid target. Atomic TS deltas are their
            // current value, so preserve the same sampled-rebind contract as
            // a direct TargetLink instead of exposing an empty target delta.
            const auto *view_schema = schema();
            if (view_schema != nullptr && view_schema->kind == TSTypeKind::TS && modified())
            {
                return data.value();
            }
            return delta;
        }
        throw std::logic_error("TSInputView::delta_value requires a live input view");
    }

    TimeSeriesReference TSInputView::reference() const
    {
        const auto *view_schema = schema();
        if (view_schema == nullptr)
        {
            throw std::logic_error("TSInputView::reference requires a typed input view");
        }

        // A REF-typed input's reference IS its VALUE (one dereference level:
        // hgraph's ts.value on a REF input) - an emptied upstream reference
        // reads empty here, so assemblies observe invalidation without any
        // unbind notification (UNBIND IS SILENT, linking_strategies.rst).
        if (view_schema->kind == TSTypeKind::REF)
        {
            if (!valid()) { return TimeSeriesReference::empty(view_schema->referenced_ts()); }
            return value().checked_as<TimeSeriesReference>();
        }

        if (is_target_position())
        {
            const auto *target_schema = detail::target_link_schema(data_.raw_data);
            const auto *link = detail::target_link_storage(data_.raw_data);
            if (target_schema == nullptr || link == nullptr)
            {
                throw std::logic_error("TSInputView::reference requires target-link storage");
            }

            auto target = link->resolved_target_at_path(*target_schema, data_.target_path_node());
            if (!target.bound()) { return TimeSeriesReference::empty(view_schema); }
            // A target landing on a from-ref ALTERNATIVE position stays a
            // reference TO that position while the alternative is BOUND (the
            // consumer keeps following its retargets - mesh liveness), but
            // reads as EMPTY when the alternative is UNBOUND (the source
            // reference emptied - race re-races on the observed invalidity).
            {
                auto        target_data = target.data_view();
                const auto *inner       = detail::target_link_storage(target_data);
                if (inner != nullptr && !inner->bound())
                {
                    return TimeSeriesReference::empty(view_schema);
                }
            }
            return TimeSeriesReference{std::move(target)};
        }

        const auto &ops = detail::input_endpoint_ops_for(view_schema);
        if (ops.reference == nullptr) { return TimeSeriesReference::empty(view_schema); }
        return ops.reference(*this);
    }

    void TSInputView::bind_output(const TSOutputView &output)
    {
        if (!is_bindable())
        {
            throw std::logic_error("TSInputView::bind_output requires a peered target-link input view");
        }
        const bool was_bound = bound();
        const bool was_active = active();
        const bool source_valid = output.valid();
        data_.bind_target(output);
        if (was_active && scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT &&
            (was_bound || source_valid || valid()))
        {
            scheduling_notifier_->notify(evaluation_time_);
        }
    }

    void TSInputView::rebind_output_silent(const TSOutputView &output)
    {
        if (!is_bindable())
        {
            throw std::logic_error(
                "TSInputView::rebind_output_silent requires a peered target-link input view");
        }
        data_.bind_target(output);
    }

    void TSInputView::bind_output_sampled(const TSOutputView &output, DateTime modified_time)
    {
        if (!is_bindable())
        {
            throw std::logic_error(
                "TSInputView::bind_output_sampled requires a peered target-link input view");
        }
        const bool was_bound = bound();
        const bool was_active = active();
        const bool source_valid = output.valid();
        data_.bind_target_sampled(output, modified_time);
        if (was_active && scheduling_notifier_ != nullptr &&
            (was_bound || source_valid || valid()))
        {
            scheduling_notifier_->notify(modified_time);
        }
    }

    void TSInputView::unbind_output()
    {
        if (!is_bindable())
        {
            throw std::logic_error("TSInputView::unbind_output requires a peered target-link input view");
        }
        const bool was_valid = valid();
        const bool was_active = active();
        data_.unbind_target();
        if (was_active && was_valid && scheduling_notifier_ != nullptr && evaluation_time_ != MIN_DT)
        {
            scheduling_notifier_->notify(evaluation_time_);
        }
    }

    void TSInputView::make_active()
    {
        detail::input_view_ops().make_active(*this);
    }

    void TSInputView::make_structural_active()
    {
        detail::input_view_ops().make_structural_active(*this);
    }

    void TSInputView::make_passive()
    {
        detail::input_view_ops().make_passive(*this);
    }

    bool TSInputView::active() const
    {
        return detail::input_view_ops().active(*this);
    }

    TSInputView TSInputView::indexed_child_at(std::size_t index) const
    {
        if (is_target_position())
        {
            auto child = data_view().indexed_child_at(index);
            return child_from_target(std::move(child), index);
        }
        return child_from_input(index);
    }

    TSSInputView TSInputView::as_set() &
    {
        return TSSInputView{borrowed_ref()};
    }

    TSSInputView TSInputView::as_set() const &
    {
        return TSSInputView{borrowed_ref()};
    }

    TSDInputView TSInputView::as_dict() &
    {
        return TSDInputView{borrowed_ref()};
    }

    TSDInputView TSInputView::as_dict() const &
    {
        return TSDInputView{borrowed_ref()};
    }

    TSBInputView TSInputView::as_bundle() &
    {
        return TSBInputView{borrowed_ref()};
    }

    TSBInputView TSInputView::as_bundle() const &
    {
        return TSBInputView{borrowed_ref()};
    }

    TSLInputView TSInputView::as_list() &
    {
        return TSLInputView{borrowed_ref()};
    }

    TSLInputView TSInputView::as_list() const &
    {
        return TSLInputView{borrowed_ref()};
    }

    TSWInputView TSInputView::as_window() &
    {
        return TSWInputView{borrowed_ref()};
    }

    TSWInputView TSInputView::as_window() const &
    {
        return TSWInputView{borrowed_ref()};
    }

    bool TSInputView::is_target_position() const noexcept
    {
        return data_.is_target_position();
    }

    bool TSInputView::inherited_sampled_transition() const noexcept
    {
        return !data_.is_target_root() && sampled_structural_transition();
    }

    bool TSInputView::sampled_structural_transition() const noexcept
    {
        if (!data_.is_target_position() || evaluation_time_ == MIN_DT) { return false; }
        const auto *link = detail::target_link_storage(data_.raw_data);
        return link != nullptr && link->sampled_structural_transition() &&
               link->structural_transition_time() == evaluation_time_;
    }

    const TSValueTypeMetaData *TSInputView::target_path_schema() const noexcept
    {
        return data_.target_path_schema();
    }

    TSDataView TSInputView::input_data_view() const noexcept
    {
        // Collection target-link ops carry sampled add/remove semantics. Keep
        // that wrapper for its transition cycle; ordinary reads take the
        // resolved-source fast path and descendants resolve through the path.
        if (data_.is_target_root())
        {
            const auto *link = detail::target_link_storage(data_.raw_data);
            if (link != nullptr && link->structural_transition_time() == evaluation_time_)
            {
                return data_.raw_data.borrowed_ref();
            }
        }
        const auto &resolved = data_.resolved_value_data();
        if (resolved.valid()) { return resolved.borrowed_ref(); }
        return data_.raw_data.borrowed_ref();
    }

    TSDataView TSInputView::resolve_target_data_view() const noexcept
    {
        return data_.resolved_value_data().borrowed_ref();
    }

    bool TSInputView::target_view_live() const noexcept
    {
        return data_.value_live();
    }

    TSDataView &TSInputView::checked_target_data_view(const char *what) const
    {
        return data_.checked_value_data(what);
    }

    TSInputView TSInputView::child_from_target(TSDataView child, std::size_t index) const
    {
        auto cursor = data_.target_child(std::move(child), index);
        return TSInputView{input_, std::move(cursor.value_data), std::move(cursor.raw_data), cursor.target_node,
                           scheduling_notifier_, evaluation_time_, InputDataCursor::Classification::Known};
    }

    TSInputView TSInputView::child_from_input(std::size_t index) const
    {
        auto parent = data_.value_data.borrowed_ref();
        auto projection = detail::input_child_projection(parent, index);
        return child_from_projection(std::move(projection), index);
    }

    TSInputView TSInputView::child_from_resolved_input(const TSDataView &parent, std::size_t index) const
    {
        auto projection = detail::input_child_projection(parent, index);
        return child_from_projection(std::move(projection), index);
    }

    TSInputView TSInputView::child_from_projection(detail::TSInputChildProjection projection,
                                                   std::size_t index) const noexcept
    {
        static_cast<void>(index);
        auto *target_node = projection.target_link.valid() ? target_root_marker() : nullptr;
        return TSInputView{input_, std::move(projection.visible), std::move(projection.target_link), target_node,
                           scheduling_notifier_, evaluation_time_, InputDataCursor::Classification::Known};
    }

    detail::PreparedInputSlotRoute TSInputView::prepare_child_route(std::size_t index) const
    {
        detail::PreparedInputSlotRoute route;
        auto parent     = data_.value_data.borrowed_ref();
        auto projection = detail::input_child_projection(parent, index);
        if (projection.target_link.valid())
        {
            route.data   = projection.target_link.storage_ref();
            route.target = true;
            const auto *storage = detail::target_link_storage(projection.target_link);
            const auto *state   = storage != nullptr ? storage->state() : nullptr;
            // The trie root pointer is stable for the storage's lifetime
            // (non-pruning contract); the trust conditions are re-checked at
            // EVERY read, not frozen here, so runtime activity toggles
            // (make_passive / make_active / make_structural_active) keep
            // exact semantics.
            route.route = state != nullptr ? state->active_root() : nullptr;
        }
        else { route.data = projection.visible.storage_ref(); }
        return route;
    }

    TSInputView TSInputView::child_from_prepared(const detail::PreparedInputSlotRoute &route) const
    {
        if (!route.target)
        {
            return TSInputView{input_, TSDataView{route.data}, TSDataView{}, nullptr,
                               scheduling_notifier_, evaluation_time_,
                               InputDataCursor::Classification::Known};
        }
        // Empty value seed: every read resolves through the cursor (the
        // prepared route or the full resolution), exactly as the projected
        // slow path does.
        TSInputView view{input_, TSDataView{}, TSDataView{route.data}, target_root_marker(),
                         scheduling_notifier_, evaluation_time_,
                         InputDataCursor::Classification::Known};
        view.data_.route_node = route.route;
        return view;
    }

}  // namespace hgraph
