#include <hgraph/types/time_series/ts_output/base_view.h>

#include <hgraph/types/metadata/type_registry.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/time_series/ts_input/target_link.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output/view_common.h>

#include <utility>

namespace hgraph
{
    TSOutputHandle::TSOutputHandle(const TSOutputView &view) noexcept
        : output_(view.output()),
          data_(view.data_view().storage_ref())
    {
    }

    TSOutputTypeRef TSOutputHandle::type_ref() const
    {
        const auto type = data_.type_ref();
        return type ? TSOutputTypeRef::checked(type) : TSOutputTypeRef{};
    }

    TSOutputView TSOutputHandle::view(DateTime evaluation_time) const noexcept
    {
        return TSOutputView{*this, evaluation_time};
    }

    NodeView TSOutputView::owner_node() const
    {
        return output_ != nullptr ? output_->owner_node() : NodeView{};
    }

    GraphView TSOutputView::owner_graph() const
    {
        return output_ != nullptr ? output_->owner_graph() : GraphView{};
    }

    TSEndpointOwnerPort TSOutputView::owner_port() const noexcept
    {
        if (output_ == nullptr || !output_->has_value()) { return TSEndpointOwnerPort::Output; }
        const auto owner = output_->data_view().root_endpoint_owner();
        return owner.node_owned() ? owner.port() : TSEndpointOwnerPort::Output;
    }

    TSOutputTypeRef TSOutputView::type_ref() const
    {
        const auto type = data_.type_ref();
        return type ? TSOutputTypeRef::checked(type) : TSOutputTypeRef{};
    }

    ValueView TSOutputView::value() const
    {
        return data_.value();
    }

    ValueView TSOutputView::delta_value() const
    {
        return data_.delta_value(evaluation_time_);
    }

    DynamicStorageMetrics TSOutputView::dynamic_storage_metrics() const noexcept
    {
        return output_ != nullptr ? output_->dynamic_storage_metrics() : DynamicStorageMetrics{};
    }

    DateTime TSOutputView::last_modified_time() const
    {
        return data_.last_modified_time();
    }

    bool TSOutputView::modified() const
    {
        return evaluation_time_ != MIN_DT && data_.modified(evaluation_time_);
    }

    bool TSOutputView::valid() const
    {
        return data_.has_current_value();
    }

    bool TSOutputView::all_valid() const
    {
        return data_.all_valid();
    }

    bool TSOutputView::forwarding() const noexcept
    {
        return detail::is_target_link_view(data_);
    }

    bool TSOutputView::forwarding_bound() const noexcept
    {
        return detail::target_link_bound(data_);
    }

    TSOutputHandle TSOutputView::forwarding_target() const noexcept
    {
        const auto *link = detail::target_link_storage(data_);
        return link != nullptr ? link->target_output() : TSOutputHandle{};
    }

    void TSOutputView::bind_forwarding_target(const TSOutputView &source) const
    {
        if (!forwarding())
        {
            throw std::logic_error("TSOutputView::bind_forwarding_target requires a forwarding output view");
        }
        const TSOutputHandle previous = forwarding_target();
        detail::bind_target_link(data_, source);
        if (evaluation_time_ != MIN_DT && previous.bound() && !previous.same_as(forwarding_target()))
        {
            detail::mutable_target_link_storage(data_)->record_target_modified(evaluation_time_);
        }
    }

    void TSOutputView::bind_forwarding_target_sampled(const TSOutputView &source) const
    {
        if (!forwarding())
        {
            throw std::logic_error(
                "TSOutputView::bind_forwarding_target_sampled requires a forwarding output view");
        }
        if (evaluation_time_ == MIN_DT)
        {
            throw std::invalid_argument(
                "TSOutputView::bind_forwarding_target_sampled requires an evaluation time");
        }
        detail::bind_target_link_sampled(data_, source, evaluation_time_);
    }

    TSCheckpointImage TSOutputView::checkpoint_forwarding() const
    {
        if (!bound()) { throw std::invalid_argument("checkpoint forwarding image requires endpoint storage"); }
        TSCheckpointImage image;
        image.schema = schema();
        image.last_modified_time = data_.tracking().last_modified_time;
        if (const auto *link = detail::target_link_storage(data_))
        {
            image.payload = Value{link->bound()};
            image.key_set_last_modified_time = link->checkpoint_key_set_time();
            return image;
        }
        if (schema()->kind != TSTypeKind::TSB &&
            (schema()->kind != TSTypeKind::TSL || schema()->is_unbounded_tsl()))
        {
            throw std::invalid_argument("checkpoint forwarding image requires a forwarding endpoint tree");
        }
        for (std::size_t slot = 0; slot < data_.indexed_child_count(); ++slot)
        {
            image.children.push_back(indexed_child_at(slot).checkpoint_forwarding());
        }
        return image;
    }

    void TSOutputView::validate_checkpoint_forwarding(const TSCheckpointImage &image) const
    {
        if (!bound() || image.version != TSCheckpointImage::current_version ||
            !time_series_schema_equivalent(schema(), image.schema) ||
            !image.keys.empty() || !image.slots.empty() || !image.free_slots.empty() ||
            !image.published.empty() || !image.window_times.empty() || image.slot_capacity != 0 ||
            image.reference.has_value() ||
            data_.tracking().last_modified_time != MIN_DT)
        {
            throw std::invalid_argument("checkpoint forwarding image or fresh target is inconsistent");
        }
        if (const auto *link = detail::target_link_storage(data_))
        {
            if (link->bound() || !image.children.empty() || !image.payload.has_value() ||
                image.key_set_last_modified_time > image.last_modified_time ||
                ((schema()->kind != TSTypeKind::TSD && schema()->kind != TSTypeKind::TSS) &&
                 image.key_set_last_modified_time != MIN_DT))
            {
                throw std::invalid_argument("checkpoint forwarding leaf is inconsistent");
            }
            static_cast<void>(image.payload.view().checked_as<bool>());
            return;
        }
        if ((schema()->kind != TSTypeKind::TSB &&
             (schema()->kind != TSTypeKind::TSL || schema()->is_unbounded_tsl())) ||
            image.payload.has_value() || image.key_set_last_modified_time != MIN_DT ||
            image.children.size() != data_.indexed_child_count())
        {
            throw std::invalid_argument("checkpoint forwarding prefix is inconsistent");
        }
        for (std::size_t slot = 0; slot < image.children.size(); ++slot)
        {
            if (image.children[slot].last_modified_time > image.last_modified_time)
            {
                throw std::invalid_argument("checkpoint forwarding child clock exceeds its parent");
            }
            indexed_child_at(slot).validate_checkpoint_forwarding(image.children[slot]);
        }
    }

    void TSOutputView::restore_checkpoint_forwarding(const TSOutputView &source,
                                                    const TSCheckpointImage &image) const
    {
        validate_checkpoint_forwarding(image);
        const auto validate_source = [](const auto &self, const TSOutputView &target,
                                         const TSOutputView &from, const TSCheckpointImage &saved) -> void {
            if (from.bound() && !time_series_schema_equivalent(target.schema(), from.schema()))
            {
                throw std::invalid_argument("checkpoint forwarding source schema mismatch");
            }
            if (target.forwarding())
            {
                if (saved.payload.view().checked_as<bool>() != from.bound())
                {
                    throw std::invalid_argument("checkpoint forwarding source binding mismatch");
                }
                return;
            }
            for (std::size_t slot = 0; slot < saved.children.size(); ++slot)
            {
                self(self, target.indexed_child_at(slot),
                     from.bound() ? from.indexed_child_at(slot) : TSOutputView{}, saved.children[slot]);
            }
        };
        validate_source(validate_source, *this, source, image);
        const auto restore = [](const auto &self, const TSOutputView &target,
                                 const TSOutputView &from, const TSCheckpointImage &saved) -> void {
            if (auto *link = detail::mutable_target_link_storage(target.data_view()))
            {
                link->restore_binding(*target.schema(), from, saved.last_modified_time,
                                      saved.key_set_last_modified_time);
                return;
            }
            for (std::size_t slot = 0; slot < saved.children.size(); ++slot)
            {
                self(self, target.indexed_child_at(slot),
                     from.bound() ? from.indexed_child_at(slot) : TSOutputView{}, saved.children[slot]);
            }
            const auto *ops = target.data_view().storage_type().ops();
            ops->mutable_tracking_impl(ops->context, const_cast<void *>(target.data_view().data()))
                ->last_modified_time = saved.last_modified_time;
        };
        restore(restore, *this, source, image);
    }

    void TSOutputView::clear_forwarding_target() const
    {
        if (!forwarding())
        {
            throw std::logic_error("TSOutputView::clear_forwarding_target requires a forwarding output view");
        }
        const TSOutputHandle previous = forwarding_target();
        detail::unbind_target_link(data_);
        if (evaluation_time_ != MIN_DT && previous.bound())
        {
            detail::mutable_target_link_storage(data_)->record_target_modified(evaluation_time_);
        }
    }

    void TSOutputView::clear_forwarding_target_sampled() const
    {
        if (!forwarding())
        {
            throw std::logic_error(
                "TSOutputView::clear_forwarding_target_sampled requires a forwarding output view");
        }
        if (evaluation_time_ == MIN_DT)
        {
            throw std::invalid_argument(
                "TSOutputView::clear_forwarding_target_sampled requires an evaluation time");
        }

        auto *link = detail::mutable_target_link_storage(data_);
        if (link == nullptr) { throw std::logic_error("Forwarding output has no target-link storage"); }
        const auto *target_schema = schema();
        if (target_schema != nullptr &&
            (target_schema->kind == TSTypeKind::TSS || target_schema->kind == TSTypeKind::TSD))
        {
            link->unbind_structural(evaluation_time_);
            return;
        }

        const bool was_bound = link->bound();
        link->unbind();
        if (was_bound) { link->record_target_modified(evaluation_time_); }
    }

    void TSOutputView::subscribe(Notifiable *observer) const
    {
        if (!data_.valid()) { throw std::logic_error("TSOutputView::subscribe requires a bound view"); }
        data_.subscribe(observer);
    }

    void TSOutputView::unsubscribe(Notifiable *observer) const
    {
        if (!data_.valid()) { throw std::logic_error("TSOutputView::unsubscribe requires a bound view"); }
        data_.unsubscribe(observer);
    }

    TSOutputHandle TSOutputView::binding_for(const TSValueTypeMetaData &requested_schema) const
    {
        if (output_ == nullptr || !data_.valid())
        {
            throw std::logic_error("TSOutputView::binding_for requires a bound output view");
        }
        return output_->binding_for(*this, requested_schema);
    }

    TSOutputView TSOutputView::through_reference() const
    {
        const auto *current = schema();
        if (current == nullptr || current->kind != TSTypeKind::REF) { return borrowed_ref(); }
        const auto *target = TypeRegistry::instance().dereference(current);
        return binding_for(*target).view(evaluation_time_);
    }

    TSDataMutationView TSOutputView::begin_mutation(DateTime evaluation_time) const
    {
        if (!data_.valid())
        {
            throw std::logic_error("TSOutputView::begin_mutation requires a bound view");
        }
        return data_.begin_mutation(evaluation_time);
    }

    TSOutputView TSOutputView::indexed_child_at(std::size_t index) const
    {
        if (detail::has_input_children(data_))
        {
            auto projection = detail::input_child_projection(data_, index);
            auto child = projection.target_link.valid() ? std::move(projection.target_link)
                                                        : std::move(projection.visible);
            return TSOutputView{output_, child, evaluation_time_};
        }
        auto child = evaluation_time_ == MIN_DT
                         ? data_.indexed_child_at(index)
                         : data_.ensure_indexed_child_at(index, evaluation_time_);
        return TSOutputView{output_, child, evaluation_time_};
    }

    TSSOutputView TSOutputView::as_set() &
    {
        return TSSOutputView{borrowed_ref()};
    }

    TSSOutputView TSOutputView::as_set() const &
    {
        return TSSOutputView{borrowed_ref()};
    }

    TSDOutputView TSOutputView::as_dict() &
    {
        return TSDOutputView{borrowed_ref()};
    }

    TSDOutputView TSOutputView::as_dict() const &
    {
        return TSDOutputView{borrowed_ref()};
    }

    TSBOutputView TSOutputView::as_bundle() &
    {
        return TSBOutputView{borrowed_ref()};
    }

    TSBOutputView TSOutputView::as_bundle() const &
    {
        return TSBOutputView{borrowed_ref()};
    }

    TSLOutputView TSOutputView::as_list() &
    {
        return TSLOutputView{borrowed_ref()};
    }

    TSLOutputView TSOutputView::as_list() const &
    {
        return TSLOutputView{borrowed_ref()};
    }

    TSWOutputView TSOutputView::as_window() &
    {
        return TSWOutputView{borrowed_ref()};
    }

    TSWOutputView TSOutputView::as_window() const &
    {
        return TSWOutputView{borrowed_ref()};
    }

}  // namespace hgraph
