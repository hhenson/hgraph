#include <hgraph/types/time_series/ts_output/bundle_view.h>

#include <hgraph/types/time_series/ts_output/view_common.h>

#include <stdexcept>
#include <utility>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] std::string_view field_name_at(const TSValueTypeMetaData *schema,
                                                     std::size_t                index) noexcept
        {
            if (schema == nullptr || schema->kind != TSTypeKind::TSB || index >= schema->field_count())
            {
                return {};
            }
            const auto *name = schema->fields()[index].name;
            return name != nullptr ? std::string_view{name} : std::string_view{};
        }

        [[nodiscard]] std::size_t field_index(const TSValueTypeMetaData *schema, std::string_view name)
        {
            if (schema != nullptr && schema->kind == TSTypeKind::TSB)
            {
                for (std::size_t index = 0; index < schema->field_count(); ++index)
                {
                    if (field_name_at(schema, index) == name) { return index; }
                }
            }
            throw std::out_of_range("TSBOutputView::at: field not found");
        }

        [[nodiscard]] bool tsb_output_valid_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBOutputView *>(context)->at(index).valid();
        }

        [[nodiscard]] bool tsb_output_modified_child(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBOutputView *>(context)->at(index).modified();
        }

        [[nodiscard]] TSOutputView tsb_output_project_value(const void *context, const void *, std::size_t index)
        {
            return static_cast<const TSBOutputView *>(context)->at(index);
        }

        [[nodiscard]] std::pair<std::string_view, TSOutputView> tsb_output_project_item(
            const void *context,
            const void *,
            std::size_t index)
        {
            const auto *view = static_cast<const TSBOutputView *>(context);
            return {field_name_at(view->schema(), index), view->at(index)};
        }

    }  // namespace

    TSBOutputView::TSBOutputView(TSOutputView view)
        : TSOutputTypedView<TSBOutputView>(std::move(view))
    {
        detail::validate_output_view_kind(schema(), TSTypeKind::TSB, "TSBOutputView");
    }

    TSBDataView TSBOutputView::data_view() const
    {
        return view_.data_view().as_bundle();
    }

    std::size_t TSBOutputView::size() const
    {
        return data_view().size();
    }

    bool TSBOutputView::empty() const
    {
        return data_view().empty();
    }

    bool TSBOutputView::has_field(std::string_view name) const noexcept
    {
        return fallback_on_exception(false, [&] { return data_view().has_field(name); });
    }

    Range<std::string_view> TSBOutputView::keys() const
    {
        return data_view().keys();
    }

    Range<TSOutputView> TSBOutputView::values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(), .predicate = nullptr,
                                   .projector = &tsb_output_project_value};
    }

    Range<TSOutputView> TSBOutputView::valid_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(),
                                   .predicate = &tsb_output_valid_child,
                                   .projector = &tsb_output_project_value};
    }

    Range<TSOutputView> TSBOutputView::modified_values() const
    {
        return Range<TSOutputView>{.context = this, .memory = nullptr, .limit = size(),
                                   .predicate = &tsb_output_modified_child,
                                   .projector = &tsb_output_project_value};
    }

    KeyValueRange<std::string_view, TSOutputView> TSBOutputView::items() const
    {
        return KeyValueRange<std::string_view, TSOutputView>{.context = this,
                                                             .memory = nullptr,
                                                             .limit = size(),
                                                             .predicate = nullptr,
                                                             .projector = &tsb_output_project_item};
    }

    KeyValueRange<std::string_view, TSOutputView> TSBOutputView::valid_items() const
    {
        return KeyValueRange<std::string_view, TSOutputView>{.context = this,
                                                             .memory = nullptr,
                                                             .limit = size(),
                                                             .predicate = &tsb_output_valid_child,
                                                             .projector = &tsb_output_project_item};
    }

    KeyValueRange<std::string_view, TSOutputView> TSBOutputView::modified_items() const
    {
        return KeyValueRange<std::string_view, TSOutputView>{.context = this,
                                                             .memory = nullptr,
                                                             .limit = size(),
                                                             .predicate = &tsb_output_modified_child,
                                                             .projector = &tsb_output_project_item};
    }

    TSOutputView TSBOutputView::at(std::size_t index) &
    {
        return view_.indexed_child_at(index);
    }

    TSOutputView TSBOutputView::at(std::size_t index) const &
    {
        return const_cast<TSBOutputView *>(this)->at(index);
    }

    TSOutputView TSBOutputView::operator[](std::size_t index) &
    {
        return at(index);
    }

    TSOutputView TSBOutputView::operator[](std::size_t index) const &
    {
        return at(index);
    }

    TSOutputView TSBOutputView::at(std::string_view name) &
    {
        return at(field_index(schema(), name));
    }

    TSOutputView TSBOutputView::at(std::string_view name) const &
    {
        return const_cast<TSBOutputView *>(this)->at(name);
    }

    TSOutputView TSBOutputView::field(std::string_view name) &
    {
        return at(name);
    }

    TSOutputView TSBOutputView::field(std::string_view name) const &
    {
        return at(name);
    }

    TSOutputView TSBOutputView::operator[](std::string_view name) &
    {
        return at(name);
    }

    TSOutputView TSBOutputView::operator[](std::string_view name) const &
    {
        return at(name);
    }

}  // namespace hgraph
