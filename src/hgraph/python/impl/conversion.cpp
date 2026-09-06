#include <hgraph/python/conversion.h>

#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>

#include <stdexcept>

namespace hgraph::python_bridge
{
    nb::object take(PyNewRef result)
    {
        if (result.ptr == nullptr)
        {
            throw std::logic_error("a Python conversion slot returned no object and no error");
        }
        return nb::steal(nb::handle{result.ptr});
    }

    nb::object to_python(const ValueOps &ops, const void *memory)
    {
        if (ops.to_python_impl == nullptr)
        {
            throw std::logic_error("ValueOps::to_python is not available for this value type");
        }
        return take(ops.to_python_impl(ops.context, memory));
    }

    void from_python(const ValueOps &ops, const ValueTypeRef &binding, void *memory, nb::handle source)
    {
        if (ops.from_python_impl == nullptr)
        {
            throw std::logic_error("ValueOps::from_python is not available for this value type");
        }
        ops.from_python_impl(ops.context, binding, memory, borrow(source));
    }

    bool can_to_python_buffer(const ValueOps &ops, const ValueTypeRef &binding) noexcept
    {
        return binding.schema() != nullptr && binding.schema()->is_buffer_compatible() &&
               ops.to_python_buffer_impl != nullptr;
    }

    nb::object to_python_buffer(const ValueOps &ops, const ValueTypeRef &binding, const ValueArraySource &source)
    {
        if (!can_to_python_buffer(ops, binding))
        {
            throw std::logic_error("ValueOps::to_python_buffer is not available for this value type");
        }
        if (source.element_at == nullptr && source.first.size + source.second.size != source.size)
        {
            throw std::logic_error("ValueOps::to_python_buffer requires an element accessor or complete spans");
        }
        return take(ops.to_python_buffer_impl(ops.context, binding, source));
    }

    nb::object to_python(const ValueView &view)
    {
        if (!view.valid()) { throw std::runtime_error("ValueView::to_python requires a non-empty view"); }
        return to_python(view.binding(), view.data());
    }

    void from_python(const ValueView &view, nb::handle source)
    {
        if (!view.valid()) { throw std::runtime_error("ValueView::from_python requires a non-empty view"); }
        if (source.is_none())
        {
            throw std::invalid_argument("ValueView::from_python cannot reset a view from None");
        }
        const auto bound = view.binding();
        from_python(bound, view.mutable_data(), source);
    }

    void assign_from_python(const ValueView &view, nb::handle source)
    {
        if (!view.valid()) { throw std::logic_error("ValueView::assign_from_python on invalid view"); }
        if (!view.writable_payload())
        {
            throw std::logic_error("ValueView::assign_from_python requires writable storage");
        }
        const auto bound = view.type();
        from_python(bound, const_cast<void *>(view.data()), source);
    }

    nb::object to_python(const Value &value)
    {
        if (!value.has_value()) { return nb::none(); }
        return to_python(value.view());
    }

    void from_python(Value &value, nb::handle source)
    {
        if (source.is_none())
        {
            value.reset();
            return;
        }
        if (!value.binding()) { throw std::logic_error("Value::from_python requires a schema-bound Value"); }
        const auto bound = value.binding();
        Value      replacement{bound};
        from_python(bound, const_cast<void *>(replacement.view().data()), source);
        value = std::move(replacement);
    }

    nb::object value_to_python(const TSDataView &view)
    {
        const auto &table = view.ops();
        if (!table.has_current_value_impl(table.context, view.data())) { return nb::none(); }
        return take(table.to_python_impl(table.context, view.data()));
    }

    nb::object delta_value_to_python(const TSDataView &view, DateTime evaluation_time)
    {
        const auto &table = view.ops();
        if (evaluation_time == MIN_DT ||
            table.tracking_impl(table.context, view.data())->last_modified_time != evaluation_time)
        {
            return nb::none();
        }
        return take(table.delta_to_python_impl(table.context, view.data(), evaluation_time));
    }

    bool from_python(TSDataMutationView &view, nb::handle source)
    {
        if (source.is_none()) { return false; }
        const auto &table          = view.ops();
        const bool  newly_modified = table.from_python_impl(table.context, view.mutable_data(), borrow(source),
                                                            view.current_mutation_time());
        if (newly_modified) { view.record_reported_modification(); }
        return newly_modified;
    }

    nb::object value_to_python(const TSInputView &view)
    {
        const auto &data = view.data_view();
        return data.valid() ? value_to_python(data) : nb::none();
    }

    nb::object delta_value_to_python(const TSInputView &view)
    {
        const auto &data = view.data_view();
        if (!data.valid()) { return nb::none(); }

        // Sampled target rebinds carry the modification on the input link,
        // not on the already-valid target. In that case the input delta is
        // the target's current value, exported by the target TSData strategy.
        if (view.delta_is_sampled_rebind()) { return value_to_python(data); }

        nb::object delta = delta_value_to_python(data, view.evaluation_time());
        if (!delta.is_none()) { return delta; }

        const auto *view_schema = view.schema();
        if (view_schema != nullptr && view_schema->kind == TSTypeKind::TS && view.modified())
        {
            return value_to_python(data);
        }
        return nb::none();
    }
}  // namespace hgraph::python_bridge
