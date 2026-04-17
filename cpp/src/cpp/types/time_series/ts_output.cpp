#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_type_registry.h>
#include <hgraph/types/value/type_meta_bindings.h>
#include <hgraph/types/ref.h>
#include <hgraph/util/tagged_ptr.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        using TSPath = std::vector<size_t>;

        [[nodiscard]] const char *safe_type_name(const value::TypeMeta *schema) noexcept
        {
            return schema != nullptr && schema->name != nullptr ? schema->name : "?";
        }

        [[nodiscard]] std::string schema_debug_name(const TSMeta *schema)
        {
            if (schema == nullptr) { return "<null>"; }

            switch (schema->kind) {
                case TSKind::TSValue:
                    return fmt::format("TS[{}]", safe_type_name(schema->value_type));
                case TSKind::TSS:
                    return fmt::format("TSS[{}]",
                                       schema->value_type != nullptr ? safe_type_name(schema->value_type->element_type) : "?");
                case TSKind::TSD:
                    return fmt::format("TSD[{}, {}]",
                                       safe_type_name(schema->key_type()),
                                       schema->element_ts() != nullptr ? schema_debug_name(schema->element_ts()) : "?");
                case TSKind::TSL:
                    return fmt::format("TSL[{}]",
                                       schema->element_ts() != nullptr ? schema_debug_name(schema->element_ts()) : "?");
                case TSKind::TSW:
                    return fmt::format("TSW[{}]",
                                       schema->value_type != nullptr && schema->value_type->field_count > 1 &&
                                               schema->value_type->fields != nullptr &&
                                               schema->value_type->fields[1].type != nullptr &&
                                               schema->value_type->fields[1].type->element_type != nullptr
                                           ? safe_type_name(schema->value_type->fields[1].type->element_type)
                                           : "?");
                case TSKind::TSB:
                    return fmt::format("TSB[{}]",
                                       schema->data.tsb.bundle_name != nullptr ? schema->data.tsb.bundle_name : "?");
                case TSKind::REF:
                    return fmt::format("REF[{}]",
                                       schema->element_ts() != nullptr ? schema_debug_name(schema->element_ts()) : "?");
                case TSKind::SIGNAL:
                    return "SIGNAL";
            }

            return "<?>";
        }

        [[nodiscard]] nb::object bundle_python_class(const TSMeta *schema)
        {
            if (schema == nullptr || schema->kind != TSKind::TSB || schema->value_type == nullptr) { return nb::none(); }
            return value::get_compound_scalar_class(schema->value_type);
        }

        [[nodiscard]] const TSMeta *ts_bool_schema()
        {
            static const TSMeta *schema = TSTypeRegistry::instance().ts(value::scalar_type_meta<bool>());
            return schema;
        }

        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSBState &state) noexcept { return &state; }
        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSLState &state) noexcept { return &state; }
        [[nodiscard]] TimeSeriesStateParentPtr parent_ptr(TSDState &state) noexcept { return &state; }
        [[nodiscard]] BaseState *notification_state_of(const LinkedTSContext &context) noexcept
        {
            return context.notification_state != nullptr ? context.notification_state : context.ts_state;
        }

        [[nodiscard]] TSOutputView output_view_from_context(const LinkedTSContext &context, engine_time_t evaluation_time)
        {
            TSViewContext view_context{
                TSContext{
                    context.schema,
                    context.value_dispatch,
                    context.ts_dispatch,
                    context.value_data,
                    context.ts_state,
                    context.owning_output,
                    context.output_view_ops,
                    context.notification_state,
                }};
            return TSOutputView{
                view_context,
                TSViewContext::none(),
                evaluation_time,
                context.owning_output,
                context.output_view_ops != nullptr ? context.output_view_ops : &detail::default_output_view_ops(),
            };
        }

        [[nodiscard]] nb::object removed_type()
        {
            static nb::object type = nb::module_::import_("hgraph").attr("Removed");
            return nb::borrow(type);
        }

        [[nodiscard]] nb::object remove_sentinel()
        {
            static nb::object value = nb::module_::import_("hgraph").attr("REMOVE");
            return nb::borrow(value);
        }

        [[nodiscard]] nb::object remove_if_exists_sentinel()
        {
            static nb::object value = nb::module_::import_("hgraph").attr("REMOVE_IF_EXISTS");
            return nb::borrow(value);
        }

        [[nodiscard]] nb::object set_delta_builder()
        {
            static nb::object fn = nb::module_::import_("hgraph").attr("set_delta");
            return nb::borrow(fn);
        }

        [[nodiscard]] Value ts_nested_value_from_python(const value::TypeMeta &schema, const nb::handle &value)
        {
            Value nested_value(schema, MutationTracking::Plain);
            nested_value.reset();
            nested_value.from_python(nb::borrow<nb::object>(value));
            return nested_value;
        }

        [[nodiscard]] Value ts_nested_clone(const View &value)
        {
            return value.clone(MutationTracking::Plain);
        }

        [[nodiscard]] bool tsd_key_is_live(const TSOutputView &view, const View &key)
        {
            const MapView map_view = view.value().as_map();
            if (key.schema() != map_view.key_schema()) { return false; }

            const size_t slot = map_view.find_slot(key);
            if (slot == static_cast<size_t>(-1)) { return false; }

            const MapDeltaView delta = map_view.delta();
            return slot < delta.slot_capacity() && delta.slot_occupied(slot) && !delta.slot_removed(slot);
        }

        [[nodiscard]] const RefLinkState *switching_ref_state(const TSViewContext &context) noexcept
        {
            const BaseState *state = context.ts_state;
            while (state != nullptr &&
                   (state->storage_kind == TSStorageKind::TargetLink || state->storage_kind == TSStorageKind::OutputLink)) {
                const LinkedTSContext *target = state->linked_target();
                state = target != nullptr ? target->ts_state : nullptr;
            }
            return state != nullptr && state->storage_kind == TSStorageKind::RefLink
                       ? static_cast<const RefLinkState *>(state)
                       : nullptr;
        }

        [[nodiscard]] bool context_valid(const TSViewContext &context) noexcept
        {
            if (context.ts_state == nullptr) { return context.value().has_value(); }
            const auto *dispatch = context.resolved().ts_dispatch;
            return dispatch != nullptr ? dispatch->valid(context) : false;
        }

        [[nodiscard]] bool context_modified(const TSViewContext &context, engine_time_t evaluation_time) noexcept
        {
            if (evaluation_time == MIN_DT) { return false; }

            const TSViewContext resolved = context.resolved();
            const auto *dispatch = resolved.ts_dispatch;
            const engine_time_t last_modified =
                dispatch != nullptr ? dispatch->last_modified_time(resolved)
                                    : (resolved.ts_state != nullptr ? resolved.ts_state->last_modified_time : MIN_DT);
            return last_modified == evaluation_time;
        }

        [[nodiscard]] TSViewContext detached_context(const TSViewContext &context) noexcept
        {
            TSViewContext detached = context.resolved();
            detached.ts_state = nullptr;
            return detached;
        }

        [[nodiscard]] TSViewContext dict_slot_context(const TSViewContext &context, size_t slot)
        {
            const auto *collection = context.resolved().ts_dispatch != nullptr
                                         ? context.resolved().ts_dispatch->as_collection()
                                         : nullptr;
            const auto *dispatch = collection != nullptr ? collection->as_keys() : nullptr;
            if (dispatch == nullptr) { return TSViewContext::none(); }

            TSViewContext child = dispatch->child_at(context, slot);
            if (child.ts_state != nullptr || child.value().has_value()) { return child; }
            return dispatch->child_at(detached_context(context), slot);
        }

        [[nodiscard]] TSOutputView ensure_tsd_child_view(const TSOutputView &view, const View &key)
        {
            TSOutputView child_view = view.as_dict().at(key);
            if (child_view.context_ref().is_bound() || child_view.value().has_value()) { return child_view; }

            BaseState *state = view.context_ref().ts_state != nullptr ? view.context_ref().ts_state->resolved_state() : nullptr;
            if (state == nullptr || state->storage_kind != TSStorageKind::Native) { return child_view; }

            auto *dict_state = static_cast<TSDState *>(state);
            const size_t slot = view.value().as_map().find_slot(key);
            if (slot == static_cast<size_t>(-1)) { return child_view; }

            dict_state->on_insert(slot);
            return view.as_dict().at(key);
        }

        [[nodiscard]] TimeSeriesStateV *owning_state_variant(BaseState *state, TSOutput *root_output) noexcept
        {
            if (state == nullptr) { return nullptr; }
            if (root_output != nullptr) {
                auto &root_state = root_output->root_state_variant();
                if (std::visit([](auto &typed_state) -> BaseState * { return &typed_state; }, root_state) == state) { return &root_state; }
            }

            TimeSeriesStateV *slot = nullptr;
            hgraph::visit(
                state->parent,
                [&](auto *parent) {
                    using T = std::remove_pointer_t<decltype(parent)>;
                    if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSDState> || std::same_as<T, TSBState> ||
                                  std::same_as<T, SignalState>) {
                        if (parent != nullptr && state->index < parent->child_states.size() && parent->child_states[state->index] != nullptr &&
                            std::visit([](auto &typed_state) -> BaseState * { return &typed_state; }, *parent->child_states[state->index]) ==
                                state) {
                            slot = parent->child_states[state->index].get();
                        }
                    }
                },
                [] {});
            return slot;
        }

    }  // namespace

    namespace detail
    {
        [[nodiscard]] nb::object to_python_impl(const TSViewContext &context, engine_time_t evaluation_time);
        [[nodiscard]] nb::object delta_to_python_impl(const TSViewContext &context, engine_time_t evaluation_time);

        [[nodiscard]] nb::object reference_to_python(const TSViewContext &context, engine_time_t evaluation_time)
        {
            BaseState *state = context.ts_state;
            if (state != nullptr) {
                if (const LinkedTSContext *target = state->linked_target(); target != nullptr && target->is_bound()) {
                    TSOutputView target_view = output_view_from_context(*target, evaluation_time);
                    const View target_value = target_view.value();
                    if (target_value.has_value()) {
                        if (const auto *ref = target_value.as_atomic().try_as<TimeSeriesReference>()) { return nb::cast(*ref); }
                    }
                    return nb::cast(TimeSeriesReference::make(target_view));
                }
            }

            const View value = context.value();
            if (value.has_value()) {
                if (const auto *ref = value.as_atomic().try_as<TimeSeriesReference>()) { return nb::cast(*ref); }
            }

            return nb::cast(TimeSeriesReference::make());
        }

        [[nodiscard]] nb::object bundle_to_python(const TSViewContext &context, engine_time_t evaluation_time)
        {
            const TSMeta *schema = context.resolved().schema;
            if (schema == nullptr || schema->kind != TSKind::TSB) { return nb::none(); }

            nb::object python_type = bundle_python_class(schema);
            if (!context.value().has_value()) {
                if (python_type.is_valid() && !python_type.is_none()) {
                    nb::dict kwargs;
                    for (size_t i = 0; i < schema->field_count(); ++i) {
                        kwargs[nb::str(schema->fields()[i].name)] = nb::none();
                    }
                    return python_type(**kwargs);
                }
                return nb::dict();
            }

            if (python_type.is_valid() && !python_type.is_none()) {
                nb::dict kwargs;
                for (size_t i = 0; i < schema->field_count(); ++i) {
                    const auto &field_info = schema->fields()[i];
                    const auto *dispatch = context.resolved().ts_dispatch != nullptr
                                               ? context.resolved().ts_dispatch->as_collection()
                                               : nullptr;
                    const auto *field_dispatch = dispatch != nullptr ? dispatch->as_fields() : nullptr;
                    const TSViewContext child =
                        field_dispatch != nullptr ? field_dispatch->child_at(context, i) : TSViewContext::none();
                    kwargs[nb::str(field_info.name)] =
                        context_valid(child) ? to_python_impl(child, evaluation_time) : nb::none();
                }
                return python_type(**kwargs);
            }

            nb::dict out;
            const auto *dispatch = context.resolved().ts_dispatch != nullptr
                                       ? context.resolved().ts_dispatch->as_collection()
                                       : nullptr;
            const auto *field_dispatch = dispatch != nullptr ? dispatch->as_fields() : nullptr;
            if (field_dispatch == nullptr) { return out; }

            for (size_t i = 0; i < schema->field_count(); ++i) {
                const TSViewContext child = field_dispatch->child_at(context, i);
                if (!context_valid(child)) { continue; }
                const auto &field_info = schema->fields()[i];
                out[nb::str(field_info.name)] = to_python_impl(child, evaluation_time);
            }
            return out;
        }

        [[nodiscard]] nb::object bundle_delta_to_python(const TSViewContext &context, engine_time_t evaluation_time)
        {
            const TSMeta *schema = context.resolved().schema;
            const auto *dispatch = context.resolved().ts_dispatch != nullptr
                                       ? context.resolved().ts_dispatch->as_collection()
                                       : nullptr;
            const auto *field_dispatch = dispatch != nullptr ? dispatch->as_fields() : nullptr;
            nb::dict out;
            if (schema == nullptr || schema->kind != TSKind::TSB || field_dispatch == nullptr || !context.value().has_value()) {
                return out;
            }

            for (size_t i = 0; i < schema->field_count(); ++i) {
                const TSViewContext child = field_dispatch->child_at(context, i);
                if (!context_modified(child, evaluation_time)) { continue; }
                if (!context_valid(child)) { continue; }
                const auto &field_info = schema->fields()[i];
                out[nb::str(field_info.name)] = delta_to_python_impl(child, evaluation_time);
            }
            if (out.empty()) { return nb::none(); }
            return std::move(out);
        }

        [[nodiscard]] nb::object list_to_python(const TSViewContext &context, engine_time_t evaluation_time)
        {
            const TSMeta *schema = context.resolved().schema;
            const auto *dispatch = context.resolved().ts_dispatch != nullptr
                                       ? context.resolved().ts_dispatch->as_collection()
                                       : nullptr;
            const size_t size =
                dispatch != nullptr && context.value().has_value() ? dispatch->size(context) : (schema != nullptr ? schema->fixed_size() : 0);

            nb::list out;
            if (dispatch == nullptr || !context.value().has_value()) {
                for (size_t i = 0; i < size; ++i) { out.append(nb::none()); }
                return nb::tuple(out);
            }

            for (size_t i = 0; i < size; ++i) {
                const TSViewContext child = dispatch->child_at(context, i);
                out.append(context_valid(child) ? to_python_impl(child, evaluation_time) : nb::none());
            }
            return nb::tuple(out);
        }

        [[nodiscard]] nb::object list_delta_to_python(const TSViewContext &context, engine_time_t evaluation_time)
        {
            if (!context.value().has_value()) { return nb::dict(); }

            if (const BaseState *state = context.ts_state; state != nullptr && state->storage_kind != TSStorageKind::Native) {
                if (const LinkedTSContext *target = state->linked_target();
                    target != nullptr && target->ts_state != nullptr &&
                    target->ts_state->resolved_state() != nullptr &&
                    target->ts_state->resolved_state()->storage_kind == TSStorageKind::Native) {
                    nb::dict out;
                    const auto *target_state = static_cast<const TSLState *>(target->ts_state->resolved_state());
                    const auto *dispatch = context.resolved().ts_dispatch != nullptr
                                               ? context.resolved().ts_dispatch->as_collection()
                                               : nullptr;
                    if (dispatch == nullptr) { return out; }

                    for (const size_t slot : target_state->modified_children) {
                        const TSViewContext child = dispatch->child_at(context, slot);
                        if (!child.value().has_value()) { continue; }
                        out[nb::int_(slot)] = delta_to_python_impl(child, evaluation_time);
                    }
                    if (out.empty()) { return nb::none(); }
                    return std::move(out);
                }
            }

            nb::dict out;
            const auto *dispatch = context.resolved().ts_dispatch != nullptr
                                       ? context.resolved().ts_dispatch->as_collection()
                                       : nullptr;
            if (dispatch == nullptr) { return out; }

            for (size_t i = 0, size = dispatch->size(context); i < size; ++i) {
                if (!dispatch->child_modified(context, i)) { continue; }
                const TSViewContext child = dispatch->child_at(context, i);
                out[nb::int_(i)] = delta_to_python_impl(child, evaluation_time);
            }
            if (out.empty()) { return nb::none(); }
            return std::move(out);
        }

        [[nodiscard]] nb::object dict_to_python(const TSViewContext &context, engine_time_t evaluation_time)
        {
            nb::dict out;
            const auto *dispatch = context.resolved().ts_dispatch != nullptr
                                       ? context.resolved().ts_dispatch->as_collection()
                                       : nullptr;
            const auto *key_dispatch = dispatch != nullptr ? dispatch->as_keys() : nullptr;
            if (key_dispatch == nullptr || !context.value().has_value()) { return out; }

            const size_t limit = key_dispatch->iteration_limit(context);
            for (size_t slot = 0; slot < limit; ++slot) {
                if (!key_dispatch->slot_is_live(context, slot)) { continue; }
                const TSViewContext child = dict_slot_context(context, slot);
                if (!context_valid(child)) { continue; }
                out[key_dispatch->key_at_slot(context, slot).to_python()] = to_python_impl(child, evaluation_time);
            }
            return out;
        }

        [[nodiscard]] nb::object dict_delta_to_python(const TSViewContext &context, engine_time_t evaluation_time)
        {
            nb::dict out;
            const auto *dispatch = context.resolved().ts_dispatch != nullptr
                                       ? context.resolved().ts_dispatch->as_collection()
                                       : nullptr;
            const auto *key_dispatch = dispatch != nullptr ? dispatch->as_keys() : nullptr;
            if (key_dispatch == nullptr || !context.value().has_value()) { return nb::none(); }

            const auto *ts_dispatch = context.resolved().ts_dispatch;
            const engine_time_t last_modified_time = ts_dispatch != nullptr ? ts_dispatch->last_modified_time(context) : MIN_DT;
            if (const auto *ref_state = switching_ref_state(context);
                ref_state != nullptr && ref_state->switch_modified_time == last_modified_time &&
                ref_state->previous_target_value.has_value()) {
                const auto current = context.value().as_map();
                const auto previous = ref_state->previous_target_value.view().as_map();
                constexpr size_t no_slot = static_cast<size_t>(-1);

                for (size_t slot = current.first_live_slot(); slot != no_slot; slot = current.next_live_slot(slot)) {
                    const View key = current.delta().key_at_slot(slot);
                    const View current_value = current.at(key);
                    if (!previous.contains(key) || previous.at(key) != current_value) {
                        const TSViewContext child = dict_slot_context(context, slot);
                        if (context_valid(child)) { out[key.to_python()] = to_python_impl(child, evaluation_time); }
                    }
                }

                for (size_t slot = previous.first_live_slot(); slot != no_slot; slot = previous.next_live_slot(slot)) {
                    const View key = previous.delta().key_at_slot(slot);
                    if (!current.contains(key)) { out[key.to_python()] = remove_sentinel(); }
                }

                return out.empty() ? nb::none() : nb::object{std::move(out)};
            }

            const View delta_view =
                ts_dispatch != nullptr ? ts_dispatch->delta_value(context)
                                       : View::invalid_for(context.resolved().schema != nullptr ? context.resolved().schema->value_type : nullptr);
            MapDeltaView delta = delta_view.as_map().delta();
            const size_t limit = key_dispatch->iteration_limit(context);
            for (size_t slot = 0; slot < limit; ++slot) {
                if (!key_dispatch->slot_is_live(context, slot)) { continue; }
                const TSViewContext child = dict_slot_context(context, slot);
                if (!context_modified(child, evaluation_time) || !context_valid(child)) { continue; }
                out[key_dispatch->key_at_slot(context, slot).to_python()] =
                    delta.slot_added(slot) ? to_python_impl(child, evaluation_time) : delta_to_python_impl(child, evaluation_time);
            }

            for (size_t slot = delta.first_removed_slot(); slot != static_cast<size_t>(-1); slot = delta.next_removed_slot(slot)) {
                out[delta.key_at_slot(slot).to_python()] = remove_sentinel();
            }

            return out.empty() ? nb::none() : nb::object{std::move(out)};
        }

        [[nodiscard]] nb::object set_to_python(const TSViewContext &context)
        {
            const View value = context.value();
            return value.has_value() ? value.to_python() : nb::set();
        }

        [[nodiscard]] nb::object set_delta_to_python(const TSViewContext &context)
        {
            nb::list added_values;
            nb::list removed_values;
            const auto *dispatch = context.resolved().ts_dispatch != nullptr ? context.resolved().ts_dispatch->as_set() : nullptr;
            if (dispatch != nullptr) {
                for (const View &item : dispatch->added_values(context)) { added_values.append(item.to_python()); }
                for (const View &item : dispatch->removed_values(context)) { removed_values.append(item.to_python()); }
            }
            return set_delta_builder()(added_values, removed_values);
        }

        [[nodiscard]] nb::object leaf_to_python(const TSViewContext &context)
        {
            const View value = context.value();
            return value.has_value() ? value.to_python() : nb::none();
        }

        [[nodiscard]] nb::object leaf_delta_to_python(const TSViewContext &context)
        {
            const auto *dispatch = context.resolved().ts_dispatch;
            const auto *value_schema = context.resolved().schema != nullptr ? context.resolved().schema->value_type : nullptr;
            const View value = dispatch != nullptr ? dispatch->delta_value(context) : View::invalid_for(value_schema);
            return value.has_value() ? value.to_python() : nb::none();
        }

        [[nodiscard]] nb::object to_python_impl(const TSViewContext &context, engine_time_t evaluation_time)
        {
            const auto *dispatch = context.resolved().ts_dispatch;
            return dispatch != nullptr ? dispatch->to_python(context, evaluation_time) : leaf_to_python(context);
        }

        [[nodiscard]] nb::object delta_to_python_impl(const TSViewContext &context, engine_time_t evaluation_time)
        {
            const auto *dispatch = context.resolved().ts_dispatch;
            return dispatch != nullptr ? dispatch->delta_to_python(context, evaluation_time) : leaf_delta_to_python(context);
        }

        void bundle_from_python(const TSOutputView &view, nb::handle value)
        {
            const TSMeta *schema = view.ts_schema();
            if (schema == nullptr || schema->kind != TSKind::TSB) {
                throw std::logic_error("TSOutputView bundle mutation requires a TSB schema");
            }

            const nb::object python_type = bundle_python_class(schema);
            if (python_type.is_valid() && !python_type.is_none() && nb::isinstance(value, python_type)) {
                for (size_t i = 0; i < schema->field_count(); ++i) {
                    const std::string_view field_name = schema->fields()[i].name;
                    nb::object attr = nb::getattr(value, field_name.data(), nb::none());
                    if (!attr.is_none()) { view.as_bundle().field(field_name).from_python(attr); }
                }
                return;
            }

            const nb::object items = nb::hasattr(value, "items") ? nb::getattr(value, "items")() : nb::borrow<nb::object>(value);
            for (auto pair : nb::iter(items)) {
                nb::object field_value = nb::borrow<nb::object>(pair[1]);
                if (!field_value.is_none()) { view.as_bundle().field(nb::cast<std::string>(pair[0])).from_python(field_value); }
            }
        }

        void list_from_python(const TSOutputView &view, nb::handle value)
        {
            const TSMeta *schema = view.ts_schema();
            if (schema == nullptr || schema->kind != TSKind::TSL) {
                throw std::logic_error("TSOutputView list mutation requires a TSL schema");
            }

            const auto list_view = view.as_list();
            if (nb::isinstance<nb::tuple>(value) || nb::isinstance<nb::list>(value)) {
                const size_t size = nb::len(value);
                if (schema->fixed_size() != 0 && size != schema->fixed_size()) {
                    throw nb::value_error(
                        fmt::format("Expected {} elements, got {}", schema->fixed_size(), size).c_str());
                }
                for (size_t i = 0; i < size; ++i) {
                    nb::object item = nb::borrow<nb::object>(value[i]);
                    if (!item.is_none()) { list_view.at(i).from_python(item); }
                }
                return;
            }

            if (nb::isinstance<nb::dict>(value)) {
                for (auto [key, item] : nb::cast<nb::dict>(value)) {
                    nb::object item_object = nb::borrow<nb::object>(item);
                    if (!item_object.is_none()) { list_view.at(nb::cast<size_t>(key)).from_python(item_object); }
                }
                return;
            }

            throw std::runtime_error("Invalid value type for TSOutputView list mutation");
        }

        void dict_from_python(const TSOutputView &view, nb::handle value)
        {
            const TSMeta *schema = view.ts_schema();
            if (schema == nullptr || schema->kind != TSKind::TSD) {
                throw std::logic_error("TSOutputView dict mutation requires a TSD schema");
            }

            const value::TypeMeta *key_schema = schema->key_type();
            const TSMeta *value_ts_schema = schema->element_ts();
            const value::TypeMeta *mapped_schema = value_ts_schema != nullptr ? value_ts_schema->value_type : nullptr;
            if (key_schema == nullptr || mapped_schema == nullptr) {
                throw std::logic_error("TSOutputView dict mutation requires key and value schemas");
            }

            const auto ensure_live_child = [&](const View &key) {
                if (tsd_key_is_live(view, key)) { return; }

                Value mapped_value(*mapped_schema, MutationTracking::Plain);
                mapped_value.reset();

                {
                    auto mutation = view.value().as_map().begin_mutation(view.evaluation_time());
                    mutation.set(key, mapped_value.view());
                }

                mark_output_view_modified(view, view.evaluation_time());
            };

            const auto apply_child_value = [&](const View &key, nb::handle entry_value, bool apply_result) {
                ensure_live_child(key);

                    TSOutputView child_view = ensure_tsd_child_view(view, key);
                    if (!child_view.context_ref().is_bound() && !child_view.value().has_value()) {
                        throw std::logic_error("TSD child mutation failed to materialize a bound child view");
                    }

                if (apply_result && !entry_value.is_none()) {
                    child_view.apply_result(entry_value);
                } else {
                    child_view.from_python(entry_value);
                }
            };

            if (!view.valid() && !nb::cast<bool>(nb::bool_(value))) {
                mark_output_view_modified(view, view.evaluation_time());
                return;
            }

            const nb::object item_attr = nb::getattr(value, "items", nb::none());
            nb::iterator items = item_attr.is_none() ? nb::iter(value) : nb::iter(item_attr());
            std::vector<std::pair<Value, nb::object>> entries;

            for (const auto &kv : items) {
                nb::object entry_value = nb::borrow<nb::object>(kv[1]);
                if (entry_value.is_none()) { continue; }
                entries.emplace_back(
                    ts_nested_value_from_python(*key_schema, nb::borrow<nb::object>(kv[0])),
                    std::move(entry_value));
            }

            bool structural_changed = false;
            {
                auto mutation = view.value().as_map().begin_mutation(view.evaluation_time());
                for (const auto &[key_value, entry_value] : entries) {
                    const View key = key_value.view();

                    if (entry_value.is(remove_sentinel()) || entry_value.is(remove_if_exists_sentinel())) {
                        const bool removed = mutation.remove(key);
                        if (!removed && entry_value.is(remove_sentinel())) {
                            throw nb::key_error("TSD key not found for REMOVE");
                        }
                        structural_changed = structural_changed || removed;
                        continue;
                    }

                    if (!tsd_key_is_live(view, key)) {
                        Value mapped_value(*mapped_schema, MutationTracking::Plain);
                        mapped_value.reset();
                        mutation.set(key, mapped_value.view());
                        structural_changed = true;
                    }
                }
            }

            if (structural_changed) {
                mark_output_view_modified(view, view.evaluation_time());
            }

            for (const auto &[key_value, entry_value] : entries) {
                if (entry_value.is(remove_sentinel()) || entry_value.is(remove_if_exists_sentinel())) { continue; }
                apply_child_value(key_value.view(), entry_value, false);
            }
        }

        void dict_apply_result(const TSOutputView &view, nb::handle value)
        {
            if (nb::hasattr(value, "items")) {
                const nb::object items_object = nb::getattr(value, "items")();
                bool has_remove_sentinel = false;
                for (auto item : nb::iter(items_object)) {
                    nb::object entry_value = nb::borrow<nb::object>(item[1]);
                    if (entry_value.is(remove_sentinel()) || entry_value.is(remove_if_exists_sentinel())) {
                        has_remove_sentinel = true;
                        break;
                    }
                }
                if (has_remove_sentinel) {
                    dict_from_python(view, value);
                    return;
                }

                const TSMeta *schema = view.ts_schema();
                if (schema == nullptr || schema->kind != TSKind::TSD) {
                    throw std::logic_error("TSOutputView dict result application requires a TSD schema");
                }

                const value::TypeMeta *key_schema = schema->key_type();
                const TSMeta *value_ts_schema = schema->element_ts();
                const value::TypeMeta *mapped_schema = value_ts_schema != nullptr ? value_ts_schema->value_type : nullptr;
                if (key_schema == nullptr || mapped_schema == nullptr) {
                    throw std::logic_error("TSOutputView dict result application requires key and value schemas");
                }

                std::vector<std::pair<Value, nb::object>> replacement;
                for (auto item : nb::iter(items_object)) {
                    Value key_value = ts_nested_value_from_python(*key_schema, nb::borrow<nb::object>(item[0]));
                    replacement.emplace_back(std::move(key_value), nb::borrow<nb::object>(item[1]));
                }

                auto map_view = view.value().as_map();
                std::vector<Value> existing_keys;
                const auto current_delta = map_view.delta();
                for (size_t slot = 0; slot < current_delta.slot_capacity(); ++slot) {
                    if (!current_delta.slot_occupied(slot) || current_delta.slot_removed(slot)) { continue; }
                    existing_keys.push_back(current_delta.key_at_slot(slot).clone());
                }

                bool structural_changed = false;
                {
                    auto mutation = map_view.begin_mutation(view.evaluation_time());

                    for (const Value &existing_key : existing_keys) {
                        const bool keep = std::any_of(
                            replacement.begin(),
                            replacement.end(),
                            [&](const auto &candidate) { return existing_key.view() == candidate.first.view(); });
                        if (!keep) { structural_changed = mutation.remove(existing_key.view()) || structural_changed; }
                    }

                    for (const auto &[key, mapped_value] : replacement) {
                        static_cast<void>(mapped_value);
                        const View key_view = key.view();
                        if (!tsd_key_is_live(view, key_view)) {
                            Value invalid_value(*mapped_schema, MutationTracking::Plain);
                            invalid_value.reset();
                            mutation.set(key_view, invalid_value.view());
                            structural_changed = true;
                        }
                    }
                }

                if (structural_changed || (!view.valid() && replacement.empty())) {
                    mark_output_view_modified(view, view.evaluation_time());
                }

                for (const auto &[key, mapped_value] : replacement) {
                    const View key_view = key.view();
                    TSOutputView child_view = ensure_tsd_child_view(view, key_view);
                    if (!child_view.context_ref().is_bound() && !child_view.value().has_value()) {
                        throw std::logic_error("TSD child result application failed to materialize a bound child view");
                    }
                    if (mapped_value.is_none()) {
                        child_view.from_python(mapped_value);
                    } else {
                        child_view.apply_result(mapped_value);
                    }
                }

                return;
            }

            dict_from_python(view, value);
        }

        void dict_child_from_python(const TSOutputView &view, const View &key, nb::handle value)
        {
            const TSMeta *schema = view.ts_schema();
            if (schema == nullptr || schema->kind != TSKind::TSD) {
                throw std::logic_error("TSOutputView dict child mutation requires a TSD schema");
            }
            if (schema->element_ts() == nullptr || schema->element_ts()->value_type == nullptr) {
                throw std::logic_error("TSOutputView dict child mutation requires a mapped value schema");
            }

            if (value.is_none()) {
                Value mapped_value(*schema->element_ts()->value_type, MutationTracking::Plain);
                mapped_value.reset();

                {
                    auto mutation = view.value().as_map().begin_mutation(view.evaluation_time());
                    mutation.set(key, mapped_value.view());
                }

                mark_output_view_modified(view, view.evaluation_time());

                TSOutputView child_view = ensure_tsd_child_view(view, key);
                if (!child_view.context_ref().is_bound() && !child_view.value().has_value()) {
                    throw std::logic_error("TSD child mutation failed to materialize a bound child view");
                }

                child_view.from_python(value);
                return;
            }

            nb::object entry_value = nb::borrow<nb::object>(value);
            if (entry_value.is(remove_sentinel()) || entry_value.is(remove_if_exists_sentinel())) {
                bool removed = false;
                {
                    auto mutation = view.value().as_map().begin_mutation(view.evaluation_time());
                    removed = mutation.remove(key);
                }
                if (!removed && entry_value.is(remove_sentinel())) {
                    throw nb::key_error("TSD key not found for REMOVE");
                }
                if (removed) { mark_output_view_modified(view, view.evaluation_time()); }
                return;
            }

            if (!tsd_key_is_live(view, key)) {
                Value mapped_value(*schema->element_ts()->value_type, MutationTracking::Plain);
                mapped_value.reset();

                {
                    auto mutation = view.value().as_map().begin_mutation(view.evaluation_time());
                    mutation.set(key, mapped_value.view());
                }

                mark_output_view_modified(view, view.evaluation_time());
            }

            TSOutputView child_view = ensure_tsd_child_view(view, key);
            if (!child_view.context_ref().is_bound() && !child_view.value().has_value()) {
                throw std::logic_error("TSD child mutation failed to materialize a bound child view");
            }

            child_view.from_python(entry_value);
        }

        void erase_dict_key(const TSOutputView &view, const View &key)
        {
            dict_child_from_python(view, key, remove_sentinel());
        }

        void set_from_python(const TSOutputView &view, nb::handle value)
        {
            const TSMeta *schema = view.ts_schema();
            if (schema == nullptr || schema->kind != TSKind::TSS) {
                throw std::logic_error("TSOutputView set mutation requires a TSS schema");
            }

            auto set_view = view.value().as_set();
            const value::TypeMeta *element_schema = set_view.element_schema();
            if (element_schema == nullptr) {
                throw std::logic_error("TSOutputView set mutation requires an element schema");
            }

            auto mutation = set_view.begin_mutation(view.evaluation_time());
            bool changed = false;

            const auto convert_value = [element_schema](const nb::handle &item) {
                return ts_nested_value_from_python(*element_schema, nb::borrow<nb::object>(item));
            };

            const auto apply_added = [&](const nb::handle &item) {
                Value element = convert_value(item);
                changed = mutation.add(element.view()) || changed;
            };

            const auto apply_removed = [&](const nb::handle &item) {
                Value element = convert_value(item);
                changed = mutation.remove(element.view()) || changed;
            };

            if (nb::hasattr(value, "added") && nb::hasattr(value, "removed")) {
                for (auto item : nb::iter(nb::getattr(value, "added"))) { apply_added(nb::borrow<nb::object>(item)); }
                for (auto item : nb::iter(nb::getattr(value, "removed"))) { apply_removed(nb::borrow<nb::object>(item)); }
            } else if (nb::isinstance<nb::frozenset>(value)) {
                std::vector<Value> replacement;
                replacement.reserve(nb::len(value));
                for (auto item : nb::iter(value)) { replacement.push_back(convert_value(nb::borrow<nb::object>(item))); }

                std::vector<Value> existing_items;
                for (const View &existing : set_view.values()) {
                    existing_items.push_back(existing.clone());
                }

                for (const Value &existing : existing_items) {
                    const bool keep = std::any_of(replacement.begin(),
                                                  replacement.end(),
                                                  [&](const Value &candidate) { return existing.view() == candidate.view(); });
                    if (!keep) { changed = mutation.remove(existing.view()) || changed; }
                }

                for (const Value &item : replacement) {
                    if (!set_view.contains(item.view())) { changed = mutation.add(item.view()) || changed; }
                }
            } else if (nb::isinstance<nb::set>(value) || nb::isinstance<nb::frozenset>(value) || nb::isinstance<nb::list>(value) ||
                       nb::isinstance<nb::tuple>(value) || nb::isinstance<nb::dict>(value)) {
                for (auto item : nb::iter(value)) {
                    nb::object item_object = nb::borrow<nb::object>(item);
                    if (nb::isinstance(item_object, removed_type())) {
                        apply_removed(nb::getattr(item_object, "item"));
                    } else {
                        apply_added(item_object);
                    }
                }
            } else {
                throw std::runtime_error("Invalid value type for TSOutputView set mutation");
            }

            if (changed || !view.valid()) { mark_output_view_modified(view, view.evaluation_time()); }
        }

        void set_apply_result(const TSOutputView &view, nb::handle value)
        {
            if (nb::hasattr(value, "added") && nb::hasattr(value, "removed")) {
                set_from_python(view, value);
                return;
            }

            if (nb::isinstance<nb::set>(value) || nb::isinstance<nb::list>(value) || nb::isinstance<nb::tuple>(value) ||
                nb::isinstance<nb::dict>(value)) {
                set_from_python(view, value);
                return;
            }

            if (!nb::isinstance<nb::frozenset>(value)) {
                set_from_python(view, value);
                return;
            }

            const TSMeta *schema = view.ts_schema();
            if (schema == nullptr || schema->kind != TSKind::TSS) {
                throw std::logic_error("TSOutputView set result application requires a TSS schema");
            }

            auto set_view = view.value().as_set();
            const value::TypeMeta *element_schema = set_view.element_schema();
            if (element_schema == nullptr) {
                throw std::logic_error("TSOutputView set result application requires an element schema");
            }

            std::vector<Value> replacement;
            replacement.reserve(nb::len(value));
            for (auto item : nb::iter(value)) {
                replacement.push_back(ts_nested_value_from_python(*element_schema, nb::borrow<nb::object>(item)));
            }

            std::vector<Value> existing_items;
            for (const View &existing : set_view.values()) {
                existing_items.push_back(existing.clone());
            }

            bool changed = false;
            {
                auto mutation = set_view.begin_mutation(view.evaluation_time());

                for (const Value &existing : existing_items) {
                    const bool keep = std::any_of(
                        replacement.begin(),
                        replacement.end(),
                        [&](const Value &candidate) { return existing.view() == candidate.view(); });
                    if (!keep) { changed = mutation.remove(existing.view()) || changed; }
                }

                for (const Value &item : replacement) {
                    const bool exists = std::any_of(
                        existing_items.begin(),
                        existing_items.end(),
                        [&](const Value &existing) { return existing.view() == item.view(); });
                    if (!exists) { changed = mutation.add(item.view()) || changed; }
                }
            }

            if (changed || (!view.valid() && replacement.empty())) { mark_output_view_modified(view, view.evaluation_time()); }
        }

        void add_set_item(const TSOutputView &view, const View &item)
        {
            bool changed = false;
            {
                auto set_view = view.value().as_set();
                auto mutation = set_view.begin_mutation(view.evaluation_time());
                changed = mutation.add(item);
            }
            if (changed) { mark_output_view_modified(view, view.evaluation_time()); }
        }

        void remove_set_item(const TSOutputView &view, const View &item)
        {
            bool changed = false;
            {
                auto set_view = view.value().as_set();
                auto mutation = set_view.begin_mutation(view.evaluation_time());
                changed = mutation.remove(item);
            }
            if (changed) { mark_output_view_modified(view, view.evaluation_time()); }
        }

        void clear_set_items(const TSOutputView &view)
        {
            bool had_live_values = false;
            {
                auto set_view = view.value().as_set();
                had_live_values = set_view.size() != 0;
                auto mutation = set_view.begin_mutation(view.evaluation_time());
                mutation.clear();
            }
            if (had_live_values) { mark_output_view_modified(view, view.evaluation_time()); }
        }

    }  // namespace detail

    namespace
    {
        [[nodiscard]] TSOutputView traverse_output_path(TSOutputView view, const TSMeta *schema, const TSPath &path);

        struct RootRefValueNotifier final : Notifiable
        {
            explicit RootRefValueNotifier(TSOutput *owner_) noexcept : owner(owner_) {}

            void notify(engine_time_t modified_time) override
            {
                if (owner == nullptr) { return; }
                if (BaseState *root = owner->view(modified_time).context_ref().ts_state; root != nullptr) {
                    root->mark_modified(modified_time);
                }
            }

            TSOutput *owner{nullptr};
        };

        struct DerivedSetFeatureOutput final : Notifiable
        {
            explicit DerivedSetFeatureOutput(const LinkedTSContext &source_context_,
                                            engine_time_t initial_time,
                                            std::optional<Value> item_ = std::nullopt)
                : output(TSOutputBuilderFactory::checked_builder_for(ts_bool_schema())),
                  source_context(source_context_),
                  item(std::move(item_))
            {
                if (BaseState *state = notification_state_of(source_context); state != nullptr) { state->subscribe(this); }
                refresh(initial_time != MIN_DT ? initial_time : MIN_ST, true);
            }

            ~DerivedSetFeatureOutput() override
            {
                if (BaseState *state = notification_state_of(source_context); state != nullptr) { state->unsubscribe(this); }
            }

            void notify(engine_time_t modified_time) override { refresh(modified_time, false); }

            void refresh(engine_time_t evaluation_time, bool initialise)
            {
                bool next_value = !item.has_value();

                try {
                    TSOutputView source_view = output_view_from_context(source_context, evaluation_time);
                    if (source_view.valid()) {
                        next_value = item.has_value()
                                         ? source_view.value().as_set().contains(item->view())
                                         : source_view.as_set().empty();
                    }
                } catch (...) {
                    next_value = !item.has_value();
                }

                if (initialised && next_value == current_value) { return; }

                TSOutputView output_view = output.view(evaluation_time);
                output_view.value().as_atomic().set(next_value);
                current_value = next_value;
                initialised = true;
                if (evaluation_time != MIN_DT || initialise) { mark_output_view_modified(output_view, evaluation_time); }
            }

            TSOutput               output;
            LinkedTSContext        source_context;
            std::optional<Value>   item;
            bool                   current_value{false};
            bool                   initialised{false};
        };

        struct ValueKeyHash
        {
            using is_transparent = void;

            [[nodiscard]] size_t operator()(const Value &value) const noexcept { return value.hash(); }
            [[nodiscard]] size_t operator()(const View &value) const noexcept { return value.hash(); }
        };

        struct ValueKeyEqual
        {
            using is_transparent = void;

            [[nodiscard]] bool operator()(const Value &lhs, const Value &rhs) const noexcept { return lhs.equals(rhs); }
            [[nodiscard]] bool operator()(const Value &lhs, const View &rhs) const noexcept { return lhs.equals(rhs); }
            [[nodiscard]] bool operator()(const View &lhs, const Value &rhs) const noexcept { return rhs.equals(lhs); }
            [[nodiscard]] bool operator()(const View &lhs, const View &rhs) const noexcept { return lhs.equals(rhs); }
        };

        [[nodiscard]] LinkedTSContext feature_source_context(const TSOutputView &view)
        {
            LinkedTSContext source_context = view.linked_context();
            if (!source_context.is_bound()) {
                throw std::invalid_argument("TSOutputView feature registration requires a resolved output position");
            }
            return source_context;
        }

        struct CollectionFeatureRegistry final : TimeSeriesFeatureRegistry
        {
            [[nodiscard]] TSOutputView register_contains_output(const LinkedTSContext &source_context,
                                                                engine_time_t evaluation_time,
                                                                const View &item)
            {
                auto it = contains_outputs.find(item);
                if (it == contains_outputs.end()) {
                    Value key = ts_nested_clone(item);
                    auto [inserted, success] = contains_outputs.emplace(
                        std::move(key),
                        std::make_unique<DerivedSetFeatureOutput>(source_context, evaluation_time, ts_nested_clone(item)));
                    static_cast<void>(success);
                    it = inserted;
                }
                return it->second->output.view(evaluation_time);
            }

            void unregister_contains_output(const View &) const noexcept {}

            [[nodiscard]] TSOutputView register_is_empty_output(const LinkedTSContext &source_context, engine_time_t evaluation_time)
            {
                if (is_empty_output == nullptr) {
                    is_empty_output = std::make_unique<DerivedSetFeatureOutput>(source_context, evaluation_time);
                }
                return is_empty_output->output.view(evaluation_time);
            }

            void unregister_is_empty_output() const noexcept {}

            std::unordered_map<Value, std::unique_ptr<DerivedSetFeatureOutput>, ValueKeyHash, ValueKeyEqual> contains_outputs;
            std::unique_ptr<DerivedSetFeatureOutput>                                                         is_empty_output;
        };

        [[nodiscard]] CollectionFeatureRegistry &ensure_collection_feature_registry(BaseState &state)
        {
            if (state.feature_registry == nullptr) {
                state.feature_registry = std::make_unique<CollectionFeatureRegistry>();
            }
            auto *registry = dynamic_cast<CollectionFeatureRegistry *>(state.feature_registry.get());
            if (registry == nullptr) { throw std::logic_error("TSOutputView state feature registry has an unexpected type"); }
            return *registry;
        }

        void collect_ref_target_states(const TimeSeriesReference &ref,
                                       std::unordered_set<BaseState *> &states) noexcept
        {
            switch (ref.kind()) {
                case TimeSeriesReference::Kind::EMPTY:
                    return;

                case TimeSeriesReference::Kind::PEERED:
                    if (BaseState *state = notification_state_of(ref.target()); state != nullptr) { states.insert(state); }
                    return;

                case TimeSeriesReference::Kind::NON_PEERED:
                    for (const auto &item : ref.items()) { collect_ref_target_states(item, states); }
                    return;
            }
        }

        template <typename TState>
        void initialize_base_state(TState &state,
                                   TimeSeriesStateParentPtr parent,
                                   size_t index,
                                   engine_time_t modified_time = MIN_DT,
                                   TSStorageKind storage_kind = TSStorageKind::Native) noexcept
        {
            state.parent = parent;
            state.index = index;
            state.last_modified_time = modified_time;
            state.storage_kind = storage_kind;
            state.subscribers.clear();
        }

        template <typename TCollectionState>
        void install_target_link(TCollectionState &parent_state, size_t slot, LinkedTSContext target)
        {
            auto link_state = std::make_unique<TimeSeriesStateV>();
            auto &typed_state = link_state->template emplace<TargetLinkState>();
            initialize_base_state(typed_state,
                                  parent_ptr(parent_state),
                                  slot,
                                  target.ts_state != nullptr ? target.ts_state->last_modified_time : MIN_DT,
                                  TSStorageKind::TargetLink);
            typed_state.target.clear();
            typed_state.scheduling_notifier.set_target(nullptr);
            typed_state.set_target(std::move(target));
            parent_state.child_states[slot] = std::move(link_state);
        }

        void initialize_ref_link_state(RefLinkState &state,
                                       TimeSeriesStateParentPtr parent,
                                       size_t index,
                                       engine_time_t modified_time = MIN_DT) noexcept
        {
            initialize_base_state(state, parent, index, modified_time, TSStorageKind::RefLink);
            state.source.clear();
            initialize_base_state(state.bound_link, static_cast<TSOutput *>(nullptr), 0, MIN_DT, TSStorageKind::TargetLink);
            state.bound_link.target.clear();
            state.bound_link.scheduling_notifier.set_target(nullptr);
            state.retain_transition_value = true;
        }

        template <typename TCollectionState>
        void install_ref_link(TCollectionState &parent_state, size_t slot, LinkedTSContext ref_source)
        {
            auto link_state = std::make_unique<TimeSeriesStateV>();
            auto &typed_state = link_state->template emplace<RefLinkState>();
            initialize_ref_link_state(typed_state, parent_ptr(parent_state), slot);
            typed_state.set_source(std::move(ref_source));
            parent_state.child_states[slot] = std::move(link_state);
        }

        [[nodiscard]] bool supports_alternative_cast(const TSMeta &source_schema, const TSMeta &target_schema)
        {
            if (&source_schema == &target_schema) { return true; }

            // The two schema-changing cases supported so far are:
            // - TS  -> REF : wrap as a TimeSeriesReference value
            // - REF -> TS  : dereference through RefLinkState
            if (target_schema.kind == TSKind::REF) {
                const TSMeta *target_element = target_schema.element_ts();
                return target_element != nullptr && supports_alternative_cast(source_schema, *target_element);
            }
            if (target_schema.kind == TSKind::SIGNAL) { return source_schema.kind != TSKind::SIGNAL; }
            if (source_schema.kind == TSKind::TSD && target_schema.kind == TSKind::TSS) {
                return TSTypeRegistry::instance().tss(source_schema.key_type()) == &target_schema;
            }
            if (source_schema.kind == TSKind::REF) {
                return source_schema.element_ts() == &target_schema ||
                       supports_alternative_cast(*source_schema.element_ts(), target_schema);
            }

            if (source_schema.kind == TSKind::TSValue && target_schema.kind == TSKind::TSValue) { return true; }

            if (source_schema.kind != target_schema.kind) { return false; }

            switch (source_schema.kind) {
                case TSKind::TSB:
                    if (source_schema.field_count() != target_schema.field_count()) { return false; }
                    for (size_t i = 0; i < source_schema.field_count(); ++i) {
                        const auto &source_field = source_schema.fields()[i];
                        const auto &target_field = target_schema.fields()[i];
                        if (std::string_view{source_field.name} != std::string_view{target_field.name}) { return false; }
                        if (!supports_alternative_cast(*source_field.ts_type, *target_field.ts_type)) { return false; }
                    }
                    return true;

                case TSKind::TSL:
                    if (source_schema.fixed_size() == 0 || source_schema.fixed_size() != target_schema.fixed_size()) { return false; }
                    return supports_alternative_cast(*source_schema.element_ts(), *target_schema.element_ts());

                case TSKind::TSD:
                    return source_schema.key_type() == target_schema.key_type() &&
                           supports_alternative_cast(*source_schema.element_ts(), *target_schema.element_ts());

                default:
                    return false;
            }
        }

        [[nodiscard]] const TSMeta *child_schema_at(const TSMeta &schema, size_t slot)
        {
            switch (schema.kind) {
                case TSKind::TSB:
                    if (slot >= schema.field_count()) { throw std::out_of_range("TSOutput alternative child slot is out of range for TSB"); }
                    return schema.fields()[slot].ts_type;

                case TSKind::TSL:
                    if (schema.fixed_size() == 0) {
                        throw std::invalid_argument("TSOutput alternatives do not yet support dynamic TSL path prefixes");
                    }
                    if (slot >= schema.fixed_size()) { throw std::out_of_range("TSOutput alternative child slot is out of range for TSL"); }
                    return schema.element_ts();

                case TSKind::TSD:
                    return schema.element_ts();

                default:
                    throw std::invalid_argument("TSOutput alternative child navigation only supports TSB, fixed-size TSL, and TSD");
            }
        }

        [[nodiscard]] BaseState *base_state_of(TimeSeriesStateV &state) noexcept
        {
            return std::visit([](auto &typed_state) -> BaseState * { return &typed_state; }, state);
        }

        [[nodiscard]] const BaseState *parent_collection_of(const BaseState *state) noexcept
        {
            if (state == nullptr) { return nullptr; }

            const BaseState *parent_state = nullptr;
            hgraph::visit(
                state->parent,
                [&](const auto *parent) {
                    using T = std::remove_pointer_t<decltype(parent)>;
                    if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSDState> || std::same_as<T, TSBState>) {
                        parent_state = parent;
                    }
                },
                [] {});
            return parent_state;
        }

        [[nodiscard]] bool is_descendant_state(const BaseState *state, const BaseState *ancestor) noexcept
        {
            for (auto *current = state; current != nullptr; current = parent_collection_of(current)) {
                if (current == ancestor) { return true; }
            }
            return false;
        }

        [[nodiscard]] bool find_state_path(const BaseState *state, const TSMeta *schema, const BaseState *target, TSPath &path)
        {
            if (state == nullptr || schema == nullptr) { return false; }
            if (state == target) { return true; }

            switch (schema->kind) {
                case TSKind::TSB:
                    {
                        if (state->storage_kind != TSStorageKind::Native) { return false; }
                        const auto &bundle_state = *static_cast<const TSBState *>(state);
                        for (size_t i = 0; i < bundle_state.child_states.size(); ++i) {
                            const auto &child = bundle_state.child_states[i];
                            if (!child) { continue; }
                            path.push_back(i);
                            if (find_state_path(base_state_of(*child), child_schema_at(*schema, i), target, path)) { return true; }
                            path.pop_back();
                        }
                        return false;
                    }

                case TSKind::TSL:
                    {
                        if (state->storage_kind != TSStorageKind::Native) { return false; }
                        const auto &list_state = *static_cast<const TSLState *>(state);
                        for (size_t i = 0; i < list_state.child_states.size(); ++i) {
                            const auto &child = list_state.child_states[i];
                            if (!child) { continue; }
                            path.push_back(i);
                            if (find_state_path(base_state_of(*child), child_schema_at(*schema, i), target, path)) { return true; }
                            path.pop_back();
                        }
                        return false;
                    }

                case TSKind::TSD:
                    {
                        if (state->storage_kind != TSStorageKind::Native) { return false; }
                        const auto &dict_state = *static_cast<const TSDState *>(state);
                        for (size_t i = 0; i < dict_state.child_states.size(); ++i) {
                            const auto &child = dict_state.child_states[i];
                            if (!child) { continue; }
                            path.push_back(i);
                            if (find_state_path(base_state_of(*child), child_schema_at(*schema, i), target, path)) { return true; }
                            path.pop_back();
                        }
                        return false;
                    }

                default:
                    return false;
            }
        }

        [[nodiscard]] const TSMeta *
        replace_schema_at_path(const TSMeta &source_schema, const TSPath &path, size_t depth, const TSMeta &replacement_schema)
        {
            if (depth == path.size()) { return &replacement_schema; }

            const size_t slot = path[depth];
            auto &registry = TSTypeRegistry::instance();

            switch (source_schema.kind) {
                case TSKind::TSB:
                    {
                        if (slot >= source_schema.field_count()) {
                            throw std::out_of_range("TSOutput alternative replacement path is out of range for TSB");
                        }

                        const TSMeta *updated_child =
                            replace_schema_at_path(*source_schema.fields()[slot].ts_type, path, depth + 1, replacement_schema);
                        if (updated_child == source_schema.fields()[slot].ts_type) { return &source_schema; }

                        std::vector<std::pair<std::string, const TSMeta *>> fields;
                        fields.reserve(source_schema.field_count());
                        for (size_t i = 0; i < source_schema.field_count(); ++i) {
                            fields.emplace_back(
                                source_schema.fields()[i].name,
                                i == slot ? updated_child : source_schema.fields()[i].ts_type);
                        }

                        return registry.tsb(
                            fields,
                            source_schema.bundle_name() != nullptr ? source_schema.bundle_name() : "",
                            source_schema.python_type());
                    }

                case TSKind::TSL:
                    {
                        if (source_schema.fixed_size() == 0) {
                            throw std::invalid_argument("TSOutput alternatives do not yet support dynamic TSL path prefixes");
                        }
                        if (slot >= source_schema.fixed_size()) {
                            throw std::out_of_range("TSOutput alternative replacement path is out of range for TSL");
                        }

                        const TSMeta *updated_child =
                            replace_schema_at_path(*source_schema.element_ts(), path, depth + 1, replacement_schema);
                        return updated_child == source_schema.element_ts() ? &source_schema
                                                                           : registry.tsl(updated_child, source_schema.fixed_size());
                    }

                case TSKind::TSD:
                    {
                        const TSMeta *updated_child =
                            replace_schema_at_path(*source_schema.element_ts(), path, depth + 1, replacement_schema);
                        return updated_child == source_schema.element_ts() ? &source_schema
                                                                           : registry.tsd(source_schema.key_type(), updated_child);
                    }

                default:
                    throw std::invalid_argument("TSOutput alternative replacement paths only support TSB, fixed-size TSL, and TSD");
            }
        }

        [[nodiscard]] TSOutputView traverse_output_path(TSOutputView view, const TSMeta *schema, const TSPath &path)
        {
            const TSMeta *current_schema = schema;
            for (const size_t slot : path) {
                if (current_schema == nullptr) { throw std::invalid_argument("TSOutput alternative traversal requires a schema"); }

                switch (current_schema->kind) {
                    case TSKind::TSB:
                        view = view.as_bundle()[slot];
                        break;

                    case TSKind::TSL:
                        view = view.as_list()[slot];
                        break;

                    case TSKind::TSD:
                        {
                            const auto delta = view.value().as_map().delta();
                            if (slot >= delta.slot_capacity() || !delta.slot_occupied(slot) || delta.slot_removed(slot)) {
                                throw std::out_of_range("TSOutput alternative traversal slot is not live for TSD");
                            }
                            view = view.as_dict().at(delta.key_at_slot(slot));
                            break;
                        }

                    default:
                        throw std::invalid_argument("TSOutput alternative traversal only supports TSB, fixed-size TSL, and TSD");
                }

                current_schema = child_schema_at(*current_schema, slot);
            }

            return view;
        }

    }  // namespace

    namespace detail
    {
        namespace
        {
            struct DefaultTSOutputViewOps final : TSOutputViewOps
            {
                [[nodiscard]] LinkedTSContext linked_context(const TSOutputView &view) const noexcept override
                {
                    const TSViewContext &context = view.context_ref();
                    const TSViewContext resolved = context.resolved();
                    return LinkedTSContext{
                        resolved.schema,
                        resolved.value_dispatch,
                        resolved.ts_dispatch,
                        resolved.value_data,
                        context.ts_state,
                        view.owning_output(),
                        view.output_view_ops(),
                        context.notification_state,
                    };
                }
            };

            struct PendingDictChildViewOps final : TSOutputViewOps
            {
                PendingDictChildViewOps(LinkedTSContext parent_context_, Value key_)
                    : parent_context(std::move(parent_context_)),
                      key(std::move(key_))
                {
                }

                [[nodiscard]] LinkedTSContext linked_context(const TSOutputView &view) const noexcept override
                {
                    static_cast<void>(view);
                    return LinkedTSContext::none();
                }

                bool from_python(const TSOutputView &view, nb::handle value) const override
                {
                    TSOutputView parent_view = output_view_from_context(parent_context, view.evaluation_time());
                    parent_view.as_dict().from_python(key.view(), value);
                    return true;
                }

                LinkedTSContext parent_context;
                Value key;
            };
        }  // namespace

        const TSOutputViewOps &default_output_view_ops() noexcept
        {
            static DefaultTSOutputViewOps ops;
            return ops;
        }

        TSOutputView make_missing_dict_child_output_view(const TSOutputView &view, const View &key)
        {
            const TSMeta *schema = view.ts_schema();
            const TSMeta *child_schema = schema != nullptr ? schema->element_ts() : nullptr;
            if (child_schema == nullptr) {
                return view.make_child_view_impl(TSViewContext::none(), view.context_ref(), view.evaluation_time());
            }

            TSViewContext child_context;
            child_context.schema = child_schema;

            auto ops = std::make_shared<PendingDictChildViewOps>(view.linked_context(), key.clone());
            child_context.output_view_ops = ops.get();
            return view.make_child_view_impl(child_context, view.context_ref(), view.evaluation_time(), std::move(ops));
        }
    }  // namespace detail

    struct TSOutput::AlternativeOutput final : TSValue, Notifiable
    {
        struct ViewOps final : detail::TSOutputViewOps
        {
            explicit ViewOps(AlternativeOutput *owner_) noexcept
                : owner(owner_)
            {
            }

            [[nodiscard]] LinkedTSContext linked_context(const TSOutputView &view) const noexcept override
            {
                const TSViewContext &context = view.context_ref();
                const TSViewContext resolved = context.resolved();
                if (owner != nullptr) {
                    for (const auto &binding : owner->m_collection_ref_bindings) {
                        if (binding != nullptr && binding->target_context.ts_state == context.ts_state &&
                            binding->source_context.ts_state != nullptr) {
                            return LinkedTSContext{
                                resolved.schema,
                                resolved.value_dispatch,
                                resolved.ts_dispatch,
                                resolved.value_data,
                                context.ts_state,
                                view.owning_output(),
                                view.output_view_ops(),
                                notification_state_of(binding->source_context),
                            };
                        }
                    }
                }
                return LinkedTSContext{
                    resolved.schema,
                    resolved.value_dispatch,
                    resolved.ts_dispatch,
                    resolved.value_data,
                    context.ts_state,
                    view.owning_output(),
                    view.output_view_ops(),
                    context.notification_state,
                };
            }

            AlternativeOutput *owner{nullptr};
        };

        struct WrappedRefNotifier final : Notifiable
        {
            explicit WrappedRefNotifier(BaseState *target_state_) noexcept
                : target_state(target_state_)
            {
            }

            void notify(engine_time_t modified_time) override
            {
                if (target_state != nullptr) { target_state->mark_modified(modified_time); }
            }

            BaseState *target_state{nullptr};
        };

        AlternativeOutput(const TSOutputView &source, const TSMeta &schema)
            : TSValue(schema)
        {
            const TSMeta *source_schema = source.ts_schema();
            if (source_schema == nullptr || !supports_alternative_cast(*source_schema, schema)) {
                throw std::invalid_argument("TSOutput alternative does not support the requested wrap cast");
            }

            configure_branch(view(nullptr), source);
        }

            ~AlternativeOutput() override
        {
            for (auto &binding : m_dynamic_dict_bindings) {
                if (binding != nullptr) {
                    if (BaseState *source_state = notification_state_of(binding->source_context); source_state != nullptr) {
                        source_state->unsubscribe(&binding->source_notifier);
                    }
                }
            }
            for (auto &binding : m_scalar_value_bindings) {
                if (binding != nullptr) {
                    if (BaseState *source_state = notification_state_of(binding->source_context); source_state != nullptr) {
                        source_state->unsubscribe(&binding->source_notifier);
                    }
                }
            }
            for (auto &binding : m_dynamic_key_set_bindings) {
                if (binding != nullptr) {
                    if (BaseState *source_state = notification_state_of(binding->source_context); source_state != nullptr) {
                        source_state->unsubscribe(&binding->source_notifier);
                    }
                }
            }
            for (auto &binding : m_collection_ref_bindings) {
                if (binding != nullptr) {
                    if (BaseState *source_state = notification_state_of(binding->source_context); source_state != nullptr) {
                        source_state->unsubscribe(&binding->source_notifier);
                    }
                    for (BaseState *target_state : binding->target_subscription_states) {
                        if (target_state != nullptr) { target_state->unsubscribe(&binding->target_notifier); }
                    }
                    binding->target_subscription_states.clear();
                }
            }
            for (auto &subscription : m_wrapped_ref_subscriptions) {
                if (subscription.source_state != nullptr && subscription.notifier) {
                    subscription.source_state->unsubscribe(subscription.notifier.get());
                }
            }
        }

        AlternativeOutput(const AlternativeOutput &) = delete;
        AlternativeOutput &operator=(const AlternativeOutput &) = delete;
        AlternativeOutput(AlternativeOutput &&) = delete;
        AlternativeOutput &operator=(AlternativeOutput &&) = delete;

        [[nodiscard]] TSOutputView view(TSOutput *owning_output, engine_time_t evaluation_time = MIN_DT)
        {
            TSViewContext context = view_context();
            return TSOutputView{context, TSViewContext::none(), evaluation_time, owning_output, &m_view_ops};
        }

        [[nodiscard]] TSOutputView bindable_view(TSOutput *owning_output, const TSOutputView &source, const TSMeta *schema)
        {
            TSOutputView root_source = view(owning_output, source.evaluation_time());
            TSPath source_path;
            BaseState *target_state = source.context_ref().ts_state;
            if (target_state != nullptr &&
                !find_state_path(root_source.context_ref().ts_state, root_source.ts_schema(), target_state, source_path)) {
                throw std::logic_error("TSOutput alternative source view is not reachable from the alternative root");
            }

            const TSMeta *root_source_schema = root_source.ts_schema();
            if (root_source_schema == nullptr) {
                throw std::logic_error("TSOutput alternative bindable view requires a rooted source schema");
            }

            const TSMeta *alternative_schema = replace_schema_at_path(*root_source_schema, source_path, 0, *schema);
            const auto [it, inserted] = m_alternatives.try_emplace(alternative_schema, nullptr);
            if (inserted) { it->second.reset(new AlternativeOutput(root_source, *alternative_schema)); }

            TSOutputView alternative_view = it->second->view(owning_output, source.evaluation_time());
            return traverse_output_path(alternative_view, alternative_schema, source_path);
        }

        void notify(engine_time_t modified_time) override
        {
            if (m_dynamic_dict_bindings.empty() && m_dynamic_key_set_bindings.empty() && m_collection_ref_bindings.empty()) {
                if (BaseState *root = ts_root_state(); root != nullptr) { root->mark_modified(modified_time); }
            }
        }

      private:
        struct WrappedRefSubscription
        {
            BaseState *source_state{nullptr};
            BaseState *target_state{nullptr};
            std::unique_ptr<WrappedRefNotifier> notifier;
        };

        struct ScalarValueBinding
        {
            struct SourceNotifier final : Notifiable
            {
                void notify(engine_time_t modified_time) override
                {
                    if (binding != nullptr && binding->owner != nullptr) {
                        binding->owner->sync_scalar_value(*binding, modified_time, false);
                    }
                }

                ScalarValueBinding *binding{nullptr};
            };

            ScalarValueBinding(AlternativeOutput *owner_, const TSOutputView &target_view, const TSOutputView &source_view) noexcept
                : owner(owner_),
                  target_context(target_view.context_ref()),
                  source_context(source_view.linked_context())
            {
                source_notifier.binding = this;
            }

            AlternativeOutput *owner{nullptr};
            TSViewContext target_context{TSViewContext::none()};
            LinkedTSContext source_context{};
            SourceNotifier source_notifier{};
        };

        struct DynamicDictBinding
        {
            struct SourceNotifier final : Notifiable
            {
                void notify(engine_time_t modified_time) override
                {
                    if (binding != nullptr && binding->owner != nullptr) {
                        binding->owner->sync_dynamic_dict(*binding, modified_time, false);
                    }
                }

                DynamicDictBinding *binding{nullptr};
            };

            // One binding records "this alternative TSD subtree mirrors that
            // source TSD subtree". The stored target context stays rooted at
            // the alternative dict boundary, while source_context identifies
            // the live producer-side dict we replay from.
            DynamicDictBinding(AlternativeOutput *owner_, const TSOutputView &target_view, const TSOutputView &source_view) noexcept
                : owner(owner_),
                  target_context(target_view.context_ref()),
                  source_context(source_view.linked_context())
            {
                source_notifier.binding = this;
                set_last_source_snapshot(
                    source_context.ts_state != nullptr ? source_context.ts_state->resolved_state() : nullptr,
                    false);
            }

            DynamicDictBinding(AlternativeOutput *owner_,
                               const TSOutputView &target_view,
                               std::unique_ptr<TimeSeriesStateV> source_bridge_state_,
                               LinkedTSContext source_context_) noexcept
                : owner(owner_),
                  target_context(target_view.context_ref()),
                  source_context(std::move(source_context_)),
                  source_bridge_state(std::move(source_bridge_state_))
            {
                source_notifier.binding = this;
                set_last_source_snapshot(
                    source_context.ts_state != nullptr ? source_context.ts_state->resolved_state() : nullptr,
                    false);
            }

            [[nodiscard]] const BaseState *last_source_root_state() const noexcept { return m_last_source_snapshot.ptr(); }
            [[nodiscard]] bool last_source_had_value() const noexcept { return m_last_source_snapshot.tag() != 0; }
            void set_last_source_snapshot(const BaseState *state, bool had_value) noexcept
            {
                m_last_source_snapshot.set(state, had_value ? 1u : 0u);
            }

            AlternativeOutput *owner{nullptr};
            TSViewContext target_context{TSViewContext::none()};
            LinkedTSContext source_context{};
            std::unique_ptr<TimeSeriesStateV> source_bridge_state{};
            tagged_ptr<const BaseState, 1> m_last_source_snapshot{};
            SourceNotifier source_notifier{};
        };

        struct DynamicKeySetBinding
        {
            struct SourceNotifier final : Notifiable
            {
                void notify(engine_time_t modified_time) override
                {
                    if (binding != nullptr && binding->owner != nullptr) {
                        binding->owner->sync_dynamic_key_set(*binding, modified_time, false);
                    }
                }

                DynamicKeySetBinding *binding{nullptr};
            };

            DynamicKeySetBinding(AlternativeOutput *owner_, const TSOutputView &target_view, const TSOutputView &source_view) noexcept
                : owner(owner_),
                  target_context(target_view.context_ref()),
                  source_context(source_view.linked_context())
            {
                source_notifier.binding = this;
                set_last_source_snapshot(
                    source_context.ts_state != nullptr ? source_context.ts_state->resolved_state() : nullptr,
                    false);
            }

            DynamicKeySetBinding(AlternativeOutput *owner_,
                                 const TSOutputView &target_view,
                                 std::unique_ptr<TimeSeriesStateV> source_bridge_state_,
                                 LinkedTSContext source_context_) noexcept
                : owner(owner_),
                  target_context(target_view.context_ref()),
                  source_context(std::move(source_context_)),
                  source_bridge_state(std::move(source_bridge_state_))
            {
                source_notifier.binding = this;
                set_last_source_snapshot(
                    source_context.ts_state != nullptr ? source_context.ts_state->resolved_state() : nullptr,
                    false);
            }

            [[nodiscard]] const BaseState *last_source_root_state() const noexcept { return m_last_source_snapshot.ptr(); }
            [[nodiscard]] bool last_source_had_value() const noexcept { return m_last_source_snapshot.tag() != 0; }
            void set_last_source_snapshot(const BaseState *state, bool had_value) noexcept
            {
                m_last_source_snapshot.set(state, had_value ? 1u : 0u);
            }

            AlternativeOutput *owner{nullptr};
            TSViewContext target_context{TSViewContext::none()};
            LinkedTSContext source_context{};
            std::unique_ptr<TimeSeriesStateV> source_bridge_state{};
            tagged_ptr<const BaseState, 1> m_last_source_snapshot{};
            SourceNotifier source_notifier{};
        };

        struct CollectionRefBinding
        {
            struct SourceNotifier final : Notifiable
            {
                void notify(engine_time_t modified_time) override
                {
                    if (binding != nullptr && binding->owner != nullptr) {
                        binding->owner->sync_collection_ref(*binding, modified_time, false);
                    }
                }

                CollectionRefBinding *binding{nullptr};
            };

            struct TargetNotifier final : Notifiable
            {
                void notify(engine_time_t modified_time) override
                {
                    if (binding != nullptr && binding->owner != nullptr) {
                        binding->owner->sync_collection_ref(*binding, modified_time, false);
                    }
                }

                CollectionRefBinding *binding{nullptr};
            };

            CollectionRefBinding(AlternativeOutput *owner_, const TSOutputView &target_view, const TSOutputView &source_view) noexcept
                : owner(owner_),
                  target_context(target_view.context_ref()),
                  source_context(source_view.linked_context())
            {
                source_notifier.binding = this;
                target_notifier.binding = this;
            }

            AlternativeOutput *owner{nullptr};
            TSViewContext target_context{TSViewContext::none()};
            LinkedTSContext source_context{};
            std::unordered_set<BaseState *> target_subscription_states{};
            SourceNotifier source_notifier{};
            TargetNotifier target_notifier{};
        };

        [[nodiscard]] std::unique_ptr<DynamicDictBinding>
        make_dereferenced_dynamic_dict_binding(const TSOutputView &target_view, const TSOutputView &ref_source_view)
        {
            const TSMeta *source_schema = ref_source_view.ts_schema();
            if (source_schema == nullptr || source_schema->kind != TSKind::REF || source_schema->element_ts() == nullptr ||
                source_schema->element_ts()->kind != TSKind::TSD) {
                throw std::invalid_argument("TSOutput dereferenced dynamic dict binding requires REF[TSD[...]] source");
            }

            auto source_bridge_state = std::make_unique<TimeSeriesStateV>();
            auto &bridge_state = source_bridge_state->template emplace<RefLinkState>();
            initialize_ref_link_state(bridge_state, TimeSeriesStateParentPtr{}, 0);
            bridge_state.retain_transition_value = false;
            bridge_state.set_source(ref_source_view.linked_context());

            LinkedTSContext source_context{
                source_schema->element_ts(),
                nullptr,
                nullptr,
                nullptr,
                base_state_of(*source_bridge_state),
            };

            return std::make_unique<DynamicDictBinding>(
                this,
                target_view,
                std::move(source_bridge_state),
                std::move(source_context));
        }

        [[nodiscard]] std::unique_ptr<DynamicKeySetBinding>
        make_dereferenced_dynamic_key_set_binding(const TSOutputView &target_view, const TSOutputView &ref_source_view)
        {
            const TSMeta *source_schema = ref_source_view.ts_schema();
            if (source_schema == nullptr || source_schema->kind != TSKind::REF || source_schema->element_ts() == nullptr ||
                source_schema->element_ts()->kind != TSKind::TSD) {
                throw std::invalid_argument("TSOutput dereferenced key-set binding requires REF[TSD[...]] source");
            }

            auto source_bridge_state = std::make_unique<TimeSeriesStateV>();
            auto &bridge_state = source_bridge_state->template emplace<RefLinkState>();
            initialize_ref_link_state(bridge_state, TimeSeriesStateParentPtr{}, 0);
            bridge_state.retain_transition_value = false;
            bridge_state.set_source(ref_source_view.linked_context());

            LinkedTSContext source_context{
                source_schema->element_ts(),
                nullptr,
                nullptr,
                nullptr,
                base_state_of(*source_bridge_state),
            };

            return std::make_unique<DynamicKeySetBinding>(
                this,
                target_view,
                std::move(source_bridge_state),
                std::move(source_context));
        }

        [[nodiscard]] TSOutputView output_view_for(const TSViewContext &context, engine_time_t evaluation_time = MIN_DT) const
        {
            return TSOutputView{context, TSViewContext::none(), evaluation_time, nullptr, &detail::default_output_view_ops()};
        }

        [[nodiscard]] TSOutputView output_view_for(const LinkedTSContext &context, engine_time_t evaluation_time = MIN_DT) const
        {
            return TSOutputView{
                TSViewContext{context.schema, context.value_dispatch, context.ts_dispatch, context.value_data, context.ts_state},
                TSViewContext::none(),
                evaluation_time,
                context.owning_output,
                context.output_view_ops != nullptr ? context.output_view_ops : &detail::default_output_view_ops(),
            };
        }

        void add_dynamic_dict_binding(std::unique_ptr<DynamicDictBinding> binding, engine_time_t initial_modified_time)
        {
            auto &binding_ref = *binding;
            auto unsubscribe_on_failure = make_scope_exit([&] {
                if (BaseState *source_state = notification_state_of(binding_ref.source_context); source_state != nullptr) {
                    source_state->unsubscribe(&binding_ref.source_notifier);
                }
            });
            if (BaseState *source_state = notification_state_of(binding_ref.source_context); source_state != nullptr) {
                source_state->subscribe(&binding_ref.source_notifier);
            }
            sync_dynamic_dict(binding_ref, initial_modified_time, true);
            unsubscribe_on_failure.release();
            m_dynamic_dict_bindings.push_back(std::move(binding));
        }

        void add_scalar_value_binding(std::unique_ptr<ScalarValueBinding> binding, engine_time_t initial_modified_time)
        {
            auto &binding_ref = *binding;
            auto unsubscribe_on_failure = make_scope_exit([&] {
                if (BaseState *source_state = notification_state_of(binding_ref.source_context); source_state != nullptr) {
                    source_state->unsubscribe(&binding_ref.source_notifier);
                }
            });
            if (BaseState *source_state = notification_state_of(binding_ref.source_context); source_state != nullptr) {
                source_state->subscribe(&binding_ref.source_notifier);
            }
            sync_scalar_value(binding_ref, initial_modified_time, true);
            unsubscribe_on_failure.release();
            m_scalar_value_bindings.push_back(std::move(binding));
        }

        void add_dynamic_key_set_binding(std::unique_ptr<DynamicKeySetBinding> binding, engine_time_t initial_modified_time)
        {
            auto &binding_ref = *binding;
            auto unsubscribe_on_failure = make_scope_exit([&] {
                if (BaseState *source_state = notification_state_of(binding_ref.source_context); source_state != nullptr) {
                    source_state->unsubscribe(&binding_ref.source_notifier);
                }
            });
            if (BaseState *source_state = notification_state_of(binding_ref.source_context); source_state != nullptr) {
                source_state->subscribe(&binding_ref.source_notifier);
            }
            sync_dynamic_key_set(binding_ref, initial_modified_time, true);
            unsubscribe_on_failure.release();
            m_dynamic_key_set_bindings.push_back(std::move(binding));
        }

        void add_collection_ref_binding(std::unique_ptr<CollectionRefBinding> binding, engine_time_t initial_modified_time)
        {
            auto &binding_ref = *binding;
            auto unsubscribe_on_failure = make_scope_exit([&] {
                if (BaseState *source_state = notification_state_of(binding_ref.source_context); source_state != nullptr) {
                    source_state->unsubscribe(&binding_ref.source_notifier);
                }
                for (BaseState *target_state : binding_ref.target_subscription_states) {
                    if (target_state != nullptr) { target_state->unsubscribe(&binding_ref.target_notifier); }
                }
                binding_ref.target_subscription_states.clear();
            });
            if (BaseState *source_state = notification_state_of(binding_ref.source_context); source_state != nullptr) {
                source_state->subscribe(&binding_ref.source_notifier);
            }
            sync_collection_ref(binding_ref, initial_modified_time, true);
            unsubscribe_on_failure.release();
            m_collection_ref_bindings.push_back(std::move(binding));
        }

        void release_wrapped_ref_subscriptions(const BaseState *subtree_root)
        {
            if (subtree_root == nullptr) { return; }

            // Dynamic TSD replay can replace an entire child subtree when a
            // key is removed or a stable slot is reused for a different key.
            // Any wrap subscriptions owned by that subtree must disappear with
            // it or the old source branch would keep driving stale state.
            auto it = m_wrapped_ref_subscriptions.begin();
            while (it != m_wrapped_ref_subscriptions.end()) {
                if (it->target_state != nullptr && is_descendant_state(it->target_state, subtree_root)) {
                    if (it->source_state != nullptr && it->notifier) {
                        it->source_state->unsubscribe(it->notifier.get());
                    }
                    it = m_wrapped_ref_subscriptions.erase(it);
                } else {
                    ++it;
                }
            }
        }

        void release_scalar_value_bindings(const BaseState *subtree_root)
        {
            if (subtree_root == nullptr) { return; }

            for (auto it = m_scalar_value_bindings.begin(); it != m_scalar_value_bindings.end();) {
                if ((*it)->target_context.ts_state != nullptr && is_descendant_state((*it)->target_context.ts_state, subtree_root)) {
                    if (BaseState *source_state = notification_state_of((*it)->source_context); source_state != nullptr) {
                        source_state->unsubscribe(&(*it)->source_notifier);
                    }
                    it = m_scalar_value_bindings.erase(it);
                } else {
                    ++it;
                }
            }
        }

        void release_dynamic_dict_bindings(const BaseState *subtree_root)
        {
            if (subtree_root == nullptr) { return; }

            for (auto it = m_dynamic_dict_bindings.begin(); it != m_dynamic_dict_bindings.end();) {
                if ((*it)->target_context.ts_state != nullptr && is_descendant_state((*it)->target_context.ts_state, subtree_root)) {
                    if (BaseState *source_state = notification_state_of((*it)->source_context); source_state != nullptr) {
                        source_state->unsubscribe(&(*it)->source_notifier);
                    }
                    it = m_dynamic_dict_bindings.erase(it);
                } else {
                    ++it;
                }
            }
        }

        void release_dynamic_key_set_bindings(const BaseState *subtree_root)
        {
            if (subtree_root == nullptr) { return; }

            for (auto it = m_dynamic_key_set_bindings.begin(); it != m_dynamic_key_set_bindings.end();) {
                if ((*it)->target_context.ts_state != nullptr && is_descendant_state((*it)->target_context.ts_state, subtree_root)) {
                    if (BaseState *source_state = notification_state_of((*it)->source_context); source_state != nullptr) {
                        source_state->unsubscribe(&(*it)->source_notifier);
                    }
                    it = m_dynamic_key_set_bindings.erase(it);
                } else {
                    ++it;
                }
            }
        }

        void release_collection_ref_bindings(const BaseState *subtree_root)
        {
            if (subtree_root == nullptr) { return; }

            for (auto it = m_collection_ref_bindings.begin(); it != m_collection_ref_bindings.end();) {
                if ((*it)->target_context.ts_state != nullptr && is_descendant_state((*it)->target_context.ts_state, subtree_root)) {
                    if (BaseState *source_state = notification_state_of((*it)->source_context); source_state != nullptr) {
                        source_state->unsubscribe(&(*it)->source_notifier);
                    }
                    for (BaseState *target_state : (*it)->target_subscription_states) {
                        if (target_state != nullptr) { target_state->unsubscribe(&(*it)->target_notifier); }
                    }
                    (*it)->target_subscription_states.clear();
                    it = m_collection_ref_bindings.erase(it);
                } else {
                    ++it;
                }
            }
        }

        void clear_dynamic_child_slot(TSDState &state, size_t slot)
        {
            if (slot >= state.child_states.size() || state.child_states[slot] == nullptr) { return; }

            // The stable slot is being vacated from the alternative map. Tear
            // down every owned subtree artifact first, then drop the child
            // state so the slot can be rebuilt from the value-layer insert
            // event if a key later reuses it.
            BaseState *child_root = base_state_of(*state.child_states[slot]);
            release_wrapped_ref_subscriptions(child_root);
            release_scalar_value_bindings(child_root);
            release_dynamic_dict_bindings(child_root);
            release_dynamic_key_set_bindings(child_root);
            release_collection_ref_bindings(child_root);
            state.on_erase(slot);
        }

        template <typename TCollectionState>
        void reset_fixed_child_slot(TCollectionState &state, size_t slot, const TSMeta &child_schema)
        {
            if (slot >= state.child_states.size()) {
                throw std::out_of_range("TSOutput collection ref binding slot is out of range");
            }

            if (state.child_states[slot] != nullptr) {
                BaseState *child_root = base_state_of(*state.child_states[slot]);
                release_wrapped_ref_subscriptions(child_root);
                release_scalar_value_bindings(child_root);
                release_dynamic_dict_bindings(child_root);
                release_dynamic_key_set_bindings(child_root);
                release_collection_ref_bindings(child_root);
            }

            state.child_states[slot] = make_time_series_state_node(child_schema, parent_ptr(state), slot);
        }

        template <typename TCollectionState, typename TCollectionView, typename TChildSchemaFn>
        bool sync_fixed_collection_root_value(TCollectionState &target_state,
                                              const TSOutputView &target_view,
                                              const TCollectionView &target_collection,
                                              engine_time_t modified_time,
                                              size_t slot_count,
                                              TChildSchemaFn &&child_schema_for_slot)
        {
            const bool new_tick = target_state.last_modified_time != modified_time;
            if (new_tick) { target_state.modified_children.clear(); }

            std::vector<size_t> changed_slots;
            changed_slots.reserve(slot_count);

            if constexpr (std::same_as<TCollectionState, TSBState>) {
                auto mutation = target_view.value().as_bundle().begin_mutation();
                auto current = target_view.value().as_bundle();
                for (size_t slot = 0; slot < slot_count; ++slot) {
                    const TSMeta *child_schema = child_schema_for_slot(slot);
                    if (child_schema == nullptr || child_schema->value_type == nullptr) {
                        throw std::logic_error("TSOutput collection ref binding requires target child value schemas");
                    }

                    const TSOutputView child = target_collection[slot];
                    const View desired = child.valid() ? child.value() : View::invalid_for(child_schema->value_type);
                    const View existing = current.at(slot);
                    if (desired == existing) { continue; }

                    mutation.set(slot, desired);
                    changed_slots.push_back(slot);
                }
            } else if constexpr (std::same_as<TCollectionState, TSLState>) {
                auto mutation = target_view.value().as_list().begin_mutation();
                auto current = target_view.value().as_list();
                for (size_t slot = 0; slot < slot_count; ++slot) {
                    const TSMeta *child_schema = child_schema_for_slot(slot);
                    if (child_schema == nullptr || child_schema->value_type == nullptr) {
                        throw std::logic_error("TSOutput collection ref binding requires target child value schemas");
                    }

                    const TSOutputView child = target_collection[slot];
                    const View desired = child.valid() ? child.value() : View::invalid_for(child_schema->value_type);
                    const View existing = current.at(slot);
                    if (desired == existing) { continue; }

                    mutation.set(slot, desired);
                    changed_slots.push_back(slot);
                }
            }

            bool child_published = false;
            for (const size_t slot : changed_slots) {
                BaseState *child_state =
                    slot < target_state.child_states.size() && target_state.child_states[slot] != nullptr
                        ? base_state_of(*target_state.child_states[slot])
                        : nullptr;

                if (child_state != nullptr && child_state->last_modified_time != modified_time) {
                    child_state->mark_modified(modified_time);
                    child_published = true;
                } else {
                    target_state.modified_children.insert(slot);
                }
            }

            if (!changed_slots.empty() && !child_published) {
                if (new_tick) {
                    target_state.mark_modified(modified_time);
                } else {
                    target_state.mark_modified_local(modified_time);
                }
            }
            return !changed_slots.empty();
        }

        void sync_collection_ref_from_output(const TSOutputView &target_view,
                                             const TSOutputView &source_view,
                                             engine_time_t       modified_time)
        {
            const TSMeta *target_schema = target_view.ts_schema();
            const TSMeta *source_schema = source_view.ts_schema();
            if (target_schema == nullptr || source_schema == nullptr || target_schema->kind != source_schema->kind) {
                throw std::invalid_argument("TSOutput collection ref binding requires matching source and target schemas");
            }

            switch (target_schema->kind) {
                case TSKind::TSB:
                    {
                        auto &target_state = *static_cast<TSBState *>(target_view.context_ref().ts_state);
                        auto target_bundle = target_view.as_bundle();
                        auto source_bundle = source_view.as_bundle();
                        for (size_t index = 0; index < target_schema->field_count(); ++index) {
                            const TSMeta *child_schema = target_schema->fields()[index].ts_type;
                            if (child_schema == nullptr) {
                                throw std::logic_error("TSOutput collection ref binding requires target child schemas");
                            }

                            reset_fixed_child_slot(target_state, index, *child_schema);
                            TSOutputView target_child = target_bundle[index];
                            TSOutputView source_child = source_bundle[index];
                            if (source_child.ts_schema() == child_schema) {
                                install_target_link(target_state, index, source_child.linked_context());
                            } else {
                                configure_branch(target_child, source_child);
                            }
                        }
                        static_cast<void>(sync_fixed_collection_root_value(
                            target_state,
                            target_view,
                            target_bundle,
                            modified_time,
                            target_schema->field_count(),
                            [&](size_t slot) { return target_schema->fields()[slot].ts_type; }));
                        return;
                    }

                case TSKind::TSL:
                    {
                        auto &target_state = *static_cast<TSLState *>(target_view.context_ref().ts_state);
                        auto target_list = target_view.as_list();
                        auto source_list = source_view.as_list();
                        for (size_t index = 0; index < target_schema->fixed_size(); ++index) {
                            const TSMeta *child_schema = target_schema->element_ts();
                            if (child_schema == nullptr) {
                                throw std::logic_error("TSOutput collection ref binding requires a target element schema");
                            }

                            reset_fixed_child_slot(target_state, index, *child_schema);
                            TSOutputView target_child = target_list[index];
                            TSOutputView source_child = source_list[index];
                            if (source_child.ts_schema() == child_schema) {
                                install_target_link(target_state, index, source_child.linked_context());
                            } else {
                                configure_branch(target_child, source_child);
                            }
                        }
                        static_cast<void>(sync_fixed_collection_root_value(
                            target_state,
                            target_view,
                            target_list,
                            modified_time,
                            target_schema->fixed_size(),
                            [&](size_t) { return target_schema->element_ts(); }));
                        return;
                    }

                default:
                    throw std::invalid_argument("TSOutput collection ref binding only supports TSB and fixed-size TSL");
            }
        }

        void sync_collection_ref_from_value(const TSOutputView &target_view,
                                            const TimeSeriesReference &ref,
                                            engine_time_t modified_time)
        {
            const TSMeta *target_schema = target_view.ts_schema();
            if (target_schema == nullptr) {
                throw std::invalid_argument("TSOutput collection ref binding requires a target schema");
            }

            switch (ref.kind()) {
                case TimeSeriesReference::Kind::EMPTY:
                    {
                        switch (target_schema->kind) {
                            case TSKind::TSB:
                                {
                                    auto &target_state = *static_cast<TSBState *>(target_view.context_ref().ts_state);
                                    if (target_state.last_modified_time != modified_time) { target_state.modified_children.clear(); }
                                    for (size_t index = 0; index < target_schema->field_count(); ++index) {
                                        const TSMeta *child_schema = target_schema->fields()[index].ts_type;
                                        if (child_schema != nullptr) { reset_fixed_child_slot(target_state, index, *child_schema); }
                                    }
                                    break;
                                }

                            case TSKind::TSL:
                                {
                                    auto &target_state = *static_cast<TSLState *>(target_view.context_ref().ts_state);
                                    if (target_state.last_modified_time != modified_time) { target_state.modified_children.clear(); }
                                    for (size_t index = 0; index < target_schema->fixed_size(); ++index) {
                                        const TSMeta *child_schema = target_schema->element_ts();
                                        if (child_schema != nullptr) { reset_fixed_child_slot(target_state, index, *child_schema); }
                                    }
                                    break;
                                }

                            default:
                                throw std::invalid_argument("TSOutput collection ref binding only supports TSB and fixed-size TSL");
                        }
                        static_cast<void>(modified_time);
                        return;
                    }

                case TimeSeriesReference::Kind::PEERED:
                    sync_collection_ref_from_output(target_view, ref.target_view(modified_time), modified_time);
                    return;

                case TimeSeriesReference::Kind::NON_PEERED:
                    {
                        switch (target_schema->kind) {
                            case TSKind::TSB:
                                {
                                    auto &target_state = *static_cast<TSBState *>(target_view.context_ref().ts_state);
                                    auto target_bundle = target_view.as_bundle();
                                    for (size_t index = 0; index < target_schema->field_count(); ++index) {
                                        const TSMeta *child_schema = target_schema->fields()[index].ts_type;
                                        if (child_schema == nullptr) {
                                            throw std::logic_error("TSOutput collection ref binding requires target child schemas");
                                        }

                                        reset_fixed_child_slot(target_state, index, *child_schema);
                                        if (index >= ref.items().size()) { continue; }

                                        const auto &item = ref.items()[index];
                                        if (item.is_empty()) { continue; }
                                        if (item.is_peered()) {
                                            install_target_link(target_state, index, item.target());
                                        } else {
                                            sync_collection_ref_from_value(target_bundle[index], item, modified_time);
                                        }
                                    }
                                    static_cast<void>(sync_fixed_collection_root_value(
                                        target_state,
                                        target_view,
                                        target_bundle,
                                        modified_time,
                                        target_schema->field_count(),
                                        [&](size_t slot) { return target_schema->fields()[slot].ts_type; }));
                                    return;
                                }

                            case TSKind::TSL:
                                {
                                    auto &target_state = *static_cast<TSLState *>(target_view.context_ref().ts_state);
                                    auto target_list = target_view.as_list();
                                    for (size_t index = 0; index < target_schema->fixed_size(); ++index) {
                                        const TSMeta *child_schema = target_schema->element_ts();
                                        if (child_schema == nullptr) {
                                            throw std::logic_error("TSOutput collection ref binding requires a target element schema");
                                        }

                                        reset_fixed_child_slot(target_state, index, *child_schema);
                                        if (index >= ref.items().size()) { continue; }

                                        const auto &item = ref.items()[index];
                                        if (item.is_empty()) { continue; }
                                        if (item.is_peered()) {
                                            install_target_link(target_state, index, item.target());
                                        } else {
                                            sync_collection_ref_from_value(target_list[index], item, modified_time);
                                        }
                                    }
                                    static_cast<void>(sync_fixed_collection_root_value(
                                        target_state,
                                        target_view,
                                        target_list,
                                        modified_time,
                                        target_schema->fixed_size(),
                                        [&](size_t) { return target_schema->element_ts(); }));
                                    return;
                                }

                            default:
                                throw std::invalid_argument("TSOutput collection ref binding only supports TSB and fixed-size TSL");
                        }
                    }
            }
        }

        void update_collection_ref_target_subscription(CollectionRefBinding &binding,
                                                       const TimeSeriesReference &ref) noexcept
        {
            std::unordered_set<BaseState *> next_target_states;
            collect_ref_target_states(ref, next_target_states);
            if (binding.target_subscription_states == next_target_states) { return; }

            for (BaseState *target_state : binding.target_subscription_states) {
                if (target_state != nullptr && !next_target_states.contains(target_state)) {
                    target_state->unsubscribe(&binding.target_notifier);
                }
            }
            for (BaseState *target_state : next_target_states) {
                if (target_state != nullptr && !binding.target_subscription_states.contains(target_state)) {
                    target_state->subscribe(&binding.target_notifier);
                }
            }
            binding.target_subscription_states = std::move(next_target_states);
        }

        void sync_scalar_value(ScalarValueBinding &binding, engine_time_t modified_time, bool initializing)
        {
            TSOutputView target_view = output_view_for(binding.target_context, modified_time);
            TSOutputView source_view = output_view_for(binding.source_context, modified_time);
            BaseState *target_state = target_view.context_ref().ts_state;
            if (target_state == nullptr) {
                throw std::logic_error("TSOutput scalar cast binding requires a live target TS state");
            }

            if (!source_view.valid()) {
                if (initializing) { target_state->last_modified_time = MIN_DT; }
                return;
            }

            target_view.value().from_python(source_view.value().to_python());
            if (initializing) {
                target_state->last_modified_time = source_view.last_modified_time();
            } else {
                target_state->mark_modified(modified_time);
            }
        }

        void sync_collection_ref(CollectionRefBinding &binding, engine_time_t modified_time, bool initializing)
        {
            TSOutputView target_view = output_view_for(binding.target_context, modified_time);
            TSOutputView source_view = output_view_for(binding.source_context, modified_time);
            const auto *ref = source_view.value().as_atomic().try_as<TimeSeriesReference>();
            if (ref == nullptr) {
                throw std::logic_error("TSOutput collection ref binding requires a REF source value");
            }

            update_collection_ref_target_subscription(binding, *ref);
            sync_collection_ref_from_value(target_view, *ref, modified_time);
            if (initializing) {
                if (BaseState *root = target_view.context_ref().ts_state; root != nullptr && root->last_modified_time == MIN_DT) {
                    root->last_modified_time = modified_time;
                }
            }
        }

        void install_ref_link_for_target(const TSOutputView &target_view, const TSOutputView &source_view)
        {
            BaseState *target_state = target_view.linked_context().ts_state;
            if (target_state == nullptr) {
                throw std::logic_error("TSOutput alternative REF dereference requires a live target state");
            }

            const LinkedTSContext ref_source = source_view.linked_context();
            bool replaced = false;
            hgraph::visit(
                target_state->parent,
                [&](auto *parent_state) {
                    using T = std::remove_pointer_t<decltype(parent_state)>;

                    if constexpr (std::same_as<T, TSLState> || std::same_as<T, TSBState> || std::same_as<T, TSDState>) {
                        if (parent_state == nullptr) {
                            throw std::logic_error("TSOutput alternative REF dereference requires a live parent collection");
                        }
                        install_ref_link(*parent_state, target_state->index, ref_source);
                        replaced = true;
                    }
                },
                [&] {
                    // Root-level REF -> TS casts replace the alternative's
                    // root state node itself rather than a collection child.
                    auto &root_state = state_variant();
                    auto &typed_state = root_state.template emplace<RefLinkState>();
                    initialize_ref_link_state(typed_state, TimeSeriesStateParentPtr{}, 0);
                    typed_state.set_source(ref_source);
                    replaced = true;
                });

            if (!replaced) {
                throw std::logic_error("TSOutput alternative REF dereference could not replace the target state");
            }
        }

        void install_signal_subscription(const TSOutputView &target_view, const TSOutputView &source_view)
        {
            BaseState *target_state = target_view.linked_context().ts_state;
            BaseState *source_state = notification_state_of(source_view.linked_context());
            if (target_state == nullptr) {
                throw std::logic_error("TSOutput alternative SIGNAL cast requires a live target state");
            }

            // SIGNAL semantics only care that the source ticked; the carried
            // value stays pinned to `True` for Python-facing reads.
            target_view.value().as_atomic().set(true);

            auto notifier = std::make_unique<WrappedRefNotifier>(target_state);
            if (source_state != nullptr) { source_state->subscribe(notifier.get()); }
            m_wrapped_ref_subscriptions.push_back(
                WrappedRefSubscription{.source_state = source_state, .target_state = target_state, .notifier = std::move(notifier)});
        }

        void install_dynamic_child(const TSOutputView &target_dict_view,
                                   const View &key,
                                   const TSOutputView &source_child,
                                   engine_time_t modified_time)
        {
            // The mutation layer has already ensured the key exists in the
            // alternative map value. Resolve the corresponding output-side TS
            // slot and then install either:
            // - a direct TargetLink for unchanged child schema, or
            // - a recursively transformed child subtree when the value schema
            //   itself still needs wrapping/dereference work below this point.
            auto target_dict = target_dict_view.as_dict();
            TSOutputView target_child = target_dict.at(key);
            BaseState *target_child_state = target_child.context_ref().ts_state;
            auto &target_state = *static_cast<TSDState *>(target_dict_view.context_ref().ts_state);
            if (target_child_state == nullptr) {
                auto target_map = target_dict_view.value().as_map();
                const size_t slot = target_map.find_slot(key);
                if (slot == static_cast<size_t>(-1)) {
                    throw std::logic_error("TSOutput alternative dynamic dict child key is missing from the target map");
                }

                // The value-layer insert owns key->slot assignment. If the TS
                // child state has not been materialized yet, create it for the
                // already-live slot before installing the child binding.
                target_state.on_insert(slot);
                target_child = target_dict.at(key);
                target_child_state = target_child.context_ref().ts_state;
            }
            if (target_child_state == nullptr) {
                throw std::logic_error("TSOutput alternative dynamic dict child requires a live target state");
            }

            const size_t target_slot = target_child_state->index;
            const TSMeta *target_child_schema = target_child.ts_schema();
            const TSMeta *source_child_schema = source_child.ts_schema();
            if (target_child_schema == nullptr || source_child_schema == nullptr) {
                throw std::logic_error("TSOutput alternative dynamic dict child requires bound source and target schemas");
            }

            if (target_child_schema == source_child_schema) {
                install_target_link(target_state, target_slot, source_child.linked_context());
            } else {
                configure_branch(target_child, source_child);
            }

        }

        void sync_dynamic_dict(DynamicDictBinding &binding, engine_time_t modified_time, bool initializing)
        {
            TSOutputView target_root = output_view_for(binding.target_context, modified_time);
            TSOutputView source_root = output_view_for(binding.source_context, modified_time);

            auto *target_root_state = static_cast<TSDState *>(target_root.context_ref().ts_state);
            if (target_root_state == nullptr) {
                throw std::logic_error("TSOutput alternative dynamic dict sync requires a live target TSD state");
            }

            const TSViewContext target_root_context = target_root.context_ref();
            if (target_root_context.value_dispatch != nullptr && target_root_context.value_data != nullptr) {
                target_root_state->bind_value_storage(
                    *target_root.ts_schema()->element_ts(),
                    static_cast<const detail::MapViewDispatch &>(*target_root_context.value_dispatch),
                    target_root_context.value_data);
            }

            const View source_value = source_root.value();
            auto target_map = target_root.value().as_map();
            auto target_dict = target_root.as_dict();
            auto mutation = target_map.begin_mutation(modified_time);
            bool map_value_changed = false;
            constexpr size_t no_slot = static_cast<size_t>(-1);
            const BaseState *current_source_root_state =
                binding.source_context.ts_state != nullptr ? binding.source_context.ts_state->resolved_state() : nullptr;
            const bool current_source_had_value = source_value.has_value();
            const bool source_rebound = !initializing && current_source_root_state != binding.last_source_root_state();
            const bool source_value_changed = !initializing && current_source_had_value != binding.last_source_had_value();
            const bool full_resync = initializing || source_rebound || source_value_changed;

            auto remove_target_key = [&](const View &key) {
                TSOutputView existing_child = target_dict.at(key);
                if (BaseState *existing_state = existing_child.context_ref().ts_state; existing_state != nullptr) {
                    clear_dynamic_child_slot(*target_root_state, existing_state->index);
                }
                static_cast<void>(mutation.remove(key));
                map_value_changed = true;
            };

            if (!source_value.has_value()) {
                if (full_resync) {
                    std::vector<Value> stale_keys;
                    for (size_t slot = target_map.first_live_slot(); slot != no_slot; slot = target_map.next_live_slot(slot)) {
                        stale_keys.emplace_back(target_map.delta().key_at_slot(slot).clone());
                    }
                    for (const auto &key : stale_keys) { remove_target_key(key.view()); }
                }

                binding.set_last_source_snapshot(current_source_root_state, false);
                if (!initializing && map_value_changed) {
                    if (BaseState *root = ts_root_state(); root != nullptr) { root->mark_modified(modified_time); }
                }
                return;
            }

            const auto source_delta = source_value.as_map().delta();
            const auto source_map = source_value.as_map();

            // TSD key flow:
            // 1. The source map delta is authoritative for structural changes.
            // 2. We replay removals first, because stable slots may be reused.
            // 3. We then replay live slots into the alternative map value.
            // 4. Finally, for each live key we ensure the TS child subtree at
            //    that key mirrors the source child shape (link, REF wrap, or a
            //    deeper recursive alternative).
            //
            // The map value and TS child state move in lock-step: the value
            // layer owns key/slot membership, while the TS state layer owns the
            // per-key binding subtree that hangs off each occupied slot.

            // Remove stale keys before replaying live slots so reused logical
            // positions tear down their old subtree subscriptions first.
            if (full_resync) {
                std::vector<Value> stale_keys;
                for (size_t slot = target_map.first_live_slot(); slot != no_slot; slot = target_map.next_live_slot(slot)) {
                    const View key = target_map.delta().key_at_slot(slot);
                    if (!source_map.contains(key)) { stale_keys.emplace_back(key.clone()); }
                }
                for (const auto &key : stale_keys) { remove_target_key(key.view()); }
            } else {
                for (size_t slot = source_delta.first_removed_slot(); slot != no_slot; slot = source_delta.next_removed_slot(slot)) {
                    remove_target_key(source_delta.key_at_slot(slot));
                }
            }

            const bool has_added_slots = source_delta.first_added_slot() != no_slot;
            const bool has_updated_slots = source_delta.first_updated_slot() != no_slot;
            if (!full_resync && !map_value_changed && !has_added_slots && !has_updated_slots) {
                binding.set_last_source_snapshot(current_source_root_state, current_source_had_value);
                return;
            }

            auto replay_live_slot = [&](size_t slot, bool source_updated, bool rebuild_child) {
                const View key = source_delta.key_at_slot(slot);
                TSOutputView source_child = ensure_tsd_child_view(source_root, key);
                if (source_updated) {
                    if (BaseState *source_child_state = source_child.context_ref().ts_state;
                        source_child_state != nullptr && source_child_state->last_modified_time != modified_time) {
                        // Dynamic alternatives can bind directly against TSD children
                        // even when the source output only published a root-level dict
                        // tick. Materialize the child state on demand and stamp it with
                        // the current replay time so peered REF wrappers observe the same
                        // validity the normal TSD publish path would expose.
                        source_child_state->mark_modified(modified_time);
                    }
                }
                TSOutputView target_child = target_dict.at(key);
                const bool target_has_key = target_child.context_ref().is_bound();
                const bool child_schema_differs = target_has_key && target_child.ts_schema() != source_child.ts_schema();

                // Dict-level slot updates can replace the child payload for an
                // existing key without changing the key itself. When the child
                // subtree is an alternative (for example REF -> TS), that
                // existing subtree must be rebuilt so it reflects the new
                // source payload rather than continuing to project the old one.
                if (source_updated && child_schema_differs) { rebuild_child = true; }

                if (!target_has_key) {
                    // Insert the key into the alternative map before touching
                    // TS child state. That guarantees dict navigation can
                    // resolve a stable slot for install_dynamic_child().
                    Value placeholder{*target_root.ts_schema()->element_ts()->value_type, MutationTracking::Plain};
                    mutation.set(key, placeholder.view());
                    map_value_changed = true;
                }

                if (rebuild_child && target_has_key) {
                    if (BaseState *existing_state = target_child.context_ref().ts_state; existing_state != nullptr) {
                        clear_dynamic_child_slot(*target_root_state, existing_state->index);
                    }
                }

                if (rebuild_child || !target_has_key) {
                    install_dynamic_child(target_root, key, source_child, modified_time);
                    target_child = target_dict.at(key);
                }

                if (source_updated && target_has_key &&
                    (rebuild_child || context_modified(target_child.context_ref(), modified_time))) {
                    // Existing keys publish a dict-level update when the
                    // projected child changed this cycle. Rebinding/rebuilding
                    // the child subtree for the same logical key is itself a
                    // value replacement at the dict level and must surface as
                    // an updated key.
                    mutation.set(key, target_child.value());
                    map_value_changed = true;
                }
            };

            if (full_resync) {
                for (size_t slot = source_map.first_live_slot(); slot != no_slot; slot = source_map.next_live_slot(slot)) {
                    replay_live_slot(slot, true, true);
                }
            } else {
                for (size_t slot = source_delta.first_added_slot(); slot != no_slot; slot = source_delta.next_added_slot(slot)) {
                    replay_live_slot(slot, true, true);
                }
                for (size_t slot = source_delta.first_updated_slot(); slot != no_slot; slot = source_delta.next_updated_slot(slot)) {
                    if (source_delta.slot_added(slot)) { continue; }
                    replay_live_slot(slot, true, false);
                }
            }

            binding.set_last_source_snapshot(current_source_root_state, true);

            if (!initializing && map_value_changed) {
                // The dynamic dict binding handles child-level validity and
                // modification directly. Only publish a root-level change when
                // replay actually changed the alternative map value. Pure
                // child-state updates already flow through the installed
                // subtree links and would otherwise notify the root twice.
                if (BaseState *root = ts_root_state(); root != nullptr) { root->mark_modified(modified_time); }
            }
        }

        void sync_dynamic_key_set(DynamicKeySetBinding &binding, engine_time_t modified_time, bool initializing)
        {
            TSOutputView target_root = output_view_for(binding.target_context, modified_time);
            TSOutputView source_root = output_view_for(binding.source_context, modified_time);

            auto *target_root_state = target_root.context_ref().ts_state;
            if (target_root_state == nullptr) {
                throw std::logic_error("TSOutput key-set sync requires a live target TSS state");
            }

            auto target_set = target_root.value().as_set();
            auto mutation = target_set.begin_mutation(modified_time);
            bool set_changed = false;
            const bool materialize_empty_set = initializing && !target_root.value().has_value();
            constexpr size_t no_slot = static_cast<size_t>(-1);
            const BaseState *current_source_root_state =
                binding.source_context.ts_state != nullptr ? binding.source_context.ts_state->resolved_state() : nullptr;
            const bool current_source_had_value = source_root.value().has_value();
            const bool source_rebound = !initializing && current_source_root_state != binding.last_source_root_state();
            const bool source_value_changed = !initializing && current_source_had_value != binding.last_source_had_value();
            const bool full_resync = initializing || source_rebound || source_value_changed;

            auto remove_target_key = [&](const View &key) {
                set_changed = mutation.remove(key) || set_changed;
            };

            const View source_value = source_root.value();
            if (!source_value.has_value()) {
                if (full_resync) {
                    std::vector<Value> stale_keys;
                    for (const View &key : target_set.values()) {
                        stale_keys.emplace_back(key.clone());
                    }
                    for (const auto &key : stale_keys) { remove_target_key(key.view()); }
                }

                binding.set_last_source_snapshot(current_source_root_state, false);
                if (materialize_empty_set) {
                    if (BaseState *root = ts_root_state(); root != nullptr) { root->mark_modified(modified_time); }
                } else if (!initializing && set_changed) {
                    if (BaseState *root = ts_root_state(); root != nullptr) { root->mark_modified(modified_time); }
                }
                return;
            }

            const auto source_map = source_value.as_map();
            const auto source_delta = source_map.delta();

            if (full_resync) {
                std::vector<Value> stale_keys;
                for (const View &key : target_set.values()) {
                    if (!source_map.contains(key)) { stale_keys.emplace_back(key.clone()); }
                }
                for (const auto &key : stale_keys) { remove_target_key(key.view()); }

                for (size_t slot = source_map.first_live_slot(); slot != no_slot; slot = source_map.next_live_slot(slot)) {
                    set_changed = mutation.add(source_map.delta().key_at_slot(slot)) || set_changed;
                }
            } else {
                for (size_t slot = source_delta.first_removed_slot(); slot != no_slot; slot = source_delta.next_removed_slot(slot)) {
                    remove_target_key(source_delta.key_at_slot(slot));
                }
                for (size_t slot = source_delta.first_added_slot(); slot != no_slot; slot = source_delta.next_added_slot(slot)) {
                    set_changed = mutation.add(source_delta.key_at_slot(slot)) || set_changed;
                }
            }

            binding.set_last_source_snapshot(current_source_root_state, true);
            if (materialize_empty_set) {
                if (BaseState *root = ts_root_state(); root != nullptr) { root->mark_modified(modified_time); }
            } else if (!initializing && set_changed) {
                if (BaseState *root = ts_root_state(); root != nullptr) { root->mark_modified(modified_time); }
            }
        }

        void configure_branch(const TSOutputView &target_view, const TSOutputView &source_view)
        {
            const TSMeta *target_schema = target_view.ts_schema();
            const TSMeta *source_schema = source_view.ts_schema();
            if (target_schema == nullptr || source_schema == nullptr) {
                throw std::invalid_argument("TSOutput alternative configuration requires bound source and target schemas");
            }

            BaseState *target_state = target_view.linked_context().ts_state;
            const LinkedTSContext source_context = source_view.linked_context();
            BaseState *source_state = source_context.ts_state;
            if (target_state != nullptr) {
                target_state->last_modified_time = source_state != nullptr ? source_state->last_modified_time : MIN_DT;
            }

            if (source_schema->kind == TSKind::REF) {
                const TSMeta *dereferenced_schema = source_schema->element_ts();
                if (dereferenced_schema == nullptr) {
                    throw std::invalid_argument("TSOutput alternative REF dereference requires a referenced schema");
                }

                if (target_schema->kind == TSKind::REF) {
                    const TSMeta *target_element = target_schema->element_ts();
                    if (target_element != nullptr && supports_alternative_cast(*dereferenced_schema, *target_element)) {
                        add_scalar_value_binding(
                            std::make_unique<ScalarValueBinding>(this, target_view, source_view),
                            source_state != nullptr ? source_state->last_modified_time : MIN_DT);
                        return;
                    }
                }

                if (target_schema->kind == TSKind::TSD && dereferenced_schema->kind == TSKind::TSD &&
                    supports_alternative_cast(*dereferenced_schema, *target_schema)) {
                    add_dynamic_dict_binding(
                        make_dereferenced_dynamic_dict_binding(target_view, source_view),
                        source_state != nullptr ? source_state->last_modified_time : MIN_DT);
                    return;
                }

                if (target_schema->kind == TSKind::TSS && dereferenced_schema->kind == TSKind::TSD &&
                    supports_alternative_cast(*dereferenced_schema, *target_schema)) {
                    add_dynamic_key_set_binding(
                        make_dereferenced_dynamic_key_set_binding(target_view, source_view),
                        target_view.evaluation_time());
                    return;
                }

                if (supports_alternative_cast(*dereferenced_schema, *target_schema)) {
                    if (target_schema->kind == TSKind::TSB || target_schema->kind == TSKind::TSL) {
                        add_collection_ref_binding(
                            std::make_unique<CollectionRefBinding>(this, target_view, source_view),
                            source_state != nullptr ? source_state->last_modified_time : MIN_DT);
                    } else {
                        // REF -> TS does not copy target data into the alternative.
                        // Instead the alternative position becomes a RefLinkState that
                        // tracks the REF source and exposes the current dereferenced
                        // target through the normal TS view surface.
                        install_ref_link_for_target(target_view, source_view);
                    }
                    return;
                }

                throw std::invalid_argument("TSOutput alternative REF dereference requires matching or recursively compatible schema");
            }

            if (target_schema->kind == TSKind::SIGNAL) {
                install_signal_subscription(target_view, source_view);
                return;
            }

            if (target_schema->kind == TSKind::TSValue && source_schema->kind == TSKind::TSValue) {
                add_scalar_value_binding(
                    std::make_unique<ScalarValueBinding>(this, target_view, source_view),
                    source_state != nullptr ? source_state->last_modified_time : MIN_DT);
                return;
            }

            if (target_schema->kind == TSKind::REF) {
                const TSMeta *target_element = target_schema->element_ts();
                if (target_element == nullptr || !supports_alternative_cast(*source_schema, *target_element)) {
                    throw std::invalid_argument("TSOutput alternative REF wrapping requires matching referenced schema");
                }

                target_view.value().as_atomic().set(hgraph::TimeSeriesReference::make(source_view));
                if (target_state != nullptr) {
                    const engine_time_t source_modified_time =
                        source_state != nullptr && source_state->last_modified_time != MIN_DT
                            ? source_state->last_modified_time
                            : source_view.evaluation_time();
                    target_state->last_modified_time = source_modified_time;
                }
                auto notifier = std::make_unique<WrappedRefNotifier>(target_state);
                BaseState *source_notification_state = notification_state_of(source_context);
                if (source_notification_state != nullptr) { source_notification_state->subscribe(notifier.get()); }
                m_wrapped_ref_subscriptions.push_back(
                    WrappedRefSubscription{
                        .source_state = source_notification_state,
                        .target_state = target_state,
                        .notifier = std::move(notifier),
                    });
                return;
            }

            if (target_schema == source_schema) {
                throw std::logic_error("TSOutput alternative configuration should not recurse into schema-identical branches");
            }

            if (target_schema->kind == TSKind::TSS && source_schema->kind == TSKind::TSD) {
                add_dynamic_key_set_binding(
                    std::make_unique<DynamicKeySetBinding>(this, target_view, source_view),
                    target_view.evaluation_time());
                return;
            }

            if (target_schema->kind != source_schema->kind) {
                throw std::invalid_argument("TSOutput alternative configuration requires matching collection kinds");
            }

            switch (target_schema->kind) {
                case TSKind::TSB:
                    {
                        auto &target_state_ref = *static_cast<TSBState *>(target_view.linked_context().ts_state);
                        auto source_bundle = source_view.as_bundle();
                        auto target_bundle = target_view.as_bundle();
                        for (size_t i = 0; i < target_schema->field_count(); ++i) {
                            const TSMeta *target_child_schema = target_schema->fields()[i].ts_type;
                            const TSMeta *source_child_schema = source_schema->fields()[i].ts_type;
                            TSOutputView source_child = source_bundle[i];
                            if (target_child_schema == source_child_schema) {
                                install_target_link(target_state_ref, i, source_child.linked_context());
                            } else {
                                configure_branch(target_bundle[i], source_child);
                            }
                        }
                        return;
                    }

                case TSKind::TSL:
                    {
                        if (target_schema->fixed_size() == 0 || target_schema->fixed_size() != source_schema->fixed_size()) {
                            throw std::invalid_argument("TSOutput alternatives only support fixed-size TSL wrap casts");
                        }
                        auto &target_state_ref = *static_cast<TSLState *>(target_view.linked_context().ts_state);
                        auto source_list = source_view.as_list();
                        auto target_list = target_view.as_list();
                        for (size_t i = 0; i < target_schema->fixed_size(); ++i) {
                            const TSMeta *target_child_schema = target_schema->element_ts();
                            const TSMeta *source_child_schema = source_schema->element_ts();
                            TSOutputView source_child = source_list[i];
                            if (target_child_schema == source_child_schema) {
                                install_target_link(target_state_ref, i, source_child.linked_context());
                            } else {
                                configure_branch(target_list[i], source_child);
                            }
                        }
                        return;
                    }

                case TSKind::TSD:
                    {
                        if (source_schema->key_type() != target_schema->key_type()) {
                            throw std::invalid_argument("TSOutput alternatives only support TSD casts with matching key schemas");
                        }

                        // Dynamic dict alternatives are kept live after
                        // construction. Instead of eagerly recursing every
                        // possible child, we subscribe to the source dict root
                        // and replay key-level structural changes on demand.
                        add_dynamic_dict_binding(
                            std::make_unique<DynamicDictBinding>(this, target_view, source_view),
                            source_state != nullptr ? source_state->last_modified_time : MIN_DT);
                        return;
                    }

                default:
                    throw std::invalid_argument("TSOutput alternatives only support collection wrap casts through TSB, TSL, TSD, and TSS");
            }
        }

        std::vector<WrappedRefSubscription> m_wrapped_ref_subscriptions;
        std::vector<std::unique_ptr<ScalarValueBinding>> m_scalar_value_bindings;
        std::vector<std::unique_ptr<DynamicDictBinding>> m_dynamic_dict_bindings;
        std::vector<std::unique_ptr<DynamicKeySetBinding>> m_dynamic_key_set_bindings;
        std::vector<std::unique_ptr<CollectionRefBinding>> m_collection_ref_bindings;
        ViewOps m_view_ops{this};
        AlternativeMap m_alternatives;
    };

    TSOutput::TSOutput() noexcept = default;

    TSOutput::TSOutput(const TSOutputBuilder &builder)
    {
        builder.construct_output(*this);
    }

    void mark_output_view_modified(const TSOutputView &view, engine_time_t evaluation_time)
    {
        LinkedTSContext context = view.linked_context();
        if (context.ts_state == nullptr) {
            throw std::logic_error("mark_output_view_modified requires a linked output state");
        }

        if (context.schema != nullptr && context.schema->kind == TSKind::TSD && context.ts_state->storage_kind == TSStorageKind::Native &&
            context.value_dispatch != nullptr && context.value_data != nullptr) {
            auto &state = *static_cast<TSDState *>(context.ts_state);
            state.bind_value_storage(
                *context.schema->element_ts(),
                static_cast<const detail::MapViewDispatch &>(*context.value_dispatch),
                context.value_data);
            if (state.publish_value_storage_delta(evaluation_time)) { return; }
        }

        if (TSOutput *owning_output = view.owning_output(); owning_output != nullptr) {
            owning_output->sync_ref_target_subscriptions(evaluation_time);
        }
        context.ts_state->mark_modified(evaluation_time);
    }

    namespace
    {
        [[nodiscard]] OutputLinkState *ensure_output_link_state(const TSOutputView &target)
        {
            const TSMeta *target_schema = target.ts_schema();
            if (target_schema == nullptr) { throw std::invalid_argument("prepare_output_link requires a target schema"); }

            BaseState *target_state = target.context_ref().ts_state;
            if (target_state == nullptr) { throw std::logic_error("prepare_output_link requires a live target state"); }

            TimeSeriesStateV *slot = owning_state_variant(target_state, target.owning_output());
            if (slot == nullptr) { throw std::logic_error("prepare_output_link could not resolve the owning target state slot"); }

            if (target_state->storage_kind == TSStorageKind::OutputLink) {
                return static_cast<OutputLinkState *>(target_state);
            }

            const TimeSeriesStateParentPtr parent = target_state->parent;
            const size_t index = target_state->index;
            const engine_time_t last_modified_time = target_state->last_modified_time;
            auto &new_state = slot->emplace<OutputLinkState>();
            initialize_base_state(new_state, parent, index, last_modified_time, TSStorageKind::OutputLink);
            new_state.target.clear();
            return &new_state;
        }
    }

    void prepare_output_link(const TSOutputView &target)
    {
        static_cast<void>(ensure_output_link_state(target));
    }

    bool bind_output_link(const TSOutputView &target, const TSOutputView &source)
    {
        const TSMeta *target_schema = target.ts_schema();
        const TSMeta *source_schema = source.ts_schema();
        if (target_schema == nullptr || source_schema == nullptr || target_schema != source_schema) {
            throw std::invalid_argument("bind_output_link requires matching source and target schemas");
        }

        BaseState *target_state = target.context_ref().ts_state;
        if (target_state == nullptr) { throw std::logic_error("bind_output_link requires a live target state"); }
        OutputLinkState *link_state = ensure_output_link_state(target);

        const LinkedTSContext source_context = source.linked_context();
        if (detail::linked_context_equal(link_state->target, source_context)) { return false; }
        link_state->set_target(source_context);
        return true;
    }

    void clear_output_link(const TSOutputView &target)
    {
        BaseState *target_state = target.context_ref().ts_state;
        if (target_state == nullptr || target_state->storage_kind != TSStorageKind::OutputLink) { return; }
        static_cast<OutputLinkState *>(target_state)->reset_target();
    }

    nb::object detail::TSDispatch::to_python(const TSViewContext &context, engine_time_t) const
    {
        return detail::leaf_to_python(context);
    }

    nb::object detail::TSDispatch::delta_to_python(const TSViewContext &context, engine_time_t) const
    {
        return detail::leaf_delta_to_python(context);
    }

    void detail::TSDispatch::from_python(const TSOutputView &view, nb::handle value) const
    {
        view.value().from_python(nb::borrow<nb::object>(value));
        mark_output_view_modified(view, view.evaluation_time());
    }

    bool detail::TSDispatch::can_apply_result(const TSOutputView &view, nb::handle value) const
    {
        static_cast<void>(value);
        return !view.modified();
    }

    void detail::TSDispatch::apply_result(const TSOutputView &view, nb::handle value) const
    {
        from_python(view, value);
    }

    void detail::TSDispatch::clear(const TSOutputView &view) const
    {
        static_cast<void>(view);
        throw std::logic_error("TS clear() is only implemented for collection schemas");
    }

    void detail::TSKeyDispatch::child_from_python(const TSOutputView &, const View &, nb::handle) const
    {
        throw std::logic_error("TSKeyDispatch child_from_python is not implemented for this dispatch");
    }

    namespace detail
    {
        nb::object to_python(const TSViewContext &context, engine_time_t evaluation_time)
        {
            return to_python_impl(context, evaluation_time);
        }

        nb::object delta_to_python(const TSViewContext &context, engine_time_t evaluation_time)
        {
            return delta_to_python_impl(context, evaluation_time);
        }
    }  // namespace detail

    void TSOutputView::apply_result(nb::handle value) const
    {
        if (value.is_none()) { return; }
        const auto *dispatch = context_ref().resolved().ts_dispatch;
        if (dispatch == nullptr) {
            from_python(value);
            return;
        }
        dispatch->apply_result(*this, value);
    }

    bool TSOutputView::can_apply_result(nb::handle value) const
    {
        if (value.is_none()) { return true; }
        const auto *dispatch = context_ref().resolved().ts_dispatch;
        return dispatch != nullptr ? dispatch->can_apply_result(*this, value) : !modified();
    }

    void TSOutputView::clear() const
    {
        const auto *dispatch = context_ref().resolved().ts_dispatch;
        if (dispatch == nullptr) {
            from_python(nb::none());
            return;
        }
        dispatch->clear(*this);
    }

    void TSOutputView::from_python(nb::handle value) const
    {
        const auto *dispatch = context_ref().resolved().ts_dispatch;
        if (dispatch == nullptr) {
            if (const auto *ops = output_view_ops(); ops != nullptr && ops->from_python(*this, value)) { return; }
            throw std::logic_error("TSOutputView Python mutation requires a dispatch");
        }
        dispatch->from_python(*this, value);
    }

    TSOutput::TSOutput(const TSOutput &other)
    {
        if (other.m_builder != nullptr) {
            other.builder().copy_construct_output(*this, other);
            sync_ref_target_subscriptions(ts_root_state() != nullptr ? ts_root_state()->last_modified_time : MIN_DT);
        }
    }

    TSOutput::TSOutput(TSOutput &&other) noexcept
    {
        if (other.m_builder != nullptr) {
            other.builder().move_construct_output(*this, other);
            sync_ref_target_subscriptions(ts_root_state() != nullptr ? ts_root_state()->last_modified_time : MIN_DT);
        }
    }

    TSOutput &TSOutput::operator=(const TSOutput &other)
    {
        if (this == &other) { return *this; }
        TSOutput replacement(other);
        return *this = std::move(replacement);
    }

    TSOutput &TSOutput::operator=(TSOutput &&other) noexcept
    {
        if (this == &other) { return *this; }

        clear_storage();
        if (other.m_builder == nullptr) {
            m_builder = nullptr;
            m_alternatives.clear();
            return *this;
        }
        other.builder().move_construct_output(*this, other);
        sync_ref_target_subscriptions(ts_root_state() != nullptr ? ts_root_state()->last_modified_time : MIN_DT);
        return *this;
    }

    TSOutput::~TSOutput()
    {
        clear_storage();
    }

    TSOutputView TSOutput::view(engine_time_t evaluation_time)
    {
        TSViewContext context = view_context();
        return TSOutputView{context, TSViewContext::none(), evaluation_time, this, &detail::default_output_view_ops()};
    }

    TSOutputView detail::register_set_contains_output(const TSOutputView &view, const View &item)
    {
        LinkedTSContext source_context = feature_source_context(view);
        auto &registry = ensure_collection_feature_registry(*source_context.ts_state);
        return registry.register_contains_output(source_context, view.evaluation_time(), item);
    }

    void detail::unregister_set_contains_output(const TSOutputView &view, const View &item)
    {
        LinkedTSContext source_context = feature_source_context(view);
        if (source_context.ts_state == nullptr || source_context.ts_state->feature_registry == nullptr) { return; }

        auto *registry = dynamic_cast<CollectionFeatureRegistry *>(source_context.ts_state->feature_registry.get());
        if (registry != nullptr) { registry->unregister_contains_output(item); }
    }

    TSOutputView detail::register_set_is_empty_output(const TSOutputView &view)
    {
        LinkedTSContext source_context = feature_source_context(view);
        auto &registry = ensure_collection_feature_registry(*source_context.ts_state);
        return registry.register_is_empty_output(source_context, view.evaluation_time());
    }

    void detail::unregister_set_is_empty_output(const TSOutputView &view)
    {
        LinkedTSContext source_context = feature_source_context(view);
        if (source_context.ts_state == nullptr || source_context.ts_state->feature_registry == nullptr) { return; }

        auto *registry = dynamic_cast<CollectionFeatureRegistry *>(source_context.ts_state->feature_registry.get());
        if (registry != nullptr) { registry->unregister_is_empty_output(); }
    }

    TSOutputView detail::project_dict_key_set_output(const TSViewContext &source_context, engine_time_t evaluation_time)
    {
        const TSViewContext resolved_context = source_context.resolved();
        const TSMeta *schema = resolved_context.schema;
        TSOutput *owning_output = resolved_context.owning_output;
        if (schema == nullptr || owning_output == nullptr) {
            throw std::logic_error("TSDView::key_set requires a bound dict view");
        }

        TSOutputView source_view{
            resolved_context,
            TSViewContext::none(),
            evaluation_time,
            owning_output,
            resolved_context.output_view_ops != nullptr ? resolved_context.output_view_ops : &detail::default_output_view_ops(),
        };
        return owning_output->bindable_view(source_view, TSTypeRegistry::instance().tss(schema->key_type()));
    }

    TSOutputView TSOutput::bindable_view(const TSOutputView &source, const TSMeta *schema)
    {
        if (schema == nullptr) { throw std::invalid_argument("TSOutput::bindable_view requires a non-null target schema"); }

        const LinkedTSContext source_context = source.linked_context();
        if (!source_context.is_bound()) { throw std::invalid_argument("TSOutput::bindable_view requires a bound source view"); }
        if (source_context.schema == schema) { return source; }

        if (source_context.schema == nullptr || !supports_alternative_cast(*source_context.schema, *schema)) {
            throw std::invalid_argument(
                fmt::format("TSOutput::bindable_view does not support the requested schema cast: {} -> {}",
                            schema_debug_name(source_context.schema),
                            schema_debug_name(schema)));
        }

        if (source.owning_output() != nullptr && source.owning_output() != this) {
            throw std::logic_error("TSOutput alternative source view is not owned by the requested output");
        }

        if (const auto *ops = dynamic_cast<const AlternativeOutput::ViewOps *>(source.output_view_ops());
            ops != nullptr && ops->owner != nullptr) {
            return ops->owner->bindable_view(this, source, schema);
        }

        TSOutputView root_source = view(source.evaluation_time());
        TSPath source_path;
        BaseState *target_state = source.context_ref().ts_state;
        if (target_state != nullptr &&
            !find_state_path(root_source.context_ref().ts_state, root_source.ts_schema(), target_state, source_path)) {
            throw std::logic_error("TSOutput alternative source view is not reachable from the owning output root");
        }

        const TSMeta *root_source_schema = root_source.ts_schema();
        if (root_source_schema == nullptr) { throw std::logic_error("TSOutput::bindable_view requires a rooted source schema"); }

        // Alternatives are owned and cached at the output boundary. Child
        // casts therefore rebuild a rooted target schema with only the
        // requested subtree transformed, then project the requested child view
        // back out of that rooted alternative.
        const TSMeta *alternative_schema = replace_schema_at_path(*root_source_schema, source_path, 0, *schema);
        const auto [it, inserted] = m_alternatives.try_emplace(alternative_schema, nullptr);
        if (inserted) { it->second.reset(new AlternativeOutput(root_source, *alternative_schema)); }

        TSOutputView alternative_view = it->second->view(this, source.evaluation_time());
        return traverse_output_path(alternative_view, alternative_schema, source_path);
    }

    void TSOutput::clear_storage() noexcept
    {
        clear_ref_target_subscriptions();
        if (m_builder == nullptr) {
            m_alternatives.clear();
            return;
        }
        builder().destruct_output(*this);
    }

    void TSOutput::clear_ref_target_subscriptions() noexcept
    {
        for (auto &subscription : m_ref_target_subscriptions) {
            if (subscription.state != nullptr && subscription.notifier) {
                subscription.state->unsubscribe(subscription.notifier.get());
            }
        }
        m_ref_target_subscriptions.clear();
    }

    void TSOutput::sync_ref_target_subscriptions(engine_time_t evaluation_time)
    {
        clear_ref_target_subscriptions();

        TSOutputView view = this->view(evaluation_time);
        const TSMeta *schema = view.ts_schema();
        if (schema == nullptr || schema->kind != TSKind::REF) { return; }

        const auto *ref = view.value().as_atomic().try_as<TimeSeriesReference>();
        if (ref == nullptr) { return; }

        std::unordered_set<BaseState *> target_states;
        collect_ref_target_states(*ref, target_states);
        for (BaseState *state : target_states) {
            if (state == nullptr) { continue; }
            auto notifier = std::make_unique<RootRefValueNotifier>(this);
            state->subscribe(notifier.get());
            m_ref_target_subscriptions.push_back(RefTargetSubscription{.state = state, .notifier = std::move(notifier)});
        }
    }

    void TSOutput::AlternativeOutputDeleter::operator()(AlternativeOutput *value) const noexcept
    {
        delete value;
    }
}  // namespace hgraph
