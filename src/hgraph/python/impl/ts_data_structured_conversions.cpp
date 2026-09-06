#include "python_ops_families.h"

#include "../../types/metadata/detail/ts_data_seams.h"

#include <hgraph/python/bridge_state.h>
#include <hgraph/python/conversion.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/scope.h>

#include <fmt/format.h>
#include <nanobind/nanobind.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

/**
 * TSData <-> Python for the structured strategies (RFC 0035, PR 4b): fixed
 * TSB / TSL, dynamic TSL, slot-backed TSS / TSD and the TSD proxy. Children
 * are reached through their own erased TSDataOps; the strategies' private
 * shapes and mutation protocols come through ``ts_data_seams.h``. Each body
 * keeps the semantics it had beside its storage.
 */
namespace hgraph::python_bridge
{
    namespace
    {
        namespace seams = ts_data_seams;

        constexpr std::size_t no_child = std::numeric_limits<std::size_t>::max();

        // -- shared Python-shape helpers ----------------------------------------------
        [[nodiscard]] bool is_python_sequence(nb::handle source)
        {
            nb::object object = nb::borrow<nb::object>(source);
            return nb::isinstance<nb::list>(object) || nb::isinstance<nb::tuple>(object);
        }

        [[nodiscard]] bool python_has_items(nb::handle source)
        {
            nb::object object = nb::borrow<nb::object>(source);
            return nb::isinstance<nb::dict>(object) || nb::hasattr(object, "items");
        }

        template <typename Visitor>
        void for_each_python_mapping_item(nb::handle source, const char *what, Visitor visitor)
        {
            if (!python_has_items(source)) { throw std::invalid_argument(std::string{what} + " expects a Python mapping"); }
            nb::object   object = nb::borrow<nb::object>(source);
            nb::object   items  = object.attr("items")();
            nb::iterator it     = nb::iter(items);
            while (it != nb::iterator::sentinel())
            {
                nb::tuple pair = nb::cast<nb::tuple>(*it);
                if (pair.size() != 2) { throw std::invalid_argument(std::string{what} + " items() must yield key/value pairs"); }
                visitor(nb::borrow<nb::object>(pair[0]), nb::borrow<nb::object>(pair[1]));
                ++it;
            }
        }

        template <typename Visitor>
        void for_each_python_iterable(nb::handle source, const char *what, Visitor visitor)
        {
            if (source.is_none()) { return; }
            nb::object   object = nb::borrow<nb::object>(source);
            nb::iterator it     = nb::iter(object);
            while (it != nb::iterator::sentinel())
            {
                nb::handle item = *it;
                if (item.is_none()) { throw std::invalid_argument(std::string{what} + " does not allow None elements"); }
                visitor(item);
                ++it;
            }
        }

        [[nodiscard]] bool python_named_field(nb::handle source, const char *name, nb::object &out)
        {
            nb::object object = nb::borrow<nb::object>(source);
            if (nb::isinstance<nb::dict>(object))
            {
                nb::dict map = nb::cast<nb::dict>(object);
                nb::str  key{name};
                if (!map.contains(key)) { return false; }
                out = map[key];
                return true;
            }
            if (nb::hasattr(object, name))
            {
                out = nb::getattr(object, name);
                return true;
            }
            return false;
        }

        [[nodiscard]] Value value_from_python(const ValueTypeRef &binding, nb::handle source, const char *what)
        {
            if (source.is_none()) { throw std::invalid_argument(std::string{what} + " requires a non-None value"); }
            Value value{binding};
            from_python(binding, const_cast<void *>(value.view().data()), source);
            return value;
        }

        /** A child strategy's Python import, then the tracking record and parent bookkeeping the
            structured parent owes for it. */
        [[nodiscard]] bool import_child(const TSDataOps &ops, void *child, nb::handle source, DateTime modified_time,
                                        const char *what)
        {
            if (!ops.from_python_impl(ops.context, child, borrow(source), modified_time)) { return false; }
            auto *tracking = ops.mutable_tracking_impl(ops.context, child);
            if (tracking == nullptr) { throw std::logic_error(std::string{what} + " child has no tracking record"); }
            if (!tracking->record_modified(modified_time))
            {
                throw std::logic_error(std::string{what} + " child reported a duplicate Python update modification");
            }
            return true;
        }

        void require_source(const void *memory, nb::handle source, DateTime modified_time, const char *what)
        {
            if (memory == nullptr) { throw std::logic_error(std::string{what} + " requires live storage"); }
            if (source.is_none()) { throw std::invalid_argument(std::string{what} + " requires a non-None source"); }
            if (modified_time == MIN_DT)
            {
                throw std::invalid_argument(std::string{what} + " requires a concrete evaluation time");
            }
        }

        // -- fixed TSB / TSL ---------------------------------------------------------------
        [[nodiscard]] nb::object fixed_value_to_python(const void *context, const void *memory)
        {
            // This is a VALUE projection, not the TSData Python surface.
            // Materialise through the erased owning-type/copy contract so a
            // projected child never needs ad-hoc recursive shape knowledge.
            return to_python(Value{ValueView{seams::fixed_layout(context).value_binding, memory}});
        }

        [[nodiscard]] nb::object fixed_delta_bundle_to_python(const void *context, const void *memory)
        {
            return to_python(Value{ValueView{seams::fixed_layout(context).delta_binding, memory}});
        }

        [[nodiscard]] nb::object fixed_delta_map_to_python(const void *context, const void *memory)
        {
            return to_python(Value{ValueView{seams::fixed_layout(context).delta_binding, memory}});
        }

        [[nodiscard]] nb::object fixed_delta_key_set_to_python(const void *context, const void *memory)
        {
            nb::set result;
            for (std::size_t index = 0; index < seams::fixed_element_count(context); ++index)
            {
                if (!seams::fixed_child_modified_for_parent_time(context, memory, index)) { continue; }
                result.add(nb::int_{seams::fixed_ordinal_key(context, index)});
            }
            return result;
        }

        // Python TimeSeries.value is a TSData operation. Recurse through each
        // child's TSDataOps so nested TSL/TSD/TSB representations retain
        // their own public Python semantics. The TSB and TSL bodies are
        // distinct provider entries the factory selects once (no kind switch).
        [[nodiscard]] nb::object fixed_child_value_to_python(const void *context, const void *memory, std::size_t index)
        {
            const auto &ops   = seams::fixed_element_type(context, index).ops_ref();
            const auto *child = seams::fixed_child_data(context, memory, index);
            return ops.has_current_value_impl(ops.context, child) ? take(ops.to_python_impl(ops.context, child))
                                                                  : nb::none();
        }

        [[nodiscard]] nb::object fixed_bundle_to_python(const void *context, const void *memory)
        {
            const auto &schema = seams::fixed_schema(context);
            nb::dict    result;
            for (std::size_t index = 0, count = seams::fixed_element_count(context); index < count; ++index)
            {
                const char *name = schema.fields()[index].name;
                if (name == nullptr || *name == '\0') { continue; }
                result[nb::str{name}] = fixed_child_value_to_python(context, memory, index);
            }
            return materialize_tsb_python_value(&schema, std::move(result));
        }

        [[nodiscard]] nb::object fixed_list_to_python(const void *context, const void *memory)
        {
            nb::list result;
            for (std::size_t index = 0, count = seams::fixed_element_count(context); index < count; ++index)
            {
                result.append(fixed_child_value_to_python(context, memory, index));
            }
            return nb::tuple(result);
        }

        /** The child's delta for this cycle, or None when it did not tick. */
        [[nodiscard]] nb::object fixed_child_delta_to_python(const void *context, const void *memory, std::size_t index,
                                                             DateTime evaluation_time)
        {
            if (!seams::fixed_child_modified_for_parent_time(context, memory, index)) { return nb::none(); }
            const auto &ops   = seams::fixed_element_type(context, index).ops_ref();
            const auto *child = seams::fixed_child_data(context, memory, index);
            return take(ops.delta_to_python_impl(ops.context, child, evaluation_time));
        }

        [[nodiscard]] nb::object fixed_bundle_delta_to_python(const void *context, const void *memory,
                                                              DateTime evaluation_time)
        {
            if (seams::fixed_tracking(context, memory).last_modified_time != evaluation_time) { return nb::none(); }
            const auto &schema = seams::fixed_schema(context);
            nb::dict    result;
            for (std::size_t index = 0, count = seams::fixed_element_count(context); index < count; ++index)
            {
                const char *name = schema.fields()[index].name;
                if (name == nullptr || *name == '\0') { continue; }
                nb::object value = fixed_child_delta_to_python(context, memory, index, evaluation_time);
                if (!value.is_none()) { result[nb::str{name}] = std::move(value); }
            }
            return result;
        }

        [[nodiscard]] nb::object fixed_list_delta_to_python(const void *context, const void *memory,
                                                            DateTime evaluation_time)
        {
            if (seams::fixed_tracking(context, memory).last_modified_time != evaluation_time) { return nb::none(); }
            nb::dict result;
            for (std::size_t index = 0, count = seams::fixed_element_count(context); index < count; ++index)
            {
                nb::object value = fixed_child_delta_to_python(context, memory, index, evaluation_time);
                if (!value.is_none()) { result[nb::int_{index}] = std::move(value); }
            }
            return result;
        }

        [[nodiscard]] std::size_t fixed_field_index_by_name(const void *context, std::string_view name) noexcept
        {
            const auto &schema = seams::fixed_schema(context);
            for (std::size_t index = 0; index < seams::fixed_element_count(context); ++index)
            {
                const char *field_name = schema.fields()[index].name;
                if (field_name != nullptr && name == field_name) { return index; }
            }
            return no_child;
        }

        [[nodiscard]] bool fixed_child_update_from_python(const void *context, void *memory, std::size_t index,
                                                          nb::handle source, DateTime modified_time)
        {
            if (source.is_none()) { return false; }
            const auto &ops = seams::fixed_element_type(context, index).ops_ref();
            return import_child(ops, seams::fixed_mutable_child_data(context, memory, index), source, modified_time,
                                "fixed TSData");
        }

        [[nodiscard]] bool fixed_from_python_sequence(const void *context, void *memory, nb::handle source,
                                                      DateTime modified_time, const char *what)
        {
            if (!is_python_sequence(source)) { throw std::invalid_argument(std::string{what} + " expects a Python list or tuple"); }

            nb::object   object   = nb::borrow<nb::object>(source);
            nb::sequence sequence = nb::cast<nb::sequence>(object);
            const auto   count    = static_cast<std::size_t>(nb::len(sequence));
            if (count != seams::fixed_element_count(context))
            {
                throw std::invalid_argument(
                    fmt::format("{} expects {} elements, got {}", what, seams::fixed_element_count(context), count));
            }

            bool touched = false;
            for (std::size_t index = 0; index < count; ++index)
            {
                nb::object child_source = sequence[index];
                touched |= fixed_child_update_from_python(context, memory, index, child_source, modified_time);
            }
            return touched;
        }

        [[nodiscard]] bool fixed_from_python_bundle(const void *context, void *memory, nb::handle source,
                                                    DateTime modified_time)
        {
            nb::object object = nb::borrow<nb::object>(source);
            if (python_has_items(source))
            {
                bool touched = false;
                for_each_python_mapping_item(source, "TSB from_python", [&](nb::handle key, nb::handle value) {
                    const auto field = nb::cast<std::string>(key);
                    const auto index = fixed_field_index_by_name(context, field);
                    if (index == no_child) { throw std::invalid_argument(fmt::format("TSB from_python unknown field '{}'", field)); }
                    touched |= fixed_child_update_from_python(context, memory, index, value, modified_time);
                });
                return touched;
            }

            if (is_python_sequence(source))
            {
                return fixed_from_python_sequence(context, memory, source, modified_time, "TSB from_python");
            }

            const auto &schema    = seams::fixed_schema(context);
            bool        saw_field = false;
            bool        touched   = false;
            for (std::size_t index = 0; index < seams::fixed_element_count(context); ++index)
            {
                const char *name = schema.fields()[index].name;
                if (name == nullptr || *name == '\0')
                {
                    throw std::invalid_argument("TSB from_python has an unnamed field and cannot load attributes");
                }
                if (!nb::hasattr(object, name)) { continue; }
                saw_field               = true;
                nb::object child_source = nb::getattr(object, name);
                touched |= fixed_child_update_from_python(context, memory, index, child_source, modified_time);
            }
            if (!saw_field) { throw std::invalid_argument("TSB from_python expects a mapping, sequence, or field attributes"); }
            return touched;
        }

        [[nodiscard]] bool fixed_from_python_list_mapping(const void *context, void *memory, nb::handle source,
                                                          DateTime modified_time)
        {
            bool touched = false;
            for_each_python_mapping_item(source, "fixed TSL from_python", [&](nb::handle key, nb::handle value) {
                const auto index = nb::cast<std::size_t>(key);
                if (index >= seams::fixed_element_count(context)) { throw std::out_of_range("fixed TSL from_python index out of range"); }
                touched |= fixed_child_update_from_python(context, memory, index, value, modified_time);
            });
            return touched;
        }

        [[nodiscard]] bool fixed_bundle_from_python(const void *context, void *memory, nb::handle source,
                                                    DateTime modified_time)
        {
            require_source(memory, source, modified_time, "fixed TSData from_python");
            const bool first_for_parent = seams::fixed_tracking(context, memory).last_modified_time != modified_time;
            const bool touched          = fixed_from_python_bundle(context, memory, source, modified_time);
            return first_for_parent && touched;
        }

        [[nodiscard]] bool fixed_list_from_python(const void *context, void *memory, nb::handle source,
                                                  DateTime modified_time)
        {
            require_source(memory, source, modified_time, "fixed TSData from_python");
            const bool first_for_parent = seams::fixed_tracking(context, memory).last_modified_time != modified_time;
            const bool touched = python_has_items(source)
                                     ? fixed_from_python_list_mapping(context, memory, source, modified_time)
                                     : fixed_from_python_sequence(context, memory, source, modified_time,
                                                                  "fixed TSL from_python");
            return first_for_parent && touched;
        }

        // -- dynamic TSL ---------------------------------------------------------------------
        /** Either REMOVE sentinel. Truncation is total, so hgraph's strict /
            lenient distinction has nothing to express for a list. */
        [[nodiscard]] bool is_removal_sentinel(nb::handle item) noexcept
        {
            const auto &strict = removed_sentinel_slot();
            if (strict.is_valid() && item.is(strict)) { return true; }
            const auto &lenient = remove_if_exists_sentinel_slot();
            return lenient.is_valid() && item.is(lenient);
        }

        [[nodiscard]] nb::object dynamic_value_projection_to_python(const void *context, const void *memory)
        {
            return to_python(Value{ValueView{seams::dynamic_layout(context).value_binding, memory}});
        }

        [[nodiscard]] nb::object dynamic_delta_projection_to_python(const void *context, const void *memory)
        {
            return to_python(Value{ValueView{seams::dynamic_layout(context).delta_binding, memory}});
        }

        [[nodiscard]] nb::object dynamic_delta_key_set_projection_to_python(const void *context, const void *memory)
        {
            return to_python(Value{ValueView{seams::dynamic_delta_key_set_binding(context), memory}});
        }

        [[nodiscard]] nb::object dynamic_removed_set_projection_to_python(const void *context, const void *memory)
        {
            return to_python(Value{ValueView{seams::dynamic_removed_set_binding(context), memory}});
        }

        [[nodiscard]] nb::object dynamic_delta_bundle_to_python(const void *context, const void *memory)
        {
            nb::dict result;
            result[nb::str{"removed"}]  = to_python(seams::dynamic_removed_set_binding(context), memory);
            result[nb::str{"modified"}] = to_python(seams::dynamic_modified_map_binding(context), memory);
            return result;
        }

        /** Dynamic TSL Python export is a TSData strategy. It deliberately
            recurses through child TSDataOps; the public facade must never
            infer this representation from TSTypeKind. */
        [[nodiscard]] nb::object dynamic_to_python(const void *context, const void *memory)
        {
            const auto &ops = seams::dynamic_element_type(context).ops_ref();
            nb::list    result;
            for (std::size_t index = 0; index < seams::dynamic_size(memory); ++index)
            {
                const auto *child = seams::dynamic_child_memory(memory, index);
                result.append(ops.has_current_value_impl(ops.context, child) ? take(ops.to_python_impl(ops.context, child))
                                                                             : nb::none());
            }
            return nb::tuple(result);
        }

        [[nodiscard]] nb::object dynamic_delta_to_python(const void *context, const void *memory, DateTime evaluation_time)
        {
            if (seams::dynamic_tracking(memory).last_modified_time != evaluation_time) { return nb::none(); }
            const auto &ops = seams::dynamic_element_type(context).ops_ref();
            nb::dict    modified;
            for (std::size_t ordinal = 0; ordinal < seams::dynamic_modified_index_count(memory); ++ordinal)
            {
                const auto  index = seams::dynamic_modified_index_at(memory, ordinal);
                const auto *child = seams::dynamic_child_memory(memory, index);
                nb::object  value = take(ops.delta_to_python_impl(ops.context, child, evaluation_time));
                if (!value.is_none()) { modified[nb::int_{index}] = std::move(value); }
            }
            // RFC 0031: the canonical {removed, modified} shape, which
            // hgraph's _simplify_delta rewrites into the friendly
            // {index: delta, removed_index: REMOVE} form.
            nb::dict result;
            result[nb::str{"removed"}]  = to_python(seams::dynamic_removed_set_binding(context), memory);
            result[nb::str{"modified"}] = std::move(modified);
            return result;
        }

        /** Import current/replacement values through the child strategies.
            A sequence IS the list: it covers consecutive indices and
            resizes, so a shorter sequence truncates (RFC 0031). A mapping
            is a sparse replacement/update in which a ``REMOVE`` sentinel
            truncates to the lowest removed index. */
        [[nodiscard]] bool dynamic_from_python(const void *context, void *memory, nb::handle source, DateTime modified_time)
        {
            require_source(memory, source, modified_time, "dynamic TSL from_python");
            const auto &ops              = seams::dynamic_element_type(context).ops_ref();
            const bool  first_for_parent = seams::dynamic_tracking(memory).last_modified_time != modified_time;
            bool        touched          = false;

            const auto update = [&](std::size_t index, nb::handle item) {
                seams::dynamic_ensure_size(context, memory, index + 1, modified_time);
                void *child = seams::dynamic_mutable_child_memory(memory, index);
                if (!ops.from_python_impl(ops.context, child, borrow(item), modified_time)) { return; }
                auto *tracking = ops.mutable_tracking_impl(ops.context, child);
                if (tracking == nullptr || !tracking->record_modified(modified_time))
                {
                    throw std::logic_error("dynamic TSL child reported an invalid modification");
                }
                seams::dynamic_record_child_modified(memory, index, modified_time);
                touched = true;
            };

            if (nb::isinstance<nb::dict>(source))
            {
                // Removal is resolved FIRST so a same-cycle re-grow through
                // `modified` behaves exactly as apply_delta does.
                auto truncate_to = static_cast<std::size_t>(-1);
                for (auto [key, item] : nb::cast<nb::dict>(source))
                {
                    if (item.is_none() || !is_removal_sentinel(item)) { continue; }
                    const auto index = nb::cast<std::int64_t>(key);
                    if (index < 0) { throw std::out_of_range("dynamic TSL from_python index must be non-negative"); }
                    truncate_to = std::min(truncate_to, static_cast<std::size_t>(index));
                }
                if (truncate_to < seams::dynamic_size(memory))
                {
                    seams::dynamic_resize(context, memory, truncate_to, modified_time);
                    touched = true;
                }
                for (auto [key, item] : nb::cast<nb::dict>(source))
                {
                    if (item.is_none() || is_removal_sentinel(item)) { continue; }
                    const auto index = nb::cast<std::int64_t>(key);
                    if (index < 0) { throw std::out_of_range("dynamic TSL from_python index must be non-negative"); }
                    update(static_cast<std::size_t>(index), item);
                }
                return first_for_parent && touched;
            }

            if (nb::isinstance<nb::str>(source) || !nb::isinstance<nb::sequence>(source))
            {
                throw std::invalid_argument("dynamic TSL from_python expects a mapping or sequence");
            }
            const auto source_size = static_cast<std::size_t>(nb::len(source));
            if (source_size != seams::dynamic_size(memory))
            {
                seams::dynamic_resize(context, memory, source_size, modified_time);
                touched = true;
            }
            std::size_t index = 0;
            for (nb::handle item : source)
            {
                if (!item.is_none()) { update(index, item); }
                ++index;
            }
            return first_for_parent && touched;
        }

        // -- slot-backed TSS -----------------------------------------------------------------
        [[nodiscard]] nb::object tss_to_python(const void *context, const void *memory)
        {
            return to_python(seams::tss_layout(context).value_binding, memory);
        }

        [[nodiscard]] nb::object tss_delta_to_python(const void *context, const void *memory, DateTime evaluation_time)
        {
            if (seams::tss_tracking(memory).last_modified_time != evaluation_time) { return nb::none(); }
            return to_python(seams::tss_layout(context).delta_binding, memory);
        }

        [[nodiscard]] bool tss_from_python(const void *context, void *memory, nb::handle source, DateTime modified_time)
        {
            require_source(memory, source, modified_time, "TSS from_python");
            const auto key_binding = seams::tss_layout(context).key_binding;

            nb::object added;
            nb::object removed;
            const bool has_added   = python_named_field(source, "added", added);
            const bool has_removed = python_named_field(source, "removed", removed);
            if (has_added || has_removed)
            {
                const bool first_for_parent = seams::tss_tracking(memory).last_modified_time != modified_time;
                if (has_added && !added.is_none())
                {
                    for_each_python_iterable(added, "TSS added update", [&](nb::handle item) {
                        Value key = value_from_python(key_binding, item, "TSS added update");
                        seams::tss_insert_key(memory, key.view(), modified_time);
                    });
                }
                if (has_removed && !removed.is_none())
                {
                    for_each_python_iterable(removed, "TSS removed update", [&](nb::handle item) {
                        Value key = value_from_python(key_binding, item, "TSS removed update");
                        seams::tss_remove_key(memory, key.view(), modified_time);
                    });
                }
                return first_for_parent;
            }

            nb::object object = nb::borrow<nb::object>(source);
            if (!nb::isinstance<nb::set>(object) && !nb::isinstance<nb::frozenset>(object) &&
                !nb::isinstance<nb::list>(object) && !nb::isinstance<nb::tuple>(object))
            {
                throw std::invalid_argument("TSS from_python expects a Python set, frozenset, list, or tuple");
            }

            std::vector<Value> replacement;
            if (nb::hasattr(object, "__len__")) { replacement.reserve(static_cast<std::size_t>(nb::len(object))); }
            for_each_python_iterable(source, "TSS value", [&](nb::handle item) {
                replacement.push_back(value_from_python(key_binding, item, "TSS value"));
            });

            const bool newly_touched = seams::tss_touch(memory, modified_time);
            for (const auto &key : replacement) { seams::tss_insert_key(memory, key.view(), modified_time); }

            const auto        &key_ops = key_binding.ops_ref();
            std::vector<Value> removals;
            for (const auto key : seams::tss_keys(context, memory, seams::SetSurface::live))
            {
                const bool keep = std::any_of(replacement.begin(), replacement.end(), [&](const Value &candidate) {
                    return key_ops.equals(key.data(), candidate.view().data());
                });
                if (!keep) { removals.emplace_back(key); }
            }
            for (const auto &key : removals) { seams::tss_remove_key(memory, key.view(), modified_time); }
            return newly_touched;
        }

        template <seams::SetSurface Surface>
        [[nodiscard]] nb::object tss_set_to_python(const void *context, const void *memory)
        {
            const auto &ops = seams::tss_layout(context).key_binding.ops_ref();
            nb::set     result;
            for (const auto key : seams::tss_keys(context, memory, Surface)) { result.add(to_python(ops, key.data())); }
            return result;
        }

        [[nodiscard]] nb::object tss_delta_bundle_to_python(const void *context, const void *memory)
        {
            nb::dict result;
            result[nb::str{"added"}]   = to_python(seams::tss_added_set_binding(context), memory);
            result[nb::str{"removed"}] = to_python(seams::tss_removed_set_binding(context), memory);
            return result;
        }

        // -- slot-backed TSD -----------------------------------------------------------------
        [[nodiscard]] nb::object tsd_to_python(const void *context, const void *memory)
        {
            const auto &layout    = seams::tsd_layout(context);
            const auto &key_ops   = layout.key_binding.ops_ref();
            const auto &child_ops = layout.element_type.ops_ref();
            nb::dict    result;
            for (std::size_t slot = 0; slot < seams::tsd_slot_capacity(memory); ++slot)
            {
                if (!seams::tsd_slot_in_surface(memory, slot, seams::MapSurface::live)) { continue; }
                const auto *child = seams::tsd_child_at_slot(memory, slot);
                if (!child_ops.has_current_value_impl(child_ops.context, child)) { continue; }
                result[to_python(key_ops, seams::tsd_key_at_slot(memory, slot))] =
                    take(child_ops.to_python_impl(child_ops.context, child));
            }
            return result;
        }

        [[nodiscard]] nb::object tsd_delta_to_python(const void *context, const void *memory, DateTime evaluation_time)
        {
            if (seams::tsd_tracking(memory).last_modified_time != evaluation_time) { return nb::none(); }
            const auto &layout    = seams::tsd_layout(context);
            const auto &key_ops   = layout.key_binding.ops_ref();
            const auto &child_ops = layout.element_type.ops_ref();
            nb::dict    modified;
            for (std::size_t slot = 0; slot < seams::tsd_slot_capacity(memory); ++slot)
            {
                if (!seams::tsd_slot_in_surface(memory, slot, seams::MapSurface::modified)) { continue; }
                const auto *child = seams::tsd_child_at_slot(memory, slot);
                nb::object  delta = take(child_ops.delta_to_python_impl(child_ops.context, child, evaluation_time));
                if (!delta.is_none()) { modified[to_python(key_ops, seams::tsd_key_at_slot(memory, slot))] = std::move(delta); }
            }
            nb::dict result;
            result[nb::str{"removed"}]  = to_python(seams::tsd_removed_set_binding(context), memory);
            result[nb::str{"modified"}] = std::move(modified);
            return result;
        }

        [[nodiscard]] bool tsd_from_python(const void *context, void *memory, nb::handle source, DateTime modified_time)
        {
            require_source(memory, source, modified_time, "TSD from_python");
            if (!python_has_items(source)) { throw std::invalid_argument("TSD from_python expects a Python mapping"); }
            const auto &layout    = seams::tsd_layout(context);
            const auto &child_ops = layout.element_type.ops_ref();

            nb::object removed;
            nb::object modified;
            const bool has_removed  = python_named_field(source, "removed", removed);
            const bool has_modified = python_named_field(source, "modified", modified);
            if (has_removed || has_modified)
            {
                const bool first_for_parent = seams::tsd_tracking(memory).last_modified_time != modified_time;
                if (has_removed && !removed.is_none())
                {
                    for_each_python_iterable(removed, "TSD removed update", [&](nb::handle item) {
                        Value key = value_from_python(layout.key_binding, item, "TSD removed update");
                        seams::tsd_remove_key(memory, key.view(), modified_time);
                    });
                }

                if (has_modified && !modified.is_none())
                {
                    for_each_python_mapping_item(modified, "TSD modified update",
                                                 [&](nb::handle key_source, nb::handle value_source) {
                        if (value_source.is_none()) { return; }

                        Value      key    = value_from_python(layout.key_binding, key_source, "TSD modified update key");
                        const auto result = seams::tsd_insert_key(memory, key.view(), modified_time);

                        auto rollback_insert = make_scope_exit<true>([&] {
                            if (result.changed) { seams::tsd_remove_key(memory, key.view(), modified_time); }
                        });

                        void *child_memory = seams::tsd_child_memory_for_write(memory, result.slot);
                        if (!import_child(child_ops, child_memory, value_source, modified_time, "TSD")) { return; }
                        seams::tsd_record_child_modified(memory, result.slot, modified_time);
                        rollback_insert.release();
                    });
                }
                return first_for_parent;
            }

            std::vector<std::pair<Value, nb::object>> entries;
            for_each_python_mapping_item(source, "TSD value", [&](nb::handle key_source, nb::handle value_source) {
                if (value_source.is_none()) { throw std::invalid_argument("TSD from_python does not allow None child values"); }
                entries.emplace_back(value_from_python(layout.key_binding, key_source, "TSD value key"),
                                     nb::borrow<nb::object>(value_source));
            });

            const bool newly_touched = seams::tsd_touch(memory, modified_time);
            for (const auto &[key, value_source] : entries)
            {
                const auto result       = seams::tsd_insert_key(memory, key.view(), modified_time);
                void      *child_memory = seams::tsd_child_memory_for_write(memory, result.slot);
                if (import_child(child_ops, child_memory, value_source, modified_time, "TSD"))
                {
                    seams::tsd_record_child_modified(memory, result.slot, modified_time);
                }
            }

            const auto        &key_ops = layout.key_binding.ops_ref();
            std::vector<Value> removals;
            for (const auto key : seams::tsd_keys(context, memory, seams::SetSurface::live))
            {
                const bool keep = std::any_of(entries.begin(), entries.end(), [&](const auto &entry) {
                    return key_ops.equals(key.data(), entry.first.view().data());
                });
                if (!keep) { removals.emplace_back(key); }
            }
            for (const auto &key : removals) { seams::tsd_remove_key(memory, key.view(), modified_time); }
            return newly_touched;
        }

        template <seams::MapSurface Surface>
        [[nodiscard]] nb::object tsd_map_to_python(const void *context, const void *memory)
        {
            const auto &key_ops       = seams::tsd_layout(context).key_binding.ops_ref();
            const auto  value_binding = seams::tsd_map_value_binding(context, memory, Surface);
            const auto &value_ops     = value_binding.ops_ref();
            nb::dict    result;
            for (const auto [key, value] : seams::tsd_map_items(context, memory, Surface))
            {
                result[to_python(key_ops, key.data())] = value.valid() ? to_python(value_ops, value.data()) : nb::none();
            }
            return result;
        }

        [[nodiscard]] nb::object tsd_map_key_set_to_python(const void *context, const void *memory)
        {
            const auto &ops = seams::tsd_layout(context).key_binding.ops_ref();
            nb::set     result;
            for (const auto key : seams::tsd_keys(context, memory, seams::SetSurface::live)) { result.add(to_python(ops, key.data())); }
            return result;
        }

        [[nodiscard]] nb::object tsd_dict_delta_to_python(const void *context, const void *memory)
        {
            nb::dict result;
            result[nb::str{"removed"}]  = to_python(seams::tsd_removed_set_binding(context), memory);
            result[nb::str{"modified"}] = to_python(seams::tsd_modified_map_binding(context), memory);
            return result;
        }

        // -- the TSD proxy ---------------------------------------------------------------------
        [[nodiscard]] nb::object proxy_delta_projection_to_python(const void *context, const void *memory)
        {
            return to_python(Value{ValueView{seams::proxy_layout(context).delta_binding, memory}});
        }

        template <seams::SetSurface Surface>
        [[nodiscard]] nb::object proxy_set_to_python(const void *context, const void *memory)
        {
            nb::list items;
            for (std::size_t slot = 0; slot < seams::proxy_slot_capacity(memory); ++slot)
            {
                if (!seams::proxy_slot_in_set_surface<Surface>(context, memory, slot)) { continue; }
                auto key = seams::proxy_key_at_slot(memory, slot);
                items.append(to_python(key.binding(), key.data()));
            }
            return nb::steal(PyFrozenSet_New(items.ptr()));
        }

        [[nodiscard]] nb::object proxy_key_set_to_python(const void *context, const void *memory)
        {
            const auto &layout = seams::proxy_layout(context);
            nb::set     result;
            for (const auto key : seams::proxy_keys<seams::SetSurface::live>(context, memory))
            {
                result.add(to_python(layout.key_binding, key.data()));
            }
            return result;
        }

        [[nodiscard]] nb::object proxy_key_set_delta_to_python(const void *context, const void *memory,
                                                               DateTime evaluation_time)
        {
            if (seams::proxy_key_set_tracking(memory).last_modified_time != evaluation_time) { return nb::none(); }
            nb::dict result;
            result[nb::str{"added"}]   = proxy_set_to_python<seams::SetSurface::added>(context, memory);
            result[nb::str{"removed"}] = proxy_set_to_python<seams::SetSurface::removed>(context, memory);
            return result;
        }

        /** TSDProxy is a concrete TSData representation. Python export
            therefore lives here and follows each child through its own
            erased TSDataOps instead of leaking proxy layout to a facade. */
        [[nodiscard]] nb::object proxy_dict_to_python(const void *context, const void *memory)
        {
            const auto &child_ops = seams::proxy_layout(context).element_type.ops_ref();
            nb::dict    result;
            for (std::size_t slot = 0; slot < seams::proxy_slot_capacity(memory); ++slot)
            {
                if (!seams::proxy_slot_live(memory, slot) || !seams::proxy_has_child(memory, slot)) { continue; }
                const auto  key   = seams::proxy_key_at_slot(memory, slot);
                const auto *child = seams::proxy_child_at_slot(memory, slot);
                if (!child_ops.has_current_value_impl(child_ops.context, child)) { continue; }
                result[to_python(key.binding(), key.data())] = take(child_ops.to_python_impl(child_ops.context, child));
            }
            return result;
        }

        [[nodiscard]] nb::object proxy_dict_delta_to_python(const void *context, const void *memory, DateTime evaluation_time)
        {
            if (seams::proxy_tracking(memory).last_modified_time != evaluation_time) { return nb::none(); }
            const auto &child_ops = seams::proxy_layout(context).element_type.ops_ref();
            nb::dict    modified;
            for (std::size_t slot = 0; slot < seams::proxy_slot_capacity(memory); ++slot)
            {
                if (!seams::proxy_slot_modified(context, memory, slot) || !seams::proxy_has_child(memory, slot)) { continue; }
                const auto  key   = seams::proxy_key_at_slot(memory, slot);
                const auto *child = seams::proxy_child_at_slot(memory, slot);
                nb::object  delta = take(child_ops.delta_to_python_impl(child_ops.context, child, evaluation_time));
                if (!delta.is_none()) { modified[to_python(key.binding(), key.data())] = std::move(delta); }
            }
            nb::dict result;
            result[nb::str{"removed"}]  = proxy_set_to_python<seams::SetSurface::removed>(context, memory);
            result[nb::str{"modified"}] = std::move(modified);
            return result;
        }

        /** Surface read-back for python (type-erasure rule: conversion
            binds to the ops). Children are read through their TS views -
            link-endpoint children resolve to their targets. */
        template <seams::ProxyMapSurface Surface>
        [[nodiscard]] nb::object proxy_map_to_python(const void *context, const void *memory)
        {
            const auto value_type = seams::proxy_map_value_binding<Surface>(context, memory);
            nb::dict   result;
            for (std::size_t slot = 0; slot < seams::proxy_slot_capacity(memory); ++slot)
            {
                if (!seams::proxy_slot_in_map_surface<Surface>(context, memory, slot) || !seams::proxy_has_child(memory, slot))
                {
                    continue;
                }
                const auto *value_memory = seams::proxy_map_value_at_slot<Surface>(context, memory, slot);
                auto        key          = seams::proxy_key_at_slot(memory, slot);
                const auto  py_key       = to_python(key.binding(), key.data());
                result[py_key] = value_memory != nullptr ? to_python(value_type, value_memory) : nb::none();
            }
            return result;
        }
    }  // namespace

    void fill_structured_ts_data_conversions(PythonOps::TSData &section) noexcept
    {
        section.fixed_bundle_from_python                   = &ts_from_python_slot<&fixed_bundle_from_python>;
        section.fixed_bundle_to_python                     = &to_python_slot<&fixed_bundle_to_python>;
        section.fixed_bundle_delta_to_python               = &ts_delta_to_python_slot<&fixed_bundle_delta_to_python>;
        section.fixed_list_from_python                     = &ts_from_python_slot<&fixed_list_from_python>;
        section.fixed_list_to_python                       = &to_python_slot<&fixed_list_to_python>;
        section.fixed_list_delta_to_python                 = &ts_delta_to_python_slot<&fixed_list_delta_to_python>;
        section.fixed_value_to_python                      = &to_python_slot<&fixed_value_to_python>;
        section.fixed_delta_bundle_to_python               = &to_python_slot<&fixed_delta_bundle_to_python>;
        section.fixed_delta_map_to_python                  = &to_python_slot<&fixed_delta_map_to_python>;
        section.fixed_delta_key_set_to_python              = &to_python_slot<&fixed_delta_key_set_to_python>;
        section.dynamic_from_python                        = &ts_from_python_slot<&dynamic_from_python>;
        section.dynamic_to_python                          = &to_python_slot<&dynamic_to_python>;
        section.dynamic_delta_to_python                    = &ts_delta_to_python_slot<&dynamic_delta_to_python>;
        section.dynamic_value_projection_to_python         = &to_python_slot<&dynamic_value_projection_to_python>;
        section.dynamic_delta_projection_to_python         = &to_python_slot<&dynamic_delta_projection_to_python>;
        section.dynamic_delta_key_set_projection_to_python = &to_python_slot<&dynamic_delta_key_set_projection_to_python>;
        section.dynamic_removed_set_projection_to_python   = &to_python_slot<&dynamic_removed_set_projection_to_python>;
        section.dynamic_delta_bundle_to_python             = &to_python_slot<&dynamic_delta_bundle_to_python>;
        section.tss_from_python                            = &ts_from_python_slot<&tss_from_python>;
        section.tss_to_python                              = &to_python_slot<&tss_to_python>;
        section.tss_delta_to_python                        = &ts_delta_to_python_slot<&tss_delta_to_python>;
        section.tss_delta_bundle_to_python                 = &to_python_slot<&tss_delta_bundle_to_python>;
        section.tss_set_live_to_python                     = &to_python_slot<&tss_set_to_python<seams::SetSurface::live>>;
        section.tss_set_added_to_python                    = &to_python_slot<&tss_set_to_python<seams::SetSurface::added>>;
        section.tss_set_removed_to_python                  = &to_python_slot<&tss_set_to_python<seams::SetSurface::removed>>;
        section.tsd_from_python                            = &ts_from_python_slot<&tsd_from_python>;
        section.tsd_to_python                              = &to_python_slot<&tsd_to_python>;
        section.tsd_delta_to_python                        = &ts_delta_to_python_slot<&tsd_delta_to_python>;
        section.tsd_map_key_set_to_python                  = &to_python_slot<&tsd_map_key_set_to_python>;
        section.tsd_dict_delta_to_python                   = &to_python_slot<&tsd_dict_delta_to_python>;
        section.tsd_map_live_to_python                     = &to_python_slot<&tsd_map_to_python<seams::MapSurface::live>>;
        section.tsd_map_modified_to_python                 = &to_python_slot<&tsd_map_to_python<seams::MapSurface::modified>>;
        section.proxy_dict_to_python                       = &to_python_slot<&proxy_dict_to_python>;
        section.proxy_dict_delta_to_python                 = &ts_delta_to_python_slot<&proxy_dict_delta_to_python>;
        section.proxy_key_set_to_python                    = &to_python_slot<&proxy_key_set_to_python>;
        section.proxy_key_set_delta_to_python              = &ts_delta_to_python_slot<&proxy_key_set_delta_to_python>;
        section.proxy_delta_projection_to_python           = &to_python_slot<&proxy_delta_projection_to_python>;
        section.proxy_set_live_to_python                   = &to_python_slot<&proxy_set_to_python<seams::SetSurface::live>>;
        section.proxy_set_added_to_python                  = &to_python_slot<&proxy_set_to_python<seams::SetSurface::added>>;
        section.proxy_set_removed_to_python                = &to_python_slot<&proxy_set_to_python<seams::SetSurface::removed>>;
        section.proxy_map_live_to_python                   = &to_python_slot<&proxy_map_to_python<seams::ProxyMapSurface::live>>;
        section.proxy_map_added_to_python                  = &to_python_slot<&proxy_map_to_python<seams::ProxyMapSurface::added>>;
        section.proxy_map_removed_to_python                = &to_python_slot<&proxy_map_to_python<seams::ProxyMapSurface::removed>>;
        section.proxy_map_modified_to_python               = &to_python_slot<&proxy_map_to_python<seams::ProxyMapSurface::modified>>;
    }
}  // namespace hgraph::python_bridge
