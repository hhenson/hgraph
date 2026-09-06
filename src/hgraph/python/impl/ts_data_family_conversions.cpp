#include "python_ops_families.h"

#include "../../types/metadata/detail/ts_data_seams.h"

#include <hgraph/python/bridge_state.h>
#include <hgraph/python/conversion.h>
#include <hgraph/python/retained_value.h>
#include <hgraph/types/value/value.h>

#include <nanobind/nanobind.h>

#include <cstddef>
#include <stdexcept>

/**
 * TSData <-> Python for the atomic and window strategies (RFC 0035, PR 4a).
 * The bodies read the strategies through ``ts_data_seams.h``: bindings and
 * memory from the layouts, the retained-object holder of a cached output,
 * and the window's replace / push seams, which construct each element and
 * hand this unit the payload to convert into.
 */
namespace hgraph::python_bridge
{
    namespace
    {
        // -- atomic TS / SIGNAL / REF, per value-storage variant ------------------
        [[nodiscard]] PythonValueHolder *retained(const void *context, const void *memory) noexcept
        {
            return static_cast<PythonValueHolder *>(
                ts_data_seams::atomic_retained_holder(context, const_cast<void *>(memory)));
        }

        void require_atomic_source(const void *memory, nb::handle source, DateTime modified_time)
        {
            if (memory == nullptr) { throw std::logic_error("TSData atomic from_python requires live TSData memory"); }
            if (source.is_none()) { throw std::invalid_argument("TSData atomic from_python requires a non-None source"); }
            if (modified_time == MIN_DT)
            {
                throw std::invalid_argument("TSData atomic from_python requires a concrete evaluation time");
            }
        }

        [[nodiscard]] bool atomic_native_from_python(const void *context, void *memory, nb::handle source,
                                                     DateTime modified_time)
        {
            require_atomic_source(memory, source, modified_time);
            const auto &layout         = ts_data_seams::atomic_layout(context);
            const bool  first_for_time = ts_data_seams::atomic_tracking(context, memory).last_modified_time != modified_time;
            from_python(layout.value_binding, ts_data_seams::atomic_mutable_value_memory(context, memory), source);
            return first_for_time;
        }

        [[nodiscard]] bool atomic_cached_from_python(const void *context, void *memory, nb::handle source,
                                                     DateTime modified_time)
        {
            require_atomic_source(memory, source, modified_time);
            const auto &layout         = ts_data_seams::atomic_layout(context);
            const bool  first_for_time = ts_data_seams::atomic_tracking(context, memory).last_modified_time != modified_time;
            nb::object  retained_value = prepare_python_storage_value(layout.value_binding.schema(), source);
            from_python(layout.value_binding, ts_data_seams::atomic_mutable_value_memory(context, memory), source);
            retained(context, memory)->set(retained_value);
            return first_for_time;
        }

        [[nodiscard]] nb::object atomic_native_to_python(const void *context, const void *memory)
        {
            const auto &layout = ts_data_seams::atomic_layout(context);
            // Scalar Python representation is already owned by the selected
            // ValueOps strategy (notably compact Set -> frozenset). Atomic
            // TSData forwards that erased result without re-normalizing it.
            return to_python(layout.value_binding, ts_data_seams::atomic_value_memory(context, memory));
        }

        [[nodiscard]] nb::object atomic_cached_to_python(const void *context, const void *memory)
        {
            if (const auto *cached = retained(context, memory); cached != nullptr && cached->has_value())
            {
                return cached->get();
            }
            nb::object converted = atomic_native_to_python(context, memory);
            if (auto *cached = retained(context, memory); cached != nullptr) { cached->set(converted); }
            return converted;
        }

        [[nodiscard]] nb::object atomic_native_delta_to_python(const void *context, const void *memory,
                                                               DateTime evaluation_time)
        {
            if (ts_data_seams::atomic_tracking(context, memory).last_modified_time != evaluation_time) { return nb::none(); }
            const auto &layout = ts_data_seams::atomic_layout(context);
            return to_python(layout.delta_binding, ts_data_seams::atomic_delta_memory(context, memory));
        }

        [[nodiscard]] nb::object atomic_python_only_delta_to_python(const void *context, const void *memory,
                                                                    DateTime evaluation_time)
        {
            if (ts_data_seams::atomic_tracking(context, memory).last_modified_time != evaluation_time) { return nb::none(); }
            return atomic_native_to_python(context, memory);
        }

        [[nodiscard]] nb::object atomic_cached_delta_to_python(const void *context, const void *memory,
                                                               DateTime evaluation_time)
        {
            if (ts_data_seams::atomic_tracking(context, memory).last_modified_time != evaluation_time) { return nb::none(); }
            return atomic_cached_to_python(context, memory);
        }

        void invalidate_retained_holder(void *holder) noexcept { static_cast<PythonValueHolder *>(holder)->clear(); }

        // -- TSW windows --------------------------------------------------------------
        [[nodiscard]] bool is_python_sequence(nb::handle source)
        {
            nb::object object = nb::borrow<nb::object>(source);
            return nb::isinstance<nb::list>(object) || nb::isinstance<nb::tuple>(object);
        }

        struct SequenceFill
        {
            nb::object sequence;
        };

        void fill_element_from_sequence(void *fill_context, std::size_t index, ValueTypeRef element_binding, void *payload)
        {
            const auto &fill = *static_cast<const SequenceFill *>(fill_context);
            nb::object  item = fill.sequence[index];
            if (item.is_none()) { throw std::invalid_argument("TSW value does not allow None elements"); }
            from_python(element_binding, payload, item);
        }

        void fill_element_from_handle(void *fill_context, std::size_t, ValueTypeRef element_binding, void *payload)
        {
            from_python(element_binding, payload, *static_cast<const nb::handle *>(fill_context));
        }

        [[nodiscard]] nb::object window_to_python(const void *context, const void *memory)
        {
            return to_python(ts_data_seams::window_layout(context).value_binding,
                             ts_data_seams::window_value_memory(context, memory));
        }

        [[nodiscard]] nb::object window_delta_to_python(const void *context, const void *memory, DateTime evaluation_time)
        {
            if (ts_data_seams::window_tracking(context, memory).last_modified_time != evaluation_time) { return nb::none(); }
            const auto *delta = ts_data_seams::window_delta_memory(context, memory);
            if (delta == nullptr) { return nb::none(); }
            return to_python(ts_data_seams::window_layout(context).delta_binding, delta);
        }

        [[nodiscard]] bool window_from_python(const void *context, void *memory, nb::handle source, DateTime modified_time)
        {
            if (memory == nullptr) { throw std::logic_error("TSW from_python requires live storage"); }
            if (source.is_none()) { throw std::invalid_argument("TSW from_python requires a non-None source"); }
            if (modified_time == MIN_DT) { throw std::invalid_argument("TSW from_python requires a concrete evaluation time"); }

            const bool newly_modified = ts_data_seams::window_tracking(context, memory).last_modified_time != modified_time;
            if (is_python_sequence(source))
            {
                SequenceFill fill{nb::borrow<nb::object>(source)};
                ts_data_seams::window_replace(context, memory, modified_time, static_cast<std::size_t>(nb::len(fill.sequence)),
                                              &fill_element_from_sequence, &fill);
                return newly_modified;
            }

            if (!newly_modified) { throw std::logic_error("TSW from_python allows only one window tick per evaluation time"); }
            ts_data_seams::window_push(context, memory, modified_time, &fill_element_from_handle, &source);
            return true;
        }

        [[nodiscard]] const void *window_buffer_element_at(const void *owner, std::size_t index)
        {
            const auto *pair = static_cast<const std::pair<const void *, const void *> *>(owner);
            return ts_data_seams::window_storage_element_at(pair->first, pair->second, index);
        }

        [[nodiscard]] nb::object window_value_to_python(const void *context, const void *memory)
        {
            if (memory == nullptr) { throw std::runtime_error("TSW value to_python requires live storage"); }
            const auto  binding = ts_data_seams::window_layout(context).element_binding;
            const auto &ops     = binding.ops_ref();
            const auto  size    = ts_data_seams::window_storage_size(context, memory);
            if (can_to_python_buffer(ops, binding))
            {
                const std::pair<const void *, const void *> owner{context, memory};
                return to_python_buffer(ops, binding,
                                        ValueArraySource{
                                            .owner      = &owner,
                                            .size       = size,
                                            .element_at = &window_buffer_element_at,
                                        });
            }

            nb::list result;
            for (std::size_t index = 0; index < size; ++index)
            {
                result.append(to_python(ops, ts_data_seams::window_storage_element_at(context, memory, index)));
            }
            return result;
        }
    }  // namespace

    void fill_ts_data_conversions(PythonOps::TSData &section) noexcept
    {
        section.atomic_native_from_python            = &ts_from_python_slot<&atomic_native_from_python>;
        section.atomic_native_to_python              = &to_python_slot<&atomic_native_to_python>;
        section.atomic_native_delta_to_python        = &ts_delta_to_python_slot<&atomic_native_delta_to_python>;
        // A Python-only output's binding IS the retained object: its value
        // slot converts straight through, like the native variant does.
        section.atomic_python_only_from_python       = &ts_from_python_slot<&atomic_native_from_python>;
        section.atomic_python_only_to_python         = &to_python_slot<&atomic_native_to_python>;
        section.atomic_python_only_delta_to_python   = &ts_delta_to_python_slot<&atomic_python_only_delta_to_python>;
        section.atomic_cached_from_python            = &ts_from_python_slot<&atomic_cached_from_python>;
        section.atomic_cached_to_python              = &to_python_slot<&atomic_cached_to_python>;
        section.atomic_cached_delta_to_python        = &ts_delta_to_python_slot<&atomic_cached_delta_to_python>;
        section.window_from_python                   = &ts_from_python_slot<&window_from_python>;
        section.window_to_python                     = &to_python_slot<&window_to_python>;
        section.window_delta_to_python               = &ts_delta_to_python_slot<&window_delta_to_python>;
        section.window_value_to_python               = &to_python_slot<&window_value_to_python>;
    }

    void fill_retained_conversions(PythonOps::Retained &section) noexcept
    {
        section.invalidate = &invalidate_retained_holder;
    }
}  // namespace hgraph::python_bridge
