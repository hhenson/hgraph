#include "checkpoint_signature.h"
#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/nested_graph_storage.h>
#include <hgraph/runtime/ordered_reduce_node.h>
#include <hgraph/runtime/node_checkpoint.h>
#include <hgraph/manifest/canonical.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

#include "reduce_output_binding.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <set>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view ordered_reduce_storage_field_name{"ordered_reduce"};

        struct OrderedReduceStorage;

        /** Identifies a link to the schedule observer of its graph. */
        struct OrderedReduceLinkSchedule
        {
            OrderedReduceStorage *storage{nullptr};
            std::size_t index{0};
        };

        struct OrderedReduceEntry
        {
            GraphValue graph{};
            OrderedReduceLinkSchedule schedule{};
            DateTime future_when{MAX_DT};
            // What this link of the chain publishes, resolved when it is bound:
            // the combiner's terminal, or -- for a combiner that returns one of
            // its arguments -- the accumulator or element it passes through.
            // Cached so that binding link n never walks links 0..n-1.
            TSOutputHandle output{};
        };

        /**
         * The chain: link ``i`` combines the output of link ``i - 1`` (the zero
         * for link 0) with element ``i``, so the input is an ordered list and
         * only its TAIL can change. Growing constructs and starts the new links
         * alone; shrinking stops the removed ones alone. No other link is
         * rebuilt, rebound or restarted, so its state survives -- which is the
         * contract of the reference implementation (hgraph 0.5,
         * ``PythonTsdNonAssociativeReduceNodeImpl._extend_nodes_to`` /
         * ``_erase_nodes_from``).
         */
        struct OrderedReduceStorage
        {
            OrderedReduceStorage() = default;
            OrderedReduceStorage(const OrderedReduceStorage &) = delete;
            OrderedReduceStorage &operator=(const OrderedReduceStorage &) = delete;
            OrderedReduceStorage(OrderedReduceStorage &&) = delete;
            OrderedReduceStorage &operator=(OrderedReduceStorage &&) = delete;

            ~OrderedReduceStorage()
            {
                for (std::size_t index = std::max(live_count, retired_end); index-- > 0;) { entries.destroy_at(index); }
            }

            void initialise(MemoryUtils::StorageLayout graph_layout) { entries.bind_graph_layout(graph_layout); }

            /** Stop links ``[from, to)``, last first. */
            void stop_links(std::size_t from, std::size_t to)
            {
                FirstExceptionRecorder errors;
                for (std::size_t index = to; index-- > from;)
                {
                    auto *entry = entries.entry_at(index);
                    if (entry != nullptr && entry->graph.has_value() && entry->graph.view().started())
                    {
                        errors.capture([&] { entry->graph.view().stop(); });
                    }
                    clear_future(index);
                }
                errors.rethrow_if_any();
            }

            /** A truncated tail is stopped at once but destroyed only after the
                cycle that removed it: the node's output, and anything sampled
                this cycle, may still be reading its last link. */
            void destroy_retired_before(DateTime evaluation_time) noexcept
            {
                if (retired_end <= live_count || retired_time >= evaluation_time) { return; }
                for (std::size_t index = retired_end; index-- > live_count;) { entries.destroy_at(index); }
                retired_end = 0;
                retired_time = MIN_DT;
            }

            // At most one future deadline per live link. Unlike lazy heap
            // duplicates, this stays O(live links) across input ticks, timer
            // replacement and slot reuse. Updating a deadline costs O(log N).
            void clear_future(std::size_t index)
            {
                auto *entry = entries.entry_at(index);
                if (entry == nullptr || entry->future_when == MAX_DT) { return; }
                future.erase({entry->future_when, index});
                entry->future_when = MAX_DT;
            }

            void set_future(std::size_t index, DateTime when)
            {
                auto &entry = *entries.entry_at(index);
                if (entry.future_when == when) { return; }
                clear_future(index);
                if (when != MAX_DT)
                {
                    future.emplace(when, index);
                    entry.future_when = when;
                }
            }

            void note_schedule(std::size_t index, DateTime when)
            {
                if (when == MAX_DT) { return; }
                if (when <= evaluating_time)
                {
                    due.push_back(index);
                    std::push_heap(due.begin(), due.end(), std::greater<>{});
                }
                else if (when < entries.entry_at(index)->future_when)
                {
                    set_future(index, when);
                }
            }

            /** Move every schedule that has come due onto the due heap. */
            void admit_due(DateTime evaluation_time)
            {
                while (!future.empty() && future.begin()->first <= evaluation_time)
                {
                    const std::size_t index = future.begin()->second;
                    clear_future(index);
                    note_schedule(index, evaluation_time);
                }
            }

            /** The lowest due link, with its duplicates; ``npos`` when none. */
            [[nodiscard]] std::size_t pop_due() noexcept
            {
                if (due.empty()) { return static_cast<std::size_t>(-1); }
                const std::size_t index = due.front();
                while (!due.empty() && due.front() == index)
                {
                    std::pop_heap(due.begin(), due.end(), std::greater<>{});
                    due.pop_back();
                }
                return index;
            }

            void observe(OrderedReduceEntry &entry, std::size_t index)
            {
                entry.schedule = OrderedReduceLinkSchedule{this, index};
                entry.graph.view().set_child_schedule_observer(
                    [](void *raw, DateTime when) {
                        auto &link = *static_cast<OrderedReduceLinkSchedule *>(raw);
                        link.storage->note_schedule(link.index, when);
                    },
                    &entry.schedule);
            }

            InPlaceGraphSlotStore<OrderedReduceEntry> entries{};   // slot == position in the chain
            std::vector<std::size_t> due{};                               // min-heap of link indices
            std::set<std::pair<DateTime, std::size_t>> future{};          // one (when, link) per pending link
            DateTime evaluating_time{MIN_DT};
            std::size_t live_count{0};
            std::size_t retired_end{0};                            // [live_count, retired_end) await destruction
            DateTime retired_time{MIN_DT};
            // The outputs the chain is bound to. A link is rebound only when one
            // of these re-points, never merely because the collection ticked.
            TSOutputHandle collection_source{};
            TSOutputHandle zero_source{};
            bool primed{false};
            bool published{false};
            std::size_t resume_index_plus_one{0};
        };

        constexpr std::size_t unvalidated = static_cast<std::size_t>(-1);

        struct OrderedReduceCollectionOps
        {
            /** The list's length. ``validated`` is the length this same source
                was last seen to have, or ``unvalidated``; it lets a dictionary
                confirm its keys are still 0..n-1 from the change alone. */
            std::size_t (*size)(TSInputView &input, std::size_t validated);
            TSOutputView (*element_output)(TSOutputView source, std::size_t index);
        };

        struct OrderedReduceContext
        {
            OrderedReduceNodeSpec spec{};
            std::size_t storage_offset{0};
            MemoryUtils::StorageLayout graph_layout{};
            const OrderedReduceCollectionOps *collection_ops{nullptr};
        };

        [[nodiscard]] std::vector<std::unique_ptr<OrderedReduceContext>> &ordered_reduce_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<OrderedReduceContext>>;
            return *contexts;
        }

        [[nodiscard]] const OrderedReduceContext *register_ordered_reduce_context(
            OrderedReduceNodeSpec spec,
            std::size_t storage_offset,
            MemoryUtils::StorageLayout graph_layout,
            const OrderedReduceCollectionOps &collection_ops)
        {
            auto context = std::make_unique<OrderedReduceContext>(OrderedReduceContext{
                .spec = std::move(spec),
                .storage_offset = storage_offset,
                .graph_layout = graph_layout,
                .collection_ops = &collection_ops,
            });
            const auto *result = context.get();
            ordered_reduce_contexts().push_back(std::move(context));
            return result;
        }

        void visit_ordered_reduce_child(const void *raw_context,
                                        const NodeBuilder &,
                                        void *visitor_context,
                                        ChildGraphVisitor visitor)
        {
            const auto &context = *static_cast<const OrderedReduceContext *>(raw_context);
            visitor(visitor_context, ChildGraphInspectionView{
                                         .graph = &context.spec.child.graph_builder,
                                         .output_binding = context.spec.child.output_binding
                                                               ? &*context.spec.child.output_binding
                                                               : nullptr,
                                     });
        }

        [[nodiscard]] NodeStorageMetrics ordered_reduce_storage_metrics(
            const void *raw_context, const void *memory) noexcept
        {
            const auto &context = *static_cast<const OrderedReduceContext *>(raw_context);
            const auto &storage = *MemoryUtils::cast<const OrderedReduceStorage>(
                MemoryUtils::advance(memory, context.storage_offset));
            NodeStorageMetrics result{};
            result.nested_graph_count += storage.entries.entry_count();
            result.nested_graph_capacity += storage.entries.slot_capacity();
            result.nested_graph_blocks += storage.entries.block_count();
            result.dynamic_live_bytes += storage.entries.live_bytes();
            result.dynamic_live_bytes += storage.future.size() * sizeof(decltype(storage.future)::value_type);
            result.dynamic_reserved_bytes += storage.entries.reserved_bytes();
            result.dynamic_reserved_bytes += storage.future.size() * sizeof(decltype(storage.future)::value_type);
            return result;
        }

        [[nodiscard]] TSOutputView collection_element_output(
            const NodeView &view,
            const OrderedReduceContext &context,
            std::size_t index,
            DateTime evaluation_time)
        {
            auto source = view.input(evaluation_time).indexed_child_at(0).bound_output();
            return source.bound() ? context.collection_ops->element_output(std::move(source), index)
                                  : TSOutputView{};
        }

        [[nodiscard]] TSOutputView zero_output(const NodeView &view, DateTime evaluation_time)
        {
            return view.input(evaluation_time).indexed_child_at(1).bound_output();
        }

        /** What link ``index`` accumulates onto: the zero, or the link before it. */
        [[nodiscard]] TSOutputView accumulator_source(
            const NodeView &view,
            const OrderedReduceStorage &storage,
            std::size_t index,
            DateTime evaluation_time)
        {
            if (index == 0) { return zero_output(view, evaluation_time); }
            const auto *previous = storage.entries.entry_at(index - 1);
            return previous != nullptr && previous->output.bound() ? previous->output.view(evaluation_time)
                                                                   : TSOutputView{};
        }

        [[nodiscard]] TSOutputView link_output(
            const OrderedReduceStorage &storage, std::size_t index, DateTime evaluation_time)
        {
            const auto *entry = storage.entries.entry_at(index);
            return entry != nullptr && entry->output.bound() ? entry->output.view(evaluation_time) : TSOutputView{};
        }

        [[nodiscard]] std::size_t ordered_key_index(const ValueView &key)
        {
            const Int value = key.checked_as<Int>();
            if (value < 0) { throw std::invalid_argument("ordered reduce requires non-negative integer keys"); }
            const auto unsigned_value = static_cast<std::uint64_t>(value);
            if (unsigned_value >= std::numeric_limits<std::size_t>::max())
            {
                throw std::overflow_error("ordered reduce key does not fit in size_t");
            }
            return static_cast<std::size_t>(unsigned_value);
        }

        [[noreturn]] void not_contiguous()
        {
            throw std::invalid_argument("ordered reduce requires contiguous integer keys from zero");
        }

        [[nodiscard]] std::size_t ordered_input_size(const TSDInputView &dict, std::size_t validated)
        {
            const std::size_t count = dict.size();
            // The keys are distinct and non-negative, so they are exactly
            // 0..count-1 as soon as none of them is count or more. When this
            // same dictionary was last seen to hold 0..validated-1, only what
            // changed can break that: a key added at or beyond the new length,
            // or a key in the removed tail that is still there. Checking those
            // is proportional to the change. Walking every key on every
            // evaluation made a list grown one element at a time quadratic.
            if (validated != unvalidated)
            {
                if (!dict.structure_modified())
                {
                    if (count != validated) { not_contiguous(); }
                    return count;
                }
                const auto data = dict.data_view();
                if (data.structural_delta_current(dict.evaluation_time()))
                {
                    for (std::size_t slot = data.next_membership_added_slot(); slot != TS_DATA_NO_CHILD_ID;
                         slot = data.next_membership_added_slot(slot))
                    {
                        if (ordered_key_index(data.key_at_slot(slot)) >= count) { not_contiguous(); }
                    }
                    for (std::size_t index = count; index < validated; ++index)
                    {
                        const Value key{static_cast<Int>(index)};
                        if (dict.contains(key.view())) { not_contiguous(); }
                    }
                    return count;
                }
            }
            for (const ValueView &key : dict.keys())
            {
                if (ordered_key_index(key) >= count) { not_contiguous(); }
            }
            return count;
        }

        [[nodiscard]] std::size_t ordered_dict_size(TSInputView &input, std::size_t validated)
        {
            return ordered_input_size(input.as_dict(), validated);
        }

        [[nodiscard]] std::size_t ordered_list_size(TSInputView &input, std::size_t)
        {
            return input.as_list().size();
        }

        [[nodiscard]] TSOutputView ordered_dict_element(TSOutputView source, std::size_t index)
        {
            Value key{static_cast<Int>(index)};
            auto dict = source.as_dict();
            return dict.contains(key.view()) ? dict.at(key.view()) : TSOutputView{};
        }

        [[nodiscard]] TSOutputView ordered_list_element(TSOutputView source, std::size_t index)
        {
            auto list = source.as_list();
            return index < list.size() ? list.at(index) : TSOutputView{};
        }

        [[nodiscard]] const OrderedReduceCollectionOps &ordered_collection_ops_for(
            const TSValueTypeMetaData &schema)
        {
            static const OrderedReduceCollectionOps dict_ops{
                .size = &ordered_dict_size,
                .element_output = &ordered_dict_element,
            };
            static const OrderedReduceCollectionOps list_ops{
                .size = &ordered_list_size,
                .element_output = &ordered_list_element,
            };
            return schema.kind == TSTypeKind::TSD ? dict_ops : list_ops;
        }

        /** Bind link ``index`` to its accumulator and element, and resolve what it publishes. */
        void bind_link(
            const NodeView &view,
            const OrderedReduceContext &context,
            OrderedReduceStorage &storage,
            std::size_t index,
            DateTime evaluation_time)
        {
            auto *entry = storage.entries.entry_at(index);
            if (entry == nullptr || !entry->graph.has_value())
            {
                throw std::logic_error("ordered reduce child entry is not constructed");
            }

            const auto source_for = [&](const std::vector<std::size_t> &path) {
                TSOutputView source = path[0] == 0
                                          ? accumulator_source(view, storage, index, evaluation_time)
                                          : collection_element_output(view, context, index, evaluation_time);
                if (path.size() > 1 && source.bound())
                {
                    source = walk_ts_path(std::move(source), std::span<const std::size_t>{path}.subspan(1));
                }
                return source;
            };

            auto child = entry->graph.view();
            for (const NestedGraphInputBinding &binding : context.spec.child.input_bindings)
            {
                auto target = walk_ts_path(
                    child.node_at(binding.target.node).input(evaluation_time),
                    binding.target.path);
                bind_input_to_source(std::move(target), source_for(binding.source_path));
            }

            const auto &output = *context.spec.child.output_binding;
            TSOutputView published = output.kind == NestedGraphOutputBinding::Kind::ParentInput
                                         ? source_for(output.parent_source_path)
                                         : walk_ts_path(child.node_at(output.source.node).output(evaluation_time),
                                                        output.source.path);
            entry->output = published.bound() ? published.handle() : TSOutputHandle{};
        }

        void publish_tail(const NodeView &view, const OrderedReduceStorage &storage, DateTime evaluation_time)
        {
            TSOutputView source = storage.live_count == 0
                                      ? zero_output(view, evaluation_time)
                                      : link_output(storage, storage.live_count - 1, evaluation_time);
            runtime_detail::bind_reduce_output(view.output(evaluation_time), source, evaluation_time);
        }

        /** Construct, bind and start links ``[live_count, next_count)``. */
        void extend_chain(
            const NodeView &view,
            const OrderedReduceContext &context,
            OrderedReduceStorage &storage,
            std::size_t next_count,
            DateTime evaluation_time)
        {
            const std::size_t first = storage.live_count;
            // The slot store grows to exactly what it is asked for, copying its
            // slot table and allocating one block each time. A list that grows
            // an element at a time must therefore ask geometrically, or growth
            // is quadratic again and every link gets its own allocation.
            if (next_count > storage.entries.slot_capacity())
            {
                storage.entries.reserve_to(std::max(next_count, storage.entries.slot_capacity() * 2));
            }

            std::size_t created = 0;
            auto rollback = UnwindCleanupGuard([&] {
                FirstExceptionRecorder errors;
                errors.capture([&] { storage.stop_links(first, first + created); });
                for (std::size_t index = first + created; index-- > first;) { storage.entries.destroy_at(index); }
                errors.rethrow_if_any();
            });

            for (std::size_t index = first; index < next_count; ++index)
            {
                auto &entry = storage.entries.construct_at(index);
                ++created;
                entry.graph = context.spec.child.graph_builder.make_nested_graph(
                    view.pointer(), storage.entries.graph_memory(index), context.graph_layout);
                storage.observe(entry, index);
                bind_link(view, context, storage, index, evaluation_time);
                entry.graph.view().start(evaluation_time);
                schedule_sampled_input_consumers(
                    entry.graph.view(), evaluation_time, context.spec.child.input_bindings);
                // Start hooks schedule before the graph reports to its observer.
                const DateTime next = entry.graph.view().next_scheduled_time();
                storage.note_schedule(index, next);
                if (next != MAX_DT && next > evaluation_time)
                {
                    view.graph().schedule_node(view.node_index(), next);
                }
            }
            storage.live_count = next_count;
            rollback.release();
        }

        /** Stop links ``[next_count, live_count)``; they are destroyed next cycle. */
        void truncate_chain(OrderedReduceStorage &storage, std::size_t next_count, DateTime evaluation_time)
        {
            const std::size_t old_count = storage.live_count;
            storage.live_count = next_count;
            storage.retired_end = old_count;
            storage.retired_time = evaluation_time;
            storage.stop_links(next_count, old_count);
        }

        void rebind_links(
            const NodeView &view,
            const OrderedReduceContext &context,
            OrderedReduceStorage &storage,
            std::size_t first,
            DateTime evaluation_time)
        {
            for (std::size_t index = first; index < storage.live_count; ++index)
            {
                bind_link(view, context, storage, index, evaluation_time);
            }
        }

        void reconcile_chain(
            const NodeView &view,
            const OrderedReduceContext &context,
            OrderedReduceStorage &storage,
            DateTime evaluation_time)
        {
            auto root = view.input(evaluation_time);
            auto ts = root.indexed_child_at(0);
            const TSOutputView collection = ts.bound_output();
            const TSOutputView zero = zero_output(view, evaluation_time);
            const bool collection_repointed =
                collection.bound() ? !collection.handle().same_as(storage.collection_source)
                                   : storage.collection_source.bound();
            const bool zero_repointed =
                zero.bound() ? !zero.handle().same_as(storage.zero_source) : storage.zero_source.bound();

            const std::size_t next_count =
                ts.valid() ? context.collection_ops->size(
                                 ts, storage.primed && !collection_repointed ? storage.live_count : unvalidated)
                           : 0;
            const std::size_t old_count = storage.live_count;
            const std::size_t kept = std::min(old_count, next_count);

            // A link is rebound only when what it is bound to has moved: the
            // whole collection re-pointed, or an element that was removed and
            // put back in the same cycle (its index is kept, its output is new).
            // Everything from there down may publish something different, so it
            // is rebound too -- and nothing above it is touched.
            std::size_t rebind_from = kept;
            if (collection_repointed) { rebind_from = 0; }
            else if (zero_repointed && kept != 0) { rebind_from = 0; }
            else if (ts.valid() && ts.modified() && ts.schema()->kind == TSTypeKind::TSD)
            {
                const auto dict = ts.as_dict();
                if (dict.structure_modified())
                {
                    const auto data = dict.data_view();
                    for (std::size_t slot = data.next_membership_added_slot(); slot != TS_DATA_NO_CHILD_ID;
                         slot = data.next_membership_added_slot(slot))
                    {
                        rebind_from = std::min(rebind_from, ordered_key_index(data.key_at_slot(slot)));
                    }
                }
            }

            storage.collection_source = collection.bound() ? collection.handle() : TSOutputHandle{};
            storage.zero_source = zero.bound() ? zero.handle() : TSOutputHandle{};

            if (next_count < old_count) { truncate_chain(storage, next_count, evaluation_time); }
            if (rebind_from < kept) { rebind_links(view, context, storage, rebind_from, evaluation_time); }
            if (next_count > old_count) { extend_chain(view, context, storage, next_count, evaluation_time); }

            if (next_count != old_count || rebind_from < kept || zero_repointed || !storage.published)
            {
                publish_tail(view, storage, evaluation_time);
                storage.published = true;
            }
            storage.primed = ts.valid();
        }

        bool ordered_reduce_evaluate(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            auto typed = view.as<OrderedReduceNodeView>();
            const auto &context = *static_cast<const OrderedReduceContext *>(typed.internal_context());
            auto &storage = *MemoryUtils::cast<OrderedReduceStorage>(typed.internal_storage());
            storage.initialise(context.graph_layout);

            const bool resuming = storage.resume_index_plus_one != 0;
            // Set before reconciling: a link bound or started below is scheduled
            // for this cycle and must land on the due heap.
            storage.evaluating_time = evaluation_time;
            if (!resuming)
            {
                storage.destroy_retired_before(evaluation_time);
                reconcile_chain(view, context, storage, evaluation_time);
                storage.admit_due(evaluation_time);
            }
            else
            {
                storage.note_schedule(storage.resume_index_plus_one - 1, evaluation_time);
            }

            const std::size_t resumed = resuming ? storage.resume_index_plus_one - 1 : static_cast<std::size_t>(-1);
            // Lowest index first: a link's tick schedules the link after it,
            // which is always still ahead of the cursor.
            for (std::size_t index = storage.pop_due(); index != static_cast<std::size_t>(-1);
                 index = storage.pop_due())
            {
                if (index >= storage.live_count) { continue; }
                auto *entry = storage.entries.entry_at(index);
                if (entry == nullptr || !entry->graph.has_value()) { continue; }
                auto child = entry->graph.view();
                if (index != resumed && child.next_scheduled_time() > evaluation_time)
                {
                    storage.set_future(index, child.next_scheduled_time());
                    continue;
                }
                if (!child.evaluate(evaluation_time))
                {
                    storage.resume_index_plus_one = index + 1;
                    return false;
                }
                // The PULL half: what a link schedules while it is itself
                // evaluating is not reported to the observer.
                const DateTime next = child.next_scheduled_time();
                storage.set_future(index, next > evaluation_time ? next : MAX_DT);
            }
            storage.resume_index_plus_one = 0;
            if (!storage.future.empty())
            {
                view.graph().schedule_node(view.node_index(), storage.future.begin()->first);
            }
            return true;
        }

        bool ordered_reduce_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            return ordered_reduce_evaluate(view, evaluation_time);
        }

        void ordered_reduce_stop(const NodeView &view, DateTime)
        {
            auto typed = view.as<OrderedReduceNodeView>();
            auto &storage = *MemoryUtils::cast<OrderedReduceStorage>(typed.internal_storage());
            storage.stop_links(0, storage.live_count);
        }

        [[nodiscard]] std::string ordered_reduce_checkpoint_signature(const NodeBuilder &builder)
        {
            const auto &context = *static_cast<const OrderedReduceContext *>(
                builder.type().ops_ref().extended_view_context);
            manifest::CanonicalWriter signature;
            signature.varint(1);
            node_checkpoint_detail::append_input_bindings(
                signature, context.spec.child.graph_builder, context.spec.child.input_bindings);
            node_checkpoint_detail::append_output_binding(signature, context.spec.child.graph_builder,
                                                           *context.spec.child.output_binding);
            const auto &bytes = signature.bytes();
            return {reinterpret_cast<const char *>(bytes.data()), bytes.size()};
        }

        [[nodiscard]] NodeCheckpointState capture_ordered_reduce_checkpoint(
            const NodeView &view, const CaptureGraphCheckpoint &capture_graph)
        {
            const auto typed = view.as<OrderedReduceNodeView>();
            const auto &storage = *MemoryUtils::cast<const OrderedReduceStorage>(typed.internal_storage());
            if (storage.resume_index_plus_one != 0)
            {
                throw std::invalid_argument("component checkpoint: ordered reduce has unfinished work");
            }
            if (storage.live_count > static_cast<std::size_t>(std::numeric_limits<Int>::max()))
            {
                throw std::overflow_error("ordered reduce checkpoint exceeds the portable integer range");
            }
            NodeCheckpointState image;
            ListBuilder metadata{TypeRegistry::instance().scalar_type<Int>()};
            metadata.push_back(Int{1});
            metadata.push_back(static_cast<Int>(storage.primed));
            metadata.push_back(static_cast<Int>(storage.published));
            metadata.push_back(static_cast<Int>(storage.live_count));
            image.payload = metadata.build();
            image.endpoints.push_back(view.output(view.graph().evaluation_time()).checkpoint_forwarding());
            for (std::size_t index = 0; index < storage.live_count; ++index)
            {
                const auto *entry = storage.entries.entry_at(index);
                if (entry == nullptr || !entry->graph.has_value() || !entry->graph.view().started() ||
                    entry->graph.view().failed_node().valid() || !capture_graph)
                {
                    throw std::invalid_argument("component checkpoint: ordered reduce child is incomplete");
                }
                ChildGraphCheckpoint child;
                child.slot = index;
                child.key = Value{static_cast<Int>(index)};
                child.graph = capture_graph(entry->graph.view());
                image.children.push_back(std::move(child));
            }
            return image;
        }

        void prepare_ordered_reduce_checkpoint(const NodeView &view, const NodeCheckpointState &image,
                                               DateTime time, const PrepareGraphCheckpoint &prepare_graph)
        {
            const auto typed = view.as<OrderedReduceNodeView>();
            const auto &context = *static_cast<const OrderedReduceContext *>(typed.internal_context());
            auto &storage = *MemoryUtils::cast<OrderedReduceStorage>(typed.internal_storage());
            if (storage.primed || storage.published || storage.live_count != 0 ||
                !image.payload.has_value() || image.endpoints.size() != 1)
            {
                throw std::invalid_argument("ordered reduce checkpoint requires a fresh instance and complete image");
            }
            const auto metadata = image.payload.as_list();
            if (metadata.size() != 4 || metadata.at(0).checked_as<Int>() != 1)
            {
                throw std::invalid_argument("ordered reduce checkpoint metadata mismatch");
            }
            const Int primed = metadata.at(1).checked_as<Int>();
            const Int published = metadata.at(2).checked_as<Int>();
            const Int count = metadata.at(3).checked_as<Int>();
            if ((primed != 0 && primed != 1) || (published != 0 && published != 1) || count < 0 ||
                static_cast<std::uint64_t>(count) != image.children.size() ||
                (!published && count != 0) || (!primed && count != 0))
            {
                throw std::invalid_argument("ordered reduce checkpoint topology is inconsistent");
            }
            auto output = view.output(time);
            output.validate_checkpoint_forwarding(image.endpoints[0]);
            for (std::size_t index = 0; index < image.children.size(); ++index)
            {
                const auto &child = image.children[index];
                if (child.slot != index || !child.key.has_value() || !child.graph ||
                    child.key.view().checked_as<Int>() != static_cast<Int>(index))
                {
                    throw std::invalid_argument("ordered reduce checkpoint child identity differs");
                }
            }
            storage.initialise(context.graph_layout);
            auto &bank = storage.entries;
            bank.reserve_to(image.children.size());
            for (std::size_t index = 0; index < image.children.size(); ++index)
            {
                auto &entry = bank.construct_at(index);
                ++storage.live_count;
                entry.graph = context.spec.child.graph_builder.make_nested_graph(
                    view.pointer(), bank.graph_memory(index), context.graph_layout);
                storage.observe(entry, index);
            }
            if (!image.children.empty() && !prepare_graph)
                throw std::logic_error("ordered reduce checkpoint preparation callback is missing");
            for (std::size_t index = 0; index < image.children.size(); ++index)
                prepare_graph(bank.entry_at(index)->graph.view(), *image.children[index].graph, time);
            storage.primed = primed != 0;
            storage.published = published != 0;
        }


        void restore_ordered_reduce_checkpoint(const NodeView &view, const NodeCheckpointState &image,
                                               DateTime time, const RestoreGraphCheckpoint &restore_graph)
        {
            const auto typed = view.as<OrderedReduceNodeView>();
            const auto &context = *static_cast<const OrderedReduceContext *>(typed.internal_context());
            auto &storage = *MemoryUtils::cast<OrderedReduceStorage>(typed.internal_storage());
            const Int primed = storage.primed;
            const Int published = storage.published;
            auto input = view.input(time).indexed_child_at(0);
            const auto current_count = input.valid() ? context.collection_ops->size(input, unvalidated) : 0;
            if ((primed != 0 && current_count != image.children.size()) ||
                (published != 0 && primed != static_cast<Int>(input.valid())))
            {
                throw std::invalid_argument("ordered reduce checkpoint input size differs");
            }
            for (std::size_t index = 0; index < image.children.size(); ++index)
            {
                bind_link(view, context, storage, index, time);
                restore_graph(storage.entries.entry_at(index)->graph.view(), *image.children[index].graph, time);
            }
            // The restored chain is bound to these; the first evaluation must
            // not mistake them for a re-point and rebind every link.
            const TSOutputView collection = input.bound_output();
            const TSOutputView zero = zero_output(view, time);
            storage.collection_source = collection.bound() ? collection.handle() : TSOutputHandle{};
            storage.zero_source = zero.bound() ? zero.handle() : TSOutputHandle{};
            auto output = view.output(time);
            TSOutputView source;
            if (published != 0)
            {
                source = storage.live_count != 0 ? link_output(storage, storage.live_count - 1, time)
                                                 : zero_output(view, time);
            }
            output.restore_checkpoint_forwarding(source, image.endpoints[0]);
        }

        void start_restored_ordered_reduce(const NodeView &view, DateTime time)
        {
            auto &storage = *MemoryUtils::cast<OrderedReduceStorage>(view.as<OrderedReduceNodeView>().internal_storage());
            storage.evaluating_time = time;
            storage.due.clear();
            storage.future.clear();
            for (std::size_t index = 0; index < storage.live_count; ++index)
            {
                auto &entry = *storage.entries.entry_at(index);
                entry.future_when = MAX_DT;
                auto child = entry.graph.view();
                child.start(time);
                // Whatever a restored link still has scheduled is not reported
                // to the observer by start; pick it up once here.
                if (const DateTime next = child.next_scheduled_time(); next != MAX_DT)
                {
                    storage.note_schedule(index, next);
                    view.graph().schedule_node(view.node_index(), next);
                }
            }
        }

        [[nodiscard]] const NodeCheckpointOps &ordered_reduce_checkpoint_ops() noexcept
        {
            static const NodeCheckpointOps ops{
                .supported = true,
                .captures_output = false,
                .capture_impl = &capture_ordered_reduce_checkpoint,
                .prepare_restore_impl = &prepare_ordered_reduce_checkpoint,
                .restore_impl = &restore_ordered_reduce_checkpoint,
                .start_restored_impl = &start_restored_ordered_reduce,
                .signature_impl = &ordered_reduce_checkpoint_signature,
            };
            return ops;
        }

        void validate_ordered_reduce_spec(
            const NodeTypeMetaData &meta,
            const OrderedReduceNodeSpec &spec)
        {
            if (!spec.child.output_binding.has_value())
            {
                throw std::invalid_argument("ordered_reduce_node requires a combiner output binding");
            }
            if (meta.input_schema == nullptr || meta.input_schema->kind != TSTypeKind::TSB ||
                meta.input_schema->field_count() != 2)
            {
                throw std::invalid_argument("ordered_reduce_node requires input schema [ts, zero]");
            }
            const auto *fields = meta.input_schema->fields();
            const auto *collection = fields[0].type;
            if (collection == nullptr ||
                ((collection->kind != TSTypeKind::TSD ||
                  collection->key_type() != scalar_descriptor<Int>::value_meta()) &&
                 (collection->kind != TSTypeKind::TSL || !collection->is_unbounded_tsl())))
            {
                throw std::invalid_argument(
                    "ordered_reduce_node requires TSD[int, E] or dynamic TSL[E] input");
            }
            if (meta.output_schema == nullptr ||
                !time_series_schema_equivalent(meta.output_schema, fields[1].type))
            {
                throw std::invalid_argument("ordered_reduce_node output must match its zero input");
            }

            const std::size_t child_node_count = spec.child.graph_builder.node_count();
            const auto &output_binding = *spec.child.output_binding;
            if (!output_binding.target_path.empty())
            {
                throw std::invalid_argument(
                    "ordered_reduce_node requires a root combiner terminal");
            }
            if (output_binding.kind == NestedGraphOutputBinding::Kind::ParentInput)
            {
                if (output_binding.parent_source_path.empty() || output_binding.parent_source_path[0] > 1)
                {
                    throw std::invalid_argument(
                        "ordered_reduce_node parent-input output must select accumulator or element");
                }
            }
            else if (output_binding.source.node >= child_node_count)
            {
                throw std::invalid_argument(
                    "ordered_reduce_node child-output terminal is out of range");
            }
            for (const NestedGraphInputBinding &binding : spec.child.input_bindings)
            {
                if (binding.source_path.empty() || binding.source_path[0] > 1 ||
                    binding.target.node >= child_node_count)
                {
                    throw std::invalid_argument(
                        "ordered_reduce_node combiner inputs must be sourced from lhs or rhs");
                }
            }
        }
    }  // namespace

    const void *OrderedReduceNodeView::node_view_type_id() noexcept
    {
        static const char token{};
        return &token;
    }

    OrderedReduceNodeView OrderedReduceNodeView::from_node(NodeView view, const void *context)
    {
        if (context == nullptr)
        {
            throw std::logic_error("OrderedReduceNodeView requires a typed view context");
        }
        const auto &typed_context = *static_cast<const OrderedReduceContext *>(context);
        void *storage = MemoryUtils::advance(view.data(), typed_context.storage_offset);
        return OrderedReduceNodeView{std::move(view), context, storage};
    }

    const NodeView &OrderedReduceNodeView::node() const noexcept { return view_; }

    std::size_t OrderedReduceNodeView::child_graph_count() const noexcept
    {
        return MemoryUtils::cast<OrderedReduceStorage>(storage_)->live_count;
    }

    bool OrderedReduceNodeView::child_graphs_use_in_place_storage() const noexcept
    {
        const auto &storage = *MemoryUtils::cast<OrderedReduceStorage>(storage_);
        for (std::size_t index = 0; index < storage.live_count; ++index)
        {
            const auto *entry = storage.entries.entry_at(index);
            if (entry != nullptr && entry->graph.has_value() && !entry->graph.uses_external_storage())
            {
                return false;
            }
        }
        return true;
    }

    OrderedReduceNodeView::OrderedReduceNodeView(
        NodeView view,
        const void *context,
        void *storage) noexcept
        : view_(std::move(view)), context_(context), storage_(storage)
    {
    }

    NodeBuilder ordered_reduce_node(NodeTypeMetaData meta, OrderedReduceNodeSpec spec)
    {
        validate_ordered_reduce_spec(meta, spec);

        meta.requires_phase_runner =
            meta.requires_phase_runner || spec.child.graph_builder.requires_phase_runner();
        meta.node_kind = NodeKind::Nested;
        meta.valid_inputs = std::vector<std::size_t>{};
        meta.output_endpoint_schema = runtime_detail::reduce_output_endpoint_schema(meta.output_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);
        const std::array fields{NodeStorageField{
            .name = ordered_reduce_storage_field_name,
            .plan = &MemoryUtils::plan_for<OrderedReduceStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const MemoryUtils::StorageLayout graph_layout = spec.child.graph_builder.nested_storage_layout();
        const OrderedReduceCollectionOps &collection_ops =
            ordered_collection_ops_for(*descriptor.schema.input_schema->fields()[0].type);
        descriptor.callbacks.stop = &ordered_reduce_stop;
        descriptor.ops.evaluate_impl = &ordered_reduce_evaluate_impl;
        descriptor.ops.storage_metrics_impl = &ordered_reduce_storage_metrics;
        descriptor.ops.extended_view_type_id = OrderedReduceNodeView::node_view_type_id();
        descriptor.ops.checkpoint_ops = &ordered_reduce_checkpoint_ops();
        const auto *context = register_ordered_reduce_context(
            std::move(spec),
            descriptor.storage_plan->component(ordered_reduce_storage_field_name).offset,
            graph_layout,
            collection_ops);
        descriptor.ops.extended_view_context = context;
        descriptor.ops.child_graph_inspection = ChildGraphInspectionOps{
            .context = context,
            .visit_impl = &visit_ordered_reduce_child,
        };
        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
