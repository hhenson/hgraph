#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/nested_graph_storage.h>
#include <hgraph/runtime/reduce_node.h>
#include <hgraph/types/wired_fn.h>
#include <hgraph/types/utils/slot_bitmap.h>
#include <hgraph/util/scope.h>

#include "reduce_output_binding.h"

#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view reduce_storage_field_name{"reduce"};

        using runtime_detail::bind_reduce_output;
        using runtime_detail::reduce_output_endpoint_schema;

        struct CombinerEntry
        {
            GraphValue     graph{};
            TSOutputHandle output{};
        };

        struct ValueKeyHash
        {
            using is_transparent = void;
            [[nodiscard]] std::size_t operator()(const Value &value) const
            {
                return value.has_value() ? value.hash() : 0;
            }
            [[nodiscard]] std::size_t operator()(const ValueView &value) const noexcept
            {
                return value.valid() ? value.hash() : 0;
            }
        };

        struct ValueKeyEqual
        {
            using is_transparent = void;
            [[nodiscard]] bool operator()(const Value &lhs, const Value &rhs) const
            {
                if (lhs.has_value() != rhs.has_value()) { return false; }
                return !lhs.has_value() || lhs.equals(rhs);
            }
            [[nodiscard]] bool operator()(const Value &lhs, const ValueView &rhs) const noexcept
            {
                return lhs.has_value() == rhs.valid() && (!lhs.has_value() || lhs.equals(rhs));
            }
            [[nodiscard]] bool operator()(const ValueView &lhs, const Value &rhs) const noexcept
            {
                return (*this)(rhs, lhs);
            }
        };

        struct ReduceNodeStorage
        {
            ReduceNodeStorage()                                     = default;
            ReduceNodeStorage(const ReduceNodeStorage &)            = delete;
            ReduceNodeStorage &operator=(const ReduceNodeStorage &) = delete;
            ReduceNodeStorage(ReduceNodeStorage &&)                 = delete;
            ReduceNodeStorage &operator=(ReduceNodeStorage &&)      = delete;

            ~ReduceNodeStorage()
            {
                // A stopped retired parent may still retain a target handle to
                // a surviving current combiner output. Destroy retired
                // subscribers before current producers; each generation is
                // still destroyed root-first internally.
                destroy_previous_generation();
                destroy_combiners();
            }

            void initialise(MemoryUtils::StorageLayout graph_layout)
            {
                for (auto &bank : combiner_banks) { bank.bind_graph_layout(graph_layout); }
            }

            // Root-first (ascending heap index) teardown: a parent combiner's
            // inputs link into its children's outputs — the subscriber must
            // unbind before its target dies. (A plain vector destructor would
            // destroy back-to-front: children first. See *Nested Graphs*.)
            void destroy_combiners() noexcept
            {
                auto &bank = combiner_banks[current_bank];
                for (std::size_t position = 0; position < combiners.size(); ++position)
                {
                    if (combiners[position] == nullptr) { continue; }
                    bank.destroy_at(position);
                    combiners[position] = nullptr;
                }
            }

            void destroy_previous_generation() noexcept
            {
                for (const auto &combiner : previous_generation)
                {
                    combiner_banks[combiner.bank].destroy_at(combiner.position);
                }
                previous_generation.clear();
                previous_generation_time = MIN_DT;
            }

            void destroy_previous_generation_before(DateTime evaluation_time) noexcept
            {
                if (!previous_generation.empty() && previous_generation_time < evaluation_time)
                {
                    destroy_previous_generation();
                }
            }

            /** dense leaf -> key (the key↔leaf mappings; leaves are dense ``0..n-1``). */
            std::vector<Value> dense_to_key{};
            /** dense leaf -> source collection slot, avoiding repeated key lookup while binding the tree. */
            std::vector<std::size_t> dense_to_source_slot{};
            /** TSL dense leaf -> effective child source, detecting same-slot re-points. */
            std::vector<TSOutputHandle> dense_to_source_handle{};
            ankerl::unordered_dense::map<Value, std::size_t, ValueKeyHash, ValueKeyEqual> key_to_leaf{};

            /** Power-of-two tree width (monotonic; 0 until the first key). */
            std::size_t leaf_capacity{0};
            /** Heap-indexed internal combine points (size ``leaf_capacity - 1``); null = no live combiner. */
            std::vector<CombinerEntry *> combiners{};

            struct RetiredCombiner
            {
                std::size_t bank{0};
                std::size_t position{0};
            };

            // Capacity growth reshapes the entire tree. Build into the other
            // bank while retaining the stopped old bank through this cycle.
            std::array<InPlaceGraphSlotStore<CombinerEntry>, 2> combiner_banks{};
            std::size_t                                         current_bank{0};
            std::vector<RetiredCombiner>                        previous_generation{};
            DateTime                                            previous_generation_time{MIN_DT};

            bool primed{false};
            bool published{false};
            bool source_handles_initialised{false};
            TSOutputHandle collection_source{};
            TSOutputHandle zero_source{};

            // Once a keyed root changes identity, publish through one stable
            // output for the rest of this node's lifetime. Downstream REF
            // values may retain child endpoint identities beyond the cycle in
            // which the re-point was sampled, so rotating temporary snapshots
            // would leave those references pointing at recycled slot storage.
            std::optional<TSOutput> publication_snapshot{};
            TSOutputHandle pending_publication_source{};
            bool publication_snapshot_active{false};
            bool publication_full_reconcile{false};
            bool publication_sample_all{false};

            // Ordinary collection ticks populate only the affected leaf-to-root
            // paths. Full scans remain the conservative path for structural
            // changes, repoints, and independently scheduled child graphs.
            std::vector<std::size_t> evaluation_positions{};
            SlotBitmap              evaluation_candidates{};
            std::vector<std::size_t> modified_leaves{};
            std::vector<std::size_t> structural_leaves{};
            std::vector<std::size_t> structural_positions{};
            std::size_t              resume_candidate_plus_one{0};
            bool                     has_future_combiner_schedule{false};
        };

        struct ReduceCollectionOps
        {
            bool (*reconcile)(ReduceNodeStorage &storage, TSInputView &input, bool full);
            bool (*structure_modified)(const TSInputView &input, DateTime evaluation_time);
            void (*append_modified_leaves)(const ReduceNodeStorage &storage,
                                           TSInputView &input,
                                           std::vector<std::size_t> &leaves);
            TSOutputView (*leaf_output)(TSInputView &input, TSOutputView source,
                                        std::size_t leaf, const Value &key,
                                        std::size_t source_slot);
        };

        [[nodiscard]] bool reconcile_dict_collection(ReduceNodeStorage &storage, TSInputView &input, bool full);
        [[nodiscard]] bool reconcile_list_collection(ReduceNodeStorage &storage, TSInputView &input, bool full);
        void append_modified_dict_leaves(const ReduceNodeStorage &storage, TSInputView &input,
                                         std::vector<std::size_t> &leaves);
        void append_modified_list_leaves(const ReduceNodeStorage &storage, TSInputView &input,
                                         std::vector<std::size_t> &leaves);
        [[nodiscard]] TSOutputView dict_leaf_output(TSInputView &input, TSOutputView source,
                                                    std::size_t leaf, const Value &key,
                                                    std::size_t source_slot);
        [[nodiscard]] TSOutputView list_leaf_output(TSInputView &input, TSOutputView source,
                                                    std::size_t leaf, const Value &key,
                                                    std::size_t source_slot);
        [[nodiscard]] TSOutputHandle effective_output_handle(TSOutputView source);

        struct ReduceNodeContext
        {
            ReduceNodeSpec spec{};
            std::size_t    storage_offset{0};
            MemoryUtils::StorageLayout graph_layout{};
            const ReduceCollectionOps *collection_ops{nullptr};
        };

        // Program-lifetime, intentionally-leaked context storage — same rationale
        // as single_nested_graph_contexts (see nested_graph_node.cpp).
        [[nodiscard]] std::vector<std::unique_ptr<ReduceNodeContext>> &reduce_node_contexts() noexcept
        {
            static auto *contexts = new std::vector<std::unique_ptr<ReduceNodeContext>>;
            return *contexts;
        }

        [[nodiscard]] const ReduceNodeContext &register_reduce_node_context(
            ReduceNodeSpec spec, std::size_t storage_offset, MemoryUtils::StorageLayout graph_layout,
            const ReduceCollectionOps &collection_ops)
        {
            auto context = std::make_unique<ReduceNodeContext>(ReduceNodeContext{
                .spec           = std::move(spec),
                .storage_offset = storage_offset,
                .graph_layout   = graph_layout,
                .collection_ops = &collection_ops,
            });
            const auto *result = context.get();
            reduce_node_contexts().push_back(std::move(context));
            return *result;
        }

        [[nodiscard]] NodeStorageMetrics reduce_storage_metrics(
            const void *raw_context, const void *memory) noexcept
        {
            const auto &context = *static_cast<const ReduceNodeContext *>(raw_context);
            const auto &storage = *MemoryUtils::cast<const ReduceNodeStorage>(
                MemoryUtils::advance(memory, context.storage_offset));
            NodeStorageMetrics result{};
            for (const auto &bank : storage.combiner_banks)
            {
                result.nested_graph_count += bank.entry_count();
                result.nested_graph_capacity += bank.slot_capacity();
                result.nested_graph_blocks += bank.block_count();
                result.dynamic_live_bytes += bank.live_bytes();
                result.dynamic_reserved_bytes += bank.reserved_bytes();
            }
            return result;
        }

        /** Where an aggregate currently comes from, without materialising it. */
        struct Aggregate
        {
            enum class Kind : std::uint8_t { Empty, Leaf, Node };
            Kind        kind{Kind::Empty};
            std::size_t index{0};
        };

        // Tree layout: internal nodes are heap positions 0..capacity-2 (root =
        // 0, children of i at 2i+1 / 2i+2); leaves are logical positions
        // internal_count + dense_leaf.
        [[nodiscard]] std::size_t internal_count(const ReduceNodeStorage &storage) noexcept
        {
            return storage.leaf_capacity > 1 ? storage.leaf_capacity - 1 : 0;
        }

        [[nodiscard]] Aggregate resolve_aggregate(const ReduceNodeStorage &storage, std::size_t position)
        {
            const std::size_t internals = internal_count(storage);
            if (position >= internals)
            {
                const std::size_t leaf = position - internals;
                if (leaf < storage.dense_to_key.size()) { return {Aggregate::Kind::Leaf, leaf}; }
                return {Aggregate::Kind::Empty, 0};
            }

            // Live leaves occupy a dense prefix. Derive this heap position's
            // leaf interval directly instead of recursively walking its entire
            // subtree on every structural reconciliation.
            const std::size_t level_start = std::bit_floor(position + 1);
            const std::size_t depth = std::bit_width(level_start) - 1;
            std::size_t span = storage.leaf_capacity >> depth;
            const std::size_t first_leaf = (position + 1 - level_start) * span;
            const std::size_t live = storage.dense_to_key.size();
            if (first_leaf >= live) { return {Aggregate::Kind::Empty, 0}; }
            const std::size_t live_in_subtree = std::min(span, live - first_leaf);
            if (live_in_subtree == 1)
            {
                return {Aggregate::Kind::Leaf, first_leaf};
            }

            // A partially populated subtree may carry its aggregate through a
            // left descendant (for example, two live leaves in a capacity-four
            // root resolve to the left child combiner). Descend only that one
            // frontier; full subtrees resolve immediately.
            std::size_t aggregate_position = position;
            while (live_in_subtree <= span / 2)
            {
                aggregate_position = 2 * aggregate_position + 1;
                span /= 2;
            }
            return {Aggregate::Kind::Node, aggregate_position};
        }

        [[nodiscard]] Aggregate root_aggregate(const ReduceNodeContext &context,
                                               const ReduceNodeStorage &storage)
        {
            const std::size_t live = storage.dense_to_key.size();
            if (live == 0) { return {Aggregate::Kind::Empty, 0}; }
            if (context.spec.has_zero && live == 1 && !storage.combiners.empty())
            {
                return {Aggregate::Kind::Node, 0};
            }
            return resolve_aggregate(storage, 0);
        }

        void stop_combiner_noexcept(CombinerEntry *entry) noexcept
        {
            if (entry == nullptr || !entry->graph.has_value()) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                entry->graph.view().stop();
                return true;
            }));
        }

        void reset_combiner_noexcept(InPlaceGraphSlotStore<CombinerEntry> &bank,
                                     std::size_t position) noexcept
        {
            CombinerEntry *entry = bank.entry_at(position);
            stop_combiner_noexcept(entry);
            bank.destroy_at(position);
        }

        /** Resolve an aggregate to the output it aliases. */
        [[nodiscard]] TSOutputView aggregate_output(const NodeView &view, const ReduceNodeContext &context,
                                                    const ReduceNodeStorage &storage, const Aggregate &aggregate,
                                                    DateTime evaluation_time)
        {
            switch (aggregate.kind)
            {
                case Aggregate::Kind::Leaf:
                {
                    auto collection_input = view.input(evaluation_time).indexed_child_at(0);
                    TSOutputView collection = storage.collection_source.bound()
                                                  ? storage.collection_source.view(evaluation_time)
                                                  : TSOutputView{};
                    const Value &key = storage.dense_to_key[aggregate.index];
                    return context.collection_ops->leaf_output(
                        collection_input, std::move(collection), aggregate.index, key,
                        storage.dense_to_source_slot[aggregate.index]);
                }
                case Aggregate::Kind::Node:
                {
                    const auto &output_binding = context.spec.child.output_binding;
                    if (output_binding->kind == NestedGraphOutputBinding::Kind::ParentInput)
                    {
                        const std::size_t side = output_binding->parent_source_path[0];
                        Aggregate source = resolve_aggregate(storage, 2 * aggregate.index + 1 + side);
                        auto output = aggregate_output(view, context, storage, source, evaluation_time);
                        if (output.bound() && output_binding->parent_source_path.size() > 1)
                        {
                            output = walk_ts_path(
                                std::move(output),
                                std::span<const std::size_t>{output_binding->parent_source_path}.subspan(1));
                        }
                        return output;
                    }
                    const auto *entry = storage.combiners[aggregate.index];
                    if (entry == nullptr || !entry->graph.has_value()) { return TSOutputView{}; }
                    if (entry->output.bound()) { return entry->output.view(evaluation_time); }
                    return walk_ts_path(
                        entry->graph.view().node_at(output_binding->source.node).output(evaluation_time),
                        output_binding->source.path);
                }
                case Aggregate::Kind::Empty:
                {
                    return storage.zero_source.bound()
                               ? storage.zero_source.view(evaluation_time)
                               : TSOutputView{};
                }
            }
            return TSOutputView{};
        }

        void bind_combiner_inputs(const NodeView &view, const ReduceNodeContext &context,
                                  const ReduceNodeStorage &storage, CombinerEntry &entry, const Aggregate &left,
                                  const Aggregate &right, DateTime evaluation_time, bool sampled)
        {
            auto child = entry.graph.view();
            for (const NestedGraphInputBinding &binding : context.spec.child.input_bindings)
            {
                const Aggregate &side = binding.source_path[0] == 0 ? left : right;
                auto source = aggregate_output(view, context, storage, side, evaluation_time);
                if (!binding.source_path.empty() && binding.source_path.size() > 1 && source.bound())
                {
                    source = walk_ts_path(
                        std::move(source),
                        std::span<const std::size_t>{binding.source_path}.subspan(1));
                }
                auto target = walk_ts_path(child.node_at(binding.target.node).input(evaluation_time),
                                           binding.target.path);
                const bool same_source = target.bound() && source.bound() &&
                                         target.bound_output().handle().same_as(source.handle());
                if (same_source || (!target.bound() && !source.bound())) { continue; }

                if (sampled)
                {
                    bind_sampled_input_to_source(std::move(target), source, evaluation_time);
                }
                else { bind_input_to_source(std::move(target), source); }
            }
        }

        void remove_leaf_at(ReduceNodeStorage &storage, std::size_t leaf)
        {
            const std::size_t last = storage.dense_to_key.size() - 1;
            storage.key_to_leaf.erase(storage.dense_to_key[leaf]);
            if (leaf != last)
            {
                storage.dense_to_key[leaf]                      = std::move(storage.dense_to_key[last]);
                storage.dense_to_source_slot[leaf]              = storage.dense_to_source_slot[last];
                storage.dense_to_source_handle[leaf]            = storage.dense_to_source_handle[last];
                storage.key_to_leaf[storage.dense_to_key[leaf]] = leaf;
            }
            storage.dense_to_key.pop_back();
            storage.dense_to_source_slot.pop_back();
            storage.dense_to_source_handle.pop_back();
        }

        void clear_leaf_state(ReduceNodeStorage &storage)
        {
            storage.dense_to_key.clear();
            storage.dense_to_source_slot.clear();
            storage.dense_to_source_handle.clear();
            storage.key_to_leaf.clear();
        }

        void record_removed_leaf_paths(ReduceNodeStorage &storage, std::size_t leaf)
        {
            const std::size_t last = storage.dense_to_key.size() - 1;
            storage.structural_leaves.push_back(leaf);
            if (leaf != last) { storage.structural_leaves.push_back(last); }
        }

        [[nodiscard]] bool reconcile_leaf_state(ReduceNodeStorage &storage, const TSDDataView &dict, bool full)
        {
            if (full)
            {
                clear_leaf_state(storage);
                storage.dense_to_key.reserve(dict.size());
                storage.dense_to_source_slot.reserve(dict.size());
                storage.dense_to_source_handle.reserve(dict.size());
                storage.key_to_leaf.reserve(dict.size());
                for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
                {
                    if (!dict.slot_live(slot) || !dict.at_slot(slot).valid()) { continue; }
                    Value key{dict.key_at_slot(slot)};
                    storage.key_to_leaf.emplace(key, storage.dense_to_key.size());
                    storage.dense_to_key.push_back(std::move(key));
                    storage.dense_to_source_slot.push_back(slot);
                    storage.dense_to_source_handle.emplace_back();
                }
                return true;
            }

            bool structural = false;
            const std::size_t first_removed_slot = dict.next_removed_slot();
            if (first_removed_slot != TS_DATA_NO_CHILD_ID && dict.slot_removed(first_removed_slot))
            {
                for (std::size_t slot = first_removed_slot; slot != TS_DATA_NO_CHILD_ID;
                     slot = dict.next_removed_slot(slot))
                {
                    const ValueView removed_key = dict.removed_key_at_slot(slot);
                    const auto found = storage.key_to_leaf.find(removed_key);
                    if (found == storage.key_to_leaf.end()) { continue; }
                    record_removed_leaf_paths(storage, found->second);
                    remove_leaf_at(storage, found->second);
                    structural = true;
                }
            }
            else
            {
                // Sampled forwarding transitions retain their own removed-key
                // value surface rather than keys in the current source slots.
                for (const ValueView &removed_key : dict.removed_keys())
                {
                    const auto found = storage.key_to_leaf.find(removed_key);
                    if (found == storage.key_to_leaf.end()) { continue; }
                    record_removed_leaf_paths(storage, found->second);
                    remove_leaf_at(storage, found->second);
                    structural = true;
                }
            }

            for (std::size_t slot = dict.next_added_slot(); slot != TS_DATA_NO_CHILD_ID;
                 slot = dict.next_added_slot(slot))
            {
                auto child = dict.at_slot(slot);
                if (!child.valid()) { continue; }
                Value typed_key{dict.key_at_slot(slot)};
                if (storage.key_to_leaf.find(typed_key) != storage.key_to_leaf.end()) { continue; }
                storage.structural_leaves.push_back(storage.dense_to_key.size());
                storage.key_to_leaf.emplace(typed_key, storage.dense_to_key.size());
                storage.dense_to_key.push_back(std::move(typed_key));
                storage.dense_to_source_slot.push_back(slot);
                storage.dense_to_source_handle.emplace_back();
                structural = true;
            }

            for (std::size_t slot = dict.next_modified_slot(); slot != TS_DATA_NO_CHILD_ID;
                 slot = dict.next_modified_slot(slot))
            {
                if (!dict.slot_live(slot)) { continue; }
                const ValueView key = dict.key_at_slot(slot);
                const auto found = storage.key_to_leaf.find(key);
                auto child = dict.at_slot(slot);
                if (!child.valid())
                {
                    if (found != storage.key_to_leaf.end())
                    {
                        record_removed_leaf_paths(storage, found->second);
                        remove_leaf_at(storage, found->second);
                        structural = true;
                    }
                    continue;
                }

                if (found == storage.key_to_leaf.end())
                {
                    const std::size_t leaf = storage.dense_to_key.size();
                    storage.structural_leaves.push_back(leaf);
                    Value typed_key{key};
                    storage.key_to_leaf.emplace(typed_key, leaf);
                    storage.dense_to_key.push_back(std::move(typed_key));
                    storage.dense_to_source_slot.push_back(slot);
                    storage.dense_to_source_handle.emplace_back();
                    structural = true;
                }
            }

            return structural;
        }

        [[nodiscard]] bool reconcile_leaf_state(ReduceNodeStorage &storage, const TSLInputView &list_input,
                                                bool full)
        {
            bool structural = false;

            if (full)
            {
                // A TSL's shape includes slots which have not acquired a value.
                // Reduction is over its currently-valid children, not its static
                // capacity, so remove invalidated/repointed children first.
                std::size_t leaf = 0;
                while (leaf < storage.dense_to_key.size())
                {
                    const std::size_t source_slot = storage.dense_to_source_slot[leaf];
                    if (source_slot >= list_input.size() || !list_input[source_slot].valid())
                    {
                        record_removed_leaf_paths(storage, leaf);
                        remove_leaf_at(storage, leaf);
                        structural = true;
                        continue;
                    }
                    TSOutputHandle source = effective_output_handle(list_input[source_slot].bound_output());
                    if (!source.same_as(storage.dense_to_source_handle[leaf]))
                    {
                        storage.structural_leaves.push_back(leaf);
                        storage.dense_to_source_handle[leaf] = source;
                        structural = true;
                    }
                    ++leaf;
                }

                for (std::size_t index = 0; index < list_input.size(); ++index)
                {
                    if (!list_input[index].valid()) { continue; }
                    Value key{static_cast<Int>(index)};
                    if (storage.key_to_leaf.find(key) != storage.key_to_leaf.end()) { continue; }
                    const std::size_t dense_leaf = storage.dense_to_key.size();
                    storage.structural_leaves.push_back(dense_leaf);
                    storage.key_to_leaf.emplace(key, dense_leaf);
                    storage.dense_to_key.push_back(std::move(key));
                    storage.dense_to_source_slot.push_back(index);
                    storage.dense_to_source_handle.push_back(
                        effective_output_handle(list_input[index].bound_output()));
                    structural = true;
                }
                return structural;
            }

            // Ordinary value ticks cannot affect unmodified TSL children. Limit
            // validity and forwarding-handle checks to the current delta instead
            // of walking the retained list twice on every sparse update.
            auto list_data = list_input.data_view();
            for (const std::size_t index : list_data.modified_indices())
            {
                auto child = list_input[index];
                Value key{static_cast<Int>(index)};
                const auto found = storage.key_to_leaf.find(key);
                if (!child.valid())
                {
                    if (found != storage.key_to_leaf.end())
                    {
                        record_removed_leaf_paths(storage, found->second);
                        remove_leaf_at(storage, found->second);
                        structural = true;
                    }
                    continue;
                }

                if (found == storage.key_to_leaf.end())
                {
                    const std::size_t dense_leaf = storage.dense_to_key.size();
                    storage.structural_leaves.push_back(dense_leaf);
                    storage.key_to_leaf.emplace(key, dense_leaf);
                    storage.dense_to_key.push_back(std::move(key));
                    storage.dense_to_source_slot.push_back(index);
                    storage.dense_to_source_handle.push_back(
                        effective_output_handle(child.bound_output()));
                    structural = true;
                    continue;
                }

                TSOutputHandle source = effective_output_handle(child.bound_output());
                if (!source.same_as(storage.dense_to_source_handle[found->second]))
                {
                    storage.structural_leaves.push_back(found->second);
                    storage.dense_to_source_handle[found->second] = source;
                    structural = true;
                }
            }
            return structural;
        }

        [[nodiscard]] bool reconcile_dict_collection(ReduceNodeStorage &storage, TSInputView &input, bool full)
        {
            static_cast<void>(input);
            auto data = storage.collection_source.data_view();
            auto dict = data.as_dict();
            return reconcile_leaf_state(storage, dict, full);
        }

        [[nodiscard]] bool reconcile_list_collection(ReduceNodeStorage &storage, TSInputView &input, bool full)
        {
            return reconcile_leaf_state(storage, input.as_list(), full);
        }

        [[nodiscard]] bool dict_structure_modified(const TSInputView &input, DateTime evaluation_time)
        {
            static_cast<void>(evaluation_time);
            // A live TSD slot can acquire or lose its first value without a
            // key-set change. Reduction is over currently-valid values, so
            // every dictionary tick needs the sparse add/remove/modified
            // reconciliation below; ordinary value updates return
            // ``structural == false`` and do not rebuild the combiner tree.
            return input.modified();
        }

        [[nodiscard]] bool list_structure_modified(const TSInputView &input, DateTime)
        {
            return input.modified();
        }

        void append_modified_dict_leaves(const ReduceNodeStorage &storage, TSInputView &input,
                                         std::vector<std::size_t> &leaves)
        {
            static_cast<void>(input);
            auto collection = storage.collection_source.data_view();
            auto dict = collection.as_dict();
            for (std::size_t slot = dict.next_modified_slot(); slot != TS_DATA_NO_CHILD_ID;
                 slot = dict.next_modified_slot(slot))
            {
                const auto found = storage.key_to_leaf.find(dict.key_at_slot(slot));
                if (found != storage.key_to_leaf.end()) { leaves.push_back(found->second); }
            }
        }

        void append_modified_list_leaves(const ReduceNodeStorage &storage, TSInputView &input,
                                         std::vector<std::size_t> &leaves)
        {
            auto list = input.as_list();
            auto list_data = list.data_view();
            for (const std::size_t index : list_data.modified_indices())
            {
                const Value key{static_cast<Int>(index)};
                const auto found = storage.key_to_leaf.find(key);
                if (found != storage.key_to_leaf.end()) { leaves.push_back(found->second); }
            }
        }

        [[nodiscard]] TSOutputView dict_leaf_output(TSInputView &, TSOutputView source, std::size_t,
                                                    const Value &, std::size_t source_slot)
        {
            auto dict = source.as_dict();
            return source_slot < dict.slot_capacity() && dict.slot_live(source_slot)
                       ? dict.at_slot(source_slot)
                       : TSOutputView{};
        }

        [[nodiscard]] TSOutputView list_leaf_output(TSInputView &input, TSOutputView,
                                                    std::size_t, const Value &,
                                                    std::size_t source_slot)
        {
            auto list = input.as_list();
            if (source_slot >= list.size()) { return TSOutputView{}; }
            TSOutputHandle source = effective_output_handle(list[source_slot].bound_output());
            return source.bound() ? source.view(input.evaluation_time()) : TSOutputView{};
        }

        [[nodiscard]] const ReduceCollectionOps &reduce_collection_ops_for(
            const TSValueTypeMetaData &schema)
        {
            static const ReduceCollectionOps dict_ops{
                .reconcile = &reconcile_dict_collection,
                .structure_modified = &dict_structure_modified,
                .append_modified_leaves = &append_modified_dict_leaves,
                .leaf_output = &dict_leaf_output,
            };
            static const ReduceCollectionOps list_ops{
                .reconcile = &reconcile_list_collection,
                .structure_modified = &list_structure_modified,
                .append_modified_leaves = &append_modified_list_leaves,
                .leaf_output = &list_leaf_output,
            };
            return schema.kind == TSTypeKind::TSD ? dict_ops : list_ops;
        }

        void begin_reduce_publication(TSOutputView output, const TSOutputView &source,
                                      ReduceNodeStorage &storage, DateTime evaluation_time,
                                      bool sample_all)
        {
            TSOutputView resolved_source = resolve_forwarding_source(source.borrowed_ref());
            if (storage.publication_snapshot_active)
            {
                const TSOutputHandle next_source = resolved_source.bound()
                                                       ? resolved_source.handle()
                                                       : TSOutputHandle{};
                storage.publication_full_reconcile =
                    storage.publication_full_reconcile ||
                    !next_source.same_as(storage.pending_publication_source);
                storage.publication_sample_all = storage.publication_sample_all || sample_all;
                storage.pending_publication_source = next_source;
                return;
            }

            const TSOutputHandle previous = output.forwarding() ? output.forwarding_target()
                                                                 : TSOutputHandle{};
            const bool changed = output.forwarding() && previous.bound() &&
                                 (!resolved_source.bound() || !previous.same_as(resolved_source.handle()));
            const auto *schema = output.schema();
            const bool keyed = schema != nullptr &&
                               (schema->kind == TSTypeKind::TSD || schema->kind == TSTypeKind::TSS);

            if (!changed || !keyed)
            {
                bind_reduce_output(std::move(output), source, evaluation_time);
                return;
            }

            TSOutputView previous_view = previous.view(evaluation_time);
            if (!previous_view.valid())
            {
                bind_reduce_output(std::move(output), source, evaluation_time);
                return;
            }

            auto &snapshot = storage.publication_snapshot;
            snapshot.emplace(schema);
            DateTime snapshot_time = previous_view.last_modified_time();
            if (snapshot_time == MIN_DT) { snapshot_time = evaluation_time; }
            auto snapshot_view = snapshot->view(snapshot_time);
            if (schema->kind == TSTypeKind::TSD)
            {
                auto mutation = snapshot_view.as_dict().begin_mutation(snapshot_time);
                static_cast<void>(mutation.copy_value_from(previous_view.value()));
            }
            else
            {
                auto mutation = snapshot_view.as_set().begin_mutation(snapshot_time);
                static_cast<void>(mutation.copy_value_from(previous_view.value()));
            }

            // First move the forwarding endpoint to an equal, stable snapshot
            // without publishing a second transition. The snapshot is updated
            // after the new combiner root evaluates, producing a minimal delta
            // while the retired source is free to stop immediately.
            auto timeless_output = output.handle().view(MIN_DT);
            timeless_output.bind_forwarding_target(snapshot_view);
            storage.pending_publication_source = resolved_source.bound()
                                                     ? resolved_source.handle()
                                                     : TSOutputHandle{};
            storage.publication_snapshot_active = true;
            storage.publication_full_reconcile = true;
            storage.publication_sample_all = sample_all;
        }

        void update_reduce_dict_publication(TSDOutputView snapshot, const TSOutputView &source,
                                            DateTime evaluation_time, bool full_reconcile,
                                            bool sample_all)
        {
            auto mutation = snapshot.begin_mutation(evaluation_time);
            if (!source.valid())
            {
                mutation.clear();
                return;
            }

            auto source_dict = source.as_dict();
            if (full_reconcile)
            {
                std::vector<Value> removed_keys;
                removed_keys.reserve(snapshot.size());
                for (const ValueView &key : snapshot.keys())
                {
                    if (!source_dict.contains(key)) { removed_keys.emplace_back(key); }
                }
                for (const Value &key : removed_keys)
                {
                    static_cast<void>(mutation.erase(key.view()));
                }
                for (auto &&[key, child] : source_dict.items())
                {
                    if (!child.valid()) { continue; }
                    const bool changed = !mutation.contains(key) || !mutation.at(key).valid() ||
                                         !mutation.at(key).value().equals(child.value());
                    if (sample_all)
                    {
                        auto child_mutation = mutation.at(key).begin_mutation(evaluation_time);
                        static_cast<void>(child_mutation.copy_value_from(child.value()));
                        child_mutation.mark_modified();
                    }
                    else if (changed) { mutation.set(key, child.value()); }
                }
                return;
            }

            for (const ValueView &key : source_dict.removed_keys())
            {
                static_cast<void>(mutation.erase(key));
            }
            for (auto &&[key, child] : source_dict.modified_items())
            {
                if (!child.valid()) { continue; }
                const bool changed = !mutation.contains(key) || !mutation.at(key).valid() ||
                                     !mutation.at(key).value().equals(child.value());
                if (changed) { mutation.set(key, child.value()); }
            }
        }

        void update_reduce_set_publication(TSSOutputView snapshot, const TSOutputView &source,
                                           DateTime evaluation_time, bool full_reconcile,
                                           bool sample_all)
        {
            auto mutation = snapshot.begin_mutation(evaluation_time);
            if (!source.valid())
            {
                mutation.clear();
                return;
            }

            auto source_set = source.as_set();
            if (full_reconcile)
            {
                std::vector<Value> removed_keys;
                removed_keys.reserve(snapshot.size());
                for (const ValueView &key : snapshot.values())
                {
                    if (!source_set.contains(key)) { removed_keys.emplace_back(key); }
                }
                for (const Value &key : removed_keys)
                {
                    static_cast<void>(mutation.remove(key.view()));
                }
                for (const ValueView &key : source_set.values())
                {
                    if (!mutation.contains(key)) { static_cast<void>(mutation.add(key)); }
                }
                if (sample_all) { mutation.touch(); }
                return;
            }

            for (const ValueView &key : source_set.removed())
            {
                static_cast<void>(mutation.remove(key));
            }
            for (const ValueView &key : source_set.added())
            {
                if (!mutation.contains(key)) { static_cast<void>(mutation.add(key)); }
            }
        }

        void finish_reduce_publication(ReduceNodeStorage &storage, DateTime evaluation_time)
        {
            if (!storage.publication_snapshot_active) { return; }

            auto &snapshot = storage.publication_snapshot;
            if (!snapshot.has_value())
            {
                throw std::logic_error("reduce publication snapshot is not available");
            }

            TSOutputView source = storage.pending_publication_source.bound()
                                      ? storage.pending_publication_source.view(evaluation_time)
                                      : TSOutputView{};

            auto snapshot_view = snapshot->view(evaluation_time);
            if (snapshot_view.schema()->kind == TSTypeKind::TSD)
            {
                update_reduce_dict_publication(snapshot_view.as_dict(), source, evaluation_time,
                                               storage.publication_full_reconcile,
                                               storage.publication_sample_all);
            }
            else
            {
                update_reduce_set_publication(snapshot_view.as_set(), source, evaluation_time,
                                              storage.publication_full_reconcile,
                                              storage.publication_sample_all);
            }
            storage.publication_full_reconcile = false;
            storage.publication_sample_all = false;
        }

        [[nodiscard]] TSOutputHandle effective_output_handle(TSOutputView source)
        {
            if (!source.bound()) { return {}; }

            TSOutputHandle current = source.handle();
            while (source.forwarding())
            {
                TSOutputHandle target = source.forwarding_target();
                if (!target.bound() || target.same_as(current)) { break; }
                current = target;
                source = target.view(source.evaluation_time());
            }
            return current;
        }

        /**
         * Recompute the internal structure after a structural event (key
         * add/remove or capacity growth) — value ticks never come through
         * here; the standing bindings carry them. Three phases, ordered so a
         * link is never bound to or unbound from a dead target:
         *
         * 1. **create** newly-needed combiners, deepest-first, and set aside
         *    no-longer-needed ones (alive, not yet destroyed);
         * 2. **bind** every live combiner's inputs (a same-handle bind is a
         *    no-op; a re-point samples) and publish the root — every old
         *    target is still alive while subscribers move off it;
         * 3. **retire** the set-aside combiners root-first (a doomed parent
         *    unbinds from its still-alive doomed child).
         */
        void append_structural_leaf_path(const ReduceNodeStorage &storage, std::size_t leaf,
                                         std::vector<std::size_t> &positions)
        {
            std::size_t position = internal_count(storage) + leaf;
            while (position > 0)
            {
                position = (position - 1) / 2;
                if (position < storage.combiners.size()) { positions.push_back(position); }
            }
        }

        void rebuild_structure(const NodeView &view, const ReduceNodeContext &context, ReduceNodeStorage &storage,
                               DateTime evaluation_time, bool full_structure)
        {
            const std::size_t old_capacity = storage.leaf_capacity;
            const std::size_t live = storage.dense_to_key.size();
            std::size_t       capacity = storage.leaf_capacity;
            // Only the explicit-zero form needs a singleton root combiner.
            // The no-zero form keeps cardinality zero/one free of combiner
            // slots and grows to the ordinary tree when a second value arrives.
            const std::size_t minimum_capacity = context.spec.has_zero ? 2U : 0U;
            capacity = std::max(
                {capacity, minimum_capacity, live > 0 ? std::bit_ceil(live) : std::size_t{0}});

            const std::size_t old_bank = storage.current_bank;
            std::vector<CombinerEntry *> retired_shape{};
            std::vector<std::pair<std::size_t, CombinerEntry *>> retired{};
            std::vector<std::size_t> created{};
            const bool bank_changed = capacity != storage.leaf_capacity;
            if (bank_changed)
            {
                full_structure = true;
                // Capacity growth changes the tree shape wholesale. Build the
                // replacement in the inactive bank and retain the stopped old
                // generation through this cycle.
                const std::size_t next_bank = 1U - storage.current_bank;
                auto &new_bank = storage.combiner_banks[next_bank];
                if (new_bank.has_entries())
                {
                    throw std::logic_error("reduce_ inactive combiner bank is still occupied");
                }
                const std::size_t next_combiner_count = capacity > 1 ? capacity - 1 : 0;
                new_bank.reserve_to(next_combiner_count);
                std::vector<CombinerEntry *> next_combiners(next_combiner_count, nullptr);

                retired_shape = std::move(storage.combiners);
                storage.combiners = std::move(next_combiners);
                storage.current_bank = next_bank;
                storage.leaf_capacity = capacity;
            }
            else
            {
                storage.combiner_banks[storage.current_bank].reserve_to(storage.combiners.size());
            }

            storage.structural_positions.clear();
            if (full_structure)
            {
                storage.structural_positions.reserve(storage.combiners.size());
                for (std::size_t position = storage.combiners.size(); position-- > 0;)
                {
                    storage.structural_positions.push_back(position);
                }
            }
            else
            {
                for (const std::size_t leaf : storage.structural_leaves)
                {
                    append_structural_leaf_path(storage, leaf, storage.structural_positions);
                }
                std::ranges::sort(storage.structural_positions, std::greater{});
                const auto unique_end = std::ranges::unique(storage.structural_positions).begin();
                storage.structural_positions.erase(unique_end, storage.structural_positions.end());
            }
            auto rollback = UnwindCleanupGuard([&] {
                auto &current_bank = storage.combiner_banks[storage.current_bank];
                if (bank_changed)
                {
                    for (const std::size_t position : created)
                    {
                        reset_combiner_noexcept(current_bank, position);
                    }
                    storage.combiners    = std::move(retired_shape);
                    storage.leaf_capacity = old_capacity;
                    storage.current_bank = old_bank;
                    return;
                }

                for (const std::size_t position : created)
                {
                    if (position < storage.combiners.size())
                    {
                        reset_combiner_noexcept(current_bank, position);
                        storage.combiners[position] = nullptr;
                    }
                }
                for (auto &entry : retired)
                {
                    if (entry.first < storage.combiners.size() && storage.combiners[entry.first] == nullptr)
                    {
                        storage.combiners[entry.first] = std::move(entry.second);
                    }
                }
            });

            // Phase 1 — deepest-first so parents can reference fresh children.
            for (const std::size_t position : storage.structural_positions)
            {
                const Aggregate left   = resolve_aggregate(storage, 2 * position + 1);
                const Aggregate right  = resolve_aggregate(storage, 2 * position + 2);
                const bool      needed = (position == 0 && context.spec.has_zero && live == 1) ||
                                         (left.kind != Aggregate::Kind::Empty &&
                                          right.kind != Aggregate::Kind::Empty);
                auto &entry = storage.combiners[position];

                if (needed && entry == nullptr)
                {
                    auto &bank = storage.combiner_banks[storage.current_bank];
                    entry = &bank.construct_at(position);
                    created.push_back(position);
                    entry->graph = context.spec.child.graph_builder.make_nested_graph(
                        view.pointer(),
                        bank.graph_memory(position), context.graph_layout);
                    if (context.spec.child.output_binding->kind != NestedGraphOutputBinding::Kind::ParentInput)
                    {
                        entry->output = walk_ts_path(
                            entry->graph.view()
                                .node_at(context.spec.child.output_binding->source.node)
                                .output(evaluation_time),
                            context.spec.child.output_binding->source.path).handle();
                    }
                }
                else if (!needed && entry != nullptr)
                {
                    retired.emplace_back(position, std::exchange(entry, nullptr));
                }
            }

            // Phase 2 — generic combiners need their child-graph inputs bound
            // and graphs started. A lifted scalar capability reads aggregate
            // outputs directly, so activating the otherwise-unused child
            // inputs would only create redundant subscription/scheduler work.
            if (context.spec.lifted_kernel == nullptr)
            {
                for (auto position_it = storage.structural_positions.rbegin();
                     position_it != storage.structural_positions.rend(); ++position_it)
                {
                    const std::size_t position = *position_it;
                    auto &entry = storage.combiners[position];
                    if (entry == nullptr) { continue; }
                    const Aggregate left  = resolve_aggregate(storage, 2 * position + 1);
                    const Aggregate right = resolve_aggregate(storage, 2 * position + 2);
                    const bool created_now = std::ranges::find(created, position) != created.end();
                    bind_combiner_inputs(view, context, storage, *entry, left, right, evaluation_time,
                                         !created_now);
                }
                for (auto it = created.rbegin(); it != created.rend(); ++it)
                {
                    auto child = storage.combiners[*it]->graph.view();
                    child.start(evaluation_time);
                    schedule_sampled_input_consumers(
                        child, evaluation_time, context.spec.child.input_bindings);
                }
            }

            // Root publication: Empty -> the zero input's bound output (or
            // unbound), Leaf -> that element, Node -> the combiner output. A
            // re-point to an already-valid source is a tick of this output at
            // the publication time (the sampled contract): the aliased VALUE
            // changed even though the new target did not write this cycle.
            const Aggregate root   = root_aggregate(context, storage);
            TSOutputView    source = aggregate_output(view, context, storage, root, evaluation_time);
            begin_reduce_publication(view.output(evaluation_time), source, storage, evaluation_time,
                                     bank_changed);
            storage.published = true;

            // Phase 3 — stop root-first, but keep the retired graph storage
            // through this engine cycle. Dynamic target links may still read
            // the old aggregate later in the same cycle.
            const std::size_t retired_shape_count = static_cast<std::size_t>(
                std::count_if(retired_shape.begin(), retired_shape.end(),
                              [](const CombinerEntry *entry) { return entry != nullptr; }));
            storage.previous_generation.reserve(storage.previous_generation.size() + retired.size() +
                                                retired_shape_count);
            for (auto it = retired.rbegin(); it != retired.rend(); ++it)
            {
                stop_combiner_noexcept(it->second);
                storage.previous_generation.push_back({storage.current_bank, it->first});
            }
            for (std::size_t position = 0; position < retired_shape.size(); ++position)
            {
                CombinerEntry *entry = retired_shape[position];
                stop_combiner_noexcept(entry);
                if (entry != nullptr) { storage.previous_generation.push_back({old_bank, position}); }
            }
            if (!storage.previous_generation.empty()) { storage.previous_generation_time = evaluation_time; }
            rollback.release();
        }

        // Reconcile the leaf state + rebuild the combiner tree for this cycle (the
        // "setup"). Skipped on a pause/resume re-entry (the structure does not change
        // mid-cycle).
        [[nodiscard]] bool reduce_reconcile(const NodeView &view, const ReduceNodeContext &context,
                                            ReduceNodeStorage &storage, DateTime evaluation_time)
        {
            auto root_input = view.input(evaluation_time);
            auto collection_input = root_input.indexed_child_at(0);

            TSOutputHandle collection_source = effective_output_handle(collection_input.bound_output());
            TSOutputHandle zero_source{};
            if (context.spec.has_zero)
            {
                zero_source = effective_output_handle(root_input.indexed_child_at(1).bound_output());
            }
            const bool list_collection = collection_input.schema()->kind == TSTypeKind::TSL;
            const bool collection_repointed = storage.source_handles_initialised &&
                                              !collection_source.same_as(storage.collection_source);
            const bool zero_repointed = context.spec.has_zero && storage.source_handles_initialised &&
                                        !zero_source.same_as(storage.zero_source);
            storage.collection_source = collection_source;
            storage.zero_source = zero_source;
            storage.source_handles_initialised = true;

            storage.structural_leaves.clear();
            storage.structural_positions.clear();
            bool structural = false;
            bool full_structure = collection_repointed || !storage.published;
            const bool collection_available = list_collection ? collection_input.bound()
                                                              : collection_input.valid();
            if (collection_available)
            {
                if (!storage.primed || collection_repointed ||
                    context.collection_ops->structure_modified(collection_input, evaluation_time))
                {
                    structural = context.collection_ops->reconcile(
                                     storage, collection_input, !storage.primed || collection_repointed) ||
                                 structural;
                    full_structure = full_structure || !storage.primed;
                    storage.primed  = true;
                }
            }
            else if (storage.primed || !storage.dense_to_key.empty())
            {
                clear_leaf_state(storage);
                structural = true;
                full_structure = true;
                storage.primed = false;
            }

            const bool zero_affects_structure =
                zero_repointed && storage.dense_to_key.size() <= 1;
            full_structure = full_structure || zero_affects_structure;
            if (structural || collection_repointed || zero_affects_structure || !storage.published)
            {
                rebuild_structure(view, context, storage, evaluation_time, full_structure);
                return true;
            }
            return false;
        }

        void append_leaf_path(const ReduceNodeStorage &storage, std::size_t leaf,
                              SlotBitmap &positions)
        {
            std::size_t position = internal_count(storage) + leaf;
            while (position > 0)
            {
                position = (position - 1) / 2;
                if (position < storage.combiners.size() && storage.combiners[position] != nullptr)
                {
                    positions.set(position);
                }
            }
        }

        void materialize_descending(const SlotBitmap &candidates,
                                    std::vector<std::size_t> &positions)
        {
            for (std::size_t word_index = candidates.word_count(); word_index-- > 0;)
            {
                std::uint64_t word = candidates.words[word_index];
                while (word != 0)
                {
                    const auto bit = static_cast<std::size_t>(63U - std::countl_zero(word));
                    const std::size_t position = word_index * SlotBitmap::bits_per_word + bit;
                    if (position < candidates.size()) { positions.push_back(position); }
                    word &= ~(std::uint64_t{1} << bit);
                }
            }
        }

        void prepare_reduce_evaluation_positions(const NodeView &view, const ReduceNodeContext &context,
                                                  ReduceNodeStorage &storage, DateTime evaluation_time,
                                                  bool rebuilt)
        {
            storage.evaluation_positions.clear();
            storage.resume_candidate_plus_one = 0;
            storage.evaluation_candidates.resize(storage.combiners.size());
            storage.evaluation_candidates.reset();

            auto root_input = view.input(evaluation_time);
            auto collection_input = root_input.indexed_child_at(0);
            const bool collection_event = collection_input.modified();
            const bool zero_event = context.spec.has_zero &&
                                    root_input.indexed_child_at(1).modified();
            const bool input_event = collection_event || zero_event;
            bool full_scan = storage.has_future_combiner_schedule || (!rebuilt && !input_event);

            if (rebuilt && !full_scan)
            {
                for (const std::size_t position : storage.structural_positions)
                {
                    if (position < storage.combiners.size() && storage.combiners[position] != nullptr)
                    {
                        storage.evaluation_candidates.set(position);
                    }
                }
            }

            // A structural delta may also modify existing leaves. Rebinding
            // the added/removed paths does not evaluate unaffected sibling
            // paths, so include value modifications even after a rebuild.
            if (!full_scan && collection_event &&
                (collection_input.valid() || collection_input.schema()->kind == TSTypeKind::TSL))
            {
                storage.modified_leaves.clear();
                context.collection_ops->append_modified_leaves(
                    storage, collection_input, storage.modified_leaves);
                for (const std::size_t leaf : storage.modified_leaves)
                {
                    append_leaf_path(storage, leaf, storage.evaluation_candidates);
                }
            }

            // The explicit zero is an operand only for a singleton. It must
            // neither schedule nor perturb a reduction containing 2+ values.
            if (!full_scan && zero_event && storage.dense_to_key.size() == 1 &&
                !storage.combiners.empty() && storage.combiners[0] != nullptr)
            {
                storage.evaluation_candidates.set(0);
            }

            if (full_scan)
            {
                storage.evaluation_positions.reserve(storage.combiners.size());
                for (std::size_t position = storage.combiners.size(); position-- > 0;)
                {
                    if (storage.combiners[position] != nullptr)
                    {
                        storage.evaluation_positions.push_back(position);
                    }
                }
            }
            else
            {
                materialize_descending(storage.evaluation_candidates, storage.evaluation_positions);
            }
            storage.has_future_combiner_schedule = false;
        }

        [[nodiscard]] bool evaluate_lifted_combiner(const NodeView &view,
                                                     const ReduceNodeContext &context,
                                                     const ReduceNodeStorage &storage,
                                                     std::size_t position,
                                                     DateTime evaluation_time)
        {
            const LiftedKernel *kernel = context.spec.lifted_kernel;
            if (kernel == nullptr) { return false; }

            TSOutputView left = aggregate_output(
                view, context, storage, resolve_aggregate(storage, 2 * position + 1), evaluation_time);
            TSOutputView right = aggregate_output(
                view, context, storage, resolve_aggregate(storage, 2 * position + 2), evaluation_time);
            // Live dynamic collection entries acquire a value when created,
            // but retain the generic sampled behavior if a future collection
            // kind can expose an unset leaf: no output is published until
            // both sides have a current value.
            if (!left.valid() || !right.valid()) { return true; }

            std::array<ValueView, 2> args{left.value(), right.value()};
            TSOutputView destination = aggregate_output(
                view, context, storage, Aggregate{Aggregate::Kind::Node, position}, evaluation_time);
            if (!destination.bound())
            {
                throw std::logic_error("reduce lifted combiner output is unbound");
            }
            auto mutation = destination.begin_mutation(evaluation_time);
            kernel->eval_into(mutation,
                              std::span<const ValueView>{args.data(), args.size()});
            return true;
        }

        // Evaluates the combiner tree, supporting pause/resume. A combiner that pauses (a
        // mesh nested in the reduce function needs a sibling) propagates the pause: save the
        // descending-position cursor and return false so the enclosing mesh resolves it and
        // re-evaluates us. On resume the reconciliation is skipped and the descending loop
        // continues from the saved position; completion resets the cursor.
        bool reduce_evaluate(const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }

            auto        reduce_view = view.as<ReduceNodeView>();
            const auto &context     = *static_cast<const ReduceNodeContext *>(reduce_view.internal_context());
            auto       &storage     = *MemoryUtils::cast<ReduceNodeStorage>(reduce_view.internal_storage());
            storage.initialise(context.graph_layout);

            const bool resuming = storage.resume_candidate_plus_one != 0;
            if (!resuming)
            {
                storage.destroy_previous_generation_before(evaluation_time);
                const bool rebuilt = reduce_reconcile(view, context, storage, evaluation_time);
                prepare_reduce_evaluation_positions(view, context, storage, evaluation_time, rebuilt);
            }

            // Evaluate due combiners deepest-first (descending heap index): a leaf tick
            // schedules its combiner directly; that combiner's output tick schedules its
            // parent (lower index, processed later), so the cascade settles in one pass.
            // On resume, continue from the paused position downward (positions above it
            // already ran this cycle). Unevaluated children pull future schedules up to
            // this reduce node.
            const std::size_t start_candidate = resuming ? storage.resume_candidate_plus_one - 1 : 0;
            for (std::size_t candidate = start_candidate;
                 candidate < storage.evaluation_positions.size(); ++candidate)
            {
                const std::size_t position = storage.evaluation_positions[candidate];
                const auto &entry = storage.combiners[position];
                if (entry == nullptr || !entry->graph.has_value()) { continue; }
                if (!resuming && evaluate_lifted_combiner(
                                     view, context, storage, position, evaluation_time))
                {
                    continue;
                }
                auto       child       = entry->graph.view();
                const bool resume_this = resuming && candidate == start_candidate;
                if ((child.next_scheduled_time() <= evaluation_time || resume_this) &&
                    !child.evaluate(evaluation_time))
                {
                    storage.resume_candidate_plus_one = candidate + 1;
                    return false;
                }
                if (const DateTime next = child.next_scheduled_time(); next != MAX_DT && next > evaluation_time)
                {
                    storage.has_future_combiner_schedule = true;
                    view.graph().schedule_node(view.node_index(), next);
                }
            }
            storage.resume_candidate_plus_one = 0;
            storage.evaluation_positions.clear();
            finish_reduce_publication(storage, evaluation_time);
            return true;
        }

        bool reduce_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            return reduce_evaluate(view, evaluation_time);
        }

        void reduce_node_stop(const NodeView &view, DateTime)
        {
            auto  reduce_view = view.as<ReduceNodeView>();
            auto &storage     = *MemoryUtils::cast<ReduceNodeStorage>(reduce_view.internal_storage());
            for (const auto *entry : storage.combiners)
            {
                if (entry != nullptr && entry->graph.has_value()) { entry->graph.view().stop(); }
            }
            storage.evaluation_positions.clear();
            storage.modified_leaves.clear();
            storage.structural_leaves.clear();
            storage.structural_positions.clear();
            storage.resume_candidate_plus_one = 0;
            storage.has_future_combiner_schedule = false;
        }

        void validate_reduce_node_spec(const NodeTypeMetaData &meta, const ReduceNodeSpec &spec)
        {
            if (!spec.child.output_binding.has_value())
            {
                throw std::invalid_argument("reduce_node requires a combiner output binding");
            }
            if (meta.input_schema == nullptr || meta.input_schema->kind != TSTypeKind::TSB ||
                meta.input_schema->field_count() != (spec.has_zero ? 2 : 1))
            {
                throw std::invalid_argument(
                    "reduce_node requires input schema [ts] with an optional trailing zero");
            }
            const auto *fields = meta.input_schema->fields();
            if (fields[0].type == nullptr ||
                (fields[0].type->kind != TSTypeKind::TSD &&
                 fields[0].type->kind != TSTypeKind::TSL))
            {
                throw std::invalid_argument("reduce_node first input must be a TSD or TSL");
            }
            if (meta.output_schema == nullptr)
            {
                throw std::invalid_argument("reduce_node requires an output schema");
            }

            const std::size_t child_node_count = spec.child.graph_builder.node_count();
            const auto       &output_binding   = *spec.child.output_binding;
            if (!output_binding.target_path.empty())
            {
                throw std::invalid_argument("reduce_node combiner output binding must target the output root");
            }
            if (output_binding.kind == NestedGraphOutputBinding::Kind::ParentInput)
            {
                if (output_binding.parent_source_path.empty() || output_binding.parent_source_path[0] > 1)
                {
                    throw std::invalid_argument(
                        "reduce_node parent-input combiner output must select lhs or rhs");
                }
            }
            else if (output_binding.source.node >= child_node_count)
            {
                throw std::invalid_argument("reduce_node combiner output source node is out of range");
            }

            for (const NestedGraphInputBinding &binding : spec.child.input_bindings)
            {
                if (binding.source_path.empty() || binding.source_path[0] > 1)
                {
                    throw std::invalid_argument("reduce_node combiner inputs must be sourced from lhs or rhs");
                }
                if (binding.target.node >= child_node_count)
                {
                    throw std::invalid_argument("reduce_node combiner input target node is out of range");
                }
            }
        }
    }  // namespace

    const void *ReduceNodeView::node_view_type_id() noexcept
    {
        static const char token{};
        return &token;
    }

    ReduceNodeView ReduceNodeView::from_node(NodeView view, const void *context)
    {
        if (context == nullptr) { throw std::logic_error("ReduceNodeView requires a typed view context"); }
        const auto &typed_context = *static_cast<const ReduceNodeContext *>(context);
        void       *storage = MemoryUtils::advance(view.data(), typed_context.storage_offset);
        return ReduceNodeView{std::move(view), context, storage};
    }

    const NodeView &ReduceNodeView::node() const noexcept { return view_; }

    std::size_t ReduceNodeView::leaf_count() const noexcept
    {
        return MemoryUtils::cast<ReduceNodeStorage>(storage_)->dense_to_key.size();
    }

    std::size_t ReduceNodeView::combiner_count() const noexcept
    {
        const auto &combiners = MemoryUtils::cast<ReduceNodeStorage>(storage_)->combiners;
        std::size_t count     = 0;
        for (const auto *entry : combiners)
        {
            if (entry != nullptr) { ++count; }
        }
        return count;
    }

    bool ReduceNodeView::child_graphs_use_in_place_storage() const noexcept
    {
        const auto &combiners = MemoryUtils::cast<ReduceNodeStorage>(storage_)->combiners;
        for (const auto *entry : combiners)
        {
            if (entry != nullptr && entry->graph.has_value() && !entry->graph.uses_external_storage()) { return false; }
        }
        return true;
    }

    ReduceNodeView::ReduceNodeView(NodeView view, const void *context, void *storage) noexcept
        : view_(std::move(view)),
          context_(context),
          storage_(storage)
    {
    }

    NodeBuilder reduce_node(NodeTypeMetaData meta, ReduceNodeSpec spec)
    {
        validate_reduce_node_spec(meta, spec);

        meta.node_kind              = NodeKind::Nested;
        meta.valid_inputs          = std::vector<std::size_t>{};
        meta.output_endpoint_schema = reduce_output_endpoint_schema(meta.output_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema = std::move(meta);

        const std::array fields{NodeStorageField{
            .name = reduce_storage_field_name,
            .plan = &MemoryUtils::plan_for<ReduceNodeStorage>(),
        }};
        // Default (before-output) placement: the node's forwarding output
        // links INTO field-held combiner outputs — the output (the link) must
        // destroy before the field (the nested_/switch_ direction).
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const MemoryUtils::StorageLayout graph_layout = spec.child.graph_builder.nested_storage_layout();
        const ReduceCollectionOps &collection_ops =
            reduce_collection_ops_for(*descriptor.schema.input_schema->fields()[0].type);

        descriptor.callbacks.stop            = &reduce_node_stop;
        descriptor.ops.evaluate_impl         = &reduce_evaluate_impl;
        descriptor.ops.storage_metrics_impl  = &reduce_storage_metrics;
        descriptor.ops.extended_view_type_id = ReduceNodeView::node_view_type_id();
        descriptor.ops.extended_view_context = &register_reduce_node_context(
            std::move(spec), descriptor.storage_plan->component(reduce_storage_field_name).offset,
            graph_layout, collection_ops);

        return NodeBuilder::from_descriptor(std::move(descriptor));
    }
}  // namespace hgraph
