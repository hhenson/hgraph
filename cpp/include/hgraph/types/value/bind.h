//
// Created by Howard Henson on 13/12/2025.
//
// Schema-driven binding for REF dereferencing
//

#ifndef HGRAPH_VALUE_BIND_H
#define HGRAPH_VALUE_BIND_H

#include <hgraph/types/value/bound_value.h>
#include <hgraph/types/value/time_series_value.h>
#include <hgraph/types/value/ref_type.h>
#include <hgraph/types/value/bundle_type.h>
#include <hgraph/types/value/list_type.h>
#include <hgraph/types/value/set_type.h>
#include <hgraph/types/value/dict_type.h>
#include <hgraph/types/value/window_type.h>
#include <hgraph/types/value/value.h>

namespace hgraph::value {

    /**
     * SchemaMatchKind - Result of comparing input and output schemas
     */
    enum class SchemaMatchKind {
        Peer,       // Exact match: input and output are the same type
        Deref,      // Output is REF[X] and input expects X
        Composite,  // Need to recursively match children (TSB/TSL)
        Mismatch    // Incompatible schemas
    };

    /**
     * Match input schema against output schema
     *
     * Schema matching rules:
     *   Input Schema    Output Schema       Result
     *   ─────────────   ─────────────────   ────────────────────
     *   TS[X]           TS[X]               Peer (same value)
     *   TS[X]           REF[TS[X]]          Deref
     *   TSB[a:X,b:Y]    TSB[a:REF[X],b:Y]   Composite (per-field)
     *   TSL[X,N]        TSL[REF[X],N]       Composite (per-element)
     *
     * @param input_schema What the input expects
     * @param output_schema What the output provides
     * @return The kind of matching/binding needed
     */
    inline SchemaMatchKind match_schemas(const TypeMeta* input_schema, const TypeMeta* output_schema) {
        if (!input_schema || !output_schema) {
            return SchemaMatchKind::Mismatch;
        }

        // Check for REF unwrapping first
        if (output_schema->kind == TypeKind::Ref) {
            auto* ref_meta = static_cast<const RefTypeMeta*>(output_schema);

            // Check if ref's value_type matches what input expects
            if (ref_meta->value_type == input_schema) {
                return SchemaMatchKind::Deref;
            }

            // Check if ref wraps a type compatible with input
            // (recursive check for nested refs)
            if (ref_meta->value_type) {
                auto nested = match_schemas(input_schema, ref_meta->value_type);
                if (nested != SchemaMatchKind::Mismatch) {
                    return SchemaMatchKind::Deref;  // Will need nested dereferencing
                }
            }
        }

        // Exact match check (same TypeMeta pointer)
        if (input_schema == output_schema) {
            return SchemaMatchKind::Peer;
        }

        // Same kind - might need composite matching
        if (input_schema->kind == output_schema->kind) {
            switch (input_schema->kind) {
                case TypeKind::Bundle: {
                    auto* in_bundle = static_cast<const BundleTypeMeta*>(input_schema);
                    auto* out_bundle = static_cast<const BundleTypeMeta*>(output_schema);

                    // Must have same number of fields
                    if (in_bundle->fields.size() != out_bundle->fields.size()) {
                        return SchemaMatchKind::Mismatch;
                    }

                    // Check if any field needs dereferencing
                    bool needs_composite = false;
                    for (size_t i = 0; i < in_bundle->fields.size(); ++i) {
                        auto field_match = match_schemas(
                            in_bundle->fields[i].type,
                            out_bundle->fields[i].type
                        );
                        if (field_match == SchemaMatchKind::Mismatch) {
                            return SchemaMatchKind::Mismatch;
                        }
                        if (field_match == SchemaMatchKind::Deref ||
                            field_match == SchemaMatchKind::Composite) {
                            needs_composite = true;
                        }
                    }
                    return needs_composite ? SchemaMatchKind::Composite : SchemaMatchKind::Peer;
                }

                case TypeKind::List: {
                    auto* in_list = static_cast<const ListTypeMeta*>(input_schema);
                    auto* out_list = static_cast<const ListTypeMeta*>(output_schema);

                    // Must have same count
                    if (in_list->count != out_list->count) {
                        return SchemaMatchKind::Mismatch;
                    }

                    // Check element type matching
                    auto elem_match = match_schemas(in_list->element_type, out_list->element_type);
                    if (elem_match == SchemaMatchKind::Mismatch) {
                        return SchemaMatchKind::Mismatch;
                    }
                    if (elem_match == SchemaMatchKind::Deref ||
                        elem_match == SchemaMatchKind::Composite) {
                        return SchemaMatchKind::Composite;
                    }
                    return SchemaMatchKind::Peer;
                }

                case TypeKind::Set: {
                    auto* in_set = static_cast<const SetTypeMeta*>(input_schema);
                    auto* out_set = static_cast<const SetTypeMeta*>(output_schema);

                    // Check element type matching
                    auto elem_match = match_schemas(in_set->element_type, out_set->element_type);
                    if (elem_match == SchemaMatchKind::Mismatch) {
                        return SchemaMatchKind::Mismatch;
                    }
                    // Sets with REF elements need composite handling for delta tracking
                    if (elem_match == SchemaMatchKind::Deref ||
                        elem_match == SchemaMatchKind::Composite) {
                        return SchemaMatchKind::Composite;
                    }
                    return SchemaMatchKind::Peer;
                }

                case TypeKind::Dict: {
                    auto* in_dict = static_cast<const DictTypeMeta*>(input_schema);
                    auto* out_dict = static_cast<const DictTypeMeta*>(output_schema);

                    // Check key type matching
                    auto key_match = match_schemas(in_dict->key_type(), out_dict->key_type());
                    if (key_match == SchemaMatchKind::Mismatch) {
                        return SchemaMatchKind::Mismatch;
                    }

                    // Check value type matching
                    auto val_match = match_schemas(in_dict->value_type, out_dict->value_type);
                    if (val_match == SchemaMatchKind::Mismatch) {
                        return SchemaMatchKind::Mismatch;
                    }

                    // Dicts with REF values need composite handling for delta tracking
                    if (key_match == SchemaMatchKind::Deref ||
                        key_match == SchemaMatchKind::Composite ||
                        val_match == SchemaMatchKind::Deref ||
                        val_match == SchemaMatchKind::Composite) {
                        return SchemaMatchKind::Composite;
                    }
                    return SchemaMatchKind::Peer;
                }

                case TypeKind::Window: {
                    auto* in_win = static_cast<const WindowTypeMeta*>(input_schema);
                    auto* out_win = static_cast<const WindowTypeMeta*>(output_schema);

                    // Window parameters must match
                    if (in_win->capacity != out_win->capacity ||
                        in_win->storage_kind != out_win->storage_kind) {
                        return SchemaMatchKind::Mismatch;
                    }

                    // Check element type matching
                    auto elem_match = match_schemas(in_win->element_type, out_win->element_type);
                    if (elem_match == SchemaMatchKind::Mismatch) {
                        return SchemaMatchKind::Mismatch;
                    }
                    // Windows with REF elements need composite handling
                    if (elem_match == SchemaMatchKind::Deref ||
                        elem_match == SchemaMatchKind::Composite) {
                        return SchemaMatchKind::Composite;
                    }
                    return SchemaMatchKind::Peer;
                }

                case TypeKind::Scalar:
                    // For scalars, we need exact type match
                    return (input_schema->type_info == output_schema->type_info)
                        ? SchemaMatchKind::Peer
                        : SchemaMatchKind::Mismatch;

                case TypeKind::Ref:
                    // REF types handled above in the deref check
                    // If we get here, it means both are REFs - check their value types
                    {
                        auto* in_ref = static_cast<const RefTypeMeta*>(input_schema);
                        auto* out_ref = static_cast<const RefTypeMeta*>(output_schema);
                        return match_schemas(in_ref->value_type, out_ref->value_type);
                    }

                default:
                    // Other types - assume peer if kinds match
                    return SchemaMatchKind::Peer;
            }
        }

        return SchemaMatchKind::Mismatch;
    }

    // Forward declaration for recursive binding
    BoundValue bind_view(const TypeMeta* input_schema, TSView output_view, engine_time_t current_time);

    /**
     * Create a binding from input schema to output value
     *
     * This is the main entry point for creating bindings during wiring.
     * It analyzes the schemas and creates the appropriate binding type:
     * - Peer for exact matches
     * - Deref for REF unwrapping
     * - Composite for TSB/TSL with mixed bindings
     *
     * @param input_schema What the input expects
     * @param output_value The output providing the value (TSValue&)
     * @param current_time Current evaluation time (for initializing deref bindings)
     * @return BoundValue representing the binding
     */
    inline BoundValue bind(const TypeMeta* input_schema, TSValue& output_value, engine_time_t current_time = MIN_DT) {
        auto match = match_schemas(input_schema, output_value.schema());

        switch (match) {
            case SchemaMatchKind::Peer:
                return BoundValue::make_peer(&output_value);

            case SchemaMatchKind::Deref: {
                // Create deref wrapper
                auto view = output_value.view(current_time);
                auto deref = std::make_unique<DerefTSValue>(view, input_schema);
                return BoundValue::make_deref(std::move(deref), input_schema);
            }

            case SchemaMatchKind::Composite: {
                // Create composite with child bindings
                auto view = output_value.view(current_time);
                return bind_view(input_schema, view, current_time);
            }

            case SchemaMatchKind::Mismatch:
            default:
                return BoundValue{};  // Invalid binding
        }
    }

    /**
     * Create a binding from a TSView
     *
     * Used for recursive binding of composite types (TSB/TSL).
     *
     * @param input_schema What the input expects
     * @param output_view View to the output value
     * @param current_time Current evaluation time
     * @return BoundValue representing the binding
     */
    inline BoundValue bind_view(const TypeMeta* input_schema, TSView output_view, engine_time_t current_time) {
        if (!output_view.valid()) {
            return BoundValue{};
        }

        auto match = match_schemas(input_schema, output_view.schema());

        switch (match) {
            case SchemaMatchKind::Peer:
                // For view-based peer binding, we don't have a TSValue*
                // This is a limitation - peer bindings from views need special handling
                // In practice, peer bindings should come from the top-level bind()
                return BoundValue{};  // Can't create peer from view alone

            case SchemaMatchKind::Deref: {
                // Create deref wrapper from view
                auto deref = std::make_unique<DerefTSValue>(output_view, input_schema);
                return BoundValue::make_deref(std::move(deref), input_schema);
            }

            case SchemaMatchKind::Composite: {
                // Recursively bind children
                std::vector<BoundValue> children;

                switch (input_schema->kind) {
                    case TypeKind::Bundle: {
                        auto* in_bundle = static_cast<const BundleTypeMeta*>(input_schema);
                        children.reserve(in_bundle->fields.size());

                        for (size_t i = 0; i < in_bundle->fields.size(); ++i) {
                            auto field_view = output_view.field(i);
                            auto child = bind_view(in_bundle->fields[i].type, field_view, current_time);
                            children.push_back(std::move(child));
                        }
                        break;
                    }

                    case TypeKind::List: {
                        auto* in_list = static_cast<const ListTypeMeta*>(input_schema);
                        children.reserve(in_list->count);

                        for (size_t i = 0; i < in_list->count; ++i) {
                            auto elem_view = output_view.element(i);
                            auto child = bind_view(in_list->element_type, elem_view, current_time);
                            children.push_back(std::move(child));
                        }
                        break;
                    }

                    case TypeKind::Set:
                    case TypeKind::Dict:
                    case TypeKind::Window:
                        // Dynamic collections (Set/Dict) and Windows with REF elements
                        // are handled via delta computation rather than static child bindings.
                        // The composite match indicates the need for special delta tracking,
                        // but the binding itself is atomic (the whole collection).
                        // Delta computation will be handled by compute_set_delta/compute_dict_delta.
                        //
                        // For now, return an empty composite to indicate special handling is needed.
                        // The caller should use delta computation functions when modified_at() is true.
                        return BoundValue::make_composite(input_schema, {});

                    default:
                        return BoundValue{};  // Unexpected composite type
                }

                return BoundValue::make_composite(input_schema, std::move(children));
            }

            case SchemaMatchKind::Mismatch:
            default:
                return BoundValue{};
        }
    }

    /**
     * SetDelta - Result of comparing two sets
     *
     * Contains elements that were added (in new but not old) and
     * elements that were removed (in old but not new).
     */
    struct SetDelta {
        std::vector<ConstTypedPtr> added;    // Elements in new but not old
        std::vector<ConstTypedPtr> removed;  // Elements in old but not new

        [[nodiscard]] bool empty() const { return added.empty() && removed.empty(); }
        [[nodiscard]] size_t total_changes() const { return added.size() + removed.size(); }
    };

    /**
     * Compute the delta between two sets
     *
     * When a REF[TSS] changes its target, this computes what elements
     * were effectively added and removed from the perspective of the input.
     *
     * @param old_set View of the old set (may be invalid if no previous target)
     * @param new_set View of the new set (may be invalid if target cleared)
     * @return SetDelta with added and removed elements
     */
    inline SetDelta compute_set_delta(const ConstValueView& old_set, const ConstValueView& new_set) {
        SetDelta delta;

        // Handle cases where one or both are invalid
        if (!old_set.valid() && !new_set.valid()) {
            return delta;  // No change
        }

        // If only old is valid, all old elements are removed
        if (old_set.valid() && !new_set.valid()) {
            auto* old_storage = static_cast<const SetStorage*>(old_set.data());
            for (auto elem : *old_storage) {
                delta.removed.push_back(elem);
            }
            return delta;
        }

        // If only new is valid, all new elements are added
        if (!old_set.valid() && new_set.valid()) {
            auto* new_storage = static_cast<const SetStorage*>(new_set.data());
            for (auto elem : *new_storage) {
                delta.added.push_back(elem);
            }
            return delta;
        }

        // Both valid - compute actual delta
        auto* old_storage = static_cast<const SetStorage*>(old_set.data());
        auto* new_storage = static_cast<const SetStorage*>(new_set.data());

        // Find added elements (in new but not old)
        for (auto elem : *new_storage) {
            if (!old_storage->contains(elem.ptr)) {
                delta.added.push_back(elem);
            }
        }

        // Find removed elements (in old but not new)
        for (auto elem : *old_storage) {
            if (!new_storage->contains(elem.ptr)) {
                delta.removed.push_back(elem);
            }
        }

        return delta;
    }

    /**
     * DictDelta - Result of comparing two dicts
     *
     * Contains entries that were added (key in new but not old),
     * removed (key in old but not new), and modified (key in both
     * but with different values).
     */
    struct DictDelta {
        std::vector<DictStorage::ConstKeyValuePair> added;     // Entries in new but not old
        std::vector<DictStorage::ConstKeyValuePair> removed;   // Entries in old but not new
        std::vector<DictStorage::ConstKeyValuePair> modified;  // Entries in both with different values

        [[nodiscard]] bool empty() const {
            return added.empty() && removed.empty() && modified.empty();
        }
        [[nodiscard]] size_t total_changes() const {
            return added.size() + removed.size() + modified.size();
        }
    };

    /**
     * Compute the delta between two dicts
     *
     * When a REF[TSD] changes its target, this computes what entries
     * were effectively added, removed, and modified from the perspective
     * of the input.
     *
     * @param old_dict View of the old dict (may be invalid if no previous target)
     * @param new_dict View of the new dict (may be invalid if target cleared)
     * @return DictDelta with added, removed, and modified entries
     */
    inline DictDelta compute_dict_delta(const ConstValueView& old_dict, const ConstValueView& new_dict) {
        DictDelta delta;

        // Handle cases where one or both are invalid
        if (!old_dict.valid() && !new_dict.valid()) {
            return delta;  // No change
        }

        // If only old is valid, all old entries are removed
        if (old_dict.valid() && !new_dict.valid()) {
            auto* old_storage = static_cast<const DictStorage*>(old_dict.data());
            for (auto kv : *old_storage) {
                delta.removed.push_back({kv.key, kv.value, kv.index});
            }
            return delta;
        }

        // If only new is valid, all new entries are added
        if (!old_dict.valid() && new_dict.valid()) {
            auto* new_storage = static_cast<const DictStorage*>(new_dict.data());
            for (auto kv : *new_storage) {
                delta.added.push_back({kv.key, kv.value, kv.index});
            }
            return delta;
        }

        // Both valid - compute actual delta
        auto* old_storage = static_cast<const DictStorage*>(old_dict.data());
        auto* new_storage = static_cast<const DictStorage*>(new_dict.data());

        // Find added and modified entries (check all keys in new)
        for (auto kv : *new_storage) {
            auto old_value = old_storage->get_typed(kv.key.ptr);
            if (!old_value.valid()) {
                // Key not in old - it's added
                delta.added.push_back({kv.key, kv.value, kv.index});
            } else if (!kv.value.equals(old_value)) {
                // Key in both but value changed - it's modified
                delta.modified.push_back({kv.key, kv.value, kv.index});
            }
            // If value equals, no change for this key
        }

        // Find removed entries (keys in old but not in new)
        for (auto kv : *old_storage) {
            if (!new_storage->contains(kv.key.ptr)) {
                delta.removed.push_back({kv.key, kv.value, kv.index});
            }
        }

        return delta;
    }

    /**
     * Compute full set delta when REF target changes completely
     *
     * This is the common case when a REF switches targets - all elements
     * from the old target are removed and all from the new are added.
     *
     * @param old_set View of the old set (may be invalid)
     * @param new_set View of the new set (may be invalid)
     * @return SetDelta treating as complete replacement
     */
    inline SetDelta compute_set_full_delta(const ConstValueView& old_set, const ConstValueView& new_set) {
        SetDelta delta;

        if (old_set.valid()) {
            auto* old_storage = static_cast<const SetStorage*>(old_set.data());
            for (auto elem : *old_storage) {
                delta.removed.push_back(elem);
            }
        }

        if (new_set.valid()) {
            auto* new_storage = static_cast<const SetStorage*>(new_set.data());
            for (auto elem : *new_storage) {
                delta.added.push_back(elem);
            }
        }

        return delta;
    }

    /**
     * Compute full dict delta when REF target changes completely
     *
     * This is the common case when a REF switches targets - all entries
     * from the old target are removed and all from the new are added.
     *
     * @param old_dict View of the old dict (may be invalid)
     * @param new_dict View of the new dict (may be invalid)
     * @return DictDelta treating as complete replacement
     */
    inline DictDelta compute_dict_full_delta(const ConstValueView& old_dict, const ConstValueView& new_dict) {
        DictDelta delta;

        if (old_dict.valid()) {
            auto* old_storage = static_cast<const DictStorage*>(old_dict.data());
            for (auto kv : *old_storage) {
                delta.removed.push_back({kv.key, kv.value, kv.index});
            }
        }

        if (new_dict.valid()) {
            auto* new_storage = static_cast<const DictStorage*>(new_dict.data());
            for (auto kv : *new_storage) {
                delta.added.push_back({kv.key, kv.value, kv.index});
            }
        }

        return delta;
    }

} // namespace hgraph::value

#endif // HGRAPH_VALUE_BIND_H
