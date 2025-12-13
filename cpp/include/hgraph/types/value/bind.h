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

                case TypeKind::Scalar:
                    // For scalars, we need exact type match
                    return (input_schema->type_info == output_schema->type_info)
                        ? SchemaMatchKind::Peer
                        : SchemaMatchKind::Mismatch;

                default:
                    // Other types - assume peer if kinds match
                    return SchemaMatchKind::Peer;
            }
        }

        return SchemaMatchKind::Mismatch;
    }

    // Forward declaration for recursive binding
    BoundValue bind_view(const TypeMeta* input_schema, TimeSeriesValueView output_view, engine_time_t current_time);

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
     * @param output_value The output providing the value (TimeSeriesValue&)
     * @param current_time Current evaluation time (for initializing deref bindings)
     * @return BoundValue representing the binding
     */
    inline BoundValue bind(const TypeMeta* input_schema, TimeSeriesValue& output_value, engine_time_t current_time = MIN_DT) {
        auto match = match_schemas(input_schema, output_value.schema());

        switch (match) {
            case SchemaMatchKind::Peer:
                return BoundValue::make_peer(&output_value);

            case SchemaMatchKind::Deref: {
                // Create deref wrapper
                auto view = output_value.view(current_time);
                auto deref = std::make_unique<DerefTimeSeriesValue>(view, input_schema);
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
     * Create a binding from a TimeSeriesValueView
     *
     * Used for recursive binding of composite types (TSB/TSL).
     *
     * @param input_schema What the input expects
     * @param output_view View to the output value
     * @param current_time Current evaluation time
     * @return BoundValue representing the binding
     */
    inline BoundValue bind_view(const TypeMeta* input_schema, TimeSeriesValueView output_view, engine_time_t current_time) {
        if (!output_view.valid()) {
            return BoundValue{};
        }

        auto match = match_schemas(input_schema, output_view.schema());

        switch (match) {
            case SchemaMatchKind::Peer:
                // For view-based peer binding, we don't have a TimeSeriesValue*
                // This is a limitation - peer bindings from views need special handling
                // In practice, peer bindings should come from the top-level bind()
                return BoundValue{};  // Can't create peer from view alone

            case SchemaMatchKind::Deref: {
                // Create deref wrapper from view
                auto deref = std::make_unique<DerefTimeSeriesValue>(output_view, input_schema);
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

    // Note: Delta computation functions (compute_set_delta, compute_dict_delta)
    // are deferred until element iteration APIs are available in ConstValueView.
    // When a REF to a TSS/TSD changes:
    // - ALL elements from old target are "removed"
    // - ALL elements from new target are "added"

} // namespace hgraph::value

#endif // HGRAPH_VALUE_BIND_H
