#include <hgraph/types/time_series/ts_output/alternative.h>

#include <hgraph/types/metadata/ts_data_plan_factory.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_data/proxy.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/time_series/ts_input/target_link.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/value.h>

#include <algorithm>
#include <array>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::detail
{
    namespace
    {
        struct ToRefBuildContext
        {
            const TSOutput *output{nullptr};
        };

        [[nodiscard]] constexpr std::size_t ts_kind_index(TSTypeKind kind) noexcept
        {
            return static_cast<std::size_t>(kind);
        }

        [[nodiscard]] constexpr std::size_t endpoint_role_index(TSEndpointRole role) noexcept
        {
            return static_cast<std::size_t>(role);
        }

        [[nodiscard]] TSRoleTypeRef alternative_type_for(TSRoleTypeRef source,
                                                             TypeRole role,
                                                             std::string_view label)
        {
            const auto *schema = source.schema();
            if (schema == nullptr || !is_migrated_ts_root_schema(schema)) { return source; }
            return TSRoleTypeRef{
                intern_ts_type(*schema, role, source.checked_plan(), source.ops_ref(), label)};
        }

        [[nodiscard]] DateTime concrete_reference_time(DateTime time) noexcept
        {
            return time != MIN_DT ? time : MIN_ST;
        }

        [[nodiscard]] bool schema_equivalent_after_dereference(const TSValueTypeMetaData *lhs,
                                                               const TSValueTypeMetaData *rhs)
        {
            auto &registry = TypeRegistry::instance();
            return time_series_schema_equivalent(registry.dereference(lhs), registry.dereference(rhs));
        }

        [[nodiscard]] TSRoleTypeRef checked_endpoint_storage_type(const TSEndpointSchema &endpoint_schema)
        {
            const auto type = output_data_storage_type_for(endpoint_schema);
            if (!type)
            {
                throw std::logic_error("TSOutput from-REF alternative could not resolve output endpoint storage");
            }
            return type;
        }

        [[nodiscard]] TSRoleTypeRef checked_from_ref_storage_type(const TSEndpointSchema &endpoint_schema)
        {
            return alternative_type_for(
                checked_endpoint_storage_type(endpoint_schema), TypeRole::Output,
                "ts.alternative.from-ref.output");
        }

        [[nodiscard]] bool field_name_equal(const TSFieldMetaData &lhs, const TSFieldMetaData &rhs) noexcept
        {
            const std::string_view lname = lhs.name != nullptr ? std::string_view{lhs.name} : std::string_view{};
            const std::string_view rname = rhs.name != nullptr ? std::string_view{rhs.name} : std::string_view{};
            return lname == rname;
        }

        [[nodiscard]] bool is_to_ref_shape(const TSValueTypeMetaData *source_schema,
                                           const TSValueTypeMetaData *requested_schema);

        [[nodiscard]] bool to_ref_shape_matches_unsupported(const TSValueTypeMetaData *,
                                                            const TSValueTypeMetaData *) noexcept
        {
            return false;
        }

        [[nodiscard]] bool to_ref_shape_matches_ref(const TSValueTypeMetaData *source_schema,
                                                    const TSValueTypeMetaData *requested_schema)
        {
            const auto *target_schema = requested_schema->referenced_ts();
            return target_schema != nullptr && target_schema->kind != TSTypeKind::REF &&
                   schema_equivalent_after_dereference(target_schema, source_schema);
        }

        [[nodiscard]] bool to_ref_shape_matches_bundle(const TSValueTypeMetaData *source_schema,
                                                       const TSValueTypeMetaData *requested_schema)
        {
            if (source_schema->field_count() != requested_schema->field_count()) { return false; }
            for (std::size_t index = 0; index < requested_schema->field_count(); ++index)
            {
                if (!field_name_equal(source_schema->fields()[index], requested_schema->fields()[index]))
                {
                    return false;
                }
                if (!is_to_ref_shape(source_schema->fields()[index].type, requested_schema->fields()[index].type))
                {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] bool to_ref_shape_matches_list(const TSValueTypeMetaData *source_schema,
                                                     const TSValueTypeMetaData *requested_schema)
        {
            return source_schema->fixed_size() == requested_schema->fixed_size() &&
                   requested_schema->fixed_size() != 0 &&
                   is_to_ref_shape(source_schema->element_ts(), requested_schema->element_ts());
        }

        [[nodiscard]] bool to_ref_shape_matches_dict(const TSValueTypeMetaData *source_schema,
                                                     const TSValueTypeMetaData *requested_schema)
        {
            return source_schema->key_type() == requested_schema->key_type() &&
                   is_to_ref_shape(source_schema->element_ts(), requested_schema->element_ts());
        }

        using ToRefShapeMatchesFn = bool (*)(const TSValueTypeMetaData *, const TSValueTypeMetaData *);

        [[nodiscard]] ToRefShapeMatchesFn to_ref_shape_matcher_for(TSTypeKind kind) noexcept
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<ToRefShapeMatchesFn, kind_count> table{
                &to_ref_shape_matches_unsupported,
                &to_ref_shape_matches_unsupported,
                &to_ref_shape_matches_dict,
                &to_ref_shape_matches_list,
                &to_ref_shape_matches_unsupported,
                &to_ref_shape_matches_bundle,
                &to_ref_shape_matches_ref,
                &to_ref_shape_matches_unsupported,
            };

            const auto index = ts_kind_index(kind);
            return index < table.size() ? table[index] : &to_ref_shape_matches_unsupported;
        }

        [[nodiscard]] bool is_to_ref_shape(const TSValueTypeMetaData *source_schema,
                                           const TSValueTypeMetaData *requested_schema)
        {
            if (source_schema == nullptr || requested_schema == nullptr) { return false; }
            if (requested_schema->kind != TSTypeKind::REF && source_schema->kind != requested_schema->kind)
            {
                return false;
            }
            return to_ref_shape_matcher_for(requested_schema->kind)(source_schema, requested_schema);
        }

        using AlternativeRouteMatchesFn = bool (*)(const TSValueTypeMetaData *, const TSValueTypeMetaData &);

        [[nodiscard]] bool alternative_route_matches_to_ref(const TSValueTypeMetaData *source_schema,
                                                            const TSValueTypeMetaData &requested_schema)
        {
            return is_to_ref_shape(source_schema, &requested_schema);
        }

        [[nodiscard]] bool alternative_route_matches_from_ref(const TSValueTypeMetaData *source_schema,
                                                              const TSValueTypeMetaData &requested_schema) noexcept
        {
            return source_schema != nullptr && source_schema->kind == TSTypeKind::REF &&
                   requested_schema.kind != TSTypeKind::REF;
        }

        /**
         * INTERIOR from-REF shapes (time_series.rst, keyed/structural
         * inverse conversion): the source carries REF positions below the
         * top level where the requested schema wants the dereferenced
         * value. ``allow_dict`` distinguishes the keyed recursion (a TSD
         * element may itself convert) from fixed prefixes (a TSD below a
         * fixed container is only supported when already reference-free -
         * such a subtree passes via schema equivalence).
         */
        [[nodiscard]] bool is_from_ref_interior_shape(const TSValueTypeMetaData *source_schema,
                                                      const TSValueTypeMetaData *requested_schema,
                                                      bool allow_dict);

        [[nodiscard]] bool from_ref_interior_shape_matches_unsupported(const TSValueTypeMetaData *,
                                                                       const TSValueTypeMetaData *, bool) noexcept
        {
            return false;
        }

        [[nodiscard]] bool from_ref_interior_shape_matches_bundle(const TSValueTypeMetaData *source_schema,
                                                                  const TSValueTypeMetaData *requested_schema, bool)
        {
            if (source_schema->field_count() != requested_schema->field_count()) { return false; }
            for (std::size_t index = 0; index < requested_schema->field_count(); ++index)
            {
                if (!field_name_equal(source_schema->fields()[index], requested_schema->fields()[index]))
                {
                    return false;
                }
                if (!is_from_ref_interior_shape(source_schema->fields()[index].type,
                                                requested_schema->fields()[index].type, false))
                {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] bool from_ref_interior_shape_matches_list(const TSValueTypeMetaData *source_schema,
                                                                const TSValueTypeMetaData *requested_schema, bool)
        {
            return source_schema->fixed_size() == requested_schema->fixed_size() &&
                   requested_schema->fixed_size() != 0 &&
                   is_from_ref_interior_shape(source_schema->element_ts(), requested_schema->element_ts(), false);
        }

        [[nodiscard]] bool from_ref_interior_shape_matches_dict(const TSValueTypeMetaData *source_schema,
                                                                const TSValueTypeMetaData *requested_schema,
                                                                bool allow_dict)
        {
            return allow_dict && source_schema->key_type() == requested_schema->key_type() &&
                   is_from_ref_interior_shape(source_schema->element_ts(), requested_schema->element_ts(), true);
        }

        using FromRefInteriorShapeMatchesFn = bool (*)(const TSValueTypeMetaData *, const TSValueTypeMetaData *,
                                                       bool);

        [[nodiscard]] FromRefInteriorShapeMatchesFn from_ref_interior_shape_matcher_for(TSTypeKind kind) noexcept
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<FromRefInteriorShapeMatchesFn, kind_count> table{
                &from_ref_interior_shape_matches_unsupported,
                &from_ref_interior_shape_matches_unsupported,
                &from_ref_interior_shape_matches_dict,
                &from_ref_interior_shape_matches_list,
                &from_ref_interior_shape_matches_unsupported,
                &from_ref_interior_shape_matches_bundle,
                &from_ref_interior_shape_matches_unsupported,
                &from_ref_interior_shape_matches_unsupported,
            };

            const auto index = ts_kind_index(kind);
            return index < table.size() ? table[index] : &from_ref_interior_shape_matches_unsupported;
        }

        /** BUILD-TIME: runs once per [source view, requested schema] route
            probe; the resulting alternative is cached by that key. */
        [[nodiscard]] bool is_from_ref_interior_shape(const TSValueTypeMetaData *source_schema,
                                                      const TSValueTypeMetaData *requested_schema,
                                                      bool allow_dict)
        {
            if (source_schema == nullptr || requested_schema == nullptr) { return false; }
            if (time_series_schema_equivalent(source_schema, requested_schema)) { return true; }
            if (source_schema->kind == TSTypeKind::REF && requested_schema->kind != TSTypeKind::REF)
            {
                return schema_equivalent_after_dereference(source_schema->referenced_ts(), requested_schema);
            }
            if (source_schema->kind != requested_schema->kind) { return false; }
            return from_ref_interior_shape_matcher_for(requested_schema->kind)(source_schema, requested_schema,
                                                                               allow_dict);
        }

        [[nodiscard]] bool alternative_route_matches_from_ref_interior(const TSValueTypeMetaData *source_schema,
                                                                       const TSValueTypeMetaData &requested_schema)
        {
            return source_schema != nullptr && source_schema->kind != TSTypeKind::REF &&
                   requested_schema.kind != TSTypeKind::REF &&
                   !time_series_schema_equivalent(source_schema, &requested_schema) &&
                   is_from_ref_interior_shape(source_schema, &requested_schema, true);
        }

        [[nodiscard]] TSOutputView source_child_view(const TSOutputView &parent, const TSDataView &child)
        {
            return TSOutputView{parent.output(), child.borrowed_ref(), parent.evaluation_time()};
        }

        [[nodiscard]] TSEndpointSchema from_ref_endpoint_schema_for(const TSValueTypeMetaData *schema);

        [[nodiscard]] TSEndpointSchema peered_from_ref_endpoint_schema_for(const TSValueTypeMetaData *schema)
        {
            return TSEndpointSchema::peered(schema);
        }

        [[nodiscard]] TSEndpointSchema bundle_from_ref_endpoint_schema_for(const TSValueTypeMetaData *schema)
        {
            std::vector<TSEndpointSchema> children;
            children.reserve(schema->field_count());
            for (std::size_t index = 0; index < schema->field_count(); ++index)
            {
                children.push_back(from_ref_endpoint_schema_for(schema->fields()[index].type));
            }
            return TSEndpointSchema::non_peered(schema, std::move(children));
        }

        [[nodiscard]] TSEndpointSchema list_from_ref_endpoint_schema_for(const TSValueTypeMetaData *schema)
        {
            if (schema->fixed_size() == 0) { return TSEndpointSchema::peered(schema); }
            return TSEndpointSchema::non_peered_list(schema, from_ref_endpoint_schema_for(schema->element_ts()));
        }

        using FromRefEndpointSchemaForFn = TSEndpointSchema (*)(const TSValueTypeMetaData *);

        [[nodiscard]] FromRefEndpointSchemaForFn from_ref_endpoint_schema_builder_for(TSTypeKind kind) noexcept
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<FromRefEndpointSchemaForFn, kind_count> table{
                &peered_from_ref_endpoint_schema_for,
                &peered_from_ref_endpoint_schema_for,
                &peered_from_ref_endpoint_schema_for,
                &list_from_ref_endpoint_schema_for,
                &peered_from_ref_endpoint_schema_for,
                &bundle_from_ref_endpoint_schema_for,
                &peered_from_ref_endpoint_schema_for,
                &peered_from_ref_endpoint_schema_for,
            };

            const auto index = ts_kind_index(kind);
            return index < table.size() ? table[index] : &peered_from_ref_endpoint_schema_for;
        }

        [[nodiscard]] TSEndpointSchema from_ref_endpoint_schema_for(const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr)
            {
                throw std::invalid_argument("TSOutput from-REF endpoint schema requires a time-series schema");
            }
            return from_ref_endpoint_schema_builder_for(schema->kind)(schema);
        }

        [[nodiscard]] TSDataView endpoint_child_view(const TSDataView &parent, std::size_t index)
        {
            auto projection = input_child_projection(parent, index);
            if (projection.target_link.valid())
            {
                return TSOutputAlternativeStore::child_view_with_parent(parent, projection.target_link, index);
            }
            if (projection.visible.valid())
            {
                return TSOutputAlternativeStore::child_view_with_parent(parent, projection.visible, index);
            }
            return {};
        }

        void unbind_target_link_at(const TSDataView &target, DateTime modified_time, bool teardown)
        {
            auto *link = mutable_target_link_storage(target);
            if (link == nullptr)
            {
                throw std::logic_error("TSOutput from-REF target unbinding requires TargetLink storage");
            }
            if (teardown) { link->unbind_noexcept(); }
            else if (target.schema() != nullptr &&
                     (target.schema()->kind == TSTypeKind::TSS || target.schema()->kind == TSTypeKind::TSD))
            {
                // Keyed structures must reconcile the published key set. The
                // source slot store remains allocated until erase, so the link
                // can project removals without copying its keys.
                link->unbind_structural(modified_time);
            }
            else { link->unbind(); }
        }

        void bind_target_link_at(const TSDataView &target, const TSOutputView &output, DateTime modified_time)
        {
            // SAME-TARGET dedup: re-applying a reference whose item is
            // unchanged (a re-published assembly re-binds every field) must
            // not record modified - consumers would sample the unchanged
            // target as a fresh tick.
            if (auto *existing = mutable_target_link_storage(target);
                existing != nullptr && existing->bound() && existing->target_output().same_as(output.handle()))
            {
                return;
            }
            auto *link = mutable_target_link_storage(target);
            if (link == nullptr)
            {
                throw std::logic_error("TSOutput from-REF target binding requires TargetLink storage");
            }
            const auto *schema = target_link_schema(target);
            if (schema == nullptr)
            {
                throw std::logic_error("TSOutput from-REF target binding requires a target schema");
            }
            if (schema->kind == TSTypeKind::TSS || schema->kind == TSTypeKind::TSD)
            {
                link->bind_sampled(*schema, output, modified_time);
                return;
            }
            // A scalar or fixed-shape reference retarget samples a live source
            // at the retarget time without creating structural slot state.
            link->bind_current_value(*schema, output, modified_time);
        }

        [[nodiscard]] TSOutputView output_child_view(const TSOutputView &parent,
                                                     const TSValueTypeMetaData &parent_schema,
                                                     std::size_t index)
        {
            if (parent_schema.kind != TSTypeKind::TSL && parent_schema.kind != TSTypeKind::TSB)
            {
                throw std::logic_error("TSOutput from-REF cannot project a child from this output schema");
            }
            auto structural_parent = parent.borrowed_ref();
            std::vector<TSOutputHandle> forwarding_path;
            while (structural_parent.forwarding() && structural_parent.forwarding_bound())
            {
                auto target = structural_parent.forwarding_target();
                if (std::ranges::any_of(forwarding_path, [&](const TSOutputHandle &visited) {
                        return visited.same_as(target);
                    }))
                {
                    throw std::logic_error("TSOutput from-REF output child projection encountered a forwarding cycle");
                }
                forwarding_path.push_back(target);
                structural_parent = target.view(parent.evaluation_time());
            }

            TSOutputView child;
            if (has_input_children(structural_parent.data_view()))
            {
                auto projection = input_child_projection(structural_parent.data_view(), index);
                auto child_data = projection.target_link.valid()
                                      ? std::move(projection.target_link)
                                      : std::move(projection.visible);
                child = TSOutputView{structural_parent.output(), child_data, parent.evaluation_time()};
            }
            else
            {
                child = structural_parent.indexed_child_at(index);
            }
            if (!child.bound())
            {
                const auto child_type = child.data_view().storage_type();
                throw std::logic_error(
                    "TSOutput from-REF output child projection failed for schema '" +
                    std::string{parent_schema.name()} + "' at child " + std::to_string(index) +
                    " from storage '" +
                    (parent.data_view().storage_type().record() != nullptr
                         ? std::string{parent.data_view().storage_type().record()->implementation_name()}
                         : std::string{"<unbound>"}) +
                    "' (input children: " +
                    (has_input_children(parent.data_view()) ? std::string{"yes"} : std::string{"no"}) +
                    ", child type: '" +
                    (child_type.record() != nullptr
                         ? std::string{child_type.record()->implementation_name()}
                         : std::string{"<unbound>"}) +
                    "', child memory: " +
                    (child.data_view().data() != nullptr ? std::string{"present"} : std::string{"null"}) +
                    ")");
            }
            return child;
        }

        struct FromRefEndpointPlan;

        void unbind_from_ref_data(const FromRefEndpointPlan &plan,
                                  const TSDataView &target,
                                  DateTime modified_time,
                                  bool teardown = false);

        void apply_output_to_from_ref_data(const FromRefEndpointPlan &plan,
                                           const TSDataView &target,
                                           const TSOutputView &output,
                                           DateTime modified_time);

        void apply_reference_to_from_ref_data(const FromRefEndpointPlan &plan,
                                              const TSDataView &target,
                                              const TimeSeriesReference &reference,
                                              DateTime modified_time);

        using FromRefUnbindFn = void (*)(const FromRefEndpointPlan &, const TSDataView &, DateTime, bool);
        using FromRefOutputApplyFn = void (*)(const FromRefEndpointPlan &, const TSDataView &,
                                              const TSOutputView &, DateTime);
        using FromRefPeeredReferenceApplyFn = void (*)(const FromRefEndpointPlan &, const TSDataView &,
                                                       const TSOutputView &, DateTime);
        using FromRefNonPeeredReferenceApplyFn = void (*)(const FromRefEndpointPlan &, const TSDataView &,
                                                          const TimeSeriesReference &, DateTime);
        using FromRefTreeUnboundFn = bool (*)(const FromRefEndpointPlan &, const TSDataView &);
        using FromRefReferenceIdentityFn = bool (*)(const FromRefEndpointPlan &, const TSDataView &,
                                                    const TimeSeriesReference &);

        struct FromRefRoleOps
        {
            FromRefUnbindFn                  unbind{nullptr};
            FromRefOutputApplyFn             apply_output{nullptr};
            FromRefPeeredReferenceApplyFn    apply_peered_reference{nullptr};
            FromRefNonPeeredReferenceApplyFn apply_non_peered_reference{nullptr};
            FromRefTreeUnboundFn             tree_unbound{nullptr};
            FromRefReferenceIdentityFn       reference_identity_matches{nullptr};
        };

        struct FromRefEndpointPlan
        {
            // Immutable endpoint topology compiled when the alternative is
            // created. Refresh and teardown recurse through these callbacks;
            // they never rediscover endpoint roles from live storage.
            TSEndpointSchema                 schema{};
            const FromRefRoleOps            *ops{nullptr};
            std::vector<FromRefEndpointPlan> children{};
        };

        void unbind_from_ref_peered(const FromRefEndpointPlan &,
                                    const TSDataView &target,
                                    DateTime modified_time,
                                    bool teardown)
        {
            unbind_target_link_at(target, modified_time, teardown);
        }

        void unbind_from_ref_non_peered(const FromRefEndpointPlan &plan,
                                        const TSDataView &target,
                                        DateTime modified_time,
                                        bool teardown)
        {
            for (std::size_t index = 0; index < plan.children.size(); ++index)
            {
                auto child = endpoint_child_view(target, index);
                unbind_from_ref_data(plan.children[index], child, modified_time, teardown);
            }
        }

        void apply_output_to_from_ref_peered(const FromRefEndpointPlan &,
                                             const TSDataView &target,
                                             const TSOutputView &output,
                                             DateTime modified_time)
        {
            bind_target_link_at(target, output, modified_time);
        }

        void apply_output_to_from_ref_non_peered(const FromRefEndpointPlan &plan,
                                                 const TSDataView &target,
                                                 const TSOutputView &output,
                                                 DateTime modified_time)
        {
            for (std::size_t index = 0; index < plan.children.size(); ++index)
            {
                auto child = endpoint_child_view(target, index);
                auto child_output = output_child_view(output, *plan.schema.schema(), index);
                apply_output_to_from_ref_data(plan.children[index], child, child_output, modified_time);
            }
        }

        void apply_peered_reference_to_peered_from_ref_data(const FromRefEndpointPlan &plan,
                                                            const TSDataView &target,
                                                            const TSOutputView &output,
                                                            DateTime modified_time)
        {
            plan.ops->apply_output(plan, target, output, modified_time);
        }

        void apply_peered_reference_to_non_peered_from_ref_data(const FromRefEndpointPlan &plan,
                                                                const TSDataView &target,
                                                                const TSOutputView &output,
                                                                DateTime modified_time)
        {
            if (output.forwarding() && !output.forwarding_bound())
            {
                plan.ops->unbind(plan, target, modified_time, false);
                return;
            }
            plan.ops->apply_output(plan, target, output, modified_time);
        }

        void apply_non_peered_reference_to_peered_from_ref_data(const FromRefEndpointPlan &,
                                                               const TSDataView &,
                                                               const TimeSeriesReference &,
                                                               DateTime)
        {
            throw std::invalid_argument("TSOutput from-REF cannot apply a non-peered reference to a peered leaf");
        }

        void unbind_from_ref_owned(const FromRefEndpointPlan &, const TSDataView &, DateTime, bool)
        {
            throw std::invalid_argument("TSOutput from-REF cannot target an owned endpoint leaf");
        }

        void apply_output_to_from_ref_owned(const FromRefEndpointPlan &,
                                            const TSDataView &,
                                            const TSOutputView &,
                                            DateTime)
        {
            throw std::invalid_argument("TSOutput from-REF cannot target an owned endpoint leaf");
        }

        void apply_peered_reference_to_owned_from_ref_data(const FromRefEndpointPlan &,
                                                           const TSDataView &,
                                                           const TSOutputView &,
                                                           DateTime)
        {
            throw std::invalid_argument("TSOutput from-REF cannot target an owned endpoint leaf");
        }

        void apply_non_peered_reference_to_owned_from_ref_data(const FromRefEndpointPlan &,
                                                              const TSDataView &,
                                                              const TimeSeriesReference &,
                                                              DateTime)
        {
            throw std::invalid_argument("TSOutput from-REF cannot target an owned endpoint leaf");
        }

        void apply_non_peered_reference_to_non_peered_from_ref_data(
            const FromRefEndpointPlan &plan,
            const TSDataView &target,
            const TimeSeriesReference &reference,
            DateTime modified_time)
        {
            if (reference.items().size() != plan.children.size())
            {
                throw std::invalid_argument("TSOutput from-REF non-peered reference has the wrong child count");
            }

            for (std::size_t index = 0; index < plan.children.size(); ++index)
            {
                auto child = endpoint_child_view(target, index);
                apply_reference_to_from_ref_data(plan.children[index], child, reference[index], modified_time);
            }
        }

        [[nodiscard]] bool from_ref_peered_tree_unbound(const FromRefEndpointPlan &,
                                                        const TSDataView &target)
        {
            const auto *link = target_link_storage(target);
            return link != nullptr && !link->bound();
        }

        [[nodiscard]] bool from_ref_non_peered_tree_unbound(const FromRefEndpointPlan &plan,
                                                            const TSDataView &target)
        {
            for (std::size_t index = 0; index < plan.children.size(); ++index)
            {
                if (!plan.children[index].ops->tree_unbound(
                        plan.children[index], endpoint_child_view(target, index)))
                {
                    return false;
                }
            }
            return !plan.children.empty();
        }

        [[nodiscard]] bool from_ref_owned_tree_unbound(const FromRefEndpointPlan &,
                                                       const TSDataView &)
        {
            return false;
        }

        [[nodiscard]] bool from_ref_peered_reference_identity_matches(
            const FromRefEndpointPlan &plan,
            const TSDataView &target,
            const TimeSeriesReference &desired)
        {
            if (desired.is_empty()) { return plan.ops->tree_unbound(plan, target); }
            const auto *link = target_link_storage(target);
            return desired.is_peered() && link != nullptr && link->bound() &&
                   desired.target_output().same_as(link->target_output());
        }

        [[nodiscard]] bool from_ref_non_peered_reference_identity_matches(
            const FromRefEndpointPlan &plan,
            const TSDataView &target,
            const TimeSeriesReference &desired)
        {
            if (desired.is_empty()) { return plan.ops->tree_unbound(plan, target); }
            if (desired.is_peered() || desired.items().size() != plan.children.size()) { return false; }
            for (std::size_t index = 0; index < plan.children.size(); ++index)
            {
                const auto &child_plan = plan.children[index];
                if (!child_plan.ops->reference_identity_matches(
                        child_plan, endpoint_child_view(target, index), desired[index]))
                {
                    return false;
                }
            }
            return true;
        }

        [[nodiscard]] bool from_ref_owned_reference_identity_matches(
            const FromRefEndpointPlan &,
            const TSDataView &,
            const TimeSeriesReference &)
        {
            return false;
        }

        [[nodiscard]] const FromRefRoleOps &from_ref_role_ops_for(TSEndpointRole role) noexcept
        {
            static const std::array<FromRefRoleOps, 3> table{{
                {
                    &unbind_from_ref_peered,
                    &apply_output_to_from_ref_peered,
                    &apply_peered_reference_to_peered_from_ref_data,
                    &apply_non_peered_reference_to_peered_from_ref_data,
                    &from_ref_peered_tree_unbound,
                    &from_ref_peered_reference_identity_matches,
                },
                {
                    &unbind_from_ref_non_peered,
                    &apply_output_to_from_ref_non_peered,
                    &apply_peered_reference_to_non_peered_from_ref_data,
                    &apply_non_peered_reference_to_non_peered_from_ref_data,
                    &from_ref_non_peered_tree_unbound,
                    &from_ref_non_peered_reference_identity_matches,
                },
                {
                    &unbind_from_ref_owned,
                    &apply_output_to_from_ref_owned,
                    &apply_peered_reference_to_owned_from_ref_data,
                    &apply_non_peered_reference_to_owned_from_ref_data,
                    &from_ref_owned_tree_unbound,
                    &from_ref_owned_reference_identity_matches,
                },
            }};

            const auto index = endpoint_role_index(role);
            return index < table.size() ? table[index] : table[0];
        }

        [[nodiscard]] FromRefEndpointPlan make_from_ref_endpoint_plan(TSEndpointSchema schema)
        {
            const auto *ops = &from_ref_role_ops_for(schema.role());
            std::vector<FromRefEndpointPlan> children;
            children.reserve(schema.child_count());
            for (std::size_t index = 0; index < schema.child_count(); ++index)
            {
                children.push_back(make_from_ref_endpoint_plan(schema.child(index)));
            }
            return FromRefEndpointPlan{
                .schema = std::move(schema),
                .ops = ops,
                .children = std::move(children),
            };
        }

        [[nodiscard]] FromRefEndpointPlan make_from_ref_endpoint_plan(const TSValueTypeMetaData *schema)
        {
            return make_from_ref_endpoint_plan(from_ref_endpoint_schema_for(schema));
        }

        void unbind_from_ref_data(const FromRefEndpointPlan &plan,
                                  const TSDataView &target,
                                  DateTime modified_time,
                                  bool teardown)
        {
            plan.ops->unbind(plan, target, modified_time, teardown);
        }

        void apply_output_to_from_ref_data(const FromRefEndpointPlan &plan,
                                           const TSDataView &target,
                                           const TSOutputView &output,
                                           DateTime modified_time)
        {
            plan.ops->apply_output(plan, target, output, modified_time);
        }

        void apply_reference_to_from_ref_data(const FromRefEndpointPlan &plan,
                                              const TSDataView &target,
                                              const TimeSeriesReference &reference,
                                              DateTime modified_time)
        {
            if (reference.is_empty())
            {
                plan.ops->unbind(plan, target, modified_time, false);
                return;
            }

            if (reference.is_peered())
            {
                const auto &output = TSOutputAlternativeStore::peered_reference_target(reference);
                auto output_view = output.view(modified_time);
                plan.ops->apply_peered_reference(plan, target, output_view, modified_time);
                return;
            }

            plan.ops->apply_non_peered_reference(plan, target, reference, modified_time);
        }

        void collect_forwarding_reference_sources(const TimeSeriesReference &reference,
                                                  DateTime modified_time,
                                                  std::vector<TSOutputHandle> &sources)
        {
            if (reference.is_peered())
            {
                const auto &source = TSOutputAlternativeStore::peered_reference_target(reference);
                if (!source.bound()) { return; }
                auto view = source.view(modified_time);
                if (!view.forwarding()) { return; }
                if (std::ranges::none_of(sources, [&](const TSOutputHandle &existing) {
                        return existing.same_as(source);
                    }))
                {
                    sources.push_back(source);
                }
                return;
            }
            if (!reference.is_non_peered()) { return; }
            for (const auto &item : reference.items())
            {
                collect_forwarding_reference_sources(item, modified_time, sources);
            }
        }

        // ----- interior from-REF (keyed / structural inverse conversion) -----

        struct FromRefBuildContext
        {
            const TSOutput *output{nullptr};
        };

        struct FromRefInteriorPlan;

        using FromRefInteriorApplyFn = void (*)(const FromRefInteriorPlan &, const TSDataView &,
                                                const TSOutputView &, DateTime);
        using FromRefInteriorIdentityFn = bool (*)(const FromRefInteriorPlan &, const TSDataView &,
                                                   const TSOutputView &);
        using FromRefInteriorReleaseFn = void (*)(const FromRefInteriorPlan &, const TSDataView &,
                                                  DateTime);

        struct FromRefInteriorOps
        {
            FromRefInteriorApplyFn    apply{nullptr};
            FromRefInteriorIdentityFn identity_matches{nullptr};
            FromRefInteriorReleaseFn  release{nullptr};
            bool                      proxy_backed{false};
        };

        struct FromRefInteriorPlan
        {
            // The source/requested route and its representation strategy are
            // fixed for the lifetime of one cached alternative. Child plans
            // also provide stable proxy callback contexts.
            const TSValueTypeMetaData          *requested_schema{nullptr};
            const TSValueTypeMetaData          *source_schema{nullptr};
            const FromRefBuildContext          *build_context{nullptr};
            const FromRefInteriorOps           *ops{nullptr};
            std::optional<FromRefEndpointPlan>  endpoint{};
            std::vector<FromRefInteriorPlan>    children{};
        };

        /**
         * TSData binding for an interior from-REF alternative. A requested
         * ``TSD`` whose source element still converts stores a ``TSDProxy``
         * over the source dictionary (recursively for nested dictionaries);
         * every other shape is a normal input endpoint tree - TargetLink
         * leaves at (and whole-subtree links below) the source's REF
         * positions.
         */
        [[nodiscard]] TSRoleTypeRef from_ref_interior_type_for(const TSValueTypeMetaData &requested,
                                                                  const TSValueTypeMetaData &source)
        {
            if (requested.kind == TSTypeKind::TSD && source.kind == TSTypeKind::TSD &&
                !time_series_schema_equivalent(&source, &requested))
            {
                return TSRoleTypeRef{tsd_proxy_data_type_for(
                    requested, from_ref_interior_type_for(*requested.element_ts(), *source.element_ts())).as_role()};
            }
            return checked_endpoint_storage_type(from_ref_endpoint_schema_for(&requested));
        }

        void build_from_ref_proxy_value(TSDProxy &, std::size_t, const TSDataView &target,
                                        const TSDataView &source, DateTime modified_time, const void *context);
        bool from_ref_proxy_source_identity_matches(const TSDProxy &, std::size_t,
                                                    const TSDataView &, const TSDataView &,
                                                    const void *);
        extern const TSDProxyValueOps from_ref_proxy_value_ops;

        void apply_from_ref_interior(const FromRefInteriorPlan &plan,
                                     const TSDataView &target,
                                     const TSOutputView &source_view,
                                     DateTime modified_time)
        {
            plan.ops->apply(plan, target, source_view, modified_time);
        }

        void apply_from_ref_interior_unsupported(const FromRefInteriorPlan &, const TSDataView &,
                                                 const TSOutputView &, DateTime)
        {
            throw std::logic_error("TSOutput interior from-REF encountered an unsupported requested schema");
        }

        void apply_from_ref_interior_reference(const FromRefInteriorPlan &plan,
                                               const TSDataView &target,
                                               const TSOutputView &source_view,
                                               DateTime modified_time)
        {
            if (!source_view.data_view().has_current_value())
            {
                unbind_from_ref_data(*plan.endpoint, target, modified_time);
                return;
            }
            // Keep the borrowed value view alive while using the referenced payload.  In
            // particular, do not extend a payload reference through a temporary ValueView.
            const auto source_value = source_view.value();
            const auto &reference   = source_value.checked_as<TimeSeriesReference>();
            apply_reference_to_from_ref_data(*plan.endpoint, target, reference, modified_time);
        }

        void apply_from_ref_interior_equivalent(const FromRefInteriorPlan &plan,
                                                const TSDataView &target,
                                                const TSOutputView &source_view,
                                                DateTime modified_time)
        {
            apply_output_to_from_ref_data(*plan.endpoint, target, source_view, modified_time);
        }

        void apply_from_ref_interior_dict(const FromRefInteriorPlan &plan,
                                          const TSDataView &target,
                                          const TSOutputView &source_view,
                                          DateTime modified_time)
        {
            bind_tsd_proxy(target.borrowed_ref(), source_view.data_view().as_dict(), &from_ref_proxy_value_ops,
                           &plan, modified_time, TSDProxyChildRefresh::OnChildTick);
        }

        void apply_from_ref_interior_fixed(const FromRefInteriorPlan &plan,
                                           const TSDataView &target,
                                           const TSOutputView &source_view,
                                           DateTime modified_time)
        {
            for (std::size_t index = 0; index < plan.children.size(); ++index)
            {
                apply_from_ref_interior(plan.children[index], endpoint_child_view(target, index),
                                        output_child_view(source_view, *plan.source_schema, index),
                                        modified_time);
            }
        }

        [[nodiscard]] bool from_ref_interior_unsupported_identity(const FromRefInteriorPlan &,
                                                                 const TSDataView &,
                                                                 const TSOutputView &)
        {
            return false;
        }

        [[nodiscard]] bool from_ref_interior_reference_identity(const FromRefInteriorPlan &plan,
                                                                const TSDataView &target,
                                                                const TSOutputView &source_view)
        {
            if (!source_view.data_view().has_current_value())
            {
                return plan.endpoint->ops->tree_unbound(*plan.endpoint, target);
            }
            return plan.endpoint->ops->reference_identity_matches(
                *plan.endpoint, target, source_view.value().checked_as<TimeSeriesReference>());
        }

        [[nodiscard]] bool from_ref_interior_equivalent_identity(const FromRefInteriorPlan &,
                                                                 const TSDataView &target,
                                                                 const TSOutputView &source_view)
        {
            const auto *link = target_link_storage(target);
            return link != nullptr && link->bound() && link->target_output().same_as(source_view.handle());
        }

        [[nodiscard]] bool from_ref_interior_dict_identity(const FromRefInteriorPlan &,
                                                           const TSDataView &target,
                                                           const TSOutputView &source_view)
        {
            const auto &source_data = source_view.data_view();
            const auto &nested = *static_cast<const TSDProxy *>(target.data());
            const auto actual_source = nested.source_view();
            return actual_source.storage_type() == source_data.storage_type() &&
                   actual_source.data() == source_data.data() && nested.source_identities_match();
        }

        [[nodiscard]] bool from_ref_interior_fixed_identity(const FromRefInteriorPlan &plan,
                                                            const TSDataView &target,
                                                            const TSOutputView &source_view)
        {
            for (std::size_t index = 0; index < plan.children.size(); ++index)
            {
                const auto &child_plan = plan.children[index];
                if (!child_plan.ops->identity_matches(
                        child_plan, endpoint_child_view(target, index),
                        output_child_view(source_view, *plan.source_schema, index)))
                {
                    return false;
                }
            }
            return true;
        }

        void release_from_ref_interior_unsupported(const FromRefInteriorPlan &,
                                                   const TSDataView &,
                                                   DateTime)
        {
        }

        void release_from_ref_interior_endpoint(const FromRefInteriorPlan &plan,
                                                const TSDataView &target,
                                                DateTime release_time)
        {
            unbind_from_ref_data(*plan.endpoint, target, release_time, true);
        }

        void release_from_ref_interior_dict(const FromRefInteriorPlan &plan,
                                            const TSDataView &target,
                                            DateTime release_time)
        {
            auto dict = target.as_dict();
            for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
            {
                if (!dict.slot_occupied(slot)) { continue; }
                auto child = dict.at_slot(slot);
                if (!child.valid()) { continue; }
                plan.children.front().ops->release(plan.children.front(), child, release_time);
            }
        }

        void release_from_ref_interior_fixed(const FromRefInteriorPlan &plan,
                                             const TSDataView &target,
                                             DateTime release_time)
        {
            for (std::size_t index = 0; index < plan.children.size(); ++index)
            {
                const auto &child_plan = plan.children[index];
                child_plan.ops->release(child_plan, endpoint_child_view(target, index), release_time);
            }
        }

        [[nodiscard]] const FromRefInteriorOps &from_ref_interior_reference_ops() noexcept
        {
            static const FromRefInteriorOps ops{
                &apply_from_ref_interior_reference,
                &from_ref_interior_reference_identity,
                &release_from_ref_interior_endpoint,
                false,
            };
            return ops;
        }

        [[nodiscard]] const FromRefInteriorOps &from_ref_interior_equivalent_ops() noexcept
        {
            static const FromRefInteriorOps ops{
                &apply_from_ref_interior_equivalent,
                &from_ref_interior_equivalent_identity,
                &release_from_ref_interior_endpoint,
                false,
            };
            return ops;
        }

        [[nodiscard]] const FromRefInteriorOps &from_ref_interior_dict_ops() noexcept
        {
            static const FromRefInteriorOps ops{
                &apply_from_ref_interior_dict,
                &from_ref_interior_dict_identity,
                &release_from_ref_interior_dict,
                true,
            };
            return ops;
        }

        [[nodiscard]] const FromRefInteriorOps &from_ref_interior_fixed_ops() noexcept
        {
            static const FromRefInteriorOps ops{
                &apply_from_ref_interior_fixed,
                &from_ref_interior_fixed_identity,
                &release_from_ref_interior_fixed,
                false,
            };
            return ops;
        }

        [[nodiscard]] const FromRefInteriorOps &from_ref_interior_unsupported_ops() noexcept
        {
            static const FromRefInteriorOps ops{
                &apply_from_ref_interior_unsupported,
                &from_ref_interior_unsupported_identity,
                &release_from_ref_interior_unsupported,
                false,
            };
            return ops;
        }

        [[nodiscard]] FromRefInteriorPlan make_from_ref_interior_plan(
            const TSValueTypeMetaData &requested,
            const TSValueTypeMetaData &source,
            const FromRefBuildContext &build_context)
        {
            FromRefInteriorPlan plan{
                .requested_schema = &requested,
                .source_schema = &source,
                .build_context = &build_context,
                .ops = &from_ref_interior_unsupported_ops(),
            };
            if (source.kind == TSTypeKind::REF)
            {
                plan.ops = &from_ref_interior_reference_ops();
                plan.endpoint = make_from_ref_endpoint_plan(&requested);
                return plan;
            }
            if (time_series_schema_equivalent(&source, &requested))
            {
                plan.ops = &from_ref_interior_equivalent_ops();
                plan.endpoint = make_from_ref_endpoint_plan(TSEndpointSchema::peered(&requested));
                return plan;
            }
            if (requested.kind == TSTypeKind::TSD && source.kind == TSTypeKind::TSD)
            {
                plan.ops = &from_ref_interior_dict_ops();
                plan.children.push_back(make_from_ref_interior_plan(
                    *requested.element_ts(), *source.element_ts(), build_context));
                return plan;
            }
            if (requested.kind == TSTypeKind::TSB && source.kind == TSTypeKind::TSB)
            {
                plan.ops = &from_ref_interior_fixed_ops();
                plan.children.reserve(requested.field_count());
                for (std::size_t index = 0; index < requested.field_count(); ++index)
                {
                    plan.children.push_back(make_from_ref_interior_plan(
                        *requested.fields()[index].type, *source.fields()[index].type, build_context));
                }
                return plan;
            }
            if (requested.kind == TSTypeKind::TSL && source.kind == TSTypeKind::TSL &&
                requested.fixed_size() != 0)
            {
                plan.ops = &from_ref_interior_fixed_ops();
                plan.children.reserve(requested.fixed_size());
                for (std::size_t index = 0; index < requested.fixed_size(); ++index)
                {
                    plan.children.push_back(make_from_ref_interior_plan(
                        *requested.element_ts(), *source.element_ts(), build_context));
                }
            }
            return plan;
        }

        bool from_ref_proxy_source_identity_matches(const TSDProxy &,
                                                    std::size_t,
                                                    const TSDataView &target,
                                                    const TSDataView &source,
                                                    const void *context)
        {
            const auto *plan = static_cast<const FromRefInteriorPlan *>(context);
            return plan != nullptr && plan->build_context != nullptr &&
                   plan->build_context->output != nullptr && !plan->children.empty() && source.valid() &&
                   plan->children.front().ops->identity_matches(
                       plan->children.front(), target,
                       TSOutputView{plan->build_context->output, source.borrowed_ref(),
                                    source.tracking().last_modified_time});
        }

        void build_from_ref_proxy_value(TSDProxy &, std::size_t, const TSDataView &target,
                                        const TSDataView &source, DateTime modified_time, const void *context)
        {
            const auto *plan = static_cast<const FromRefInteriorPlan *>(context);
            if (plan == nullptr || plan->build_context == nullptr || plan->build_context->output == nullptr)
            {
                throw std::logic_error("TSOutput from-REF proxy value builder requires an output context");
            }
            if (!source.valid()) { throw std::logic_error("TSOutput from-REF proxy source child is not live"); }

            apply_from_ref_interior(
                plan->children.front(), target.borrowed_ref(),
                TSOutputView{plan->build_context->output, source.borrowed_ref(), modified_time},
                modified_time);
        }

        const TSDProxyValueOps from_ref_proxy_value_ops{
            &build_from_ref_proxy_value,
            &from_ref_proxy_source_identity_matches,
        };

        [[nodiscard]] TSRoleTypeRef to_ref_ts_data_type_for(const TSValueTypeMetaData &schema);
        struct ToRefPlan;
        using ToRefPopulateFn = void (*)(const ToRefPlan &, const TSDataView &,
                                         const TSOutputView &, DateTime);

        struct ToRefOps
        {
            ToRefPopulateFn populate{nullptr};
        };

        struct ToRefPlan
        {
            // Fixed traversal selected from the requested schema before any
            // refresh. TSD proxies retain the relevant child-plan address as
            // their callback context.
            const TSValueTypeMetaData *target_schema{nullptr};
            const ToRefBuildContext   *build_context{nullptr};
            const ToRefOps            *ops{nullptr};
            std::vector<ToRefPlan>      children{};
        };

        void populate_to_ref_data(const ToRefPlan &plan,
                                  const TSDataView &target,
                                  const TSOutputView &source_view,
                                  DateTime modified_time)
        {
            plan.ops->populate(plan, target, source_view, modified_time);
        }

        void build_to_ref_proxy_value(TSDProxy      &,
                                      std::size_t,
                                      const TSDataView &target,
                                      const TSDataView &source,
                                      DateTime  modified_time,
                                      const void    *context)
        {
            const auto *plan = static_cast<const ToRefPlan *>(context);
            if (plan == nullptr || plan->build_context == nullptr || plan->build_context->output == nullptr)
            {
                throw std::logic_error("TSOutput to-REF proxy value builder requires an output context");
            }
            if (!source.valid()) { throw std::logic_error("TSOutput to-REF proxy source child is not live"); }
            if (target.schema() == nullptr)
            {
                throw std::logic_error("TSOutput to-REF proxy target child is not typed");
            }
            if (target.has_current_value()) { return; }

            populate_to_ref_data(
                plan->children.front(), target.borrowed_ref(),
                TSOutputView{plan->build_context->output, source.borrowed_ref(), modified_time},
                modified_time);
        }

        const TSDProxyValueOps to_ref_proxy_value_ops{
            &build_to_ref_proxy_value,
            nullptr,
        };

        [[nodiscard]] TSRoleTypeRef to_ref_regular_type_for(const TSValueTypeMetaData &schema)
        {
            return TSDataPlanFactory::instance().data_type_for(&schema).as_role();
        }

        [[nodiscard]] TSRoleTypeRef to_ref_dict_type_for(const TSValueTypeMetaData &schema)
        {
            return TSRoleTypeRef{tsd_proxy_data_type_for(
                schema, to_ref_ts_data_type_for(*schema.element_ts())).as_role()};
        }

        using ToRefTypeForFn = TSRoleTypeRef (*)(const TSValueTypeMetaData &);

        [[nodiscard]] ToRefTypeForFn to_ref_type_for_kind(TSTypeKind kind) noexcept
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<ToRefTypeForFn, kind_count> table{
                &to_ref_regular_type_for,
                &to_ref_regular_type_for,
                &to_ref_dict_type_for,
                &to_ref_regular_type_for,
                &to_ref_regular_type_for,
                &to_ref_regular_type_for,
                &to_ref_regular_type_for,
                &to_ref_regular_type_for,
            };

            const auto index = ts_kind_index(kind);
            return index < table.size() ? table[index] : &to_ref_regular_type_for;
        }

        [[nodiscard]] TSRoleTypeRef to_ref_ts_data_type_for(const TSValueTypeMetaData &schema)
        {
            return to_ref_type_for_kind(schema.kind)(schema);
        }

        [[nodiscard]] TSData make_to_ref_data(const TSValueTypeMetaData &schema)
        {
            return TSData{alternative_type_for(
                to_ref_ts_data_type_for(schema), TypeRole::Data,
                "ts.alternative.to-ref.data")};
        }

        [[nodiscard]] TSOutputTypeRef checked_to_ref_output_type(const TSData                  &data,
                                                                 const TSValueTypeMetaData &schema)
        {
            const auto data_type = TSDataTypeRef::checked(data.type_ref());
            TSOutputTypeRef output_type;
            if (data_type.plan() == &MemoryUtils::plan_for<TSDProxy>())
            {
                auto view = data.view();
                output_type = tsd_proxy_output_type_for(schema, view.as_dict().layout().element_type);
            }
            else
            {
                output_type = TSDataPlanFactory::instance().output_type_for(&schema);
            }
            output_type = TSOutputTypeRef::checked(alternative_type_for(
                output_type.as_role(), TypeRole::Output,
                "ts.alternative.to-ref.output"));
            if (data_type.plan() != output_type.plan())
            {
                throw std::logic_error("TSOutput to-REF Data owner and Output facade require the same storage plan");
            }

            const auto *data_ops = data_type.ops();
            const auto *output_ops = output_type.ops();
            const auto *data_layout = data_ops != nullptr && data_ops->layout_impl != nullptr
                                          ? data_ops->layout_impl(data_ops->context)
                                          : nullptr;
            const auto *output_layout = output_ops != nullptr && output_ops->layout_impl != nullptr
                                            ? output_ops->layout_impl(output_ops->context)
                                            : nullptr;
            const bool compatible = data_ops != nullptr && output_ops != nullptr &&
                                    data_ops->kind == output_ops->kind && data_layout != nullptr &&
                                    output_layout != nullptr &&
                                    data_layout->value_offset == output_layout->value_offset &&
                                    data_layout->tracking_offset == output_layout->tracking_offset &&
                                    data_layout->value_binding.schema() == output_layout->value_binding.schema() &&
                                    data_layout->delta_binding.schema() == output_layout->delta_binding.schema();
            if (!compatible)
            {
                throw std::logic_error("TSOutput to-REF Data owner and Output facade require layout-compatible ops");
            }
            return output_type;
        }

        void populate_to_ref_unsupported(const ToRefPlan &, const TSDataView &,
                                         const TSOutputView &, DateTime)
        {
            throw std::logic_error("TSOutput to-REF alternative encountered unsupported requested schema");
        }

        void populate_to_ref_dict(const ToRefPlan &plan,
                                  const TSDataView &target,
                                  const TSOutputView &source_view,
                                  DateTime modified_time)
        {
            bind_tsd_proxy(target.borrowed_ref(),
                           source_view.data_view().as_dict(),
                           &to_ref_proxy_value_ops,
                           &plan,
                           modified_time);
        }

        void populate_to_ref_ref(const ToRefPlan &plan,
                                 const TSDataView &target,
                                 const TSOutputView &source_view,
                                 DateTime modified_time)
        {
            auto reference = TSOutputAlternativeStore::peered_reference_as(plan.target_schema->referenced_ts(),
                                                                            source_view.handle());
            // SAME-REFERENCE dedup (the getitem_ lesson): boundary rebinds
            // re-populate every refresh; an unchanged reference must not
            // record modified, or every rebind wakes downstream consumers.
            if (target.has_current_value() &&
                target.value().checked_as<TimeSeriesReference>() == reference)
            {
                return;
            }
            auto mutation = target.begin_mutation(modified_time);
            // move_value_from returns FIRST-FOR-TIME, not success: a same-cycle
            // re-populate writes the value and returns false - benign.
            static_cast<void>(mutation.move_value_from(Value{std::move(reference)}));
        }

        void populate_to_ref_bundle(const ToRefPlan &plan,
                                    const TSDataView &target,
                                    const TSOutputView &source_view,
                                    DateTime modified_time)
        {
            auto target_bundle = target.as_bundle();
            auto source_bundle = source_view.data_view().as_bundle();
            for (std::size_t index = 0; index < plan.children.size(); ++index)
            {
                auto target_child = target_bundle.at(index);
                auto source_child_data = source_bundle.at(index);
                auto source_child = source_child_view(source_view, source_child_data);
                populate_to_ref_data(plan.children[index], target_child, source_child, modified_time);
            }
        }

        void populate_to_ref_list(const ToRefPlan &plan,
                                  const TSDataView &target,
                                  const TSOutputView &source_view,
                                  DateTime modified_time)
        {
            auto target_list = target.as_list();
            auto source_list = source_view.data_view().as_list();
            for (std::size_t index = 0; index < plan.children.size(); ++index)
            {
                auto target_child = target_list.at(index);
                auto source_child_data = source_list.at(index);
                auto source_child = source_child_view(source_view, source_child_data);
                populate_to_ref_data(plan.children[index], target_child, source_child, modified_time);
            }
        }

        [[nodiscard]] const ToRefOps &to_ref_ops_for(TSTypeKind kind) noexcept
        {
            static constexpr std::size_t kind_count = ts_kind_index(TSTypeKind::SIGNAL) + 1U;
            static const std::array<ToRefOps, kind_count> table{
                ToRefOps{&populate_to_ref_unsupported},
                ToRefOps{&populate_to_ref_unsupported},
                ToRefOps{&populate_to_ref_dict},
                ToRefOps{&populate_to_ref_list},
                ToRefOps{&populate_to_ref_unsupported},
                ToRefOps{&populate_to_ref_bundle},
                ToRefOps{&populate_to_ref_ref},
                ToRefOps{&populate_to_ref_unsupported},
            };

            const auto index = ts_kind_index(kind);
            return index < table.size() ? table[index] : table.front();
        }

        [[nodiscard]] ToRefPlan make_to_ref_plan(const TSValueTypeMetaData &target_schema,
                                                 const ToRefBuildContext &build_context)
        {
            ToRefPlan plan{
                .target_schema = &target_schema,
                .build_context = &build_context,
                .ops = &to_ref_ops_for(target_schema.kind),
            };
            if (target_schema.kind == TSTypeKind::TSD)
            {
                plan.children.push_back(make_to_ref_plan(*target_schema.element_ts(), build_context));
            }
            else if (target_schema.kind == TSTypeKind::TSB)
            {
                plan.children.reserve(target_schema.field_count());
                for (std::size_t index = 0; index < target_schema.field_count(); ++index)
                {
                    plan.children.push_back(make_to_ref_plan(
                        *target_schema.fields()[index].type, build_context));
                }
            }
            else if (target_schema.kind == TSTypeKind::TSL)
            {
                plan.children.reserve(target_schema.fixed_size());
                for (std::size_t index = 0; index < target_schema.fixed_size(); ++index)
                {
                    plan.children.push_back(make_to_ref_plan(*target_schema.element_ts(), build_context));
                }
            }
            return plan;
        }
    }  // namespace

    struct TSOutputAlternativeStore::ToRefAlternativeState final
    {
        ToRefAlternativeState(const TSValueTypeMetaData &requested_schema, const TSOutputView &source)
            : requested_schema{&requested_schema},
              build_context{source.output()},
              plan{make_to_ref_plan(requested_schema, build_context)},
              data{make_to_ref_data(requested_schema)}
        {
            rebind(source);
        }

        ToRefAlternativeState(const ToRefAlternativeState &) = delete;
        ToRefAlternativeState &operator=(const ToRefAlternativeState &) = delete;
        ~ToRefAlternativeState() = default;

        /** Stop-time teardown: drop the (unsubscribed) source references. */
        void release_subscriptions() noexcept
        {
            source.reset();
            build_context.output = nullptr;
        }

        const TSValueTypeMetaData *requested_schema{nullptr};
        ToRefBuildContext          build_context{};
        ToRefPlan                  plan{};
        TSData                     data{};
        TSOutputHandle             source{};

        [[nodiscard]] TSOutputHandle handle(const TSOutput *output)
        {
            if (!is_migrated_ts_root_schema(requested_schema))
            {
                return TSOutputHandle{output, data.view()};
            }
            const auto output_type = checked_to_ref_output_type(data, *requested_schema);
            auto       owner_view = data.view();
            return TSOutputHandle{
                output,
                TSDataView{output_type.as_role(), owner_view.data()},
            };
        }

        void rebind(const TSOutputView &new_source)
        {
            source               = new_source.handle();
            build_context.output = new_source.output();
            refresh(new_source.evaluation_time());
        }

      private:
        void refresh(DateTime modified_time)
        {
            if (requested_schema == nullptr || !source.bound()) { return; }
            modified_time = concrete_reference_time(modified_time);
            auto target = data.view();
            populate_to_ref_data(plan, target, source.view(modified_time), modified_time);
        }
    };

    struct TSOutputAlternativeStore::RefLinkAlternativeState final
    {
        struct SourceNotifier final : Notifiable
        {
            explicit SourceNotifier(RefLinkAlternativeState &owner) noexcept
                : owner{&owner}
            {
            }

            void notify(DateTime modified_time) override
            {
                if (owner != nullptr) { owner->refresh(modified_time); }
            }

            RefLinkAlternativeState *owner{nullptr};
        };

        RefLinkAlternativeState(const TSValueTypeMetaData &requested_schema, const TSOutputView &source)
            : requested_schema{&requested_schema},
              plan{make_from_ref_endpoint_plan(&requested_schema)},
              data{checked_from_ref_storage_type(plan.schema)},
              notifier{*this}
        {
            rebind(source);
        }

        RefLinkAlternativeState(const RefLinkAlternativeState &) = delete;
        RefLinkAlternativeState &operator=(const RefLinkAlternativeState &) = delete;

        ~RefLinkAlternativeState() noexcept
        {
            unsubscribe_reference_sources(false);
            unsubscribe_source(false);
        }

        const TSValueTypeMetaData *requested_schema{nullptr};
        FromRefEndpointPlan        plan{};
        TSData                     data{};
        TSOutputHandle             source{};
        SourceNotifier             notifier;
        std::vector<TSOutputHandle> reference_sources{};

        [[nodiscard]] TSOutputHandle handle(const TSOutput *output) noexcept
        {
            return TSOutputHandle{output, data.view()};
        }

        void rebind(const TSOutputView &new_source)
        {
            const auto next_source = new_source.handle();
            if (!source.same_as(next_source))
            {
                unsubscribe_source();
                source = next_source;
                subscribe_source();
            }
            refresh(new_source.evaluation_time());
        }

        /**
         * Stop-time teardown: unsubscribe from the reference source and unbind
         * the projected data's links to the currently referenced output, while
         * both are still alive. Leaves the destructor's tolerant cleanup a
         * no-op.
         */
        void release_subscriptions(DateTime release_time) noexcept
        {
            unsubscribe_reference_sources(false);
            unsubscribe_source(false);
            source.reset();
            static_cast<void>(fallback_on_exception(false, [&] {
                auto target = data.view();
                unbind_from_ref_data(plan, target, release_time, true);
                return true;
            }));
        }

      private:
        void subscribe_source()
        {
            if (source.bound()) { source.data_view().subscribe(&notifier); }
        }

        void unsubscribe_source(bool strict = true) noexcept
        {
            if (!source.bound()) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                auto view = source.data_view();
                if (strict || (view.valid() && view.tracking().observers.contains(&notifier)))
                {
                    view.unsubscribe(&notifier);
                }
                return true;
            }));
        }

        void unsubscribe_reference_sources(bool strict = true) noexcept
        {
            for (const auto &observed : reference_sources)
            {
                if (!observed.bound()) { continue; }
                static_cast<void>(fallback_on_exception(false, [&] {
                    auto view = observed.data_view();
                    if (strict || (view.valid() && view.tracking().observers.contains(&notifier)))
                    {
                        view.unsubscribe(&notifier);
                    }
                    return true;
                }));
            }
            reference_sources.clear();
        }

        void replace_reference_sources(std::vector<TSOutputHandle> next)
        {
            unsubscribe_reference_sources();
            reference_sources = std::move(next);
            for (const auto &observed : reference_sources)
            {
                if (observed.bound()) { observed.data_view().subscribe(&notifier); }
            }
        }

        void refresh(DateTime modified_time)
        {
            if (modified_time == MIN_DT || requested_schema == nullptr || !source.bound()) { return; }

            auto source_view = source.view(modified_time);
            auto target = data.view();
            if (!source_view.valid())
            {
                unbind_from_ref_data(plan, target, modified_time);
                replace_reference_sources({});
                return;
            }

            const auto  source_value = source_view.value();
            const auto &reference = source_value.checked_as<TimeSeriesReference>();
            std::vector<TSOutputHandle> next_reference_sources;
            collect_forwarding_reference_sources(reference, modified_time, next_reference_sources);
            apply_reference_to_from_ref_data(plan, target, reference, modified_time);
            replace_reference_sources(std::move(next_reference_sources));
        }
    };

    struct TSOutputAlternativeStore::InteriorFromRefAlternativeState final
    {
        struct SourceNotifier final : Notifiable
        {
            explicit SourceNotifier(InteriorFromRefAlternativeState &owner) noexcept
                : owner{&owner}
            {
            }

            void notify(DateTime modified_time) override
            {
                if (owner != nullptr) { owner->refresh(modified_time); }
            }

            InteriorFromRefAlternativeState *owner{nullptr};
        };

        InteriorFromRefAlternativeState(const TSValueTypeMetaData &requested_schema, const TSOutputView &source)
            : requested_schema{&requested_schema},
              build_context{source.output()},
              plan{make_from_ref_interior_plan(requested_schema, *source.schema(), build_context)},
              data{alternative_type_for(
                  from_ref_interior_type_for(requested_schema, *source.schema()), TypeRole::Output,
                  "ts.alternative.interior-ref.output")},
              notifier{*this}
        {
            rebind(source);
        }

        InteriorFromRefAlternativeState(const InteriorFromRefAlternativeState &) = delete;
        InteriorFromRefAlternativeState &operator=(const InteriorFromRefAlternativeState &) = delete;

        ~InteriorFromRefAlternativeState() noexcept
        {
            if (!source.bound()) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                auto view = source.data_view();
                if (view.valid() && view.tracking().observers.contains(&notifier)) { view.unsubscribe(&notifier); }
                return true;
            }));
        }

        const TSValueTypeMetaData *requested_schema{nullptr};
        FromRefBuildContext        build_context{};
        FromRefInteriorPlan        plan{};
        TSData                     data{};
        TSOutputHandle             source{};
        SourceNotifier             notifier;

        [[nodiscard]] TSOutputHandle handle(const TSOutput *output) noexcept
        {
            return TSOutputHandle{output, data.view()};
        }

        [[nodiscard]] bool proxy_backed() const noexcept
        {
            return plan.ops->proxy_backed;
        }

        void rebind(const TSOutputView &new_source)
        {
            const auto next_source = new_source.handle();
            const bool source_changed = !source.same_as(next_source);
            if (source_changed)
            {
                unsubscribe_source();
                source               = next_source;
                build_context.output = new_source.output();
                // A proxy-backed alternative subscribes THROUGH its proxy
                // (key sync + child refresh); only structural shapes need the
                // state-level notifier to drive re-application.
                if (!proxy_backed()) { subscribe_source(); }
            }
            const auto modified_time = concrete_reference_time(new_source.evaluation_time());
            if (proxy_backed())
            {
                // (Re)binding the proxy also performs the initial sync.
                if (source_changed)
                {
                    apply_from_ref_interior(plan, data.view(), source.view(modified_time), modified_time);
                }
                return;
            }
            refresh(modified_time);
        }

        void release_subscriptions(DateTime release_time) noexcept
        {
            unsubscribe_source(false);
            source.reset();
            build_context.output = nullptr;
            static_cast<void>(fallback_on_exception(false, [&] {
                release_links(release_time);
                return true;
            }));
        }

      private:
        void release_links(DateTime release_time)
        {
            plan.ops->release(plan, data.view(), release_time);
        }

        void subscribe_source()
        {
            if (source.bound()) { source.data_view().subscribe(&notifier); }
        }

        void unsubscribe_source(bool strict = true) noexcept
        {
            if (proxy_backed() || !source.bound()) { return; }
            static_cast<void>(fallback_on_exception(false, [&] {
                auto view = source.data_view();
                if (strict || (view.valid() && view.tracking().observers.contains(&notifier)))
                {
                    view.unsubscribe(&notifier);
                }
                return true;
            }));
        }

        void refresh(DateTime modified_time)
        {
            if (modified_time == MIN_DT || requested_schema == nullptr || !source.bound()) { return; }
            apply_from_ref_interior(plan, data.view(), source.view(modified_time), modified_time);
        }
    };

}  // namespace hgraph::detail

namespace hgraph::detail
{
    void TSOutputAlternativeStore::ToRefAlternativeDelete::operator()(
        ToRefAlternativeState *p) const noexcept
    {
        delete p;
    }

    void TSOutputAlternativeStore::RefLinkAlternativeDelete::operator()(
        RefLinkAlternativeState *p) const noexcept
    {
        delete p;
    }

    void TSOutputAlternativeStore::InteriorFromRefAlternativeDelete::operator()(
        InteriorFromRefAlternativeState *p) const noexcept
    {
        delete p;
    }
}  // namespace hgraph::detail

namespace hgraph::detail
{
    TSOutputAlternativeStore::TSOutputAlternativeStore() noexcept = default;
    TSOutputAlternativeStore::TSOutputAlternativeStore(TSOutputAlternativeStore &&) noexcept = default;
    TSOutputAlternativeStore &TSOutputAlternativeStore::operator=(TSOutputAlternativeStore &&) noexcept = default;
    TSOutputAlternativeStore::~TSOutputAlternativeStore() noexcept = default;

    void TSOutputAlternativeStore::release_subscriptions(DateTime release_time) noexcept
    {
        to_ref_alternatives_.for_each([](const AlternativeKey &, ToRefAlternativeState &state) {
            state.release_subscriptions();
        });
        ref_link_alternatives_.for_each([&](const AlternativeKey &, RefLinkAlternativeState &state) {
            state.release_subscriptions(release_time);
        });
        interior_from_ref_alternatives_.for_each(
            [&](const AlternativeKey &, InteriorFromRefAlternativeState &state) {
                state.release_subscriptions(release_time);
            });
    }

    DynamicStorageMetrics TSOutputAlternativeStore::dynamic_storage_metrics() const noexcept
    {
        DynamicStorageMetrics result = to_ref_alternatives_.dynamic_storage_metrics();
        result += ref_link_alternatives_.dynamic_storage_metrics();
        result += interior_from_ref_alternatives_.dynamic_storage_metrics();

        to_ref_alternatives_.for_each([&](const AlternativeKey &, const ToRefAlternativeState &state) {
            result.live_bytes += sizeof(ToRefAlternativeState);
            result.reserved_bytes += sizeof(ToRefAlternativeState);
            const auto view = state.data.view();
            result += view.dynamic_storage_metrics();
            result += input_target_link_dynamic_storage_metrics(view);
        });
        ref_link_alternatives_.for_each([&](const AlternativeKey &, const RefLinkAlternativeState &state) {
            result.live_bytes += sizeof(RefLinkAlternativeState) +
                                 state.reference_sources.size() * sizeof(TSOutputHandle);
            result.reserved_bytes += sizeof(RefLinkAlternativeState) +
                                     state.reference_sources.capacity() * sizeof(TSOutputHandle);
            const auto view = state.data.view();
            result += view.dynamic_storage_metrics();
            result += input_target_link_dynamic_storage_metrics(view);
        });
        interior_from_ref_alternatives_.for_each(
            [&](const AlternativeKey &, const InteriorFromRefAlternativeState &state) {
                result.live_bytes += sizeof(InteriorFromRefAlternativeState);
                result.reserved_bytes += sizeof(InteriorFromRefAlternativeState);
                const auto view = state.data.view();
                result += view.dynamic_storage_metrics();
                result += input_target_link_dynamic_storage_metrics(view);
            });
        return result;
    }

    std::size_t TSOutputAlternativeStore::AlternativeKeyHash::operator()(const AlternativeKey &key) const noexcept
    {
        auto combine = [](std::size_t seed, std::size_t h) noexcept {
            return seed ^ (h + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U));
        };

        std::size_t seed = 0;
        seed = combine(seed, std::hash<const void *>{}(key.source_output));
        seed = combine(seed, std::hash<const TypeRecord *>{}(key.source_type.record()));
        seed = combine(seed, std::hash<const void *>{}(key.source_data));
        seed = combine(seed, std::hash<const void *>{}(key.requested_schema));
        return seed;
    }

    TSOutputAlternativeStore::AlternativeKey TSOutputAlternativeStore::key_for(
        const TSOutputView &source,
        const TSValueTypeMetaData &requested_schema) noexcept
    {
        return AlternativeKey{
            .source_output    = source.output(),
            .source_type      = source.storage_type(),
            .source_data      = source.data_view().data(),
            .requested_schema = &requested_schema,
        };
    }

    TSOutputHandle TSOutputAlternativeStore::binding_for(const TSOutputView &source,
                                                         const TSValueTypeMetaData &requested_schema)
    {
        struct AlternativeRoute
        {
            using BindFn = TSOutputHandle (TSOutputAlternativeStore::*)(
                const AlternativeKey &,
                const TSOutputView &,
                const TSValueTypeMetaData &);

            AlternativeRouteMatchesFn matches{nullptr};
            BindFn                    bind{nullptr};
        };

        static constexpr std::array<AlternativeRoute, 3> routes{{
            {&alternative_route_matches_to_ref, &TSOutputAlternativeStore::to_ref_binding},
            {&alternative_route_matches_from_ref, &TSOutputAlternativeStore::from_ref_binding},
            {&alternative_route_matches_from_ref_interior, &TSOutputAlternativeStore::from_ref_interior_binding},
        }};

        const auto *source_schema = source.schema();
        if (source_schema == nullptr)
        {
            throw std::invalid_argument("TSOutput alternative binding requires a typed source view");
        }

        const auto key = key_for(source, requested_schema);
        for (const auto &route : routes)
        {
            if (route.matches(source_schema, requested_schema))
            {
                return (this->*route.bind)(key, source, requested_schema);
            }
        }

        throw std::logic_error("TSOutput structural reference alternatives are not implemented yet");
    }

    TimeSeriesReference TSOutputAlternativeStore::peered_reference_as(const TSValueTypeMetaData *target_schema,
                                                                      TSOutputHandle target)
    {
        return TimeSeriesReference::peered_as(target_schema, target);
    }

    const TSOutputHandle &TSOutputAlternativeStore::peered_reference_target(const TimeSeriesReference &reference)
    {
        return reference.target_output();
    }

    TSDataView TSOutputAlternativeStore::child_view_with_parent(const TSDataView &parent,
                                                                const TSDataView &child,
                                                                std::size_t child_id)
    {
        return TSDataView{child.storage_type(), child.data(), parent, child_id};
    }

    TSOutputHandle TSOutputAlternativeStore::to_ref_binding(const AlternativeKey &key,
                                                            const TSOutputView &source,
                                                            const TSValueTypeMetaData &requested_schema)
    {
        auto *state = to_ref_alternatives_.find(key);
        if (state == nullptr)
        {
            std::unique_ptr<ToRefAlternativeState, ToRefAlternativeDelete> inserted{
                new ToRefAlternativeState(requested_schema, source)};
            state = to_ref_alternatives_.insert(key, std::move(inserted)).first;
        }
        else
        {
            if (state->requested_schema != &requested_schema)
            {
                throw std::logic_error("TSOutput to-REF alternative cache key resolved to the wrong requested schema");
            }
            state->rebind(source);
        }
        return state->handle(source.output());
    }

    TSOutputHandle TSOutputAlternativeStore::from_ref_binding(const AlternativeKey &key,
                                                              const TSOutputView &source,
                                                              const TSValueTypeMetaData &requested_schema)
    {
        auto *state = ref_link_alternatives_.find(key);
        if (state == nullptr)
        {
            std::unique_ptr<RefLinkAlternativeState, RefLinkAlternativeDelete> inserted{
                new RefLinkAlternativeState(requested_schema, source)};
            state = ref_link_alternatives_.insert(key, std::move(inserted)).first;
        }
        else
        {
            if (state->requested_schema != &requested_schema)
            {
                throw std::logic_error("TSOutput from-REF alternative cache key resolved to the wrong requested schema");
            }
            state->rebind(source);
        }
        return state->handle(source.output());
    }

    TSOutputHandle TSOutputAlternativeStore::from_ref_interior_binding(const AlternativeKey &key,
                                                                       const TSOutputView &source,
                                                                       const TSValueTypeMetaData &requested_schema)
    {
        auto *state = interior_from_ref_alternatives_.find(key);
        if (state == nullptr)
        {
            std::unique_ptr<InteriorFromRefAlternativeState, InteriorFromRefAlternativeDelete> inserted{
                new InteriorFromRefAlternativeState(requested_schema, source)};
            state = interior_from_ref_alternatives_.insert(key, std::move(inserted)).first;
        }
        else
        {
            if (state->requested_schema != &requested_schema)
            {
                throw std::logic_error(
                    "TSOutput interior from-REF alternative cache key resolved to the wrong requested schema");
            }
            state->rebind(source);
        }
        return state->handle(source.output());
    }
}  // namespace hgraph::detail
