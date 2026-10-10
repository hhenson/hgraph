#include <hgraph/types/type_pattern.h>

#include <hgraph/types/time_series/endpoint_schema.h>

#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>  // time_series_schema_equivalent

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <ankerl/unordered_dense.h>
#include <optional>

namespace hgraph
{
    namespace
    {
        // A bare time-series variable (``TIME_SERIES_TYPE``) is always the least
        // specific match: strictly larger than any var nested inside a collection of
        // realistic depth. This reproduces the 2603 rule that a top-level generic is
        // less specific than a generic *inside* a structure (recursively).
        constexpr int LARGE_RANK = 10000;

        // Nested scalar genericness (inside TS / TSS / TSD): a scalar variable counts
        // far less than a bare time-series variable, but more than a concrete leaf.
        constexpr int SCALAR_VAR_RANK = 100;

        [[nodiscard]] bool scalar_allowed_by_constraints(const ScalarPattern &pattern,
                                                         const ValueTypeMetaData *concrete)
        {
            if (!pattern.constraints.empty() &&
                !std::ranges::any_of(pattern.constraints, [concrete](const ValueTypeMetaData *constraint) {
                    return constraint != nullptr && constraint == concrete;
                }))
            {
                return false;
            }
            if (pattern.bound == nullptr) { return true; }
            return TypeRegistry::instance().value_is_a(concrete, pattern.bound);
        }

        [[nodiscard]] const ValueTypeMetaData *matching_input_constraint(
            const ScalarPattern &pattern,
            const ValueTypeMetaData *concrete)
        {
            const ValueTypeMetaData *best = nullptr;
            std::optional<std::size_t> best_distance;
            auto &registry = TypeRegistry::instance();
            for (const ValueTypeMetaData *constraint : pattern.constraints)
            {
                if (constraint == nullptr) { continue; }
                const auto distance = registry.value_inheritance_distance(concrete, constraint);
                if (distance.has_value() && (!best_distance.has_value() || *distance < *best_distance))
                {
                    best = constraint;
                    best_distance = distance;
                }
            }
            return best;
        }

        [[nodiscard]] bool ts_allowed_by_constraints(const TypePattern &pattern,
                                                     const TSValueTypeMetaData *concrete)
        {
            if (pattern.constraints.empty()) { return true; }
            return std::ranges::any_of(pattern.constraints, [concrete](const TSValueTypeMetaData *constraint) {
                return constraint != nullptr && time_series_schema_equivalent(constraint, concrete);
            });
        }

        [[nodiscard]] bool input_ts_allowed_by_constraints(const TypePattern &pattern,
                                                            const TSValueTypeMetaData *concrete)
        {
            if (pattern.constraints.empty()) { return true; }
            return std::ranges::any_of(pattern.constraints, [concrete](const TSValueTypeMetaData *constraint) {
                return constraint != nullptr && time_series_value_equivalent(constraint, concrete);
            });
        }

        [[nodiscard]] bool size_allowed_by_constraints(const TypePattern &pattern, std::size_t concrete)
        {
            if (pattern.size_constraints.empty()) { return true; }
            return std::ranges::any_of(pattern.size_constraints, [concrete](std::size_t constraint) {
                return constraint == concrete;
            });
        }

        [[nodiscard]] const ValueTypeMetaData *homogeneous_tuple_element(const ValueTypeMetaData *value)
        {
            if (value == nullptr || value->value_kind() != ValueTypeKind::Tuple || value->field_count == 0)
            {
                return nullptr;
            }
            const ValueTypeMetaData *first = value->fields[0].type;
            for (std::size_t index = 1; index < value->field_count; ++index)
            {
                if (value->fields[index].type != first) { return nullptr; }
            }
            return first;
        }

        [[nodiscard]] const ValueTypeMetaData *resolve_required_scalar_child(
            const ScalarPattern &pattern,
            const ResolutionMap &map)
        {
            return !pattern.children.empty() ? scalar_pattern_resolve(pattern.children[0], map) : nullptr;
        }

        [[nodiscard]] bool input_scalar_pattern_match(
            const ScalarPattern &pattern,
            const ValueTypeMetaData *concrete,
            ResolutionMap &map)
        {
            if (pattern.kind == ScalarPattern::Kind::HomogeneousTuple)
            {
                concrete = value_schema_without_storage(concrete);
                const ValueTypeMetaData *element = nullptr;
                if (concrete != nullptr && concrete->value_kind() == ValueTypeKind::List)
                {
                    element = concrete->element_type;
                }
                else if (concrete != nullptr && concrete->value_kind() == ValueTypeKind::Tuple)
                {
                    element = homogeneous_tuple_element(concrete);
                }
                return element != nullptr && !pattern.children.empty() &&
                       input_scalar_pattern_match(pattern.children[0], element, map);
            }
            if (pattern.kind == ScalarPattern::Kind::Frame)
            {
                auto &registry = TypeRegistry::instance();
                if (!registry.is_frame(concrete) || concrete->element_type == nullptr ||
                    pattern.children.empty() || pattern.children.size() > 2 ||
                    !input_scalar_pattern_match(pattern.children[0], concrete->element_type, map))
                {
                    return false;
                }
                if (pattern.children.size() == 1) { return concrete->key_type == nullptr; }
                if (concrete->key_type == nullptr) { return pattern.optional_metadata; }
                return scalar_pattern_match(pattern.children[1], concrete->key_type, map);
            }
            if (pattern.kind == ScalarPattern::Kind::Concrete)
            {
                return TypeRegistry::instance().value_is_a(concrete, pattern.meta);
            }
            if (pattern.kind == ScalarPattern::Kind::Var)
            {
                if (const auto *bound = map.find_scalar(pattern.name); bound != nullptr)
                {
                    return concrete != nullptr && TypeRegistry::instance().value_is_a(concrete, bound);
                }
                if (!pattern.constraints.empty())
                {
                    const ValueTypeMetaData *constraint = matching_input_constraint(pattern, concrete);
                    if (constraint == nullptr ||
                        (pattern.bound != nullptr &&
                         !TypeRegistry::instance().value_is_a(constraint, pattern.bound)))
                    {
                        return false;
                    }
                    map.bind_scalar(pattern.name, constraint);
                    return true;
                }
            }
            return scalar_pattern_match(pattern, concrete, map);
        }

        [[nodiscard]] bool named_tsb_pattern_match(const TypePattern &pattern,
                                                   const TSValueTypeMetaData *concrete,
                                                   ResolutionMap &map)
        {
            if (!pattern.named_bundle) { return true; }
            // An unnamed bundle is compared by its fields alone, which the
            // caller checks next: a bundle's name counts only when both are
            // named (runtime spec WIR-15).
            if (!concrete->is_named_tsb() || concrete->bundle_name() == nullptr) { return true; }
            if (pattern.scalar.kind == ScalarPattern::Kind::Bundle && !pattern.scalar.bundle_origin.empty())
            {
                const auto *origin = concrete->value_type;
                if (origin != nullptr && origin->bundle_hierarchy != nullptr &&
                    origin->bundle_hierarchy->ordinary_origin != nullptr) {
                    origin = origin->bundle_hierarchy->ordinary_origin;
                }
                return scalar_pattern_match(pattern.scalar, origin, map);
            }
            return pattern.bundle_name == concrete->bundle_name();
        }

        // The #847 rule for generic variables, shared by the input and strict
        // matchers (writing_nodes.rst, "The matcher and unifier contract").
        //
        // A variable bound up front (an initial resolution) states the
        // schema, a top-level REF included, so the port as supplied matches
        // it before REF transparency strips the reference.
        [[nodiscard]] bool prebound_as_supplied(const TypePattern &pattern,
                                                const TSValueTypeMetaData *concrete,
                                                const ResolutionMap &map)
        {
            if (pattern.kind != TypePattern::Kind::Var) { return false; }
            const TSValueTypeMetaData *bound = map.find_ts(pattern.name);
            return bound != nullptr && time_series_schema_equivalent(bound, concrete) &&
                   ts_allowed_by_constraints(pattern, bound);
        }

        // A TSB schema variable is a generic too: it binds the dereferenced
        // pack, and a variable bound up front matches the pack as supplied or
        // dereferenced. ``concrete`` is already known to be a TSB.
        [[nodiscard]] bool tsb_schema_var_match(const TypePattern &pattern,
                                                const TSValueTypeMetaData *concrete,
                                                ResolutionMap &map)
        {
            const TSValueTypeMetaData *value = TypeRegistry::instance().dereference(concrete);
            if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
            {
                return (time_series_schema_equivalent(bound, concrete) ||
                        time_series_schema_equivalent(bound, value)) &&
                       ts_allowed_by_constraints(pattern, bound);
            }
            if (!ts_allowed_by_constraints(pattern, value)) { return false; }
            map.bind_ts(pattern.name, value);
            return true;
        }
    }  // namespace

    bool scalar_pattern_match(const ScalarPattern &pattern, const ValueTypeMetaData *concrete, ResolutionMap &map)
    {
        // The storage category takes no part in type resolution, so it is
        // stripped before anything is compared or bound: a variable binds the
        // type, never the category it happens to be stored behind.
        concrete = value_schema_without_storage(concrete);
        if (concrete == nullptr) { return false; }
        switch (pattern.kind)
        {
            case ScalarPattern::Kind::Var:
            {
                if (const ValueTypeMetaData *bound = map.find_scalar(pattern.name))
                {
                    return bound == concrete && scalar_allowed_by_constraints(pattern, concrete);
                }
                if (!scalar_allowed_by_constraints(pattern, concrete)) { return false; }
                map.bind_scalar(pattern.name, concrete);
                return true;
            }
            case ScalarPattern::Kind::Concrete: return pattern.meta == concrete;  // interned: pointer identity
            case ScalarPattern::Kind::UnknownTuple:
                if (concrete->value_kind() == ValueTypeKind::List)
                {
                    return pattern.children.empty() ||
                           scalar_pattern_match(pattern.children[0], concrete->element_type, map);
                }
                if (concrete->value_kind() == ValueTypeKind::Tuple)
                {
                    if (pattern.children.empty()) { return true; }
                    const ValueTypeMetaData *element = homogeneous_tuple_element(concrete);
                    return element != nullptr && scalar_pattern_match(pattern.children[0], element, map);
                }
                return false;
            case ScalarPattern::Kind::HomogeneousTuple:
            {
                const ValueTypeMetaData *element = nullptr;
                if (concrete->value_kind() == ValueTypeKind::List) { element = concrete->element_type; }
                else if (concrete->value_kind() == ValueTypeKind::Tuple) { element = homogeneous_tuple_element(concrete); }
                return element != nullptr && !pattern.children.empty() &&
                       scalar_pattern_match(pattern.children[0], element, map);
            }
            case ScalarPattern::Kind::List:
                return concrete->value_kind() == ValueTypeKind::List && !concrete->is_variadic_tuple() &&
                       !TypeRegistry::is_array(concrete) && concrete->is_fixed_size() == pattern.list_size.has_value() &&
                       (!pattern.list_size || concrete->fixed_size == *pattern.list_size) &&
                       pattern.children.size() == 1 && scalar_pattern_match(pattern.children[0], concrete->element_type, map);
            case ScalarPattern::Kind::SchemaProjection:
            {
                if (!pattern.projected || !pattern.project_source || !pattern.project_value) { return false; }
                // A projection need not be invertible (an atomic payload can
                // also be an ordinary composite). A bound source determines
                // its value directly without inventing another source shape.
                if (const auto *bound_source = ts_pattern_resolve(*pattern.projected, map)) {
                    return pattern.project_value(bound_source) == concrete &&
                           output_ts_pattern_match(*pattern.projected, bound_source, map);
                }
                const auto *source = pattern.project_source(concrete);
                if (!source || pattern.project_value(source) != concrete) { return false; }
                return output_ts_pattern_match(*pattern.projected, source, map);
            }
            case ScalarPattern::Kind::FixedTuple:
                if (concrete->value_kind() != ValueTypeKind::Tuple || concrete->field_count != pattern.children.size())
                {
                    return false;
                }
                for (std::size_t index = 0; index < pattern.children.size(); ++index)
                {
                    if (!scalar_pattern_match(pattern.children[index], concrete->fields[index].type, map)) { return false; }
                }
                return true;
            case ScalarPattern::Kind::Set:
                return concrete->value_kind() == ValueTypeKind::Set && !pattern.children.empty() &&
                       scalar_pattern_match(pattern.children[0], concrete->element_type, map);
            case ScalarPattern::Kind::Map:
                return concrete->value_kind() == ValueTypeKind::Map && pattern.children.size() == 2 &&
                       scalar_pattern_match(pattern.children[0], concrete->key_type, map) &&
                       scalar_pattern_match(pattern.children[1], concrete->element_type, map);
            case ScalarPattern::Kind::Series:
                return TypeRegistry::instance().is_series(concrete) &&
                       concrete->element_type != nullptr && !pattern.children.empty() &&
                       scalar_pattern_match(pattern.children[0], concrete->element_type, map);
            case ScalarPattern::Kind::Frame:
                if (!TypeRegistry::instance().is_frame(concrete) || concrete->element_type == nullptr ||
                    pattern.children.empty() || pattern.children.size() > 2 ||
                    !scalar_pattern_match(pattern.children[0], concrete->element_type, map))
                {
                    return false;
                }
                if (pattern.children.size() == 1) { return concrete->key_type == nullptr; }
                if (concrete->key_type == nullptr) { return pattern.optional_metadata; }
                return scalar_pattern_match(pattern.children[1], concrete->key_type, map);
            case ScalarPattern::Kind::Array:
            {
                if (!TypeRegistry::is_array(concrete) || pattern.children.empty()) { return false; }
                const ValueTypeMetaData *current = concrete;
                for (const auto &dimension : pattern.dimensions)
                {
                    if (!TypeRegistry::is_array(current)) { return false; }
                    const std::size_t concrete_size = current->fixed_size;
                    if (dimension.variable)
                    {
                        if (const auto bound = map.find_size(dimension.name))
                        {
                            if (*bound != concrete_size) { return false; }
                        }
                        else
                        {
                            map.bind_size(dimension.name, concrete_size);
                        }
                    }
                    else if (dimension.value != 0 && dimension.value != concrete_size)
                    {
                        return false;
                    }
                    current = current->element_type;
                }
                return !TypeRegistry::is_array(current) &&
                       scalar_pattern_match(pattern.children[0], current, map);
            }
            case ScalarPattern::Kind::Bundle:
            {
                if (concrete->value_kind() != ValueTypeKind::Bundle) { return false; }
                if (!pattern.bundle_origin.empty())
                {
                    // An ordinary nominal scalar pattern describes its origin,
                    // never the separate held representation. Temporal nominal
                    // matching supplies the explicit ordinary origin above.
                    if (concrete->bundle_hierarchy != nullptr &&
                        concrete->bundle_hierarchy->ordinary_origin != nullptr) { return false; }
                    const std::string_view actual = concrete->name();
                    if (!actual.starts_with(pattern.bundle_origin) ||
                        actual.size() <= pattern.bundle_origin.size() ||
                        actual[pattern.bundle_origin.size()] != '[')
                    {
                        return false;
                    }
                    const auto &arguments = concrete->bundle_generic_arguments();
                    if (arguments.size() != pattern.children.size()) { return false; }
                    for (std::size_t index = 0; index < arguments.size(); ++index)
                    {
                        if (!scalar_pattern_match(pattern.children[index], arguments[index], map)) { return false; }
                    }
                }
                if (!pattern.schema_var) { return true; }
                if (const ValueTypeMetaData *bound = map.find_scalar(pattern.name)) { return bound == concrete; }
                map.bind_scalar(pattern.name, concrete);
                return true;
            }
        }
        return false;
    }

    bool size_pattern_match(const TypePattern &pattern, std::size_t concrete_size, ResolutionMap &map)
    {
        if (!pattern.size_var)
        {
            return pattern.fixed_size == unbounded_tsl_size || pattern.fixed_size == concrete_size;
        }

        if (const std::optional<std::size_t> bound = map.find_size(pattern.size_name))
        {
            return *bound == concrete_size && size_allowed_by_constraints(pattern, concrete_size);
        }
        if (!size_allowed_by_constraints(pattern, concrete_size)) { return false; }
        map.bind_size(pattern.size_name, concrete_size);
        return true;
    }

    namespace
    {
        /**
         * Whether every shape ``specific`` accepts, ``general`` accepts: the same
         * rank; a fixed size in ``general`` is matched by the same fixed size; a
         * dynamic size (0) or a variable accepts any; and a variable ``general``
         * repeats (a square matrix) is repeated identically in ``specific``.
         */
        bool array_dimensions_cover(const std::vector<DimensionPattern> &general,
                                    const std::vector<DimensionPattern> &specific)
        {
            if (general.size() != specific.size()) { return false; }
            std::vector<std::pair<std::string_view, const DimensionPattern *>> repeated;
            for (std::size_t index = 0; index < general.size(); ++index)
            {
                const DimensionPattern &g = general[index];
                const DimensionPattern &c = specific[index];
                if (g.variable)
                {
                    const auto seen = std::ranges::find(repeated, std::string_view{g.name},
                                                        &std::pair<std::string_view, const DimensionPattern *>::first);
                    if (seen == repeated.end()) { repeated.emplace_back(g.name, &c); continue; }
                    const DimensionPattern &first = *seen->second;
                    const bool same = first.variable ? c.variable && c.name == first.name
                                                     : !c.variable && c.value != 0 && c.value == first.value;
                    if (!same) { return false; }
                }
                else if (g.value != 0 && (c.variable || c.value != g.value))
                {
                    return false;
                }
            }
            return true;
        }
    }  // namespace

    bool scalar_pattern_covers(const ScalarPattern &general, const ScalarPattern &specific, PatternCoverageMode mode)
    {
        // A concrete candidate type is covered exactly when the operator's
        // matching direction accepts it. Inputs permit nominal subtypes;
        // carried types use the exact scalar matcher.
        if (specific.kind == ScalarPattern::Kind::Concrete)
        {
            ResolutionMap scratch;
            return specific.meta != nullptr && (mode == PatternCoverageMode::Input
                ? input_scalar_pattern_match(general, specific.meta, scratch)
                : scalar_pattern_match(general, specific.meta, scratch));
        }
        if (general.kind == ScalarPattern::Kind::Var)
        {
            if (general.constraints.empty() && general.bound == nullptr) { return true; }
            // A constrained or bounded variable covers a variable whose every
            // accepted type it accepts.
            if (specific.kind != ScalarPattern::Kind::Var || specific.constraints.empty()) { return false; }
            return std::ranges::all_of(specific.constraints, [&](const ValueTypeMetaData *constraint) {
                ResolutionMap scratch;
                return scalar_pattern_match(general, constraint, scratch);
            });
        }
        if (specific.kind == ScalarPattern::Kind::Var) { return false; }  // wider than a structure
        if (general.kind == ScalarPattern::Kind::Frame && specific.kind == ScalarPattern::Kind::Frame &&
            general.optional_metadata && !specific.optional_metadata && general.children.size() == 2 &&
            specific.children.size() == 1)
        {
            // A frame whose metadata may be absent covers a frame without it.
            return scalar_pattern_covers(general.children[0], specific.children[0], mode);
        }
        const auto tuple_pattern = [](ScalarPattern::Kind kind) {
            return kind == ScalarPattern::Kind::UnknownTuple || kind == ScalarPattern::Kind::HomogeneousTuple ||
                   kind == ScalarPattern::Kind::FixedTuple;
        };
        if (general.kind == ScalarPattern::Kind::UnknownTuple && general.children.empty())
        {
            return tuple_pattern(specific.kind);
        }
        // UnknownTuple<T> and HomogeneousTuple<T> both admit homogeneous tuples
        // and lists. A fixed tuple refines them only if every position must have
        // the same type; independently bound variables do not guarantee that.
        if ((general.kind == ScalarPattern::Kind::UnknownTuple ||
             general.kind == ScalarPattern::Kind::HomogeneousTuple) && tuple_pattern(specific.kind))
        {
            if (general.children.empty() || specific.children.empty()) { return false; }
            if (specific.kind == ScalarPattern::Kind::FixedTuple)
            {
                // Repeated variables share a binding; concrete children name a
                // single type. Repeated structural wildcards can still match
                // different shapes, so they do not prove homogeneity.
                const auto kind = specific.children.front().kind;
                if (kind != ScalarPattern::Kind::Var && kind != ScalarPattern::Kind::Concrete) { return false; }
                const auto first = scalar_pattern_to_string(specific.children.front());
                if (!std::ranges::all_of(specific.children, [&](const ScalarPattern &child) {
                        return scalar_pattern_to_string(child) == first;
                    })) { return false; }
            }
            // UnknownTuple and fixed tuple elements use strict scalar matching.
            // HomogeneousTuple inputs additionally admit nominal subtypes. Do
            // not mistake that broader candidate for a strict tuple refinement.
            const auto &element = general.children[0];
            if (mode == PatternCoverageMode::Input && general.kind == ScalarPattern::Kind::UnknownTuple &&
                specific.kind == ScalarPattern::Kind::HomogeneousTuple &&
                !(element.kind == ScalarPattern::Kind::Var && element.constraints.empty() && element.bound == nullptr))
            {
                const auto admits_subtypes = [](const auto &self, const ScalarPattern &pattern) -> bool {
                    if (pattern.kind == ScalarPattern::Kind::Concrete)
                    {
                        return pattern.meta != nullptr && pattern.meta->value_kind() == ValueTypeKind::Bundle;
                    }
                    if (pattern.kind == ScalarPattern::Kind::Var)
                    {
                        return std::ranges::any_of(pattern.constraints, [](const ValueTypeMetaData *constraint) {
                            return constraint != nullptr && constraint->value_kind() == ValueTypeKind::Bundle;
                        });
                    }
                    return (pattern.kind == ScalarPattern::Kind::HomogeneousTuple || pattern.kind == ScalarPattern::Kind::Frame) &&
                           !pattern.children.empty() && self(self, pattern.children[0]);
                };
                if (admits_subtypes(admits_subtypes, specific.children[0])) { return false; }
            }
            const auto element_mode = general.kind == ScalarPattern::Kind::UnknownTuple
                                          ? PatternCoverageMode::TypeCarrier : mode;
            return scalar_pattern_covers(element, specific.children[0], element_mode);
        }
        if (specific.optional_metadata && !general.optional_metadata) { return false; }
        if (general.kind != specific.kind || general.children.size() != specific.children.size()) { return false; }
        // A nominal bundle constrained to a generic origin covers only bundles of
        // that origin; a candidate without one accepts any bundle.
        if (general.kind == ScalarPattern::Kind::Bundle && !general.bundle_origin.empty() &&
            specific.bundle_origin != general.bundle_origin)
        {
            return false;
        }
        if (general.kind == ScalarPattern::Kind::List && general.list_size != specific.list_size) { return false; }
        if (general.kind == ScalarPattern::Kind::SchemaProjection)
        {
            return general.project_source == specific.project_source && general.project_value == specific.project_value &&
                   general.projected && specific.projected &&
                   ts_pattern_covers(*general.projected, *specific.projected, PatternCoverageMode::TypeCarrier);
        }
        if (general.kind == ScalarPattern::Kind::Array && !array_dimensions_cover(general.dimensions, specific.dimensions))
        {
            return false;
        }
        for (std::size_t index = 0; index < general.children.size(); ++index)
        {
            if (!scalar_pattern_covers(general.children[index], specific.children[index], mode)) { return false; }
        }
        return true;
    }

    namespace
    {
        void collect_ts_variables(const TypePattern &, std::vector<std::string> &,
                                  std::vector<std::string> &, std::vector<std::string> &);

        void collect_scalar_variables(const ScalarPattern &pattern, std::vector<std::string> &series, std::vector<std::string> &scalars,
                                      std::vector<std::string> &sizes)
        {
            if (pattern.kind == ScalarPattern::Kind::Var) { scalars.push_back(pattern.name); }
            if (pattern.projected) { collect_ts_variables(*pattern.projected, series, scalars, sizes); }
            for (const DimensionPattern &dimension : pattern.dimensions)
            {
                if (dimension.variable) { sizes.push_back(dimension.name); }
            }
            for (const ScalarPattern &child : pattern.children) { collect_scalar_variables(child, series, scalars, sizes); }
        }

        void collect_ts_variables(const TypePattern &pattern, std::vector<std::string> &series,
                                  std::vector<std::string> &scalars, std::vector<std::string> &sizes)
        {
            if (pattern.kind == TypePattern::Kind::Var || pattern.schema_var) { series.push_back(pattern.name); }
            if (pattern.size_var) { sizes.push_back(pattern.size_name); }
            collect_scalar_variables(pattern.scalar, series, scalars, sizes);
            for (const TypePattern &child : pattern.children) { collect_ts_variables(child, series, scalars, sizes); }
            for (const TypePattern &parent : pattern.nominal_parents) { collect_ts_variables(parent, series, scalars, sizes); }
        }

        /** A concrete candidate type: the matcher binds the operator's variables,
            and each binding is read back. */
        void uses_from_bindings(const ResolutionMap &map, const std::vector<std::string> &series,
                                const std::vector<std::string> &scalars, const std::vector<std::string> &sizes,
                                PatternVariableUses &uses)
        {
            for (const std::string &name : series)
            {
                if (const auto *bound = map.find_ts(name)) { uses.emplace_back("ts:" + name, std::string{bound->name()}); }
            }
            for (const std::string &name : scalars)
            {
                if (const auto *bound = map.find_scalar(name))
                {
                    uses.emplace_back("scalar:" + name, std::string{bound->name()});
                }
            }
            for (const std::string &name : sizes)
            {
                if (const auto bound = map.find_size(name)) { uses.emplace_back("size:" + name, std::to_string(*bound)); }
            }
        }
    }  // namespace

    void scalar_pattern_variable_uses(const ScalarPattern &general, const ScalarPattern &specific,
                                      PatternVariableUses &uses)
    {
        if (specific.kind == ScalarPattern::Kind::Concrete)
        {
            std::vector<std::string> series, scalars, sizes;
            collect_scalar_variables(general, series, scalars, sizes);
            ResolutionMap map;
            if (specific.meta != nullptr && scalar_pattern_match(general, specific.meta, map))
            {
                uses_from_bindings(map, series, scalars, sizes, uses);
            }
            return;
        }
        if (general.kind == ScalarPattern::Kind::Var)
        {
            // A structural concrete pattern and a concrete metadata binding must
            // use the same registry spelling. Display formatting is not type
            // identity (for example Map[int,int] versus Map[int, int]).
            const auto *resolved = scalar_pattern_resolve(specific, ResolutionMap{});
            uses.emplace_back("scalar:" + general.name,
                              resolved ? std::string{resolved->name()} : scalar_pattern_to_string(specific));
            return;
        }
        if ((general.kind == ScalarPattern::Kind::UnknownTuple || general.kind == ScalarPattern::Kind::HomogeneousTuple) &&
            !general.children.empty() &&
            (specific.kind == ScalarPattern::Kind::UnknownTuple || specific.kind == ScalarPattern::Kind::HomogeneousTuple ||
             specific.kind == ScalarPattern::Kind::FixedTuple))
        {
            for (const auto &child : specific.children) { scalar_pattern_variable_uses(general.children[0], child, uses); }
            return;
        }
        if (general.kind != specific.kind) { return; }
        if (general.kind == ScalarPattern::Kind::SchemaProjection && general.projected && specific.projected)
        {
            ts_pattern_variable_uses(*general.projected, *specific.projected, uses);
            return;
        }
        if (general.kind == ScalarPattern::Kind::Array && general.dimensions.size() == specific.dimensions.size())
        {
            for (std::size_t index = 0; index < general.dimensions.size(); ++index)
            {
                const DimensionPattern &g = general.dimensions[index];
                const DimensionPattern &c = specific.dimensions[index];
                if (g.variable) { uses.emplace_back("size:" + g.name, c.variable ? "~" + c.name : std::to_string(c.value)); }
            }
        }
        const std::size_t count = std::min(general.children.size(), specific.children.size());
        for (std::size_t index = 0; index < count; ++index)
        {
            scalar_pattern_variable_uses(general.children[index], specific.children[index], uses);
        }
    }

    void ts_pattern_variable_uses(const TypePattern &general, const TypePattern &specific, PatternVariableUses &uses)
    {
        if (specific.kind == TypePattern::Kind::Concrete)
        {
            std::vector<std::string> series, scalars, sizes;
            collect_ts_variables(general, series, scalars, sizes);
            ResolutionMap map;
            if (specific.meta != nullptr && input_ts_pattern_match(general, specific.meta, map))
            {
                uses_from_bindings(map, series, scalars, sizes, uses);
            }
            return;
        }
        // References are transparent (WIR-6).
        if (general.kind == TypePattern::Kind::REF) { return ts_pattern_variable_uses(general.children[0], specific, uses); }
        if (specific.kind == TypePattern::Kind::REF) { return ts_pattern_variable_uses(general, specific.children[0], uses); }
        if (general.kind == TypePattern::Kind::Var)
        {
            const auto *resolved = ts_pattern_resolve(specific, ResolutionMap{});
            uses.emplace_back("ts:" + general.name,
                              resolved ? std::string{resolved->name()} : ts_pattern_to_string(specific));
            return;
        }
        if (general.kind != specific.kind) { return; }
        switch (general.kind)
        {
            case TypePattern::Kind::TS:
            case TypePattern::Kind::TSS:
            case TypePattern::Kind::TSW: scalar_pattern_variable_uses(general.scalar, specific.scalar, uses); return;
            case TypePattern::Kind::TSL:
                if (general.size_var)
                {
                    uses.emplace_back("size:" + general.size_name, specific.size_var ? "~" + specific.size_name
                                                                                     : std::to_string(specific.fixed_size));
                }
                ts_pattern_variable_uses(general.children[0], specific.children[0], uses);
                return;
            case TypePattern::Kind::TSD:
                scalar_pattern_variable_uses(general.scalar, specific.scalar, uses);
                ts_pattern_variable_uses(general.children[0], specific.children[0], uses);
                return;
            case TypePattern::Kind::TSB:
            {
                if (general.schema_var)
                {
                    uses.emplace_back("ts:" + general.name, ts_pattern_to_string(specific));
                    return;
                }
                // Fields pair by name (WIR-15), through a name index built once.
                ankerl::unordered_dense::map<std::string_view, std::size_t> by_name;
                for (std::size_t index = 0; index < specific.field_names.size(); ++index)
                {
                    by_name.emplace(specific.field_names[index], index);
                }
                for (std::size_t index = 0; index < general.field_names.size(); ++index)
                {
                    if (const auto found = by_name.find(general.field_names[index]); found != by_name.end())
                    {
                        ts_pattern_variable_uses(general.children[index], specific.children[found->second], uses);
                    }
                }
                return;
            }
            default: return;
        }
    }

    bool ts_pattern_covers(const TypePattern &general, const TypePattern &specific, PatternCoverageMode mode)
    {
        if (specific.kind == TypePattern::Kind::Concrete)
        {
            ResolutionMap scratch;
            return specific.meta != nullptr && (mode == PatternCoverageMode::Input
                ? input_ts_pattern_match(general, specific.meta, scratch)
                : output_ts_pattern_match(general, specific.meta, scratch));
        }
        // References are transparent for inputs (WIR-6). An explicit
        // carried reference must be matched by a reference.
        if (general.kind == TypePattern::Kind::REF)
        {
            if (mode == PatternCoverageMode::TypeCarrier)
            {
                return specific.kind == TypePattern::Kind::REF &&
                       ts_pattern_covers(general.children[0], specific.children[0], mode);
            }
            return ts_pattern_covers(general.children[0], specific, mode);
        }
        if (specific.kind == TypePattern::Kind::REF)
        {
            // A top-level carrier variable checks its constraints against the
            // reference as supplied, before ordinary reference transparency.
            if (mode == PatternCoverageMode::TypeCarrier && general.kind == TypePattern::Kind::Var)
            {
                return general.constraints.empty();
            }
            return ts_pattern_covers(general, specific.children[0], mode);
        }
        if (general.kind == TypePattern::Kind::Signal)
        {
            return mode == PatternCoverageMode::Input || specific.kind == TypePattern::Kind::Signal;
        }
        if (general.kind == TypePattern::Kind::Var)
        {
            if (general.constraints.empty()) { return true; }
            if (specific.kind != TypePattern::Kind::Var || specific.constraints.empty()) { return false; }
            return std::ranges::all_of(specific.constraints, [&](const TSValueTypeMetaData *constraint) {
                return ts_allowed_by_constraints(general, constraint);
            });
        }
        if (specific.kind == TypePattern::Kind::Var || specific.kind == TypePattern::Kind::Signal) { return false; }
        if (general.kind != specific.kind) { return false; }
        switch (general.kind)
        {
            case TypePattern::Kind::TS:
            case TypePattern::Kind::TSS: return scalar_pattern_covers(general.scalar, specific.scalar, mode);
            case TypePattern::Kind::TSL:
            {
                const bool any_size = general.size_var ? general.size_constraints.empty()
                                                       : general.fixed_size == unbounded_tsl_size;
                const auto allowed  = [&](std::size_t size) {
                    return std::ranges::find(general.size_constraints, size) != general.size_constraints.end();
                };
                const bool size_ok  = any_size ||
                                     (!specific.size_var && general.size_var && allowed(specific.fixed_size)) ||
                                     (specific.size_var && general.size_var && !specific.size_constraints.empty() &&
                                      std::ranges::all_of(specific.size_constraints, allowed)) ||
                                     (!general.size_var && !specific.size_var && general.fixed_size == specific.fixed_size);
                return size_ok && ts_pattern_covers(general.children[0], specific.children[0], mode);
            }
            case TypePattern::Kind::TSD:
                return scalar_pattern_covers(general.scalar, specific.scalar, mode) &&
                       ts_pattern_covers(general.children[0], specific.children[0], mode);
            case TypePattern::Kind::TSW:
                return scalar_pattern_covers(general.scalar, specific.scalar, mode) &&
                       (general.any_window ||
                        (general.duration_window == specific.duration_window && general.fixed_size == specific.fixed_size &&
                         general.min_size == specific.min_size && general.duration_micros == specific.duration_micros &&
                         general.min_duration_micros == specific.min_duration_micros));
            case TypePattern::Kind::TSB:
            {
                if (general.schema_var) { return true; }
                if (specific.schema_var) { return false; }
                // WIR-15: a named bundle matches its own name or an unnamed
                // bundle, so an unnamed candidate (which also takes other named
                // bundles) is wider than a named declaration. A generic nominal
                // bundle covers only its own origin.
                const bool general_generic = general.scalar.kind == ScalarPattern::Kind::Bundle &&
                                             !general.scalar.bundle_origin.empty();
                if (general.named_bundle && !specific.named_bundle) { return false; }
                if (general_generic)
                {
                    if (specific.scalar.bundle_origin != general.scalar.bundle_origin) { return false; }
                }
                else if (general.named_bundle && general.bundle_name != specific.bundle_name) { return false; }
                // Fields pair by name, in any order (WIR-15).
                if (general.field_names.size() != specific.field_names.size()) { return false; }
                ankerl::unordered_dense::map<std::string_view, std::size_t> by_name;
                for (std::size_t index = 0; index < specific.field_names.size(); ++index)
                {
                    by_name.emplace(specific.field_names[index], index);
                }
                for (std::size_t index = 0; index < general.children.size(); ++index)
                {
                    const auto found = by_name.find(general.field_names[index]);
                    if (found == by_name.end() ||
                        !ts_pattern_covers(general.children[index], specific.children[found->second], mode))
                    {
                        return false;
                    }
                }
                return true;
            }
            default: return false;
        }
    }

    bool output_ts_pattern_match(const TypePattern &pattern,
                                 const TSValueTypeMetaData *concrete,
                                 ResolutionMap &map)
    {
        // A REF around a whole requested bundle is followed: a bundle
        // pattern cannot produce a reference (the static unifier's
        // unify_requested_tsb_field_pack does the same).
        const bool tsb_schema_var = pattern.kind == TypePattern::Kind::TSB && pattern.schema_var;
        if (tsb_schema_var) { concrete = unify_dereference(concrete); }
        const bool top_level_variable =
            pattern.kind == TypePattern::Kind::Var ||
            (tsb_schema_var && concrete != nullptr && concrete->kind == TSTypeKind::TSB);
        if (top_level_variable && concrete != nullptr && TypeRegistry::contains_ref(concrete))
        {
            // OUTPUT direction: a requested output is the caller EXPRESSING
            // the schema it wants, so a top-level variable -- bare or a TSB
            // schema variable -- binds it verbatim, a REF at any depth kept:
            // the produced port must carry it (#847). A variable nested in a
            // structural pattern binds dereferenced, as an input's does.
            // (Input-side transparency stands: a generic input binds the
            // dereferenced schema and consumers adapt at input binding.)
            if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
            {
                // Bound up front (an initial resolution, the only binding
                // made before the output is matched), the stated schema must
                // BE the requested one, references included: a binding that
                // is merely value-equivalent would resolve an output without
                // the REF the caller asked for.
                return time_series_schema_equivalent(bound, concrete);
            }
            if (!ts_allowed_by_constraints(pattern, concrete)) { return false; }
            map.bind_ts(pattern.name, concrete);
            return true;
        }
        return ts_pattern_match(pattern, concrete, map);
    }

    bool input_ts_pattern_match(const TypePattern &pattern,
                                const TSValueTypeMetaData *concrete,
                                ResolutionMap &map)
    {
        if (concrete == nullptr) { return false; }
        if (pattern.kind == TypePattern::Kind::Signal) { return true; }
        if (pattern.kind != TypePattern::Kind::REF && concrete->kind == TSTypeKind::REF)
        {
            if (prebound_as_supplied(pattern, concrete, map)) { return true; }
            return input_ts_pattern_match(pattern, concrete->referenced_ts(), map);
        }

        switch (pattern.kind)
        {
            case TypePattern::Kind::Concrete:
                return graph_wiring_detail::input_accepts_output_schema(pattern.meta, concrete);
            case TypePattern::Kind::TSL:
                if (concrete->kind != TSTypeKind::TSL) { return false; }
                if (!size_pattern_match(pattern, concrete->fixed_size(), map)) { return false; }
                return input_ts_pattern_match(pattern.children[0], concrete->element_ts(), map);
            case TypePattern::Kind::TSD:
                return concrete->kind == TSTypeKind::TSD &&
                       scalar_pattern_match(pattern.scalar, concrete->key_type(), map) &&
                       input_ts_pattern_match(pattern.children[0], concrete->element_ts(), map);
            case TypePattern::Kind::TSB:
                if (concrete->kind != TSTypeKind::TSB) { return false; }
                if (pattern.schema_var) { return tsb_schema_var_match(pattern, concrete, map); }
                if (!named_tsb_pattern_match(pattern, concrete, map)) { return false; }
                if (concrete->field_count() != pattern.children.size()) { return false; }
                for (std::size_t i = 0; i < pattern.children.size(); ++i)
                {
                    const TSFieldMetaData &field = concrete->fields()[i];
                    if (field.name == nullptr || pattern.field_names[i] != field.name) { return false; }
                    if (!input_ts_pattern_match(pattern.children[i], field.type, map)) { return false; }
                }
                return true;
            case TypePattern::Kind::REF:
                return input_ts_pattern_match(pattern.children[0],
                                              concrete->kind == TSTypeKind::REF ? concrete->referenced_ts() : concrete,
                                              map);
            case TypePattern::Kind::TS:
                return concrete->kind == TSTypeKind::TS &&
                       input_scalar_pattern_match(pattern.scalar, concrete->value_schema, map);
            case TypePattern::Kind::Var:
            {
                if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
                {
                    return graph_wiring_detail::input_accepts_output_schema(bound, concrete) &&
                           input_ts_allowed_by_constraints(pattern, bound);
                }
                const auto *value = TypeRegistry::instance().dereference(concrete);
                if (!input_ts_allowed_by_constraints(pattern, value)) { return false; }
                map.bind_ts(pattern.name, value);
                return true;
            }
            case TypePattern::Kind::TSS:
            case TypePattern::Kind::TSW:
            case TypePattern::Kind::Signal:
                return ts_pattern_match(pattern, concrete, map);
        }
        return false;
    }

    bool ts_pattern_match(const TypePattern &pattern, const TSValueTypeMetaData *concrete, ResolutionMap &map)
    {
        if (concrete == nullptr) { return false; }
        // REF is transparent to matching (Python parity: ``REF[X]`` is
        // type-compatible with ``X``; consumers bind through the reference at
        // runtime — a type variable binds the *dereferenced* schema). A
        // reference schema only matches *as* a reference when the pattern asks
        // for one explicitly.
        if (pattern.kind != TypePattern::Kind::REF && concrete->kind == TSTypeKind::REF)
        {
            if (prebound_as_supplied(pattern, concrete, map)) { return true; }
            return ts_pattern_match(pattern, concrete->referenced_ts(), map);
        }
        switch (pattern.kind)
        {
            case TypePattern::Kind::Var:
            {
                // Resolving a generic dereferences everything: the variable
                // binds the schema with every reference followed at every
                // depth. Code that depends on a REF says so with a REF pattern
                // (owner ruling 2026-09-24, #847).
                const TSValueTypeMetaData *value = TypeRegistry::instance().dereference(concrete);
                if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
                {
                    // A variable bound up front (an initial resolution) is
                    // the caller EXPRESSING the schema it wants, references
                    // included: it matches an argument exactly as supplied as
                    // well as dereferenced. Bindings made by matching are
                    // always dereferenced, so only an explicit one keeps a REF.
                    // Compared as types are (WIR-15): a named bundle and the
                    // same unnamed bundle are one type.
                    return (time_series_schema_equivalent(bound, value) || time_series_schema_equivalent(bound, concrete)) &&
                           ts_allowed_by_constraints(pattern, bound);
                }
                if (!ts_allowed_by_constraints(pattern, value)) { return false; }
                map.bind_ts(pattern.name, value);
                return true;
            }
            case TypePattern::Kind::Concrete: return time_series_schema_equivalent(pattern.meta, concrete);
            case TypePattern::Kind::TS:
                return concrete->kind == TSTypeKind::TS && scalar_pattern_match(pattern.scalar, concrete->value_schema, map);
            case TypePattern::Kind::TSS:
                return concrete->kind == TSTypeKind::TSS && concrete->value_schema != nullptr &&
                       scalar_pattern_match(pattern.scalar, concrete->value_schema->element_type, map);
            case TypePattern::Kind::TSL:
                if (concrete->kind != TSTypeKind::TSL) { return false; }
                if (!size_pattern_match(pattern, concrete->fixed_size(), map)) { return false; }
                return ts_pattern_match(pattern.children[0], concrete->element_ts(), map);
            case TypePattern::Kind::TSD:
                return concrete->kind == TSTypeKind::TSD && scalar_pattern_match(pattern.scalar, concrete->key_type(), map) &&
                       ts_pattern_match(pattern.children[0], concrete->element_ts(), map);
            case TypePattern::Kind::TSW:
                if (concrete->kind != TSTypeKind::TSW ||
                    !scalar_pattern_match(pattern.scalar, concrete->value_type, map))
                {
                    return false;
                }
                return pattern.any_window ||
                       (pattern.duration_window && concrete->is_duration_based() &&
                        pattern.duration_micros == concrete->time_range().count() &&
                        pattern.min_duration_micros == concrete->min_time_range().count()) ||
                       (!pattern.duration_window && !concrete->is_duration_based() && pattern.fixed_size == concrete->period() &&
                        pattern.min_size == concrete->min_period());
            case TypePattern::Kind::TSB:
                if (concrete->kind != TSTypeKind::TSB) { return false; }
                if (pattern.schema_var) { return tsb_schema_var_match(pattern, concrete, map); }
                if (!named_tsb_pattern_match(pattern, concrete, map)) { return false; }
                if (concrete->field_count() != pattern.children.size()) { return false; }
                for (std::size_t i = 0; i < pattern.children.size(); ++i)
                {
                    const TSFieldMetaData &field = concrete->fields()[i];
                    if (field.name == nullptr || pattern.field_names[i] != field.name) { return false; }
                    if (!ts_pattern_match(pattern.children[i], field.type, map)) { return false; }
                }
                return true;
            case TypePattern::Kind::REF:
                return concrete->kind == TSTypeKind::REF && ts_pattern_match(pattern.children[0], concrete->referenced_ts(), map);
            case TypePattern::Kind::Signal: return concrete->kind == TSTypeKind::SIGNAL;
        }
        return false;
    }

    int scalar_pattern_rank(const ScalarPattern &pattern)
    {
        switch (pattern.kind)
        {
            case ScalarPattern::Kind::Var:
                return pattern.constraints.empty() && pattern.bound == nullptr
                           ? SCALAR_VAR_RANK
                           : SCALAR_VAR_RANK / 2;
            case ScalarPattern::Kind::Concrete: return 0;
            case ScalarPattern::Kind::SchemaProjection:
                return pattern.projected ? ts_pattern_rank(*pattern.projected) : SCALAR_VAR_RANK;
            case ScalarPattern::Kind::UnknownTuple:
                return 1 + (pattern.children.empty() ? 0 : scalar_pattern_rank(pattern.children[0]) / 2);
            case ScalarPattern::Kind::List:
            case ScalarPattern::Kind::HomogeneousTuple:
            case ScalarPattern::Kind::Set:
            case ScalarPattern::Kind::Series:
            case ScalarPattern::Kind::Frame:
            case ScalarPattern::Kind::Array:
            {
                int rank = 1;
                for (const ScalarPattern &child : pattern.children) { rank += scalar_pattern_rank(child) / 2; }
                return rank;
            }
            case ScalarPattern::Kind::FixedTuple:
            case ScalarPattern::Kind::Map:
            {
                int rank = 1;
                for (const ScalarPattern &child : pattern.children) { rank += scalar_pattern_rank(child) / 2; }
                return rank;
            }
            case ScalarPattern::Kind::Bundle:
            {
                if (pattern.bundle_origin.empty()) { return pattern.schema_var ? SCALAR_VAR_RANK / 2 : 1; }
                int rank = 1;
                for (const ScalarPattern &child : pattern.children) { rank += scalar_pattern_rank(child) / 2; }
                return rank;
            }
        }
        return 0;
    }

    int ts_pattern_rank(const TypePattern &pattern)
    {
        switch (pattern.kind)
        {
            case TypePattern::Kind::Var: return pattern.constraints.empty() ? LARGE_RANK : LARGE_RANK / 2;
            case TypePattern::Kind::Concrete: return 0;
            case TypePattern::Kind::TS: return 1 + scalar_pattern_rank(pattern.scalar);
            case TypePattern::Kind::TSS: return 1 + scalar_pattern_rank(pattern.scalar);
            case TypePattern::Kind::TSL:
                return 1 + ts_pattern_rank(pattern.children[0]) +
                       (pattern.size_var ? 5 : pattern.fixed_size == unbounded_tsl_size ? 10 : 0);
            case TypePattern::Kind::TSD: return 1 + scalar_pattern_rank(pattern.scalar) + ts_pattern_rank(pattern.children[0]);
            case TypePattern::Kind::TSW: return 1 + scalar_pattern_rank(pattern.scalar) + (pattern.any_window ? 10 : 0);
            case TypePattern::Kind::TSB:
            {
                if (pattern.schema_var) { return LARGE_RANK / 2; }
                int rank = 1;
                for (const TypePattern &child : pattern.children) { rank += ts_pattern_rank(child); }
                return rank;
            }
            case TypePattern::Kind::REF: return ts_pattern_rank(pattern.children[0]);
            case TypePattern::Kind::Signal: return 0;
        }
        return 0;
    }

    const ValueTypeMetaData *scalar_pattern_resolve(const ScalarPattern &pattern, const ResolutionMap &map)
    {
        switch (pattern.kind)
        {
            case ScalarPattern::Kind::Var: return map.find_scalar(pattern.name);
            case ScalarPattern::Kind::Concrete: return pattern.meta;
            case ScalarPattern::Kind::UnknownTuple: return nullptr;
            case ScalarPattern::Kind::List:
            {
                const auto *element = resolve_required_scalar_child(pattern, map);
                if (!element) { return nullptr; }
                return pattern.list_size ? TypeRegistry::instance().fixed_list(element, *pattern.list_size)
                                         : TypeRegistry::instance().list(element);
            }
            case ScalarPattern::Kind::SchemaProjection:
            {
                if (!pattern.projected || !pattern.project_value) { return nullptr; }
                const auto *source = ts_pattern_resolve(*pattern.projected, map);
                return source ? pattern.project_value(source) : nullptr;
            }
            case ScalarPattern::Kind::HomogeneousTuple:
            {
                const ValueTypeMetaData *element = resolve_required_scalar_child(pattern, map);
                return element != nullptr ? TypeRegistry::instance().list(element, 0, true) : nullptr;
            }
            case ScalarPattern::Kind::FixedTuple:
            {
                std::vector<const ValueTypeMetaData *> fields;
                fields.reserve(pattern.children.size());
                for (const ScalarPattern &child : pattern.children)
                {
                    const ValueTypeMetaData *field = scalar_pattern_resolve(child, map);
                    if (field == nullptr) { return nullptr; }
                    fields.push_back(field);
                }
                return TypeRegistry::instance().tuple(fields);
            }
            case ScalarPattern::Kind::Set:
            {
                const ValueTypeMetaData *element = resolve_required_scalar_child(pattern, map);
                return element != nullptr ? TypeRegistry::instance().set(element) : nullptr;
            }
            case ScalarPattern::Kind::Map:
            {
                if (pattern.children.size() != 2) { return nullptr; }
                const ValueTypeMetaData *key = scalar_pattern_resolve(pattern.children[0], map);
                const ValueTypeMetaData *value = scalar_pattern_resolve(pattern.children[1], map);
                return key != nullptr && value != nullptr ? TypeRegistry::instance().map(key, value) : nullptr;
            }
            case ScalarPattern::Kind::Series:
            {
                const ValueTypeMetaData *element = resolve_required_scalar_child(pattern, map);
                return element != nullptr ? TypeRegistry::instance().series(element) : nullptr;
            }
            case ScalarPattern::Kind::Frame:
            {
                if (pattern.children.empty() || pattern.children.size() > 2) { return nullptr; }
                const ValueTypeMetaData *schema = scalar_pattern_resolve(pattern.children[0], map);
                const ValueTypeMetaData *metadata = pattern.children.size() == 2
                                                        ? scalar_pattern_resolve(pattern.children[1], map)
                                                        : nullptr;
                return schema != nullptr && (pattern.children.size() == 1 || metadata != nullptr || pattern.optional_metadata)
                           ? TypeRegistry::instance().frame(schema, metadata)
                           : nullptr;
            }
            case ScalarPattern::Kind::Array:
            {
                const ValueTypeMetaData *element = resolve_required_scalar_child(pattern, map);
                if (element == nullptr) { return nullptr; }
                std::vector<std::size_t> dimensions;
                dimensions.reserve(pattern.dimensions.size());
                for (const auto &dimension : pattern.dimensions)
                {
                    if (!dimension.variable)
                    {
                        dimensions.push_back(dimension.value);
                        continue;
                    }
                    const auto resolved = map.find_size(dimension.name);
                    if (!resolved.has_value()) { return nullptr; }
                    dimensions.push_back(*resolved);
                }
                return TypeRegistry::instance().array(element, dimensions);
            }
            case ScalarPattern::Kind::Bundle: return pattern.schema_var ? map.find_scalar(pattern.name) : nullptr;
        }
        return nullptr;
    }

    const TSValueTypeMetaData *ts_pattern_resolve(const TypePattern &pattern, const ResolutionMap &map)
    {
        TypeRegistry &registry = TypeRegistry::instance();
        switch (pattern.kind)
        {
            case TypePattern::Kind::Var: return map.find_ts(pattern.name);
            case TypePattern::Kind::Concrete: return pattern.meta;
            case TypePattern::Kind::TS:
            {
                const ValueTypeMetaData *value = scalar_pattern_resolve(pattern.scalar, map);
                return value != nullptr ? registry.ts(value) : nullptr;
            }
            case TypePattern::Kind::TSS:
            {
                const ValueTypeMetaData *element = scalar_pattern_resolve(pattern.scalar, map);
                return element != nullptr ? registry.tss(element) : nullptr;
            }
            case TypePattern::Kind::TSL:
            {
                const TSValueTypeMetaData *element = ts_pattern_resolve(pattern.children[0], map);
                if (element == nullptr) { return nullptr; }
                std::size_t size = pattern.fixed_size;
                if (pattern.size_var)
                {
                    const std::optional<std::size_t> bound = map.find_size(pattern.size_name);
                    if (!bound.has_value()) { return nullptr; }
                    size = *bound;
                }
                return registry.tsl(element, size);
            }
            case TypePattern::Kind::TSD:
            {
                const ValueTypeMetaData   *key   = scalar_pattern_resolve(pattern.scalar, map);
                const TSValueTypeMetaData *value = ts_pattern_resolve(pattern.children[0], map);
                return (key != nullptr && value != nullptr) ? registry.tsd(key, value) : nullptr;
            }
            case TypePattern::Kind::TSW:
            {
                const ValueTypeMetaData *element = scalar_pattern_resolve(pattern.scalar, map);
                if (element == nullptr || pattern.any_window) { return nullptr; }
                return pattern.duration_window ? registry.tsw_duration(element, TimeDelta{pattern.duration_micros},
                                                                       TimeDelta{pattern.min_duration_micros})
                                               : registry.tsw(element, pattern.fixed_size, pattern.min_size);
            }
            case TypePattern::Kind::TSB:
            {
                if (pattern.schema_var) { return map.find_ts(pattern.name); }
                std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                fields.reserve(pattern.children.size());
                for (std::size_t i = 0; i < pattern.children.size(); ++i)
                {
                    const TSValueTypeMetaData *child = ts_pattern_resolve(pattern.children[i], map);
                    if (child == nullptr) { return nullptr; }
                    fields.emplace_back(pattern.field_names[i], child);
                }
                if (pattern.nominal_projected) {
                    const auto *origin = scalar_pattern_resolve(pattern.scalar, map);
                    if (origin == nullptr && pattern.nominal_origin_resolver != nullptr)
                    {
                        origin = pattern.nominal_origin_resolver(map);
                    }
                    if (origin == nullptr) { return nullptr; }
                    std::vector<const ValueTypeMetaData *> parents;
                    for (const auto &parent_pattern : pattern.nominal_parents) {
                        const auto *parent = ts_pattern_resolve(parent_pattern, map);
                        if (parent == nullptr) { return nullptr; }
                        parents.push_back(parent->value_schema);
                    }
                    std::vector<std::pair<std::string, const ValueTypeMetaData *>> held_fields;
                    for (const auto &[name, type] : fields) { held_fields.emplace_back(name, type->value_schema); }
                    return registry.tsb(registry.projected_bundle(origin, held_fields, parents), fields);
                }
                if (pattern.named_bundle && pattern.scalar.kind == ScalarPattern::Kind::Bundle &&
                    !pattern.scalar.bundle_origin.empty())
                {
                    const ValueTypeMetaData *value = scalar_pattern_resolve(pattern.scalar, map);
                    return value != nullptr ? registry.tsb(value->name(), fields) : nullptr;
                }
                return pattern.named_bundle ? registry.tsb(pattern.bundle_name, fields) : registry.un_named_tsb(fields);
            }
            case TypePattern::Kind::REF:
            {
                const TSValueTypeMetaData *target = ts_pattern_resolve(pattern.children[0], map);
                return target != nullptr ? registry.ref(target) : nullptr;
            }
            case TypePattern::Kind::Signal: return registry.signal();
        }
        return nullptr;
    }

    ScalarPattern substitute_scalar_patterns(
        ScalarPattern pattern,
        const std::unordered_map<std::string, ScalarPattern> &replacements)
    {
        if (pattern.kind == ScalarPattern::Kind::Var)
        {
            const auto replacement = replacements.find(pattern.name);
            if (replacement != replacements.end()) { return replacement->second; }
        }
        for (ScalarPattern &child : pattern.children)
        {
            child = substitute_scalar_patterns(std::move(child), replacements);
        }
        if (pattern.projected)
        {
            pattern.projected = std::make_shared<const TypePattern>(
                substitute_scalar_patterns(*pattern.projected, replacements));
        }
        return pattern;
    }

    TypePattern substitute_scalar_patterns(
        TypePattern pattern,
        const std::unordered_map<std::string, ScalarPattern> &replacements)
    {
        pattern.scalar = substitute_scalar_patterns(std::move(pattern.scalar), replacements);
        for (TypePattern &child : pattern.children)
        {
            child = substitute_scalar_patterns(std::move(child), replacements);
        }
        for (TypePattern &parent : pattern.nominal_parents) { parent = substitute_scalar_patterns(std::move(parent), replacements); }
        return pattern;
    }

    ScalarPattern substitute_size_patterns(
        ScalarPattern pattern,
        const std::unordered_map<std::string, DimensionPattern> &replacements)
    {
        for (DimensionPattern &dimension : pattern.dimensions)
        {
            if (!dimension.variable) { continue; }
            const auto replacement = replacements.find(dimension.name);
            if (replacement != replacements.end()) { dimension = replacement->second; }
        }
        for (ScalarPattern &child : pattern.children)
        {
            child = substitute_size_patterns(std::move(child), replacements);
        }
        if (pattern.projected)
        {
            pattern.projected = std::make_shared<const TypePattern>(
                substitute_size_patterns(*pattern.projected, replacements));
        }
        return pattern;
    }

    TypePattern substitute_size_patterns(
        TypePattern pattern,
        const std::unordered_map<std::string, DimensionPattern> &replacements)
    {
        pattern.scalar = substitute_size_patterns(std::move(pattern.scalar), replacements);
        if (pattern.size_var)
        {
            const auto replacement = replacements.find(pattern.size_name);
            if (replacement != replacements.end())
            {
                if (replacement->second.variable)
                {
                    pattern.size_name = replacement->second.name;
                }
                else
                {
                    pattern.fixed_size = replacement->second.value;
                    pattern.size_name.clear();
                    pattern.size_var = false;
                }
            }
        }
        for (TypePattern &child : pattern.children)
        {
            child = substitute_size_patterns(std::move(child), replacements);
        }
        for (TypePattern &parent : pattern.nominal_parents) { parent = substitute_size_patterns(std::move(parent), replacements); }
        return pattern;
    }

    std::string scalar_pattern_to_string(const ScalarPattern &pattern)
    {
        switch (pattern.kind)
        {
            case ScalarPattern::Kind::Var: return "~" + pattern.name;
            case ScalarPattern::Kind::Concrete:
                return (pattern.meta != nullptr && !pattern.meta->name().empty())
                           ? std::string{pattern.meta->name()}
                           : std::string{"scalar"};
            case ScalarPattern::Kind::SchemaProjection:
                return fmt::format("{}<{}>", pattern.name,
                                   pattern.projected ? ts_pattern_to_string(*pattern.projected) : "?");
            case ScalarPattern::Kind::List:
                return fmt::format("list[{}{}]", pattern.children.empty() ? "?" : scalar_pattern_to_string(pattern.children[0]),
                                   pattern.list_size ? ", " + std::to_string(*pattern.list_size) : "");
            case ScalarPattern::Kind::UnknownTuple:
                return pattern.children.empty()
                           ? std::string{"UnknownTuple"}
                           : fmt::format("UnknownTuple[{}]", scalar_pattern_to_string(pattern.children[0]));
            case ScalarPattern::Kind::HomogeneousTuple:
                return fmt::format("tuple[{}, ...]",
                                   pattern.children.empty() ? std::string{"scalar"}
                                                            : scalar_pattern_to_string(pattern.children[0]));
            case ScalarPattern::Kind::FixedTuple:
            {
                std::vector<std::string> parts;
                parts.reserve(pattern.children.size());
                for (const ScalarPattern &child : pattern.children) { parts.push_back(scalar_pattern_to_string(child)); }
                return fmt::format("tuple[{}]", fmt::join(parts, ", "));
            }
            case ScalarPattern::Kind::Set:
                return fmt::format("set[{}]",
                                   pattern.children.empty() ? std::string{"scalar"}
                                                            : scalar_pattern_to_string(pattern.children[0]));
            case ScalarPattern::Kind::Map:
                return pattern.children.size() == 2
                           ? fmt::format("Mapping[{}, {}]",
                                         scalar_pattern_to_string(pattern.children[0]),
                                         scalar_pattern_to_string(pattern.children[1]))
                           : std::string{"Mapping"};
            case ScalarPattern::Kind::Series:
                return fmt::format("Series[{}]",
                                   pattern.children.empty() ? std::string{"scalar"}
                                                            : scalar_pattern_to_string(pattern.children[0]));
            case ScalarPattern::Kind::Frame:
                if (pattern.children.empty()) { return "Frame[schema]"; }
                if (pattern.children.size() == 1)
                {
                    return fmt::format("Frame[{}]", scalar_pattern_to_string(pattern.children[0]));
                }
                return fmt::format("Frame[{}, {}{}]", scalar_pattern_to_string(pattern.children[0]),
                                   scalar_pattern_to_string(pattern.children[1]), pattern.optional_metadata ? "?" : "");
            case ScalarPattern::Kind::Array:
            {
                std::vector<std::string> dimensions;
                dimensions.reserve(pattern.dimensions.size());
                for (const auto &dimension : pattern.dimensions)
                {
                    dimensions.push_back(dimension.variable ? "~" + dimension.name
                                                            : dimension.value == 0 ? "*"
                                                                                   : std::to_string(dimension.value));
                }
                return fmt::format("Array[{}, {}]",
                                   pattern.children.empty() ? std::string{"scalar"}
                                                            : scalar_pattern_to_string(pattern.children[0]),
                                   fmt::join(dimensions, ", "));
            }
            case ScalarPattern::Kind::Bundle:
                if (!pattern.bundle_origin.empty())
                {
                    std::vector<std::string> arguments;
                    arguments.reserve(pattern.children.size());
                    for (const ScalarPattern &child : pattern.children)
                    {
                        arguments.push_back(scalar_pattern_to_string(child));
                    }
                    return fmt::format("{}[{}]", pattern.bundle_origin, fmt::join(arguments, ", "));
                }
                return pattern.schema_var ? fmt::format("CompoundScalar[~{}]", pattern.name)
                                          : std::string{"CompoundScalar"};
        }
        return "?";
    }

    std::string ts_pattern_to_string(const TypePattern &pattern)
    {
        switch (pattern.kind)
        {
            case TypePattern::Kind::Var: return "~" + pattern.name;
            case TypePattern::Kind::Concrete:
                return (pattern.meta != nullptr && !pattern.meta->name().empty())
                           ? std::string{pattern.meta->name()}
                           : std::string{"TS"};
            case TypePattern::Kind::TS: return fmt::format("TS[{}]", scalar_pattern_to_string(pattern.scalar));
            case TypePattern::Kind::TSS: return fmt::format("TSS[{}]", scalar_pattern_to_string(pattern.scalar));
            case TypePattern::Kind::TSL:
                return fmt::format("TSL[{}, {}]",
                                   ts_pattern_to_string(pattern.children[0]),
                                   pattern.size_var
                                       ? "~" + pattern.size_name
                                       : pattern.fixed_size == unbounded_tsl_size
                                           ? "*"
                                           : std::to_string(pattern.fixed_size));
            case TypePattern::Kind::TSD:
                return fmt::format("TSD[{}, {}]", scalar_pattern_to_string(pattern.scalar),
                                   ts_pattern_to_string(pattern.children[0]));
            case TypePattern::Kind::TSW:
                if (pattern.any_window)
                {
                    return fmt::format("TSW[{}, *]", scalar_pattern_to_string(pattern.scalar));
                }
                if (pattern.duration_window) {
                    return fmt::format("TSW[{}, duration={}us, min={}us]", scalar_pattern_to_string(pattern.scalar),
                                       pattern.duration_micros, pattern.min_duration_micros);
                }
                return fmt::format("TSW[{}, {}, {}]", scalar_pattern_to_string(pattern.scalar), pattern.fixed_size,
                                   pattern.min_size);
            case TypePattern::Kind::TSB:
            {
                if (pattern.schema_var) { return fmt::format("TSB[~{}]", pattern.name); }
                std::vector<std::string> fields;
                fields.reserve(pattern.children.size());
                for (std::size_t i = 0; i < pattern.children.size(); ++i)
                {
                    fields.push_back(fmt::format("{}: {}", pattern.field_names[i], ts_pattern_to_string(pattern.children[i])));
                }
                if (!pattern.named_bundle) { return fmt::format("TSB[{}]", fmt::join(fields, ", ")); }
                const std::string name = pattern.scalar.kind == ScalarPattern::Kind::Bundle &&
                                                 !pattern.scalar.bundle_origin.empty()
                                             ? scalar_pattern_to_string(pattern.scalar)
                                             : pattern.bundle_name;
                return fmt::format("TSB<{}>[{}]", name, fmt::join(fields, ", "));
            }
            case TypePattern::Kind::REF: return fmt::format("REF[{}]", ts_pattern_to_string(pattern.children[0]));
            case TypePattern::Kind::Signal: return "SIGNAL";
        }
        return "?";
    }
}  // namespace hgraph
