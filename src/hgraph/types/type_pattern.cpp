#include <hgraph/types/type_pattern.h>

#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/endpoint_schema.h>  // time_series_schema_equivalent

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
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
            if (pattern.constraints.empty()) { return true; }
            return std::ranges::any_of(pattern.constraints, [concrete](const ValueTypeMetaData *constraint) {
                return constraint != nullptr && constraint == concrete;
            });
        }

        [[nodiscard]] bool ts_allowed_by_constraints(const TypePattern &pattern,
                                                     const TSValueTypeMetaData *concrete)
        {
            if (pattern.constraints.empty()) { return true; }
            return std::ranges::any_of(pattern.constraints, [concrete](const TSValueTypeMetaData *constraint) {
                return constraint != nullptr && time_series_schema_equivalent(constraint, concrete);
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
            if (pattern.kind == ScalarPattern::Kind::Var)
            {
                if (const auto *bound = map.find_scalar(pattern.name);
                    bound != nullptr && bound->is_named_bundle() && concrete != nullptr &&
                    concrete->is_named_bundle() &&
                    TypeRegistry::instance().bundle_is_a(concrete, bound))
                {
                    return true;
                }
            }
            return scalar_pattern_match(pattern, concrete, map);
        }
    }  // namespace

    bool scalar_pattern_match(const ScalarPattern &pattern, const ValueTypeMetaData *concrete, ResolutionMap &map)
    {
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
                return pattern.children.size() == 1
                           ? concrete->key_type == nullptr
                           : concrete->key_type != nullptr &&
                                 scalar_pattern_match(pattern.children[1], concrete->key_type, map);
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
            return pattern.fixed_size == 0 || pattern.fixed_size == concrete_size;
        }

        if (const std::optional<std::size_t> bound = map.find_size(pattern.size_name))
        {
            return *bound == concrete_size && size_allowed_by_constraints(pattern, concrete_size);
        }
        if (!size_allowed_by_constraints(pattern, concrete_size)) { return false; }
        map.bind_size(pattern.size_name, concrete_size);
        return true;
    }

    bool output_ts_pattern_match(const TypePattern &pattern,
                                 const TSValueTypeMetaData *concrete,
                                 ResolutionMap &map)
    {
        if (pattern.kind == TypePattern::Kind::Var && concrete != nullptr && concrete->kind == TSTypeKind::REF)
        {
            // OUTPUT direction: no REF transparency for a top-level variable -
            // the caller asked for a reference, so the produced port must BE
            // one. (Input-side transparency stands: consumers of value ports
            // adapt at input binding.)
            if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
            {
                auto &registry = TypeRegistry::instance();
                return time_series_schema_equivalent(registry.dereference(bound), registry.dereference(concrete));
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
                if (pattern.schema_var)
                {
                    if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
                    {
                        return time_series_schema_equivalent(bound, concrete);
                    }
                    map.bind_ts(pattern.name, concrete);
                    return true;
                }
                if (pattern.named_bundle)
                {
                    if (!concrete->is_named_tsb() || concrete->bundle_name() == nullptr ||
                        pattern.bundle_name != concrete->bundle_name())
                    {
                        return false;
                    }
                }
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
            return ts_pattern_match(pattern, concrete->referenced_ts(), map);
        }
        switch (pattern.kind)
        {
            case TypePattern::Kind::Var:
            {
                if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
                {
                    return bound == concrete && ts_allowed_by_constraints(pattern, concrete);
                }
                if (!ts_allowed_by_constraints(pattern, concrete)) { return false; }
                map.bind_ts(pattern.name, concrete);
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
                       (!concrete->is_duration_based() && pattern.fixed_size == concrete->period() &&
                        pattern.min_size == concrete->min_period());
            case TypePattern::Kind::TSB:
                if (concrete->kind != TSTypeKind::TSB) { return false; }
                if (pattern.schema_var)
                {
                    if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
                    {
                        return time_series_schema_equivalent(bound, concrete);
                    }
                    if (!ts_allowed_by_constraints(pattern, concrete)) { return false; }
                    map.bind_ts(pattern.name, concrete);
                    return true;
                }
                if (pattern.named_bundle)
                {
                    if (!concrete->is_named_tsb() || concrete->bundle_name() == nullptr ||
                        pattern.bundle_name != concrete->bundle_name())
                    {
                        return false;
                    }
                }
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
            case ScalarPattern::Kind::Var: return pattern.constraints.empty() ? SCALAR_VAR_RANK : SCALAR_VAR_RANK / 2;
            case ScalarPattern::Kind::Concrete: return 0;
            case ScalarPattern::Kind::UnknownTuple:
                return 1 + (pattern.children.empty() ? 0 : scalar_pattern_rank(pattern.children[0]) / 2);
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
                       (pattern.size_var ? 5 : pattern.fixed_size == 0 ? 10 : 0);
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
                return schema != nullptr && (pattern.children.size() == 1 || metadata != nullptr)
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
                return element != nullptr && !pattern.any_window ? registry.tsw(element, pattern.fixed_size, pattern.min_size)
                                                                 : nullptr;
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
                return fmt::format("Frame[{}, {}]", scalar_pattern_to_string(pattern.children[0]),
                                   scalar_pattern_to_string(pattern.children[1]));
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
                                   pattern.size_var ? "~" + pattern.size_name : std::to_string(pattern.fixed_size));
            case TypePattern::Kind::TSD:
                return fmt::format("TSD[{}, {}]", scalar_pattern_to_string(pattern.scalar),
                                   ts_pattern_to_string(pattern.children[0]));
            case TypePattern::Kind::TSW:
                if (pattern.any_window)
                {
                    return fmt::format("TSW[{}, *]", scalar_pattern_to_string(pattern.scalar));
                }
                return fmt::format("TSW[{}, {}, {}]", scalar_pattern_to_string(pattern.scalar),
                                   pattern.fixed_size, pattern.min_size);
            case TypePattern::Kind::TSB:
            {
                if (pattern.schema_var) { return fmt::format("TSB[~{}]", pattern.name); }
                std::vector<std::string> fields;
                fields.reserve(pattern.children.size());
                for (std::size_t i = 0; i < pattern.children.size(); ++i)
                {
                    fields.push_back(fmt::format("{}: {}", pattern.field_names[i], ts_pattern_to_string(pattern.children[i])));
                }
                return pattern.named_bundle ? fmt::format("TSB<{}>[{}]", pattern.bundle_name, fmt::join(fields, ", "))
                                            : fmt::format("TSB[{}]", fmt::join(fields, ", "));
            }
            case TypePattern::Kind::REF: return fmt::format("REF[{}]", ts_pattern_to_string(pattern.children[0]));
            case TypePattern::Kind::Signal: return "SIGNAL";
        }
        return "?";
    }
}  // namespace hgraph
