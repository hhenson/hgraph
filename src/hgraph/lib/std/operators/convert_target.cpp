#include <hgraph/lib/std/operators/convert_target.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/operator_type_resolution.h>

#include <fmt/format.h>

#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    namespace
    {
        using hgraph::operator_type_resolution::collection_element_or_self_schema;
        using hgraph::operator_type_resolution::collection_element_schema;
        using hgraph::operator_type_resolution::homogeneous_tuple_element_schema;
        using hgraph::operator_type_resolution::AnyTS;
        using hgraph::operator_type_resolution::AnyTSB;
        using hgraph::operator_type_resolution::AnyTSD;
        using hgraph::operator_type_resolution::AnyTSL;
        using hgraph::operator_type_resolution::AnyTSS;
        using hgraph::operator_type_resolution::time_series_schema_as;
        using hgraph::operator_type_resolution::ts_value_schema;

        [[nodiscard]] std::string schema_name(const TSValueTypeMetaData *schema)
        {
            return schema != nullptr && !schema->name().empty() ? std::string{schema->name()}
                                                                        : std::string{"<null>"};
        }

        [[nodiscard]] const TSValueTypeMetaData *deref(const TSValueTypeMetaData *schema)
        {
            return TypeRegistry::instance().dereference(schema);
        }

        [[nodiscard]] const TSValueTypeMetaData *ts_element_value_as_ts(const TSValueTypeMetaData *schema)
        {
            const ValueTypeMetaData *value = ts_value_schema(deref(schema));
            const ValueTypeMetaData *element = collection_element_schema(value);
            return element != nullptr ? TypeRegistry::instance().ts(element) : schema;
        }

        [[nodiscard]] const ValueTypeMetaData *tss_element(const TSValueTypeMetaData *schema)
        {
            schema = time_series_schema_as<AnyTSS>(schema);
            return schema != nullptr && schema->value_schema != nullptr
                       ? schema->value_schema->element_type
                       : nullptr;
        }

        [[nodiscard]] const ValueTypeMetaData *tsd_value_scalar(const TSValueTypeMetaData *schema)
        {
            schema = time_series_schema_as<AnyTSD>(schema);
            if (schema == nullptr) { return nullptr; }
            return ts_value_schema(deref(schema->element_ts()));
        }

        [[nodiscard]] const ValueTypeMetaData *tsl_element_scalar(const TSValueTypeMetaData *schema)
        {
            schema = time_series_schema_as<AnyTSL>(schema);
            if (schema == nullptr) { return nullptr; }
            return ts_value_schema(deref(schema->element_ts()));
        }

        [[nodiscard]] const ValueTypeMetaData *key_scalar_from_schema(const TSValueTypeMetaData *schema)
        {
            schema = deref(schema);
            if (schema == nullptr) { return nullptr; }
            if (time_series_schema_as<AnyTSS>(schema) != nullptr) { return tss_element(schema); }
            return collection_element_or_self_schema(ts_value_schema(schema));
        }

        [[nodiscard]] std::vector<std::size_t> selected_tsb_field_indices(
            const TSValueTypeMetaData *schema,
            std::span<const std::string> selected_keys)
        {
            std::vector<std::size_t> indices;
            schema = time_series_schema_as<AnyTSB>(schema);
            if (schema == nullptr) { return indices; }
            if (selected_keys.empty())
            {
                indices.reserve(schema->field_count());
                for (std::size_t index = 0; index < schema->field_count(); ++index) { indices.push_back(index); }
                return indices;
            }

            for (const std::string &key : selected_keys)
            {
                bool found = false;
                for (std::size_t index = 0; index < schema->field_count(); ++index)
                {
                    const char *name = schema->fields()[index].name;
                    if (name != nullptr && key == name)
                    {
                        indices.push_back(index);
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    throw std::invalid_argument(
                        fmt::format("TSB conversion key '{}' is not present in {}", key, schema_name(schema)));
                }
            }
            return indices;
        }

        [[nodiscard]] const TSValueTypeMetaData *homogeneous_tsb_value_ts(
            const TSValueTypeMetaData *schema,
            std::span<const std::string> selected_keys)
        {
            auto indices = selected_tsb_field_indices(schema, selected_keys);
            if (indices.empty())
            {
                throw std::invalid_argument("cannot infer a TSD value type from an empty TSB field selection");
            }

            const TSValueTypeMetaData *first = nullptr;
            for (std::size_t index : indices)
            {
                const TSValueTypeMetaData *field = schema->fields()[index].type;
                const ValueTypeMetaData   *value = ts_value_schema(field);
                if (value == nullptr)
                {
                    throw std::invalid_argument("TSB conversion requires selected fields to be plain TS values");
                }
                if (first == nullptr)
                {
                    first = field;
                    continue;
                }
                if (first->value_schema != value)
                {
                    throw std::invalid_argument(
                        "TSB conversion requires all selected fields to have the same value type");
                }
            }
            return first;
        }

        [[nodiscard]] const ValueTypeMetaData *homogeneous_tsb_value_scalar(
            const TSValueTypeMetaData *schema,
            std::span<const std::string> selected_keys)
        {
            const TSValueTypeMetaData *field = homogeneous_tsb_value_ts(schema, selected_keys);
            return field != nullptr ? field->value_schema : nullptr;
        }

        [[nodiscard]] const TSValueTypeMetaData *tsb_for_bundle_value(const ValueTypeMetaData *bundle)
        {
            if (bundle == nullptr || !bundle->is_named_bundle())
            {
                return nullptr;
            }
            auto &registry = TypeRegistry::instance();
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
            fields.reserve(bundle->field_count);
            for (std::size_t index = 0; index < bundle->field_count; ++index)
            {
                const ValueFieldMetaData &field = bundle->fields[index];
                fields.emplace_back(std::string{field.name != nullptr ? field.name : ""},
                                    registry.ts(field.type));
            }
            return registry.tsb(bundle->name(), fields);
        }

        [[nodiscard]] const TSValueTypeMetaData *require_one_input(
            std::span<const TSValueTypeMetaData *const> inputs,
            std::string_view pattern)
        {
            if (inputs.empty())
            {
                throw std::invalid_argument(fmt::format("{} target resolution requires at least one input", pattern));
            }
            return deref(inputs[0]);
        }

        [[nodiscard]] const TSValueTypeMetaData *require_one_raw_input(
            std::span<const TSValueTypeMetaData *const> inputs,
            std::string_view pattern)
        {
            if (inputs.empty())
            {
                throw std::invalid_argument(fmt::format("{} target resolution requires at least one input", pattern));
            }
            return inputs[0];
        }

        [[nodiscard]] const TSValueTypeMetaData *match_pattern_target(
            const TypePattern &pattern,
            const TSValueTypeMetaData *candidate)
        {
            if (candidate == nullptr)
            {
                throw std::invalid_argument(
                    fmt::format("cannot resolve type pattern {} against <null>", ts_pattern_to_string(pattern)));
            }
            ResolutionMap resolution;
            if (!output_ts_pattern_match(pattern, candidate, resolution))
            {
                throw std::invalid_argument(
                    fmt::format("resolved target {} does not satisfy requested {}",
                                schema_name(candidate),
                                ts_pattern_to_string(pattern)));
            }
            return candidate;
        }

        [[nodiscard]] const ValueTypeMetaData *infer_tuple_value_candidate(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *input = require_one_input(inputs, "TS[tuple]");
            if (const TSValueTypeMetaData *ts = time_series_schema_as<AnyTS>(input))
            {
                const ValueTypeMetaData *value = ts->value_schema;
                if (value != nullptr && (value->value_kind() == ValueTypeKind::Tuple || value->value_kind() == ValueTypeKind::List))
                {
                    return value;
                }
                if (value != nullptr && registry.is_series(value) && value->element_type != nullptr)
                {
                    return registry.list(value->element_type, 0, true);
                }
                const ValueTypeMetaData *element = collection_element_or_self_schema(value);
                if (element != nullptr) { return registry.list(element, 0, true); }
            }
            if (time_series_schema_as<AnyTSS>(input) != nullptr)
            {
                const ValueTypeMetaData *element = tss_element(input);
                if (element != nullptr) { return registry.list(element, 0, true); }
            }
            if (const TSValueTypeMetaData *tsl = time_series_schema_as<AnyTSL>(input))
            {
                const ValueTypeMetaData *element = tsl_element_scalar(tsl);
                if (element == nullptr || tsl->fixed_size() == 0)
                {
                    throw std::invalid_argument(fmt::format("cannot infer fixed tuple target from {}", schema_name(input)));
                }
                std::vector<const ValueTypeMetaData *> fields(tsl->fixed_size(), element);
                return registry.tuple(fields);
            }
            throw std::invalid_argument(fmt::format("cannot infer TS[tuple] target from {}", schema_name(input)));
        }

        [[nodiscard]] const ValueTypeMetaData *infer_set_value_candidate(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *input = require_one_input(inputs, "TS[set]");
            const ValueTypeMetaData *element = nullptr;
            if (const TSValueTypeMetaData *ts = time_series_schema_as<AnyTS>(input))
            {
                element = collection_element_or_self_schema(ts->value_schema);
            }
            else if (time_series_schema_as<AnyTSS>(input) != nullptr) { element = tss_element(input); }
            if (element == nullptr)
            {
                throw std::invalid_argument(fmt::format("cannot infer TS[set] target from {}", schema_name(input)));
            }
            return registry.set(element);
        }

        [[nodiscard]] const ValueTypeMetaData *infer_mapping_value_candidate(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *first = require_one_input(inputs, "TS[Mapping]");
            if (inputs.size() == 1)
            {
                if (const TSValueTypeMetaData *ts = time_series_schema_as<AnyTS>(first))
                {
                    if (ts->value_schema != nullptr && ts->value_schema->value_kind() == ValueTypeKind::Map)
                    {
                        return ts->value_schema;
                    }
                }
                if (const TSValueTypeMetaData *tsd = time_series_schema_as<AnyTSD>(first))
                {
                    const ValueTypeMetaData *value = tsd_value_scalar(tsd);
                    if (value != nullptr) { return registry.map(tsd->key_type(), value); }
                }
                if (time_series_schema_as<AnyTSL>(first) != nullptr)
                {
                    const ValueTypeMetaData *value = tsl_element_scalar(first);
                    if (value != nullptr) { return registry.map(registry.value_type("int"), value); }
                }
                if (time_series_schema_as<AnyTSB>(first) != nullptr)
                {
                    const ValueTypeMetaData *value = homogeneous_tsb_value_scalar(first, {});
                    return registry.map(registry.value_type("str"), value);
                }
                throw std::invalid_argument(fmt::format("cannot infer TS[Mapping] target from {}", schema_name(first)));
            }

            const ValueTypeMetaData *key = key_scalar_from_schema(first);
            const ValueTypeMetaData *value = ts_value_schema(deref(inputs[1]));
            value = collection_element_or_self_schema(value);
            if (key == nullptr || value == nullptr)
            {
                throw std::invalid_argument("cannot infer TS[Mapping] target from key/value inputs");
            }
            return registry.map(key, value);
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_tsd_candidate(
            std::span<const TSValueTypeMetaData *const> inputs,
            std::span<const std::string> selected_keys)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *first = require_one_input(inputs, "TSD");
            if (inputs.size() == 1)
            {
                if (time_series_schema_as<AnyTSB>(first) != nullptr)
                {
                    return registry.tsd(registry.value_type("str"), homogeneous_tsb_value_ts(first, selected_keys));
                }
                if (const TSValueTypeMetaData *ts = time_series_schema_as<AnyTS>(first))
                {
                    const ValueTypeMetaData *value = ts->value_schema;
                    if (value != nullptr && value->value_kind() == ValueTypeKind::Map)
                    {
                        return registry.tsd(value->key_type, registry.ts(value->element_type));
                    }
                }
                if (const TSValueTypeMetaData *tsl = time_series_schema_as<AnyTSL>(first))
                {
                    const TSValueTypeMetaData *element = deref(tsl->element_ts());
                    if (element != nullptr)
                    {
                        return registry.tsd(registry.value_type("int"), registry.ref(element));
                    }
                }
                throw std::invalid_argument(fmt::format("cannot infer TSD target from {}", schema_name(first)));
            }

            const ValueTypeMetaData *key = key_scalar_from_schema(first);
            const TSValueTypeMetaData *value = ts_element_value_as_ts(deref(inputs[1]));
            if (key == nullptr || value == nullptr)
            {
                throw std::invalid_argument("cannot infer TSD target from key/value inputs");
            }
            return registry.tsd(key, value);
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_tsl_candidate(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *input = require_one_input(inputs, "TSL");
            if (time_series_schema_as<AnyTSL>(input) != nullptr) { return input; }
            if (const TSValueTypeMetaData *ts = time_series_schema_as<AnyTS>(input);
                ts != nullptr && ts->value_schema != nullptr)
            {
                const ValueTypeMetaData *value = ts->value_schema;
                if (value->value_kind() == ValueTypeKind::List && value->fixed_size > 0)
                {
                    return registry.tsl(registry.ts(value->element_type), value->fixed_size);
                }
                if (value->value_kind() == ValueTypeKind::Tuple)
                {
                    if (const ValueTypeMetaData *element = homogeneous_tuple_element_schema(value))
                    {
                        return registry.tsl(registry.ts(element), value->field_count);
                    }
                    throw std::invalid_argument("cannot infer TSL[V, SIZE] target from heterogeneous fixed tuple");
                }
            }
            throw std::invalid_argument(fmt::format("cannot infer TSL target from {}", schema_name(input)));
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_tsb_candidate(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            const TSValueTypeMetaData *input = require_one_input(inputs, "TSB");
            if (time_series_schema_as<AnyTSB>(input) != nullptr) { return input; }
            const ValueTypeMetaData *value = ts_value_schema(input);
            if (const TSValueTypeMetaData *bundle = tsb_for_bundle_value(value)) { return bundle; }
            throw std::invalid_argument(fmt::format("cannot infer TSB target from {}", schema_name(input)));
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_ts_candidate(
            const ScalarPattern &scalar_pattern,
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            switch (scalar_pattern.kind)
            {
                case ScalarPattern::Kind::Var:
                {
                    const TSValueTypeMetaData *input = require_one_input(inputs, "TS[T]");
                    if (time_series_schema_as<AnyTS>(input) == nullptr)
                    {
                        throw std::invalid_argument(
                            fmt::format("cannot infer TS[T] target from {}", schema_name(input)));
                    }
                    return input;
                }
                case ScalarPattern::Kind::Concrete: return registry.ts(scalar_pattern.meta);
                case ScalarPattern::Kind::UnknownTuple:
                case ScalarPattern::Kind::HomogeneousTuple:
                case ScalarPattern::Kind::FixedTuple: return registry.ts(infer_tuple_value_candidate(inputs));
                case ScalarPattern::Kind::Set: return registry.ts(infer_set_value_candidate(inputs));
                case ScalarPattern::Kind::Map: return registry.ts(infer_mapping_value_candidate(inputs));
                case ScalarPattern::Kind::Series:
                case ScalarPattern::Kind::Frame:
                case ScalarPattern::Kind::Array:
                {
                    const TSValueTypeMetaData *input = require_one_input(
                        inputs, scalar_pattern_to_string(scalar_pattern));
                    if (input->kind != TSTypeKind::TS || input->value_schema == nullptr)
                    {
                        throw std::invalid_argument(
                            fmt::format("cannot infer {} target from {}",
                                        scalar_pattern_to_string(scalar_pattern), schema_name(input)));
                    }
                    const bool matches =
                        scalar_pattern.kind == ScalarPattern::Kind::Series
                            ? registry.is_series(input->value_schema)
                            : scalar_pattern.kind == ScalarPattern::Kind::Frame
                                  ? registry.is_frame(input->value_schema)
                                  : registry.is_array(input->value_schema);
                    if (!matches)
                    {
                        throw std::invalid_argument(
                            fmt::format("cannot infer {} target from {}",
                                        scalar_pattern_to_string(scalar_pattern), schema_name(input)));
                    }
                    return input;
                }
                case ScalarPattern::Kind::Bundle:
                {
                    const TSValueTypeMetaData *input = require_one_input(inputs, "TS[CompoundScalar]");
                    if (const TSValueTypeMetaData *tsb = time_series_schema_as<AnyTSB>(input);
                        tsb != nullptr && tsb->value_schema != nullptr)
                    {
                        return registry.ts(tsb->value_schema);
                    }
                    if (const TSValueTypeMetaData *ts = time_series_schema_as<AnyTS>(input);
                        ts != nullptr && ts->value_schema != nullptr &&
                        ts->value_schema->value_kind() == ValueTypeKind::Bundle)
                    {
                        return ts;
                    }
                    throw std::invalid_argument(
                        fmt::format("cannot infer TS[CompoundScalar] target from {}", schema_name(input)));
                }
            }
            throw std::invalid_argument("unsupported scalar pattern for TS target inference");
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_convert_candidate(
            const TypePattern &pattern,
            std::span<const TSValueTypeMetaData *const> inputs,
            std::span<const std::string> selected_keys)
        {
            auto &registry = TypeRegistry::instance();
            switch (pattern.kind)
            {
                case TypePattern::Kind::Var: return require_one_input(inputs, ts_pattern_to_string(pattern));
                case TypePattern::Kind::Concrete: return pattern.meta;
                case TypePattern::Kind::TS: return infer_ts_candidate(pattern.scalar, inputs);
                case TypePattern::Kind::TSS:
                {
                    const TSValueTypeMetaData *input = require_one_input(inputs, ts_pattern_to_string(pattern));
                    const ValueTypeMetaData *element = nullptr;
                    if (const TSValueTypeMetaData *ts = time_series_schema_as<AnyTS>(input))
                    {
                        element = collection_element_or_self_schema(ts->value_schema);
                    }
                    else if (time_series_schema_as<AnyTSS>(input) != nullptr) { element = tss_element(input); }
                    else if (const TSValueTypeMetaData *tsd = time_series_schema_as<AnyTSD>(input))
                    {
                        element = tsd->key_type();
                    }
                    if (element == nullptr)
                    {
                        throw std::invalid_argument(
                            fmt::format("cannot infer TSS target from {}", schema_name(input)));
                    }
                    return registry.tss(element);
                }
                case TypePattern::Kind::TSD: return infer_tsd_candidate(inputs, selected_keys);
                case TypePattern::Kind::TSL: return infer_tsl_candidate(inputs);
                case TypePattern::Kind::TSB: return infer_tsb_candidate(inputs);
                case TypePattern::Kind::REF:
                {
                    const TSValueTypeMetaData *input = require_one_raw_input(inputs, ts_pattern_to_string(pattern));
                    if (input->kind != TSTypeKind::REF)
                    {
                        throw std::invalid_argument(
                            fmt::format("cannot infer REF target from {}", schema_name(input)));
                    }
                    return input;
                }
                case TypePattern::Kind::TSW:
                case TypePattern::Kind::Signal:
                    throw std::invalid_argument(
                        fmt::format("cannot infer convert target for {}", ts_pattern_to_string(pattern)));
            }
            throw std::invalid_argument("unsupported time-series pattern for convert target inference");
        }

        [[nodiscard]] const TSValueTypeMetaData *infer_collect_tsd_candidate(
            std::span<const TSValueTypeMetaData *const> inputs)
        {
            auto &registry = TypeRegistry::instance();
            const TSValueTypeMetaData *first = require_one_input(inputs, "collect[TSD]");
            if (time_series_schema_as<AnyTSD>(first) != nullptr) { return first; }
            const auto *first_ts = time_series_schema_as<AnyTS>(first);
            if (inputs.size() == 1 && first_ts != nullptr &&
                first_ts->value_schema != nullptr && first_ts->value_schema->value_kind() == ValueTypeKind::Map)
            {
                return registry.tsd(first_ts->value_schema->key_type, registry.ts(first_ts->value_schema->element_type));
            }
            if (inputs.size() >= 2)
            {
                const ValueTypeMetaData *key = key_scalar_from_schema(first);
                const TSValueTypeMetaData *value = ts_element_value_as_ts(deref(inputs[1]));
                if (key != nullptr && value != nullptr) { return registry.tsd(key, value); }
            }
            throw std::invalid_argument(fmt::format("cannot infer collect TSD target from {}", schema_name(first)));
        }

        [[nodiscard]] bool is_tsd_pattern(const TypePattern &pattern)
        {
            return pattern.kind == TypePattern::Kind::TSD;
        }

        [[nodiscard]] bool is_ts_mapping_pattern(const TypePattern &pattern)
        {
            return pattern.kind == TypePattern::Kind::TS && pattern.scalar.kind == ScalarPattern::Kind::Map;
        }
    }  // namespace

    const TSValueTypeMetaData *resolve_convert_target(const TypePattern &pattern,
                                                      std::span<const TSValueTypeMetaData *const> inputs,
                                                      std::span<const std::string> selected_keys)
    {
        const TSValueTypeMetaData *candidate = infer_convert_candidate(pattern, inputs, selected_keys);
        return match_pattern_target(pattern, candidate);
    }

    const TSValueTypeMetaData *resolve_combine_target(const TypePattern &pattern,
                                                      std::span<const TSValueTypeMetaData *const> inputs)
    {
        auto &registry = TypeRegistry::instance();
        switch (pattern.kind)
        {
            case TypePattern::Kind::Concrete:
            {
                const auto *meta = pattern.meta;
                if (inputs.size() > 1 && meta != nullptr && meta->kind == TSTypeKind::TS &&
                    meta->value_schema != nullptr &&
                    meta->value_schema->value_kind() == ValueTypeKind::List &&
                    meta->value_schema->has(ValueTypeFlags::VariadicTuple))
                {
                    // combine[TS[Tuple[T, ...]]](a, b, ...): hgraph emits the
                    // CONCRETE fixed row built from the ports (the same rule
                    // as the generic tuple pattern below).
                    std::vector<const ValueTypeMetaData *> fields;
                    fields.reserve(inputs.size());
                    for (const auto *raw : inputs)
                    {
                        const auto *ts = time_series_schema_as<AnyTS>(raw);
                        if (ts == nullptr) { return meta; }
                        fields.push_back(ts->value_schema);
                    }
                    return registry.ts(registry.tuple(fields));
                }
                return meta;
            }
            case TypePattern::Kind::TSS:
            {
                // combine[TSS](a, b, ...): the union set of N same-element
                // scalar time-series.
                if (inputs.empty())
                {
                    throw std::invalid_argument("combine[TSS] requires at least one input");
                }
                const ValueTypeMetaData *element = nullptr;
                for (const auto *raw : inputs)
                {
                    const auto *ts = time_series_schema_as<AnyTS>(raw);
                    if (ts == nullptr)
                    {
                        throw std::invalid_argument(
                            fmt::format("cannot infer combine[TSS] target from {}", schema_name(raw)));
                    }
                    if (element == nullptr) { element = ts->value_schema; }
                    else if (element != ts->value_schema)
                    {
                        throw std::invalid_argument("combine[TSS]: inputs have mixed element types");
                    }
                }
                return registry.tss(element);
            }
            case TypePattern::Kind::TSD:
            {
                if (inputs.size() == 2)
                {
                    // combine[TSD](keys, values) with ticking TSL pairs.
                    const auto *first  = time_series_schema_as<AnyTSL>(deref(inputs[0]));
                    const auto *second = time_series_schema_as<AnyTSL>(deref(inputs[1]));
                    if (first != nullptr && second != nullptr)
                    {
                        const auto *key   = tsl_element_scalar(first);
                        const auto *value = deref(second->element_ts());
                        if (key != nullptr && value != nullptr) { return registry.tsd(key, value); }
                    }
                }
                // The TS[tuple] (keys, values) zip pair (the convert k/v rule).
                return match_pattern_target(pattern, infer_tsd_candidate(inputs, {}));
            }
            case TypePattern::Kind::TSL:
            {
                // combine[TSL](a, b, ...): a fixed list of N same-typed ports.
                if (inputs.empty())
                {
                    throw std::invalid_argument("combine[TSL] requires at least one input");
                }
                const TSValueTypeMetaData *element = deref(inputs.front());
                for (const auto *raw : inputs)
                {
                    if (deref(raw) != element)
                    {
                        throw std::invalid_argument("combine[TSL]: inputs have mixed element types");
                    }
                }
                return registry.tsl(element, inputs.size());
            }
            case TypePattern::Kind::TS:
                if (inputs.size() > 1 &&
                    (pattern.scalar.kind == ScalarPattern::Kind::UnknownTuple ||
                     pattern.scalar.kind == ScalarPattern::Kind::HomogeneousTuple ||
                     pattern.scalar.kind == ScalarPattern::Kind::FixedTuple))
                {
                    // combine[TS[Tuple...]](a, b, ...): a fixed tuple of the
                    // port elements (hgraph parity: the concrete row shape
                    // built from the ports wins over the pattern's spelling).
                    std::vector<const ValueTypeMetaData *> fields;
                    fields.reserve(inputs.size());
                    for (const auto *raw : inputs)
                    {
                        const auto *ts = time_series_schema_as<AnyTS>(raw);
                        if (ts == nullptr)
                        {
                            throw std::invalid_argument(fmt::format(
                                "cannot infer combine tuple target from {}", schema_name(raw)));
                        }
                        fields.push_back(ts->value_schema);
                    }
                    return registry.ts(registry.tuple(fields));
                }
                break;
            default: break;
        }
        return resolve_convert_target(pattern, inputs, {});
    }

    const TSValueTypeMetaData *resolve_emit_target(const TSValueTypeMetaData *value_ts,
                                                   std::span<const TSValueTypeMetaData *const> inputs)
    {
        if (value_ts == nullptr) { throw std::invalid_argument("emit target resolution requires a value hint"); }
        const TSValueTypeMetaData *input = require_one_input(inputs, "emit");
        const ValueTypeMetaData   *key   = nullptr;
        if (const auto *tsd = time_series_schema_as<AnyTSD>(input)) { key = tsd->key_type(); }
        else if (const auto *ts = time_series_schema_as<AnyTS>(input);
                 ts != nullptr && ts->value_schema != nullptr &&
                 ts->value_schema->value_kind() == ValueTypeKind::Map)
        {
            key = ts->value_schema->key_type;
        }
        if (key == nullptr)
        {
            throw std::invalid_argument(
                fmt::format("cannot infer the emit key from {}", schema_name(input)));
        }
        auto &registry = TypeRegistry::instance();
        return registry.un_named_tsb({{"key", registry.ts(key)}, {"value", value_ts}});
    }

    const TSValueTypeMetaData *resolve_collect_target(const TypePattern &pattern,
                                                      std::span<const TSValueTypeMetaData *const> inputs)
    {
        if (inputs.empty()) { throw std::invalid_argument("collect target resolution requires at least one input"); }
        const TSValueTypeMetaData *candidate = nullptr;
        if (is_tsd_pattern(pattern))
        {
            candidate = infer_collect_tsd_candidate(inputs);
        }
        else if (is_ts_mapping_pattern(pattern))
        {
            candidate = TypeRegistry::instance().ts(infer_mapping_value_candidate(inputs));
        }
        else
        {
            candidate = infer_convert_candidate(pattern, inputs, {});
        }
        return match_pattern_target(pattern, candidate);
    }
}  // namespace hgraph::stdlib
