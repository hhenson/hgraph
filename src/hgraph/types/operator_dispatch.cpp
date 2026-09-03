#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/util/scope.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <exception>
#include <limits>

namespace hgraph
{
    namespace
    {
        /** Render a constrained scalar variable as its actual accepted public
            type when it has exactly one constraint. The variable name is an
            implementation detail in that case; exposing ``PN`` instead of
            ``tuple[str, ...]`` made generated Python signatures less precise
            than native dispatch. */
        [[nodiscard]] std::string scalar_signature_pattern(const ScalarPattern &pattern)
        {
            if (pattern.kind != ScalarPattern::Kind::Var || pattern.constraints.size() != 1)
            {
                return scalar_pattern_to_string(pattern);
            }
            const ValueTypeMetaData *constraint = pattern.constraints.front();
            if (constraint == nullptr) { return scalar_pattern_to_string(pattern); }
            if (constraint->value_kind() == ValueTypeKind::List &&
                constraint->has(ValueTypeFlags::VariadicTuple))
            {
                const std::string_view element =
                    constraint->element_type != nullptr
                        ? constraint->element_type->name()
                        : std::string_view{"scalar"};
                return fmt::format("tuple[{}, ...]", element);
            }
            return std::string{constraint->name()};
        }

        [[nodiscard]] bool value_schema_matches_ts_pattern(const TypePattern &pattern,
                                                           const ValueTypeMetaData *value_schema,
                                                           ResolutionMap &map)
        {
            if (value_schema == nullptr) { return false; }
            TypeRegistry &registry = TypeRegistry::instance();
            switch (pattern.kind)
            {
                case TypePattern::Kind::Var:
                    if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
                    {
                        return bound->value_schema == value_schema;
                    }
                    return ts_pattern_match(pattern, registry.ts(value_schema), map);
                case TypePattern::Kind::Concrete:
                    return pattern.meta != nullptr && pattern.meta->value_schema == value_schema;
                case TypePattern::Kind::TS:
                    return scalar_pattern_match(pattern.scalar, value_schema, map);
                case TypePattern::Kind::TSS:
                    return value_schema->value_kind() == ValueTypeKind::Set &&
                           scalar_pattern_match(pattern.scalar, value_schema->element_type, map);
                case TypePattern::Kind::TSL:
                    return value_schema->value_kind() == ValueTypeKind::List &&
                           size_pattern_match(pattern, value_schema->fixed_size, map) &&
                           value_schema_matches_ts_pattern(pattern.children[0], value_schema->element_type, map);
                case TypePattern::Kind::TSD:
                    return value_schema->value_kind() == ValueTypeKind::Map &&
                           scalar_pattern_match(pattern.scalar, value_schema->key_type, map) &&
                           value_schema_matches_ts_pattern(pattern.children[0], value_schema->element_type, map);
                case TypePattern::Kind::TSW:
                    return value_schema->value_kind() == ValueTypeKind::List &&
                           pattern.fixed_size == value_schema->fixed_size &&
                           scalar_pattern_match(pattern.scalar, value_schema->element_type, map);
                case TypePattern::Kind::TSB:
                    if (value_schema->value_kind() != ValueTypeKind::Bundle ||
                        value_schema->field_count != pattern.children.size())
                    {
                        return false;
                    }
                    for (std::size_t i = 0; i < pattern.children.size(); ++i)
                    {
                        const ValueFieldMetaData &field = value_schema->fields[i];
                        if (field.name == nullptr || pattern.field_names[i] != field.name) { return false; }
                        if (!value_schema_matches_ts_pattern(pattern.children[i], field.type, map)) { return false; }
                    }
                    return true;
                case TypePattern::Kind::REF:
                    return false;
                case TypePattern::Kind::Signal:
                    return value_schema == scalar_type<Bool>();
            }
            return false;
        }

        [[nodiscard]] bool scalar_value_matches_ts_schema(
            const TSValueTypeMetaData *target, const Value &value, int &rank_adjustment)
        {
            const auto *source = value.schema();
            if (target == nullptr || source == nullptr ||
                !operator_dispatch_detail::auto_const_value_matches(*target, *source))
            {
                return false;
            }
            rank_adjustment += static_cast<int>(
                operator_dispatch_detail::opaque_auto_const_distance(*target, *source)
                    .value_or(0));
            return true;
        }

        [[nodiscard]] bool scalar_value_matches_ts_pattern(
            const TypePattern &pattern, const Value &value, ResolutionMap &map,
            int &rank_adjustment)
        {
            if (pattern.kind == TypePattern::Kind::REF && !pattern.children.empty())
            {
                // A scalar promoting into a REF input: the const it lifts to
                // adapts through the to-REF binding, so match the TARGET.
                return scalar_value_matches_ts_pattern(
                    pattern.children[0], value, map, rank_adjustment);
            }
            if (pattern.kind == TypePattern::Kind::Var)
            {
                if (const TSValueTypeMetaData *bound = map.find_ts(pattern.name))
                {
                    return scalar_value_matches_ts_schema(bound, value, rank_adjustment);
                }
            }
            if (pattern.kind == TypePattern::Kind::Concrete)
            {
                return scalar_value_matches_ts_schema(
                    pattern.meta, value, rank_adjustment);
            }
            if (pattern.kind == TypePattern::Kind::TS &&
                pattern.scalar.kind == ScalarPattern::Kind::Concrete)
            {
                const auto *target = TypeRegistry::instance().ts(pattern.scalar.meta);
                return scalar_value_matches_ts_schema(target, value, rank_adjustment);
            }
            if (pattern.kind == TypePattern::Kind::TS &&
                pattern.scalar.kind == ScalarPattern::Kind::Var)
            {
                if (const ValueTypeMetaData *bound = map.find_scalar(pattern.scalar.name))
                {
                    const auto *target = TypeRegistry::instance().ts(bound);
                    return scalar_value_matches_ts_schema(target, value, rank_adjustment);
                }
            }
            return value_schema_matches_ts_pattern(pattern, value.schema(), map);
        }

        // Match one candidate against the supplied arguments, binding into a fresh
        // ``map``. On failure ``why`` records a human-readable reason and ``map`` is
        // discarded by the caller. This is the runtime matcher shared by every
        // candidate (C++ today, Python later).
        struct NormalizedCall
        {
            std::vector<WiringArg>                          args{};
            /** Collected ``**kwargs`` in call order: ports pass through;
                plain VALUES lift to const sources once a winner is chosen
                (Python's scalar-kwargs rule). */
            std::vector<std::pair<std::string, WiringArg>>  kwargs{};
            int                                             defaults_used{0};
        };

        constexpr int variadic_pack_fixed_input_penalty = 100'000'000;

        [[nodiscard]] int input_adaptation_rank(const TypePattern &pattern,
                                                const TSValueTypeMetaData *concrete,
                                                const ResolutionMap &map)
        {
            if (concrete == nullptr)
            {
                return 0;
            }

            TypeRegistry &registry = TypeRegistry::instance();
            const auto *expected = registry.dereference(ts_pattern_resolve(pattern, map));
            const auto *actual = registry.dereference(concrete);
            if (expected == nullptr || actual == nullptr ||
                time_series_schema_equivalent(expected, actual) ||
                expected->kind != TSTypeKind::TS || actual->kind != TSTypeKind::TS)
            {
                return 0;
            }

            const auto distance = registry.value_inheritance_distance(
                actual->value_schema, expected->value_schema);
            return distance.has_value() ? static_cast<int>(*distance) : 0;
        }

        [[nodiscard]] bool append_tail_arg(const WiringArg &arg,
                                           std::vector<WiringArg> &tail,
                                           std::string *why)
        {
            if (!arg.from_variadic_tail)
            {
                tail.push_back(arg);
                return true;
            }
            if (arg.kind != WiringArg::Kind::TimeSeries || !arg.port.is_structural_source())
            {
                if (why != nullptr) { *why = "packed variadic tail is not a structural time-series source"; }
                return false;
            }

            const auto *schema = TypeRegistry::instance().dereference(arg.port.schema);
            if (schema == nullptr || schema->kind != TSTypeKind::TSL)
            {
                if (why != nullptr) { *why = "packed variadic tail is not a TSL"; }
                return false;
            }

            for (const WiringPortRef &child : arg.port.structural_children())
            {
                WiringArg child_arg;
                child_arg.kind = WiringArg::Kind::TimeSeries;
                child_arg.port = child;
                tail.push_back(std::move(child_arg));
            }
            return true;
        }

        /**
         * Map the call onto the candidate's declared parameters using the
         * Python calling rules: positional arguments fill parameters in
         * order (overflow goes to the variadic tail), named arguments target
         * named parameters (after all positional ones), omitted parameters
         * take their declared defaults, and named arguments matching no
         * parameter collect into ``**kwargs`` when the candidate has one.
         * The output is positional: arguments in declared parameter order,
         * defaults materialised, tail appended.
         */
        bool normalize_call(const OperatorImpl &impl,
                            std::span<const WiringArg> args,
                            NormalizedCall &out,
                            std::string *why)
        {
            const std::size_t fixed =
                impl.variadic && !impl.params.empty() ? impl.params.size() - 1 : impl.params.size();
            // Params beyond ``positional_params`` are keyword-only (Python's
            // params after *args): positional arguments never fill them.
            const std::size_t positional_limit = std::min(impl.positional_params, fixed);

            std::size_t positional = 0;
            while (positional < args.size() && args[positional].name.empty()) { ++positional; }
            for (std::size_t i = positional; i < args.size(); ++i)
            {
                if (args[i].name.empty())
                {
                    if (why != nullptr) { *why = "positional argument follows a named argument"; }
                    return false;
                }
            }

            if (!impl.variadic && positional > positional_limit)
            {
                if (why != nullptr)
                {
                    *why = fmt::format("expects at most {} positional argument(s), got {}", positional_limit,
                                       positional);
                }
                return false;
            }

            std::vector<std::optional<WiringArg>> filled(fixed);
            std::vector<WiringArg>                tail;
            for (std::size_t i = 0; i < positional; ++i)
            {
                if (i < positional_limit) { filled[i] = args[i]; }
                else if (!append_tail_arg(args[i], tail, why)) { return false; }
            }

            for (std::size_t i = positional; i < args.size(); ++i)
            {
                const WiringArg &named = args[i];
                std::size_t      index = fixed;
                for (std::size_t p = 0; p < fixed; ++p)
                {
                    if (!impl.params[p].name.empty() && impl.params[p].name == named.name)
                    {
                        index = p;
                        break;
                    }
                }
                if (index < fixed)
                {
                    if (filled[index].has_value())
                    {
                        if (why != nullptr) { *why = fmt::format("got multiple values for argument '{}'", named.name); }
                        return false;
                    }
                    filled[index] = named;
                }
                else if (impl.has_kwargs)
                {
                    // Ports collect directly; a plain VALUE is accepted and
                    // lifts to a const source once this candidate wins
                    // (Python's scalar-kwargs rule - value -> const).
                    if (std::find_if(out.kwargs.begin(), out.kwargs.end(),
                                     [&](const auto &kw) { return kw.first == named.name; }) !=
                        out.kwargs.end())
                    {
                        if (why != nullptr) { *why = fmt::format("got multiple values for argument '{}'", named.name); }
                        return false;
                    }
                    out.kwargs.emplace_back(named.name, named);
                }
                else
                {
                    if (why != nullptr) { *why = fmt::format("got an unexpected keyword argument '{}'", named.name); }
                    return false;
                }
            }

            for (std::size_t p = 0; p < fixed; ++p)
            {
                if (filled[p].has_value()) { continue; }
                if (impl.params[p].default_value.has_value())
                {
                    WiringArg synthesised;
                    if (impl.params[p].kind == ParamPattern::Kind::Input &&
                        !impl.params[p].default_value->has_value())
                    {
                        // None default on a time-series parameter: a null
                        // source — the input is left unwired.
                        synthesised.kind = WiringArg::Kind::TimeSeries;
                    }
                    else
                    {
                        synthesised.kind         = WiringArg::Kind::Scalar;
                        synthesised.scalar_value = *impl.params[p].default_value;
                        synthesised.scalar_meta  = synthesised.scalar_value.schema();
                    }
                    filled[p] = std::move(synthesised);
                    ++out.defaults_used;
                    continue;
                }
                if (why != nullptr)
                {
                    *why = impl.params[p].name.empty()
                               ? fmt::format("missing required argument {}", p)
                               : fmt::format("missing required argument '{}'", impl.params[p].name);
                }
                return false;
            }

            out.args.reserve(fixed + tail.size());
            for (auto &slot : filled) { out.args.push_back(std::move(*slot)); }
            for (auto &arg : tail) { out.args.push_back(std::move(arg)); }
            return true;
        }

        bool try_match(const OperatorImpl &impl,
                       std::span<const WiringArg> args,
                       std::span<const std::pair<std::string, WiringArg>> kwargs,
                       std::optional<bool> output_required,
                       const TSValueTypeMetaData *expected_output,
                       ResolutionMap &map,
                       int &rank_adjustment,
                       std::string *why,
                       GlobalStateView global_state,
                       Wiring *wiring,
                       bool &requires_rejected)
        {
            if (output_required.has_value() && impl.has_output != *output_required)
            {
                if (why != nullptr)
                {
                    *why = *output_required ? "candidate has no output" : "candidate unexpectedly has an output";
                }
                return false;
            }
            const std::size_t fixed_params =
                impl.variadic && !impl.params.empty() ? impl.params.size() - 1 : impl.params.size();
            if (impl.variadic ? args.size() < fixed_params : impl.params.size() != args.size())
            {
                if (why != nullptr)
                {
                    *why = impl.variadic
                               ? fmt::format("expects at least {} argument(s), got {}", fixed_params, args.size())
                               : fmt::format("expects {} argument(s), got {}", impl.params.size(), args.size());
                }
                return false;
            }
            // Variadic tails are matched independently per supplied argument, so
            // they must also be ranked per supplied argument. The base rank for a
            // variadic impl excludes its tail pattern; add it back for each
            // consumed tail argument plus a small fixed penalty so exact fixed
            // arity wins at equal specificity.
            if (impl.variadic)
            {
                const int tail_rank = operator_dispatch_detail::param_pattern_rank(impl.params.back());
                rank_adjustment +=
                    tail_rank * static_cast<int>(args.size() - fixed_params) + 1;
            }
            // A kwargs collector is less specific than an exact signature; an
            // ANNOTATED collector additionally ranks by its pack pattern so a
            // concrete pack beats TSB[TS_SCHEMA] at equal fixed specificity.
            if (impl.has_kwargs)
            {
                ++rank_adjustment;
                if (impl.has_kwargs_pattern)
                {
                    rank_adjustment += ts_pattern_rank(impl.kwargs_pattern);
                }
            }

            if (expected_output != nullptr && impl.has_output)
            {
                if (!output_ts_pattern_match(impl.output, expected_output, map))
                {
                    if (why != nullptr) { *why = fmt::format("output does not match requested {}", expected_output->name()); }
                    return false;
                }
            }

            for (std::size_t i = 0; i < args.size(); ++i)
            {
                const bool          tail  = impl.variadic && i >= fixed_params;
                const ParamPattern &param = impl.params[std::min(i, impl.params.size() - 1)];
                const WiringArg    &arg   = args[i];

                if (!tail && arg.from_variadic_tail)
                {
                    rank_adjustment += variadic_pack_fixed_input_penalty;
                }

                if (tail)
                {
                    // Match each tail argument independently. Plain values
                    // use the same const-promotion rules as fixed inputs;
                    // their throwaway binding keeps heterogeneous tails from
                    // binding one another.
                    ResolutionMap tail_scope = map;
                    bool matched = false;
                    if (arg.kind == WiringArg::Kind::TimeSeries)
                    {
                        matched = input_ts_pattern_match(param.ts, arg.port.schema, tail_scope);
                    }
                    else
                    {
                        ++rank_adjustment;
                        matched = scalar_value_matches_ts_pattern(
                            param.ts, arg.scalar_value, tail_scope, rank_adjustment);
                    }
                    if (!matched)
                    {
                        if (why != nullptr)
                        {
                            *why = fmt::format("variadic argument {} does not match {}", i,
                                               ts_pattern_to_string(param.ts));
                        }
                        return false;
                    }
                    if (arg.kind == WiringArg::Kind::TimeSeries)
                    {
                        rank_adjustment += input_adaptation_rank(param.ts, arg.port.schema, map);
                    }
                    continue;
                }

                if (param.kind == ParamPattern::Kind::Input)
                {
                    if (arg.kind == WiringArg::Kind::TimeSeries && arg.port.schema == nullptr)
                    {
                        // A null source (None default / unwired input): matches
                        // any input pattern, binds no type variables.
                        continue;
                    }
                    if (arg.kind == WiringArg::Kind::TimeSeries)
                    {
                        if (!input_ts_pattern_match(param.ts, arg.port.schema, map))
                        {
                            if (why != nullptr)
                            {
                                *why = fmt::format("argument {} (a {}) does not match {}", i,
                                                   arg.port.schema != nullptr ? arg.port.schema->name()
                                                                              : std::string_view{"time-series"},
                                                   ts_pattern_to_string(param.ts));
                            }
                            return false;
                        }
                        rank_adjustment += input_adaptation_rank(param.ts, arg.port.schema, map);
                    }
                    else
                    {
                        // A plain value PROMOTING to a const source is less
                        // specific than a candidate taking it as a true
                        // scalar parameter (python's overload rule).
                        ++rank_adjustment;
                        if (!scalar_value_matches_ts_pattern(
                                param.ts, arg.scalar_value, map, rank_adjustment))
                        {
                            if (why != nullptr)
                            {
                                *why = fmt::format("scalar argument {} cannot be promoted to {}", i,
                                                   ts_pattern_to_string(param.ts));
                            }
                            return false;
                        }
                    }
                }
                else
                {
                    if (arg.kind != WiringArg::Kind::Scalar)
                    {
                        if (why != nullptr) { *why = fmt::format("argument {} should be a scalar", i); }
                        return false;
                    }
                    if (!arg.scalar_value.has_value() && param.scalar.kind == ScalarPattern::Kind::Var)
                    {
                        // A Python ``None`` default is an absent wiring-time
                        // scalar. It may satisfy an unconstrained scalar/type
                        // carrier but cannot bind the variable by itself.
                        continue;
                    }
                    bool scalar_matches = false;
                    if (param.scalar.kind == ScalarPattern::Kind::Concrete)
                    {
                        scalar_matches = arg.scalar_meta == param.scalar.meta;
                        if (!scalar_matches)
                        {
                            scalar_matches = operator_dispatch_detail::coerce_scalar_value_to_meta(
                                                 arg.scalar_value, param.scalar.meta)
                                                 .has_value();
                            if (scalar_matches)
                            {
                                const auto distance = TypeRegistry::instance().value_inheritance_distance(
                                    arg.scalar_meta, param.scalar.meta);
                                rank_adjustment += static_cast<int>(distance.value_or(1));
                            }
                        }
                    }
                    else
                    {
                        scalar_matches = scalar_pattern_match(param.scalar, arg.scalar_meta, map);
                    }
                    if (!scalar_matches)
                    {
                        if (why != nullptr)
                        {
                            *why = fmt::format("scalar argument {} does not match {}", i,
                                               scalar_pattern_to_string(param.scalar));
                        }
                        return false;
                    }
                }
            }

            // Issue #224: a declared ``**kwargs`` pack pattern (e.g.
            // TSB[TS_SCHEMA]) matches against the synthesized un-named TSB of
            // the supplied keywords — call order, ports verbatim (no REF
            // deref; upstream takes output_type as-is), scalars as
            // TS[inferred] (the const-lift rule). This is what binds
            // pack-level schema vars so the output pattern can resolve. An
            // empty pack deliberately binds nothing: a zero-keyword call
            // keeps today's rejection rather than selecting the collector.
            if (impl.has_kwargs && impl.has_kwargs_pattern && !kwargs.empty())
            {
                std::vector<std::pair<std::string, const TSValueTypeMetaData *>> pack_fields;
                pack_fields.reserve(kwargs.size());
                for (const auto &[kw_name, kw_arg] : kwargs)
                {
                    const TSValueTypeMetaData *field = nullptr;
                    if (kw_arg.kind == WiringArg::Kind::TimeSeries) { field = kw_arg.port.schema; }
                    else if (kw_arg.scalar_meta != nullptr)
                    {
                        field = TypeRegistry::instance().ts(kw_arg.scalar_meta);
                    }
                    if (field == nullptr)
                    {
                        if (why != nullptr) { *why = fmt::format("keyword argument '{}' has no wireable type", kw_name); }
                        return false;
                    }
                    pack_fields.emplace_back(kw_name, field);
                }
                const TSValueTypeMetaData *pack = nullptr;
                const bool tsd_collector =
                    impl.kwargs_pattern.kind == TypePattern::Kind::TSD ||
                    (impl.kwargs_pattern.kind == TypePattern::Kind::Concrete &&
                     impl.kwargs_pattern.meta != nullptr &&
                     impl.kwargs_pattern.meta->kind == TSTypeKind::TSD);
                if (tsd_collector)
                {
                    // A TSD-annotated collector (mirrors the python
                    // combine_tsd branch): string keys, one common value
                    // schema across every supplied keyword.
                    for (const auto &[kw_name, field] : pack_fields)
                    {
                        static_cast<void>(kw_name);
                        if (field != pack_fields.front().second)
                        {
                            if (why != nullptr)
                            {
                                *why = fmt::format(
                                    "TSD **kwargs requires one common value type; got {} and {}",
                                    pack_fields.front().second->name(), field->name());
                            }
                            return false;
                        }
                    }
                    pack = TypeRegistry::instance().tsd(
                        scalar_descriptor<Str>::value_meta(), pack_fields.front().second);
                }
                else { pack = TypeRegistry::instance().un_named_tsb(pack_fields); }
                if (!input_ts_pattern_match(impl.kwargs_pattern, pack, map))
                {
                    if (why != nullptr)
                    {
                        *why = fmt::format("supplied keywords {} do not match **kwargs pattern {}",
                                           pack->name(), ts_pattern_to_string(impl.kwargs_pattern));
                    }
                    return false;
                }
            }

            OperatorCallContext context{args, impl.params, kwargs, global_state,
                                        wiring};
            if (impl.default_resolver)
            {
                const bool resolved = fallback_on_exception(
                    false,
                    [&] {
                        impl.default_resolver(map, context);
                        return true;
                    },
                    [&](const char *message) {
                        if (why != nullptr) { *why = fmt::format("default type resolution failed: {}", message); }
                    });
                if (!resolved) { return false; }
            }

            if (impl.has_output && ts_pattern_resolve(impl.output, map) == nullptr)
            {
                if (why != nullptr) { *why = "output type could not be resolved"; }
                return false;
            }

            if (impl.requires_predicate)
            {
                bool threw = false;
                const bool accepted = fallback_on_exception(
                    false,
                    [&] { return impl.requires_predicate(map, context); },
                    [&](const char *message) {
                        if (why != nullptr) { *why = fmt::format("requires predicate threw: {}", message); }
                        threw = true;
                    });
                if (threw) { return false; }
                if (!accepted)
                {
                    if (why != nullptr) { *why = "rejected by requires predicate"; }
                    requires_rejected = true;
                    return false;
                }
            }
            return true;
        }
    }  // namespace

    OperatorRegistry &OperatorRegistry::instance() noexcept
    {
        // Immortal: registries are process-lifetime; destroying them at exit
        // is unordered across TUs in a shared module (Python bridge) and the
        // impls' default Values reference interned bindings owned elsewhere.
        static OperatorRegistry *registry = new OperatorRegistry();
        return *registry;
    }

    void OperatorRegistry::register_overload(OperatorImpl impl)
    {
        const std::string name = impl.name;
        auto &overloads = overloads_[name];
        overloads.push_back(std::move(impl));

        auto &suffix = suffix_min_ranks_[name];
        suffix.resize(overloads.size());
        int minimum = std::numeric_limits<int>::max();
        for (std::size_t i = overloads.size(); i-- > 0;)
        {
            minimum = std::min(minimum, overloads[i].rank);
            suffix[i] = minimum;
        }
    }

    void OperatorRegistry::register_installer(std::string_view key, std::function<void()> installer)
    {
        for (Installer &entry : installers_)
        {
            if (entry.key == key)
            {
                // Replace the callback, keep the applied state: entry points
                // stay idempotent between resets. A previously FAILED
                // attempt left applied=false, so a replaced callback runs on
                // the next rebuild.
                entry.fn = std::move(installer);
                return;
            }
        }
        installers_.push_back(Installer{.key = std::string{key}, .fn = std::move(installer)});
    }

    void OperatorRegistry::run_installers()
    {
        // Indexed iteration: an installer may register further installers
        // (an entry point that fans out), which reallocates the vector — and
        // freshly appended entries are picked up in the same pass.
        for (std::size_t i = 0; i < installers_.size(); ++i)
        {
            if (installers_[i].applied || installers_[i].running || !installers_[i].fn)
            {
                continue;
            }
            // ``running`` guards re-entrancy (an installer that indirectly
            // re-enters the rebuild must not replay itself); ``applied`` is
            // set only AFTER success so a throwing installer is retried by
            // the next rebuild rather than silently stranded.
            installers_[i].running = true;
            try
            {
                // Copy: the stored entry may move if the callback appends.
                const std::function<void()> fn = installers_[i].fn;
                fn();
            }
            catch (...)
            {
                installers_[i].running = false;
                throw;
            }
            installers_[i].running = false;
            installers_[i].applied = true;
        }
    }

    Value OperatorRegistry::evaluate_const(std::string_view name, std::span<const WiringArg> args,
                                           const TSValueTypeMetaData *expected_output,
                                           GlobalStateView global_state) const
    {
        ResolvedOperatorCall resolved = resolve(name, args, std::nullopt, expected_output, {}, global_state);
        if (!resolved.impl->const_kernel)
        {
            throw OperatorResolutionError(fmt::format(
                "operator '{}' resolved to an overload that is not const-evaluable", name));
        }
        const TSValueTypeMetaData *resolved_output =
            resolved.impl->has_output ? ts_pattern_resolve(resolved.impl->output, resolved.map) : nullptr;
        std::vector<std::pair<std::string, WiringArg>> kwargs;
        kwargs.reserve(resolved.kwargs.size());
        for (const auto &[kw_name, port] : resolved.kwargs)
        {
            WiringArg arg;
            arg.kind = WiringArg::Kind::TimeSeries;
            arg.port = port;
            kwargs.emplace_back(kw_name, std::move(arg));
        }
        return resolved.impl->const_kernel(
            resolved_output,
            OperatorCallContext{resolved.args, resolved.impl->params,
                                std::span<const std::pair<std::string, WiringArg>>{kwargs},
                                global_state, nullptr});
    }

    namespace
    {
        [[nodiscard]] bool scalar_pattern_has_var(const ScalarPattern &pattern)
        {
            if (pattern.kind == ScalarPattern::Kind::Var || pattern.schema_var) { return true; }
            for (const ScalarPattern &child : pattern.children)
            {
                if (scalar_pattern_has_var(child)) { return true; }
            }
            return false;
        }

        [[nodiscard]] bool ts_pattern_has_var(const TypePattern &pattern)
        {
            if (pattern.kind == TypePattern::Kind::Var || pattern.schema_var || pattern.size_var)
            {
                return true;
            }
            if (scalar_pattern_has_var(pattern.scalar)) { return true; }
            for (const TypePattern &child : pattern.children)
            {
                if (ts_pattern_has_var(child)) { return true; }
            }
            return false;
        }
    }  // namespace

    bool OperatorRegistry::output_is_selective(std::string_view name) const
    {
        // Can a requested output type influence overload selection? True
        // when any candidate's output carries type variables, or when
        // candidates disagree on a concrete output. False for sinks and for
        // operators whose every candidate shares ONE fixed output - there a
        // bare subscript type can only be an INPUT constraint (to_json[tp]).
        auto it = overloads_.find(std::string{name});
        if (it == overloads_.end() || it->second.empty()) { return true; }
        const TSValueTypeMetaData *shared = nullptr;
        for (const OperatorImpl &impl : it->second)
        {
            if (!impl.has_output) { continue; }
            if (ts_pattern_has_var(impl.output)) { return true; }
            ResolutionMap              empty;
            const TSValueTypeMetaData *meta = ts_pattern_resolve(impl.output, empty);
            if (meta == nullptr) { return true; }
            if (shared == nullptr) { shared = meta; }
            else if (shared != meta) { return true; }
        }
        return false;
    }

    std::optional<OperatorCallableShape> OperatorRegistry::callable_shape(std::string_view name) const
    {
        const auto found = overloads_.find(std::string{name});
        if (found == overloads_.end() || found->second.empty()) { return std::nullopt; }

        std::optional<OperatorCallableShape> result;
        for (const OperatorImpl &impl : found->second)
        {
            OperatorCallableShape candidate;
            candidate.variadic   = impl.variadic;
            candidate.has_output = impl.has_output;
            for (const ParamPattern &parameter : impl.params)
            {
                if (parameter.kind != ParamPattern::Kind::Input)
                {
                    if (!parameter.default_value.has_value()) { return std::nullopt; }
                    continue;
                }
                candidate.parameter_names.push_back(parameter.name);
                ++candidate.arity;
            }
            if (!result.has_value())
            {
                result = std::move(candidate);
                continue;
            }
            if (result->arity != candidate.arity ||
                result->variadic != candidate.variadic ||
                result->has_output != candidate.has_output ||
                result->parameter_names != candidate.parameter_names)
            {
                return std::nullopt;
            }
        }
        return result;
    }

    std::optional<OperatorParameterListShape> OperatorRegistry::parameter_shape(std::string_view name) const
    {
        const auto found = overloads_.find(std::string{name});
        if (found == overloads_.end() || found->second.empty()) { return std::nullopt; }

        std::optional<OperatorParameterListShape> result;
        for (const OperatorImpl &impl : found->second)
        {
            OperatorParameterListShape candidate;
            candidate.variadic = impl.variadic;
            candidate.parameters.reserve(impl.params.size());
            for (const ParamPattern &parameter : impl.params)
            {
                const bool variable = parameter.kind == ParamPattern::Kind::Input
                                          ? ts_pattern_has_var(parameter.ts)
                                          : scalar_pattern_has_var(parameter.scalar);
                const TSValueTypeMetaData *fixed = nullptr;
                if (parameter.kind == ParamPattern::Kind::Input && !variable)
                {
                    ResolutionMap empty;
                    const TSValueTypeMetaData *resolved = ts_pattern_resolve(parameter.ts, empty);
                    // SIGNAL is an input interface accepting any time-series,
                    // not a concrete producer schema suitable for a harness.
                    if (resolved != nullptr && resolved->kind != TSTypeKind::SIGNAL) { fixed = resolved; }
                }
                candidate.parameters.push_back(OperatorParameterShape{
                    .name = parameter.name,
                    .kind = parameter.kind,
                    .type_variable = variable,
                    .fixed_ts = fixed,
                });
            }

            if (!result.has_value())
            {
                result = std::move(candidate);
                continue;
            }
            std::size_t common_size = std::min(result->parameters.size(), candidate.parameters.size());
            std::size_t index = 0;
            for (; index < common_size; ++index)
            {
                OperatorParameterShape       &merged = result->parameters[index];
                const OperatorParameterShape &next = candidate.parameters[index];
                if (merged.name != next.name || merged.kind != next.kind) { break; }
                merged.type_variable = merged.type_variable || next.type_variable;
                if (merged.type_variable || merged.fixed_ts != next.fixed_ts) { merged.fixed_ts = nullptr; }
            }
            common_size = index;
            if (common_size != result->parameters.size() ||
                common_size != candidate.parameters.size() ||
                result->variadic != candidate.variadic)
            {
                result->variadic = false;
            }
            result->parameters.resize(common_size);
        }
        return result;
    }

    std::vector<OperatorOverloadSignature> OperatorRegistry::overload_signatures(std::string_view name) const
    {
        const auto found = overloads_.find(std::string{name});
        if (found == overloads_.end()) { return {}; }

        std::vector<OperatorOverloadSignature> signatures;
        signatures.reserve(found->second.size());
        for (const OperatorImpl &impl : found->second)
        {
            OperatorOverloadSignature signature;
            signature.variadic = impl.variadic;
            signature.has_kwargs = impl.has_kwargs;
            signature.has_output = impl.has_output;
            signature.parameters.reserve(impl.params.size());
            for (const ParamPattern &parameter : impl.params)
            {
                signature.parameters.push_back(OperatorSignatureParameter{
                    .name = parameter.name,
                    .kind = parameter.kind,
                    .type_pattern = parameter.kind == ParamPattern::Kind::Input
                                        ? ts_pattern_to_string(parameter.ts)
                                        : scalar_signature_pattern(parameter.scalar),
                    .has_default = parameter.default_value.has_value(),
                });
            }
            const std::size_t non_variadic_count =
                signature.parameters.size() - (signature.variadic && !signature.parameters.empty() ? 1 : 0);
            signature.positional_params = std::min(impl.positional_params, non_variadic_count);
            if (impl.has_kwargs && impl.has_kwargs_pattern)
            {
                signature.kwargs_pattern = ts_pattern_to_string(impl.kwargs_pattern);
            }
            if (impl.has_output)
            {
                signature.output_pattern = ts_pattern_to_string(impl.output);
            }
            signatures.push_back(std::move(signature));
        }
        return signatures;
    }

    std::vector<std::string> OperatorRegistry::registered_names() const
    {
        std::vector<std::string> names;
        names.reserve(overloads_.size());
        for (const auto &[name, impls] : overloads_) { names.push_back(name); }
        std::sort(names.begin(), names.end());
        return names;
    }

    void OperatorRegistry::reset() noexcept
    {
        overloads_.clear();
        suffix_min_ranks_.clear();
        mesh_scopes_.clear();
        context_scopes_.clear();
        record_replay::reset();   // config + mode scopes (types/record_replay.h)
        // Registration INTENT survives the reset: clear only the applied
        // flags so the next run_installers() replays every installer —
        // extensions exactly as core (RFC 0025 checkpoint 3).
        for (Installer &entry : installers_) { entry.applied = false; }
    }

    void OperatorRegistry::push_context_scope(std::string_view name, WiringPortRef port, const void *wiring)
    {
        if (name.empty()) { throw std::invalid_argument("context scope requires a non-empty name"); }
        context_scopes_.push_back(ContextScopeEntry{std::string{name}, std::move(port), wiring});
    }

    void OperatorRegistry::pop_context_scope() noexcept
    {
        if (!context_scopes_.empty()) { context_scopes_.pop_back(); }
    }

    const OperatorRegistry::ContextScopeEntry *
    OperatorRegistry::resolve_context_scope(std::string_view name) const noexcept
    {
        for (auto it = context_scopes_.rbegin(); it != context_scopes_.rend(); ++it)
        {
            if (it->name == name) { return &*it; }
        }
        return nullptr;
    }

    void OperatorRegistry::push_mesh_scope(const TSValueTypeMetaData *element_schema,
                                           const ValueTypeMetaData *key_type,
                                           std::string name)
    {
        mesh_scopes_.push_back(MeshScope{
            .element_schema = element_schema,
            .key_type       = key_type,
            .name           = std::move(name),
        });
    }

    void OperatorRegistry::pop_mesh_scope() noexcept
    {
        if (!mesh_scopes_.empty()) { mesh_scopes_.pop_back(); }
    }

    const TSValueTypeMetaData *OperatorRegistry::resolve_mesh_scope(std::string_view name) const noexcept
    {
        for (auto it = mesh_scopes_.rbegin(); it != mesh_scopes_.rend(); ++it)
        {
            if (name.empty() || it->name == name) { return it->element_schema; }
        }
        return nullptr;
    }

    const ValueTypeMetaData *OperatorRegistry::resolve_mesh_key_scope(std::string_view name) const noexcept
    {
        for (auto it = mesh_scopes_.rbegin(); it != mesh_scopes_.rend(); ++it)
        {
            if (name.empty() || it->name == name) { return it->key_type; }
        }
        return nullptr;
    }

    namespace
    {
        void collect_size_vars(const TypePattern &pattern, std::vector<std::string> &names)
        {
            if (pattern.size_var && !pattern.size_name.empty() &&
                std::find(names.begin(), names.end(), pattern.size_name) == names.end())
            {
                names.push_back(pattern.size_name);
            }
            for (const TypePattern &child : pattern.children) { collect_size_vars(child, names); }
        }
    }  // namespace

    ResolvedOperatorCall OperatorRegistry::resolve(
        std::string_view name,
        std::span<const WiringArg> args,
        std::optional<bool> output_required,
        const TSValueTypeMetaData *expected_output,
        std::span<const std::size_t> size_hints,
        GlobalStateView global_state,
        Wiring *wiring,
        const ResolutionMap *initial_resolution) const
    {
        const bool diagnostics_enabled =
            wiring != nullptr && wiring->has_wiring_observers();
        WiringResolutionEvent diagnostic{};
        if (diagnostics_enabled)
        {
            diagnostic.path = wiring->current_wiring_path();
            diagnostic.operator_name = name;
            diagnostic.argument_types.reserve(args.size());
            diagnostic.argument_schemas.reserve(args.size());
            for (const WiringArg &arg : args)
            {
                if (arg.kind == WiringArg::Kind::TimeSeries)
                {
                    diagnostic.argument_types.emplace_back(arg.port.schema);
                    diagnostic.argument_schemas.push_back(
                        arg.port.schema != nullptr
                            ? std::string{arg.port.schema->name()}
                            : std::string{"<unwired>"});
                }
                else
                {
                    diagnostic.argument_types.emplace_back(arg.scalar_meta);
                    diagnostic.argument_schemas.push_back(
                        arg.scalar_meta != nullptr
                            ? std::string{arg.scalar_meta->name()}
                            : std::string{"<scalar>"});
                }
            }
        }
        const auto emit_diagnostic = [&] {
            if (diagnostics_enabled) { wiring->notify_overload_resolution(diagnostic); }
        };
        const auto candidate_source = [](const OperatorImpl &impl) {
            return impl.source == OperatorImpl::Source::Cpp
                       ? WiringCandidateSource::Cpp
                       : WiringCandidateSource::Python;
        };

        auto it = overloads_.find(std::string{name});
        if (it == overloads_.end() || it->second.empty())
        {
            std::string message = fmt::format("no operator '{}' is registered", name);
            if (diagnostics_enabled)
            {
                diagnostic.error = message;
                emit_diagnostic();
            }
            throw OperatorResolutionError(std::move(message));
        }

        struct Survivor
        {
            const OperatorImpl *impl;
            ResolutionMap       map;
            NormalizedCall      call;
            int                 rank{0};
        };

        std::vector<Survivor>    survivors;
        std::vector<std::string> rejected;
        bool                     any_requires_rejected = false;
        int                      best_rank = std::numeric_limits<int>::max();
        const auto suffix_it = suffix_min_ranks_.find(std::string{name});
        const std::vector<int> *suffix = suffix_it != suffix_min_ranks_.end()
                                             ? &suffix_it->second
                                             : nullptr;
        const auto consider = [&](const OperatorImpl &impl, std::string *why,
                                  int &effective_rank) -> std::optional<Survivor> {
            NormalizedCall call;
            effective_rank = impl.rank;
            if (!normalize_call(impl, args, call, why)) { return std::nullopt; }

            ResolutionMap map = initial_resolution != nullptr
                                    ? *initial_resolution
                                    : ResolutionMap{};
            // Caller-pinned SIZE variables (op[SIZE: Size[4]]): bind the
            // impl's size vars positionally from the hints.
            if (!size_hints.empty())
            {
                std::vector<std::string> size_names;
                for (const ParamPattern &param : impl.params)
                {
                    if (param.kind == ParamPattern::Kind::Input) { collect_size_vars(param.ts, size_names); }
                }
                if (impl.has_output) { collect_size_vars(impl.output, size_names); }
                for (std::size_t index = 0; index < size_names.size() && index < size_hints.size(); ++index)
                {
                    map.bind_size(size_names[index], size_hints[index]);
                }
            }
            // Each default an overload falls back on makes it a little less
            // specific than one whose parameters were all supplied.
            int rank_adjustment = call.defaults_used;
            if (try_match(impl, call.args, call.kwargs, output_required, expected_output, map, rank_adjustment, why,
                          global_state, wiring, any_requires_rejected))
            {
                effective_rank = impl.rank + rank_adjustment;
                return Survivor{&impl, std::move(map), std::move(call), effective_rank};
            }
            effective_rank = impl.rank + rank_adjustment;
            return std::nullopt;
        };

        for (std::size_t candidate = 0; candidate < it->second.size(); ++candidate)
        {
            const OperatorImpl &impl = it->second[candidate];
            std::string why;
            int effective_rank = impl.rank;
            auto survivor = consider(
                impl, diagnostics_enabled ? &why : nullptr, effective_rank);
            if (survivor.has_value())
            {
                best_rank = std::min(best_rank, survivor->rank);
                survivors.push_back(std::move(*survivor));
            }
            else if (diagnostics_enabled)
            {
                rejected.push_back(fmt::format("  {} [rank {}]: {}", impl.label, effective_rank, why));
                diagnostic.rejected.push_back(WiringCandidateDiagnostic{
                    .label = impl.label,
                    .rank = effective_rank,
                    .source = candidate_source(impl),
                    .rejection_reason = why,
                });
            }
            if (!diagnostics_enabled && suffix != nullptr &&
                candidate + 1 < suffix->size() &&
                best_rank < (*suffix)[candidate + 1])
            {
                break;
            }
        }

        if (survivors.empty())
        {
            if (!diagnostics_enabled)
            {
                // Successful resolution never needs human-readable rejection
                // strings. Only repeat an all-rejected call to retain the full
                // public diagnostic, keeping the common path allocation-free.
                any_requires_rejected = false;
                for (const OperatorImpl &impl : it->second)
                {
                    std::string why;
                    int effective_rank = impl.rank;
                    auto survivor = consider(impl, &why, effective_rank);
                    if (survivor.has_value())
                    {
                        throw std::logic_error(
                            "operator resolution changed while rendering diagnostics");
                    }
                    rejected.push_back(fmt::format(
                        "  {} [rank {}]: {}", impl.label, effective_rank, why));
                }
            }
            std::string message =
                fmt::format("no matching overload for operator '{}' with {} argument(s)\nrejected candidates:\n{}", name,
                            args.size(), fmt::join(rejected, "\n"));
            if (diagnostics_enabled)
            {
                diagnostic.error = message;
                emit_diagnostic();
            }
            // A structured signal, not an error-text convention: at least one
            // candidate matched types but failed its requires predicate.
            if (any_requires_rejected) { throw OperatorRequirementsError(std::move(message)); }
            throw OperatorResolutionError(std::move(message));
        }

        // Lowest rank wins; a stable sort preserves registration order on ties.
        std::stable_sort(survivors.begin(), survivors.end(),
                         [](const Survivor &a, const Survivor &b) { return a.rank < b.rank; });

        if (survivors.size() > 1 && survivors[0].rank == survivors[1].rank)
        {
            std::vector<std::string> tied;
            for (const Survivor &s : survivors)
            {
                if (s.rank == survivors[0].rank)
                {
                    tied.push_back(fmt::format("  {} [rank {}]", s.impl->label, s.rank));
                    if (diagnostics_enabled)
                    {
                        diagnostic.ambiguous.push_back(WiringCandidateDiagnostic{
                            .label = s.impl->label,
                            .rank = s.rank,
                            .source = candidate_source(*s.impl),
                        });
                    }
                }
            }
            std::string message = fmt::format(
                "ambiguous overloads for operator '{}':\n{}", name,
                fmt::join(tied, "\n"));
            if (diagnostics_enabled)
            {
                diagnostic.error = message;
                emit_diagnostic();
            }
            throw OperatorResolutionError(std::move(message));
        }

        // Materialise the winner's kwargs: ports pass through; plain VALUES
        // lift to const sources (Python's scalar-kwargs rule). Only the
        // winning candidate wires nodes - losers never touch the graph.
        Survivor &winner = survivors[0];
        if (diagnostics_enabled)
        {
            diagnostic.selected = WiringCandidateDiagnostic{
                .label = winner.impl->label,
                .rank = winner.rank,
                .source = candidate_source(*winner.impl),
            };
            emit_diagnostic();
        }
        std::vector<std::pair<std::string, WiringPortRef>> kwargs;
        kwargs.reserve(winner.call.kwargs.size());
        for (auto &[kw_name, kw_arg] : winner.call.kwargs)
        {
            if (kw_arg.kind == WiringArg::Kind::TimeSeries)
            {
                kwargs.emplace_back(kw_name, kw_arg.port);
                continue;
            }
            if (wiring == nullptr)
            {
                throw OperatorResolutionError(fmt::format(
                    "keyword argument '{}' of '{}' is a plain value and no wiring context is "
                    "available to lift it to a const source",
                    kw_name, name));
            }
            WiringArg positional = kw_arg;
            positional.name.clear();   // const takes the value positionally
            ResolvedOperatorCall lifted =
                resolve("const", std::span<const WiringArg>{&positional, 1}, true, nullptr, {}, global_state);
            OperatorWireResult source = lifted.impl->wire(*wiring, lifted.map, lifted.args, lifted.kwargs);
            kwargs.emplace_back(kw_name, source.output.erased());
        }

        return ResolvedOperatorCall{winner.impl, std::move(winner.map), std::move(winner.call.args),
                                    std::move(kwargs)};
    }
}  // namespace hgraph
