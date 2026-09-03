#ifndef HGRAPH_TYPES_WIRED_FN_H
#define HGRAPH_TYPES_WIRED_FN_H

#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/type_pattern.h>
#include <hgraph/types/value/value_view.h>

#include <cstddef>
#include <functional>
#include <array>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <stdexcept>
#include <typeinfo>
#include <utility>
#include <vector>

namespace hgraph
{
    class TSDataMutationView;

    struct LiftedKernel
    {
        using EvalValuesThunk = Value (*)(std::span<const ValueView>);
        using EvalBoundValuesThunk = Value (*)(ValueTypeRef, std::span<const ValueView>);
        using EvalIntoThunk = void (*)(TSDataMutationView &, std::span<const ValueView>);

        const char *name{nullptr};
        const std::type_info *identity{nullptr};
        std::size_t arity{0};

        const TSValueTypeMetaData *(*input_schema_fn)(std::size_t index) = nullptr;
        const TSValueTypeMetaData *(*output_schema_fn)() = nullptr;
        std::span<const std::string_view> (*param_names_fn)() = nullptr;
        EvalValuesThunk eval_values_fn{nullptr};
        EvalBoundValuesThunk eval_bound_values_fn{nullptr};
        EvalIntoThunk eval_into_fn{nullptr};
        Value (*identity_value_fn)() = nullptr;

        bool associative{false};
        bool commutative{false};

        [[nodiscard]] bool valid() const noexcept
        {
            return identity != nullptr && input_schema_fn != nullptr && output_schema_fn != nullptr &&
                   eval_values_fn != nullptr;
        }

        [[nodiscard]] const TSValueTypeMetaData *input_schema(std::size_t index) const
        {
            if (input_schema_fn == nullptr) { throw std::logic_error("LiftedKernel has no input schema thunk"); }
            return input_schema_fn(index);
        }

        [[nodiscard]] const TSValueTypeMetaData *output_schema() const
        {
            if (output_schema_fn == nullptr) { throw std::logic_error("LiftedKernel has no output schema thunk"); }
            return output_schema_fn();
        }

        [[nodiscard]] std::span<const std::string_view> param_names() const
        {
            return param_names_fn != nullptr ? param_names_fn() : std::span<const std::string_view>{};
        }

        [[nodiscard]] bool has_identity() const noexcept { return identity_value_fn != nullptr; }

        [[nodiscard]] Value identity_value() const
        {
            if (identity_value_fn == nullptr) { throw std::logic_error("LiftedKernel has no identity value"); }
            return identity_value_fn();
        }

        [[nodiscard]] Value eval(std::span<const ValueView> args) const
        {
            if (eval_values_fn == nullptr) { throw std::logic_error("LiftedKernel has no eval thunk"); }
            return eval_values_fn(args);
        }

        [[nodiscard]] Value eval(ValueTypeRef output_binding, std::span<const ValueView> args) const
        {
            if (eval_bound_values_fn != nullptr) { return eval_bound_values_fn(output_binding, args); }
            return eval(args);
        }

        void eval_into(TSDataMutationView &destination, std::span<const ValueView> args) const
        {
            if (eval_into_fn == nullptr) { throw std::logic_error("LiftedKernel has no in-place eval thunk"); }
            eval_into_fn(destination, args);
        }
    };

    /**
     * A wirable function value — the C++ analogue of Python's ``func: Callable``
     * argument to the higher-order operators (``reduce`` / ``map_`` / ``switch_``).
     *
     * ``fn<X>()`` erases an operator marker, static node, or sub-graph ``X`` into
     * a small **scalar value**: a stable identity (its ``type_info``) plus a
     * wiring thunk that dispatches ``wire<X>`` over erased ports. Because it is
     * an ordinary registered scalar, it participates fully in operator overload
     * selection — a ``Scalar<"func", WiredFn>`` parameter matches by type, an
     * overload's ``requires_`` can gate on the *value* (identity), and equal
     * functions intern/dedup like any other scalar configuration.
     *
     * Wiring an operator-marker ``X`` through the thunk requires
     * ``operator_dispatch.h`` at the ``fn<X>()`` instantiation point (the same
     * rule as ``wire<X>``).
     */
    /**
     * The WiredFn backend ops table (the codebase's type-erased pattern:
     * a struct of fn-ptrs whose FIRST parameter is the backend context).
     * C++ template erasures (``fn<X>()``) use a per-``X`` table with a null
     * context — all state lives in the instantiation. Runtime backends (the
     * Python bridge's graph callables) share ONE table and carry their
     * state in ``context`` (a stable, registry-owned record). The two kinds
     * coexist as ordinary scalar values.
     */
    struct WiredFnOps
    {
        WiringPortRef (*wire)(const void *context, Wiring &, std::span<const WiringPortRef>){nullptr};
        CompiledSubGraph (*compile)(const void *context, Wiring *parent,
                                    std::span<const TSValueTypeMetaData *const>){nullptr};
        const CompiledSubGraph *(*probe_compile)(
            const void *context, Wiring *parent,
            std::span<const TSValueTypeMetaData *const>){nullptr};
        std::span<const std::string_view> (*param_names)(const void *context){nullptr};
        const TSValueTypeMetaData *(*input_schema)(const void *context, std::size_t index){nullptr};
        std::optional<TypePattern> (*input_pattern)(const void *context, std::size_t index){nullptr};
        const TSValueTypeMetaData *(*output_schema)(const void *context){nullptr};
        std::optional<const TSValueTypeMetaData *> (*cached_output_schema)(
            const void *context,
            std::span<const TSValueTypeMetaData *const> input_schemas){nullptr};
        void (*record_output_schema)(
            const void *context,
            std::span<const TSValueTypeMetaData *const> input_schemas,
            const TSValueTypeMetaData *output_schema){nullptr};
        std::string_view (*diagnostic_label)(const void *context){nullptr};
    };

    struct WiredFn
    {
        const WiredFnOps     *ops{nullptr};
        /**
         * Backend payload — PART OF IDENTITY. Null for C++ template
         * erasures; for runtime callables the interned per-function record
         * (Python ruling: function-object identity), which must outlive
         * every WiredFn value referencing it (registry-owned).
         */
        const void           *context{nullptr};
        const std::type_info *identity{nullptr};
        std::string_view      operator_name{};
        const LiftedKernel   *lifted{nullptr};
        std::size_t           arity{0};
        bool                  variadic{false};   // operator marker with a VarIn tail
        bool                  has_output{false};

        /**
         * The function's statically-known time-series output schema, or ``nullptr``
         * when it is not concretely known ahead of compilation (an operator with a
         * type-var output) or the function has no output. Sub-graphs and nodes carry
         * a concrete output type from their signature; ``mesh_`` uses this to learn
         * its element type before compiling the child (so an in-body ``mesh_(func)[k]``
         * has a type without a self-referential compile).
         */
        [[nodiscard]] const TSValueTypeMetaData *output_schema() const
        {
            return ops != nullptr && ops->output_schema != nullptr ? ops->output_schema(context) : nullptr;
        }

        /** A previously compiled output/sink result for these exact inputs.

            Runtime callables can use this to avoid a second speculative child
            compilation during overload selection. An empty optional means no
            matching compilation has been observed; an engaged null pointer
            records a sink. The input key prevents a generic callable's output
            from being reused for a different input shape. */
        [[nodiscard]] std::optional<const TSValueTypeMetaData *> cached_output_schema(
            std::span<const TSValueTypeMetaData *const> input_schemas) const
        {
            if (!has_output) { return nullptr; }
            if (const auto *declared = output_schema(); declared != nullptr)
            {
                return declared;
            }
            return ops != nullptr && ops->cached_output_schema != nullptr
                       ? ops->cached_output_schema(context, input_schemas)
                       : std::nullopt;
        }

        [[nodiscard]] std::optional<bool> cached_output_mode(
            std::span<const TSValueTypeMetaData *const> input_schemas) const
        {
            const auto cached = cached_output_schema(input_schemas);
            return cached.has_value()
                       ? std::optional<bool>{*cached != nullptr}
                       : std::nullopt;
        }

        /** The statically-known schema of input ``index``, or ``nullptr`` when
            the callable is generic and must be compiled to resolve it. */
        [[nodiscard]] const TSValueTypeMetaData *input_schema(std::size_t index) const
        {
            if (index >= arity) { return nullptr; }
            if (lifted != nullptr) { return lifted->input_schema(index); }
            return ops != nullptr && ops->input_schema != nullptr
                       ? ops->input_schema(context, index)
                       : nullptr;
        }

        /** The declared input type pattern, including generic structure.

            Unlike ``input_schema()``, this distinguishes a whole-time-series
            variable from a structured generic such as ``TS<ScalarVar>``.
            Runtime callables which cannot expose a pattern return nullopt. */
        [[nodiscard]] std::optional<TypePattern> input_pattern(std::size_t index) const
        {
            if (index >= arity) { return std::nullopt; }
            if (lifted != nullptr)
            {
                return TypePattern::concrete(lifted->input_schema(index));
            }
            return ops != nullptr && ops->input_pattern != nullptr
                       ? ops->input_pattern(context, index)
                       : std::nullopt;
        }

        /**
         * The function's time-series parameter names, in order (empty string =
         * unnamed). Sub-graphs name ports via ``NamedPort<"name", S>``, nodes
         * via ``In<"name", …>``, operator markers via their ``In`` selectors.
         * Higher-order operators use these to resolve ``**kwargs`` onto the
         * function's parameters, Python-style.
         */
        [[nodiscard]] std::span<const std::string_view> param_names() const
        {
            if (lifted != nullptr) { return lifted->param_names(); }
            return ops != nullptr && ops->param_names != nullptr ? ops->param_names(context)
                                                                 : std::span<const std::string_view>{};
        }

        [[nodiscard]] std::string_view diagnostic_label() const noexcept
        {
            if (ops != nullptr && ops->diagnostic_label != nullptr)
            {
                return ops->diagnostic_label(context);
            }
            if (!operator_name.empty()) { return operator_name; }
            return identity != nullptr ? std::string_view{identity->name()}
                                       : std::string_view{"<wired-fn>"};
        }

        [[nodiscard]] bool valid() const noexcept
        {
            return ops != nullptr && ops->wire != nullptr && identity != nullptr;
        }

        /** Wire one application **inline** over the given (already-erased) argument ports. */
        [[nodiscard]] WiringPortRef wire(Wiring &w, std::span<const WiringPortRef> args) const
        {
            if (!valid()) { throw std::logic_error("WiredFn::wire called on an empty function value"); }
            return ops->wire(context, w, args);
        }

        /**
         * Compile one application as a **child graph** (``CompiledSubGraph``):
         * boundary args at the supplied runtime schemas, in order. This is what
         * the nested operators (``switch_`` / ``map_``) consume — the same
         * function value either inlines (``wire``) or compiles, the caller
         * chooses.
         */
        [[nodiscard]] CompiledSubGraph compile(std::span<const TSValueTypeMetaData *const> input_schemas) const
        {
            if (ops == nullptr || ops->compile == nullptr)
            {
                throw std::logic_error("WiredFn::compile called on an empty function value");
            }
            return ops->compile(context, nullptr, input_schemas);
        }

        [[nodiscard]] CompiledSubGraph compile(
            Wiring &parent,
            std::span<const TSValueTypeMetaData *const> input_schemas) const
        {
            if (ops == nullptr || ops->compile == nullptr)
            {
                throw std::logic_error("WiredFn::compile called on an empty function value");
            }
            return ops->compile(context, &parent, input_schemas);
        }

        /** Retain a speculative compilation for a following real compile.

            Runtime backends may implement this when child compilation is
            expensive and movable. A null result asks the caller to use the
            ordinary non-retained compile path. */
        [[nodiscard]] const CompiledSubGraph *probe_compile(
            Wiring &parent,
            std::span<const TSValueTypeMetaData *const> input_schemas) const
        {
            return ops != nullptr && ops->probe_compile != nullptr
                       ? ops->probe_compile(context, &parent, input_schemas)
                       : nullptr;
        }

        /**
         * Compile against explicit child-boundary shapes. Unlike the schema-only
         * overload, this preserves a structurally assembled fixed TSB/TSL so a
         * nested branch receives non-peered children instead of inventing a
         * peered root that cannot be bound at runtime.
         */
        [[nodiscard]] CompiledSubGraph compile(std::span<const WiringPortRef> boundary_shapes) const
        {
            return compile_boundary_shapes(nullptr, boundary_shapes);
        }

        [[nodiscard]] CompiledSubGraph compile(
            Wiring &parent, std::span<const WiringPortRef> boundary_shapes) const
        {
            return compile_boundary_shapes(&parent, boundary_shapes);
        }

      private:
        [[nodiscard]] CompiledSubGraph compile_boundary_shapes(
            Wiring *parent, std::span<const WiringPortRef> boundary_shapes) const
        {
            if (!valid()) { throw std::logic_error("WiredFn::compile called on an empty function value"); }

            Wiring child = parent != nullptr ? parent->child_wiring()
                                             : Wiring{WiringKind::SubGraph};
            std::vector<const TSValueTypeMetaData *> schemas;
            schemas.reserve(boundary_shapes.size());
            for (const WiringPortRef &shape : boundary_shapes)
            {
                if (shape.schema == nullptr)
                {
                    throw std::invalid_argument("WiredFn::compile boundary shape must be typed");
                }
                schemas.push_back(shape.schema);
            }

            // A root-only boundary has exactly the shape synthesized by the
            // schema compile backend. Route Python callables through it so a
            // retained output probe can be consumed; structural/null shapes
            // continue through the shape-preserving path below.
            bool canonical_boundary =
                ops->probe_compile != nullptr && output_schema() == nullptr;
            for (std::size_t index = 0;
                 canonical_boundary && index < boundary_shapes.size(); ++index)
            {
                const WiringPortRef &shape = boundary_shapes[index];
                canonical_boundary =
                    shape.is_boundary_source() &&
                    !shape.is_captured_boundary_source() &&
                    shape.boundary_arg_index() == index &&
                    shape.boundary_path().empty();
            }
            if (canonical_boundary)
            {
                return ops->compile(
                    context, parent, {schemas.data(), schemas.size()});
            }

            auto compile = [&] {
                WiringPortRef out = wire(child, boundary_shapes);
                if (ops->record_output_schema != nullptr)
                {
                    ops->record_output_schema(
                        context, {schemas.data(), schemas.size()}, out.schema);
                }
                if (out.schema != nullptr)
                {
                    if (const auto *declared = output_schema(); declared != nullptr)
                    {
                        if (!graph_wiring_detail::input_accepts_output_schema(
                                declared, out.schema))
                        {
                            throw std::invalid_argument(
                                "WiredFn::compile output is incompatible with its declared schema");
                        }
                        out = graph_wiring_detail::adapt_source_for_input(
                            child, declared, std::move(out));
                    }
                    return std::move(child).finish_subgraph(
                        out, std::move(schemas));
                }
                return std::move(child).finish_subgraph(
                    std::nullopt, std::move(schemas));
            };
            if (!child.has_wiring_observers()) { return compile(); }

            const std::string label{diagnostic_label()};
            return child.observe(
                WiringScopeEvent{
                    .kind = WiringScopeKind::NestedGraph,
                    .label = label,
                    .signature = label,
                },
                compile);
        }

      public:

        [[nodiscard]] bool operator==(const WiredFn &other) const noexcept
        {
            if (context != other.context) { return false; }
            if (identity == other.identity) { return true; }
            if (identity == nullptr || other.identity == nullptr) { return false; }
            return *identity == *other.identity;
        }
    };

#if HGRAPH_ENABLE_PYTHON_USER_NODES
    /** Python conversion is installed by the optional bridge. Keeping the
        hook on the scalar's ops preserves ordinary type-erased conversion
        for requirements/resolvers without adding a WiredFn case there. */
    template <>
    struct python_conversion_traits<WiredFn>
    {
        using ToPythonHook   = nanobind::object (*)(const WiredFn &);
        using FromPythonHook = WiredFn (*)(nanobind::handle);

        [[nodiscard]] HGRAPH_EXPORT static ToPythonHook &to_python_hook() noexcept;
        [[nodiscard]] HGRAPH_EXPORT static FromPythonHook &from_python_hook() noexcept;
        HGRAPH_EXPORT static nanobind::object to_python(const WiredFn &value);
        HGRAPH_EXPORT static WiredFn from_python(nanobind::handle source);
    };
#endif

    template <typename T>
    struct WiredFnArgBinding
    {
        std::vector<T> ordered{};
        bool           takes_leading_key{false};
    };

    /**
     * Resolve positional + keyword time-series arguments onto a ``WiredFn``'s
     * parameter order. Higher-order operators use this after their own operator
     * call has been normalised and unmatched keywords have collected into
     * ``**kwargs``.
     *
     * Key detection is **name-based** (the Python rule): ``func`` consumes the
     * operator-supplied key iff its FIRST parameter is named ``key_arg``
     * (``"key"`` for keyed forms, ``"ndx"`` for indexed ones; overridable via
     * the operator's ``__key_arg__``; empty = never). The arity must then be
     * one more than the supplied arguments — a named key parameter with a
     * mismatched arity is an error rather than a silent re-interpretation.
     */
    template <typename T>
    [[nodiscard]] WiredFnArgBinding<T> bind_wired_fn_args(std::string_view op_name,
                                                          const WiredFn &func,
                                                          std::span<const T> positional,
                                                          std::span<const std::pair<std::string, T>> named,
                                                          std::string_view key_arg = "key")
    {
        const std::size_t filled = positional.size() + named.size();

        WiredFnArgBinding<T> result;
        const auto fn_names      = func.param_names();
        result.takes_leading_key = !key_arg.empty() && !fn_names.empty() && fn_names[0] == key_arg;
        if (result.takes_leading_key && func.arity != filled + 1)
        {
            throw std::invalid_argument(std::string{op_name} + ": 'func' names its first parameter '" +
                                        std::string{key_arg} +
                                        "' (the key) but its arity does not leave room for it");
        }
        // A VarIn-tailed operator takes any argument count at or above its
        // fixed inputs (runtime-matcher capability).
        if (!result.takes_leading_key && (func.variadic ? filled < func.arity : func.arity != filled))
        {
            throw std::invalid_argument(
                std::string{op_name} + ": 'func' takes a different number of time-series arguments (name the "
                                       "first parameter '" +
                std::string{key_arg.empty() ? std::string_view{"key"} : key_arg} +
                "' to consume the operator key)");
        }

        const std::size_t offset = result.takes_leading_key ? 1 : 0;
        std::vector<std::optional<T>> slots(filled);
        for (std::size_t i = 0; i < positional.size(); ++i) { slots[i] = positional[i]; }

        const auto names = func.param_names();
        for (const auto &[name, value] : named)
        {
            std::size_t found = filled;
            for (std::size_t j = 0; j < filled; ++j)
            {
                const std::size_t p = offset + j;
                if (p < names.size() && !names[p].empty() && names[p] == name)
                {
                    found = j;
                    break;
                }
            }
            if (found == filled)
            {
                throw std::invalid_argument(std::string{op_name} + ": 'func' has no parameter named '" + name +
                                            "' (name ports with NamedPort / In<\"name\">)");
            }
            if (slots[found].has_value())
            {
                throw std::invalid_argument(std::string{op_name} + ": got multiple values for 'func' parameter '" +
                                            name + "'");
            }
            slots[found] = value;
        }

        result.ordered.reserve(filled);
        for (auto &slot : slots)
        {
            if (!slot.has_value())
            {
                throw std::invalid_argument(std::string{op_name} + ": a 'func' parameter was not supplied");
            }
            result.ordered.push_back(std::move(*slot));
        }
        return result;
    }

    namespace static_schema_detail
    {
        template <>
        struct scalar_name<WiredFn>
        {
            static constexpr std::string_view value{"fn"};
        };
    }  // namespace static_schema_detail

    template <fixed_string Name, typename S>
    struct VarIn;   // operator_dispatch.h

    /** Erased variadic-operator dispatch (defined in operator_dispatch.h -
        wired_fn.h precedes the registry in the include order). */
    [[nodiscard]] WiringPortRef wire_erased_operator(Wiring &w, std::string_view name,
                                                     std::span<const WiringPortRef> args, bool has_output,
                                                     const TSValueTypeMetaData *expected_output = nullptr);

    namespace wired_fn_detail
    {
        template <typename T> struct is_var_input_selector : std::false_type {};
        template <fixed_string N, typename S> struct is_var_input_selector<VarIn<N, S>> : std::true_type {};

        /** Operator markers with a VarIn tail accept ANY argument count at or
            above the fixed inputs (runtime-matcher capability). */
        template <typename X>
        [[nodiscard]] consteval bool variadic_of()
        {
            if constexpr (std::is_base_of_v<operator_tag, X>)
            {
                using params = typename X::param_types;
                return []<std::size_t... I>(std::index_sequence<I...>) {
                    return (false || ... ||
                            is_var_input_selector<std::tuple_element_t<I, params>>::value);
                }(std::make_index_sequence<std::tuple_size_v<params>>{});
            }
            else { return false; }
        }

        template <typename X>
        [[nodiscard]] consteval std::size_t arity_of()
        {
            if constexpr (std::is_base_of_v<operator_tag, X>)
            {
                using params = typename X::param_types;
                return []<std::size_t... I>(std::index_sequence<I...>) {
                    return (std::size_t{0} + ... +
                            (static_node_detail::is_input_selector<std::tuple_element_t<I, params>>::value
                                 ? std::size_t{1}
                                 : std::size_t{0}));
                }(std::make_index_sequence<std::tuple_size_v<params>>{});
            }
            else if constexpr (graph_wiring_detail::is_graph_def<X>)
            {
                static_assert(StaticGraphSignature<X>::scalar_count() == 0,
                              "fn<X>: a wirable sub-graph must take time-series inputs only");
                return StaticGraphSignature<X>::input_count();
            }
            else
            {
                static_assert(StaticNodeSignature<X>::scalar_count() == 0,
                              "fn<X>: a wirable node must take time-series inputs only");
                return StaticNodeSignature<X>::input_count();
            }
        }

        template <typename X>
        [[nodiscard]] consteval bool has_output_of()
        {
            if constexpr (std::is_base_of_v<operator_tag, X>) { return X::has_output; }
            else if constexpr (graph_wiring_detail::is_graph_def<X>)
            {
                return !std::is_void_v<typename StaticGraphSignature<X>::output_type>;
            }
            else { return StaticNodeSignature<X>::has_output(); }
        }

        template <typename X>
        [[nodiscard]] WiringPortRef wire_thunk(Wiring &w, std::span<const WiringPortRef> args)
        {
            constexpr std::size_t arity = arity_of<X>();
            if constexpr (variadic_of<X>())
            {
                // A VarIn-tailed operator takes ANY argument count: dispatch
                // through the erased registry entry (runtime matcher).
                if (args.size() < arity)
                {
                    throw std::invalid_argument("fn<X>: wired argument count does not match the function's inputs");
                }
                return wire_erased_operator(w, std::string_view{X::name}, args, X::has_output);
            }
            else
            {
                if (args.size() != arity)
                {
                    throw std::invalid_argument("fn<X>: wired argument count does not match the function's inputs");
                }
                return [&]<std::size_t... I>(std::index_sequence<I...>) -> WiringPortRef {
                    if constexpr (has_output_of<X>())
                    {
                        auto out = wire<X>(w, Port<void>{w, args[I]}...);
                        return out.erased();
                    }
                    else
                    {
                        wire<X>(w, Port<void>{w, args[I]}...);
                        return {};
                    }
                }(std::make_index_sequence<arity>{});
            }
        }

        // Compile one application of X as a child graph: a fresh Wiring whose
        // arguments are boundary placeholders at the supplied runtime schemas,
        // wired through the same wire<X> dispatch (node / operator / sub-graph),
        // then converted to binding specs by finish_subgraph.
        template <typename X>
        [[nodiscard]] CompiledSubGraph compile_thunk(
            Wiring *parent,
            std::span<const TSValueTypeMetaData *const> input_schemas)
        {
            constexpr std::size_t arity = arity_of<X>();
            if constexpr (variadic_of<X>())
            {
                if (input_schemas.size() < arity)
                {
                    throw std::invalid_argument(
                        "fn<X>: compiled input schema count does not match the function's inputs");
                }
                Wiring w = parent != nullptr ? parent->child_wiring()
                                             : Wiring{WiringKind::SubGraph};
                std::vector<const TSValueTypeMetaData *> schemas{input_schemas.begin(), input_schemas.end()};
                std::vector<WiringPortRef>               ports;
                ports.reserve(input_schemas.size());
                for (std::size_t index = 0; index < input_schemas.size(); ++index)
                {
                    ports.push_back(WiringPortRef::boundary_source(index, {}, input_schemas[index]));
                }
                auto compile = [&] {
                    auto out = wire_thunk<X>(w, {ports.data(), ports.size()});
                    if constexpr (has_output_of<X>())
                    {
                        return std::move(w).finish_subgraph(out, std::move(schemas));
                    }
                    else
                    {
                        return std::move(w).finish_subgraph(std::nullopt,
                                                            std::move(schemas));
                    }
                };
                if constexpr (graph_wiring_detail::is_graph_def<X>)
                {
                    return compile();
                }
                else
                {
                    if (!w.has_wiring_observers()) { return compile(); }
                    const std::string label = static_node_detail::diagnostic_name<X>();
                    return w.observe(
                        WiringScopeEvent{
                            .kind = WiringScopeKind::NestedGraph,
                            .label = label,
                            .signature = label,
                        },
                        compile);
                }
            }
            if (input_schemas.size() != arity)
            {
                throw std::invalid_argument("fn<X>: compiled input schema count does not match the function's inputs");
            }

            Wiring w = parent != nullptr ? parent->child_wiring()
                                         : Wiring{WiringKind::SubGraph};
            std::vector<const TSValueTypeMetaData *> schemas{input_schemas.begin(), input_schemas.end()};
            auto compile = [&] {
                return [&]<std::size_t... I>(std::index_sequence<I...>) -> CompiledSubGraph {
                    if constexpr (has_output_of<X>())
                    {
                        auto out = wire<X>(
                            w, Port<void>{w, WiringPortRef::boundary_source(
                                                   I, {}, input_schemas[I])}...);
                        return std::move(w).finish_subgraph(
                            out.erased(), std::move(schemas));
                    }
                    else
                    {
                        wire<X>(w, Port<void>{w, WiringPortRef::boundary_source(
                                                   I, {}, input_schemas[I])}...);
                        return std::move(w).finish_subgraph(
                            std::nullopt, std::move(schemas));
                    }
                }(std::make_index_sequence<arity>{});
            };
            if constexpr (graph_wiring_detail::is_graph_def<X>)
            {
                return compile();
            }
            else
            {
                if (!w.has_wiring_observers()) { return compile(); }
                const std::string label = static_node_detail::diagnostic_name<X>();
                return w.observe(
                    WiringScopeEvent{
                        .kind = WiringScopeKind::NestedGraph,
                        .label = label,
                        .signature = label,
                    },
                    compile);
            }
        }
        template <typename X>
        [[nodiscard]] std::span<const std::string_view> param_names_thunk()
        {
            static const auto names = [] {
                std::array<std::string_view, arity_of<X>()> out{};
                std::size_t                                 next = 0;
                if constexpr (std::is_base_of_v<operator_tag, X>)
                {
                    using params = typename X::param_types;
                    [&]<std::size_t... I>(std::index_sequence<I...>) {
                        (
                            [&] {
                                using P = std::tuple_element_t<I, params>;
                                if constexpr (static_node_detail::is_input_selector<P>::value)
                                {
                                    out[next++] = P::field_name.sv();
                                }
                            }(),
                            ...);
                    }(std::make_index_sequence<std::tuple_size_v<params>>{});
                }
                else if constexpr (graph_wiring_detail::is_graph_def<X>)
                {
                    using params = typename StaticGraphSignature<X>::param_types;
                    [&]<std::size_t... I>(std::index_sequence<I...>) {
                        (
                            [&] {
                                using P = std::tuple_element_t<I, params>;
                                if constexpr (graph_wiring_detail::is_named_port<P>::value)
                                {
                                    out[next++] = P::field_name.sv();
                                }
                                else if constexpr (graph_wiring_detail::is_port<P>::value)
                                {
                                    out[next++] = std::string_view{};
                                }
                            }(),
                            ...);
                    }(std::make_index_sequence<std::tuple_size_v<params>>{});
                }
                else
                {
                    using params = typename StaticNodeSignature<X>::wire_param_types;
                    [&]<std::size_t... I>(std::index_sequence<I...>) {
                        (
                            [&] {
                                using P = std::tuple_element_t<I, params>;
                                if constexpr (static_node_detail::is_input_selector<P>::value)
                                {
                                    out[next++] = P::field_name.sv();
                                }
                            }(),
                            ...);
                    }(std::make_index_sequence<std::tuple_size_v<params>>{});
                }
                return out;
            }();
            return {names.data(), names.size()};
        }

        // The statically-known output schema of X: concrete for sub-graphs (the
        // compose return Port's schema) and nodes (the Out<S> selector); nullptr for
        // operators (a type-var output is only known after resolution) and for X
        // without an output.
        template <typename X>
        [[nodiscard]] const TSValueTypeMetaData *input_schema_thunk(std::size_t index)
        {
            // Schema descriptors are registry-owned. Recompute the interned
            // pointers after a registry reset instead of retaining dangling
            // pointers in function-static storage.
            const auto schemas = [] {
                std::array<const TSValueTypeMetaData *, arity_of<X>()> out{};
                if constexpr (graph_wiring_detail::is_graph_def<X>)
                {
                    using params = typename StaticGraphSignature<X>::param_types;
                    std::size_t next = 0;
                    [&]<std::size_t... I>(std::index_sequence<I...>) {
                        (
                            [&] {
                                using P = std::tuple_element_t<I, params>;
                                if constexpr (graph_wiring_detail::is_port<P>::value)
                                {
                                    using S = typename graph_wiring_detail::port_static_schema<P>::type;
                                    if constexpr (!std::is_void_v<S>)
                                    {
                                        if constexpr (schema_descriptor<S>::is_concrete())
                                        {
                                            out[next] = schema_descriptor<S>::ts_meta();
                                        }
                                    }
                                    ++next;
                                }
                            }(),
                            ...);
                    }(std::make_index_sequence<std::tuple_size_v<params>>{});
                }
                else if constexpr (!std::is_base_of_v<operator_tag, X>)
                {
                    using inputs = typename StaticNodeSignature<X>::input_schema_types;
                    [&]<std::size_t... I>(std::index_sequence<I...>) {
                        (
                            [&] {
                                using S = std::tuple_element_t<I, inputs>;
                                if constexpr (schema_descriptor<S>::is_concrete())
                                {
                                    out[I] = schema_descriptor<S>::ts_meta();
                                }
                            }(),
                            ...);
                    }(std::make_index_sequence<std::tuple_size_v<inputs>>{});
                }
                return out;
            }();
            return index < schemas.size() ? schemas[index] : nullptr;
        }

        template <typename X>
        [[nodiscard]] std::optional<TypePattern> input_pattern_thunk(std::size_t index)
        {
            std::optional<TypePattern> out;
            if constexpr (std::is_base_of_v<operator_tag, X>)
            {
                using params = typename X::param_types;
                std::size_t next = 0;
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    (
                        [&] {
                            using P = std::tuple_element_t<I, params>;
                            if constexpr (static_node_detail::is_input_selector<P>::value)
                            {
                                if (next == index) { out = to_pattern<typename P::schema>(); }
                                ++next;
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<std::tuple_size_v<params>>{});
            }
            else if constexpr (graph_wiring_detail::is_graph_def<X>)
            {
                using params = typename StaticGraphSignature<X>::param_types;
                std::size_t next = 0;
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    (
                        [&] {
                            using P = std::tuple_element_t<I, params>;
                            if constexpr (graph_wiring_detail::is_port<P>::value)
                            {
                                if (next == index)
                                {
                                    using S = typename graph_wiring_detail::port_static_schema<P>::type;
                                    if constexpr (std::is_void_v<S>)
                                    {
                                        out = TypePattern::var(
                                            std::string{"__erased_input_"} + std::to_string(index));
                                    }
                                    else { out = to_pattern<S>(); }
                                }
                                ++next;
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<std::tuple_size_v<params>>{});
            }
            else
            {
                using inputs = typename StaticNodeSignature<X>::input_schema_types;
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    ((index == I ? void(out = to_pattern<std::tuple_element_t<I, inputs>>()) : void()), ...);
                }(std::make_index_sequence<std::tuple_size_v<inputs>>{});
            }
            return out;
        }

        template <typename X>
        [[nodiscard]] const TSValueTypeMetaData *output_schema_thunk()
        {
            if constexpr (!has_output_of<X>()) { return nullptr; }
            else if constexpr (std::is_base_of_v<operator_tag, X>) { return nullptr; }
            else if constexpr (graph_wiring_detail::is_graph_def<X>)
            {
                using OutS = typename graph_wiring_detail::port_static_schema<
                    typename StaticGraphSignature<X>::output_type>::type;
                if constexpr (std::is_void_v<OutS>) { return nullptr; }
                else { return schema_descriptor<OutS>::ts_meta(); }
            }
            else { return StaticNodeSignature<X>::output_schema(); }
        }

        template <typename X>
        constexpr void mark_name_used() noexcept
        {
            if constexpr (static_node_detail::has_name<X>) { (void)X::name; }
        }
    }  // namespace wired_fn_detail

    namespace wired_fn_detail
    {
        /** The per-``X`` ops table: context-ignoring wrappers over the template thunks. */
        template <typename X>
        [[nodiscard]] const WiredFnOps &ops_for()
        {
            static constexpr WiredFnOps ops{
                [](const void *, Wiring &w, std::span<const WiringPortRef> args) { return wire_thunk<X>(w, args); },
                [](const void *, Wiring *parent,
                   std::span<const TSValueTypeMetaData *const> schemas) {
                    return compile_thunk<X>(parent, schemas);
                },
                nullptr,
                [](const void *) { return param_names_thunk<X>(); },
                [](const void *, std::size_t index) { return input_schema_thunk<X>(index); },
                [](const void *, std::size_t index) { return input_pattern_thunk<X>(index); },
                [](const void *) { return output_schema_thunk<X>(); },
                nullptr,
                nullptr,
                [](const void *) -> std::string_view {
                    if constexpr (static_node_detail::has_name<X>) {
                        return static_node_detail::name_view<X>();
                    }
                    else { return typeid(X).name(); }
                },
            };
            return ops;
        }
    }  // namespace wired_fn_detail

    /** Erase the operator marker / node / sub-graph ``X`` into a ``WiredFn`` value. */
    template <typename X>
    [[nodiscard]] WiredFn fn()
    {
        wired_fn_detail::mark_name_used<X>();
        return WiredFn{
            .ops            = &wired_fn_detail::ops_for<X>(),
            .identity       = &typeid(X),
            .operator_name  = [] {
                if constexpr (std::is_base_of_v<operator_tag, X>) { return std::string_view{X::name}; }
                else { return std::string_view{}; }
            }(),
            .arity          = wired_fn_detail::arity_of<X>(),
            .variadic       = wired_fn_detail::variadic_of<X>(),
            .has_output     = wired_fn_detail::has_output_of<X>(),
        };
    }
}  // namespace hgraph

template <>
struct std::hash<hgraph::WiredFn>
{
    [[nodiscard]] std::size_t operator()(const hgraph::WiredFn &fn) const noexcept
    {
        const std::size_t base = fn.identity != nullptr ? fn.identity->hash_code() : 0;
        return base ^ (std::hash<const void *>{}(fn.context) << 1);
    }
};

#endif  // HGRAPH_TYPES_WIRED_FN_H
