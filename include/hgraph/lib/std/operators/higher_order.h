#ifndef HGRAPH_LIB_STD_OPERATORS_HIGHER_ORDER_H
#define HGRAPH_LIB_STD_OPERATORS_HIGHER_ORDER_H

#include <hgraph/runtime/node_error.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/wired_fn.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <optional>
#include <utility>
#include <vector>

namespace hgraph::stdlib
{
    /**
     * Higher-order operator **definitions** (markers only), mirroring the
     * ``release/0.5`` Python direction where higher-order constructs are
     * ``@operator``s whose default implementations are ordinary
     * registered overloads. The wirable-function argument is the ``WiredFn``
     * scalar (``fn<X>()``), so overload selection — including user
     * specialisations gated by ``requires_`` on the function's identity — runs
     * through the standard registry best-match machinery. There is nothing
     * special about these operators now that sub-graph compilation is
     * standardised (``compile_subgraph`` / ``nested_``); their default overloads
     * live in ``impl/higher_order_impl.h``.
     */

    /**
     * ``reduce`` — reduce a time-series **collection** into a single
     * time-series with the (associative) combiner ``func``. Mirrors Python
     * ``reduce(func, ts[, zero], is_associative=true)``. For associative
     * reduction, ``zero`` has deliberately narrow semantics:
     *
     * - no live values: publish ``zero`` when supplied, otherwise remain invalid;
     * - one live value: return that value directly when ``zero`` is omitted, or
     *   evaluate ``func(value, zero)`` when it is supplied;
     * - two or more live values: reduce only those values; ``zero`` is not an
     *   operand.
     *
     * - ``ts`` — any multiplexed collection (``TSL`` / ``TSD``; ``TSS`` once
     *   the Python reference grows one). Each collection kind is its own
     *   registered overload of this name, selected by pattern rank —
     *   implemented today: fixed/dynamic ``TSL`` and ``TSD``.
     * - ``zero`` — **optional**, modelled as arity overloads (like ``const``):
     *   when supplied as a scalar, the value is wired as ``const(zero)`` at
     *   the element schema. Unset ``TSL`` slots are not live values.
     * - ``is_associative=false`` — select an ordered left fold. A fixed TSL
     *   folds statically; a contiguous ``TSD[int, E]`` uses the live ``zero``
     *   input as its initial accumulator and permits an accumulator type that
     *   differs from ``E``.
     *
     * @param func Binary graph combining two values into one accumulator.
     * @param ts Collection whose live elements are reduced.
     * @param zero Optional empty-collection result. In associative mode it is
     *             not injected into reductions containing two or more live values.
     * @param is_associative True for tree reduction; false for deterministic ordered left fold.
     * @return The current reduction of the live collection elements.
     * @par Python example
     * @code{.py}
     * total = hg.reduce(hg.add_, values, zero=0)
     * @endcode
     */
    struct reduce_ : Operator<"reduce",
                              Scalar<"func", WiredFn>,
                              In<"ts", TsVar<"C">>,           // the collection (TSL / TSD / TSS ...)
                              Scalar<"zero", ScalarVar<"Z">>, // optional arity; never inferred
                              Scalar<"is_associative", Bool>,
                              Out<TsVar<"V">>>
    {
    };

    /**
     * The ``switch_`` case table: key value -> wirable branch, with an optional
     * default branch. An ordinary registered scalar, so it participates in
     * interning and overload selection like any other configuration value.
     *
     * A branch may consume the switch key as its **first** argument when that
     * parameter is named ``key``. Branches may also be pure sources (arity zero
     * with no ts args).
     */
    struct SwitchCase
    {
        Value   key{};
        WiredFn branch{};

        [[nodiscard]] bool operator==(const SwitchCase &other) const
        {
            if (!(branch == other.branch)) { return false; }
            if (key.has_value() != other.key.has_value()) { return false; }
            return !key.has_value() || key.equals(other.key);
        }
    };

    struct SwitchCases
    {
        std::vector<SwitchCase> cases{};
        std::optional<WiredFn>  default_branch{};
        /** Rebuild the active branch on EVERY key tick, not just a key change (Python ``reload_on_ticked``). */
        bool reload_on_ticked{false};

        [[nodiscard]] SwitchCases &&reload(bool value = true) &&
        {
            reload_on_ticked = value;
            return std::move(*this);
        }

        [[nodiscard]] bool operator==(const SwitchCases &other) const
        {
            return cases == other.cases && default_branch == other.default_branch &&
                   reload_on_ticked == other.reload_on_ticked;
        }
    };

    [[nodiscard]] inline SwitchCases switch_cases(std::initializer_list<SwitchCase> entries)
    {
        return SwitchCases{.cases = std::vector<SwitchCase>{entries}};
    }

    [[nodiscard]] inline SwitchCases switch_cases(std::initializer_list<SwitchCase> entries, WiredFn default_branch)
    {
        return SwitchCases{.cases = std::vector<SwitchCase>{entries}, .default_branch = default_branch};
    }

    /**
     * ``switch_`` — route through **one** child graph at a time, selected by
     * ``key``. Mirrors Python ``switch_(key, cases, *ts)``:
     *
     * - on a key change the active branch is stopped and destroyed, the branch
     *   for the new key (else the default, else a runtime error) is built,
     *   bound and started, and the output **re-points** — sampling the new
     *   branch at the switch time (the sampled-runtime contract; a deliberate
     *   divergence from Python's ``value = None`` reset);
     * - a branch may take the key as its first argument when that parameter is
     *   named ``key``;
     * - the time-series arguments are variadic (``*ts``): branches bind them
     *   positionally, optionally preceded by the key.
     *
     * @param key Live branch selector.
     * @param cases Mapping from selector values to graph callables; ``DEFAULT`` supplies fallback.
     * @param ts Positional inputs forwarded to the selected branch.
     * @param kwargs Named inputs forwarded to the selected branch.
     * @param reload_on_ticked Rebuild the branch on every key tick even when its value is unchanged.
     * @return The active child graph's output, repointed when the branch changes.
     * @par Python example
     * @code{.py}
     * selected = hg.switch_(mode, {"live": live_graph, "cached": cached_graph}, source)
     * @endcode
     */
    struct switch_ : Operator<"switch_",
                              In<"key", TS<ScalarVar<"K">>>,
                              Scalar<"cases", SwitchCases>,
                              VarIn<"ts", TsVar<"TS">>,       // *ts — branches bind these positionally
                              VarKwIn<"kwargs">,              // **kwargs — resolved per branch by name
                              Out<TsVar<"O">>>
    {
    };

    /**
     * The outputless form of ``switch_``. Both markers resolve through the
     * same operator registry name; this marker makes the output contract
     * explicit to C++'s static ``wire<>`` return type.
     */
    struct switch_sink_ : Operator<"switch_",
                                   In<"key", TS<ScalarVar<"K">>>,
                                   Scalar<"cases", SwitchCases>,
                                   VarIn<"ts", TsVar<"TS">>,
                                   VarKwIn<"kwargs">>
    {
    };

    /** One closed-union runtime-dispatch branch.

        ``types`` has one named Bundle schema for every selected dispatch
        argument. A concrete runtime leaf matches when it derives from the
        corresponding schema in every position. */
    struct DispatchCase
    {
        std::vector<const ValueTypeMetaData *> types{};
        WiredFn                                branch{};

        [[nodiscard]] bool operator==(const DispatchCase &other) const
        {
            return types == other.types && branch == other.branch;
        }
    };

    /**
     * Closed case table for native C++ ``dispatch_``.
     *
     * ``dispatch_args`` contains indexes into the flattened time-series call
     * arguments (positional arguments first, followed by named arguments in
     * call order). It defaults to the first argument. All Bundle descendants
     * must be registered before this value is wired so the selector can freeze
     * an allocation-free lookup table for the graph.
     */
    struct DispatchCases
    {
        std::vector<DispatchCase> cases{};
        std::vector<std::size_t>  dispatch_args{0};
        std::optional<WiredFn>    default_branch{};

        [[nodiscard]] DispatchCases &&on(std::initializer_list<std::size_t> indexes) &&
        {
            dispatch_args.assign(indexes.begin(), indexes.end());
            return std::move(*this);
        }

        [[nodiscard]] bool operator==(const DispatchCases &other) const
        {
            return cases == other.cases && dispatch_args == other.dispatch_args &&
                   default_branch == other.default_branch;
        }
    };

    [[nodiscard]] inline DispatchCase dispatch_case(
        std::initializer_list<const ValueTypeMetaData *> types, WiredFn branch)
    {
        return DispatchCase{
            .types = std::vector<const ValueTypeMetaData *>{types},
            .branch = branch,
        };
    }

    [[nodiscard]] inline DispatchCase dispatch_case(
        const ValueTypeMetaData *type, WiredFn branch)
    {
        return dispatch_case({type}, branch);
    }

    [[nodiscard]] inline DispatchCases dispatch_cases(std::initializer_list<DispatchCase> entries)
    {
        return DispatchCases{.cases = std::vector<DispatchCase>{entries}};
    }

    [[nodiscard]] inline DispatchCases dispatch_cases(std::initializer_list<DispatchCase> entries,
                                                       WiredFn default_branch)
    {
        return DispatchCases{
            .cases = std::vector<DispatchCase>{entries},
            .default_branch = default_branch,
        };
    }

    /**
     * ``dispatch_`` — select a child graph from the active concrete Bundle
     * leaf types of one or more ``TS[Bundle]`` arguments. The small native
     * selector feeds the existing ``switch_`` runtime; branch arguments are
     * checked-downcast to their declared case types inside the child graph.
     * @param overloaded Operator or dispatch function whose registered implementations form the cases.
     * @param ts Runtime values whose concrete types select the implementation.
     * @param kwargs Named runtime values forwarded to the implementation.
     * @param __on__ Optional names restricting which typed parameters participate in dispatch.
     * @return The output of the implementation selected from the current concrete types.
     * @par Python example
     * @code{.py}
     * result = hg.dispatch_(price_operator, instrument, market)
     * @endcode
     */
    struct dispatch_ : Operator<"dispatch_",
                                Scalar<"cases", DispatchCases>,
                                VarIn<"ts", TsVar<"TS">>,
                                VarKwIn<"kwargs">,
                                Out<TsVar<"O">>>
    {
    };

    /**
     * ``try_except(func, *args, **kwargs)`` — compile ``func`` as one protected
     * child graph. A value-producing function returns
     * ``TSB[{exception: TS<NodeError>, out: O}]``; a sink returns the bare
     * ``TS<NodeError>``. The output schema is derived from the compiled
     * ``WiredFn`` by the default overload.
     */
    struct TryExceptCallConfig
    {
        WiredFn             func{};
        ErrorCaptureOptions error_capture{};

        [[nodiscard]] bool operator==(const TryExceptCallConfig &) const noexcept = default;
    };

    /** Run one child graph behind an exception boundary.
        Value-producing graphs return a bundle with ``out`` and ``exception`` fields;
        sink graphs return the exception stream alone.
        @param func Graph callable to protect.
        @param args Positional time-series inputs forwarded to ``func``.
        @param kwargs Named time-series inputs forwarded to ``func``.
        @param __trace_back_depth__ Maximum captured graph traceback depth.
        @param __capture_values__ Include current input values in captured node errors when true.
        @return Protected output together with any captured ``NodeError``.
        @par Python example
        @code{.py}
        attempted = hg.try_except(parse_message, payload)
        @endcode */
    struct try_except : Operator<"try_except",
                                 Scalar<"func", WiredFn>,
                                 VarIn<"args", TsVar<"A">>,
                                 Scalar<"__trace_back_depth__", Int>,
                                 Scalar<"__capture_values__", Bool>,
                                 VarKwIn<"kwargs">,
                                 Out<TsVar<"O">>>
    {
    };

    /**
     * ``map_`` — apply ``func`` element-wise over a multiplexed collection,
     * with one runtime child graph instance **per key or dynamic-list index**.
     * This is the current C++ subset of
     * Python ``map_(func, *args)``:
     *
     * - the multiplexed input is a ``TSD`` (keyed runtime children), a
     *   fixed-size ``TSL`` (wiring-time expansion: one inline application of
     *   ``func`` per index), or a grow-only dynamic ``TSL`` (one stable,
     *   in-place child graph slot per observed index). TSD child lifecycle follows
     *   a required ``__keys__`` TSS input; wiring may supply it explicitly or infer
     *   it from the union of multiplexed TSD keys. A TSD multiplexes when the
     *   corresponding ``func`` parameter accepts its element. A whole-time-series
     *   type variable multiplexes the first TSD argument and later same-key
     *   TSDs; later different-key TSDs and all non-TSDs pass through whole. A
     *   concrete whole-TSD parameter always receives it directly.
     *   Unresolved operator element variables retain element-wise behaviour.
     *   The first multiplexed TSD establishes the key type. Membership changes
     *   only create/destroy children through ``__keys__``. Same-size TSLs in the TSL form
     *   multiplex per index; non-collection args broadcast whole;
     * - ``func`` may take the key/index as its first argument when that
     *   parameter is named ``key`` for TSD maps or ``ndx`` for TSL maps.
     *   ``arg<"__key_arg__">(Str{...})`` renames that parameter and ``""``
     *   disables key consumption. After the optional key, argument positions
     *   match the time-series arguments: multiplexed inputs pass the
     *   per-key/per-index element and broadcast inputs pass the whole
     *   time-series;
     * - for value-producing functions, the output is an owned ``TSD<K, OUT>``
     *   or dynamic ``TSL<OUT>`` with a real element instantiated per key/index;
     *   the child's terminal output is a forwarding endpoint bound onto that
     *   element, so the child **writes the parent's storage directly** (no copy);
     * - broadcast arguments are variadic; ``pass_through(port)`` forces an
     *   argument to bind whole (broadcast, whatever its kind) and
     *   ``no_key(port)`` keeps a TSD demultiplexed but excludes it from
     *   key-set inference (Python's wrappers, as wiring-time port tags);
     * - outputless functions use ``map_sink_`` from C++. They share the same
     *   keyed/indexed child lifecycle but do not allocate a parent output.
     *
     * Dynamic-TSL children currently require an ordinary owned whole-node
     * terminal output. Pass-through and already-forwarding child outputs are
     * rejected because a grow-only TSL cannot represent their non-peered
     * endpoint shape safely.
     */
    /**
     * Python's ``pass_through()`` — tag a ``map_`` / ``mesh_`` input so it is NOT
     * demultiplexed: the child binds the input whole (broadcast), whatever
     * its kind. A wiring-time tag on the port; never part of graph structure.
     */
    template <typename S>
    [[nodiscard]] Port<S> pass_through(const Port<S> &port)
    {
        return Port<S>{port.wiring(), port.erased().with_arg_tag(WiringPortRef::ArgTag::PassThrough)};
    }

    /**
     * Python's ``no_key()`` — tag a multiplexed ``map_`` / ``mesh_`` input so it is
     * demultiplexed as usual but EXCLUDED from key-set inference (its keys do
     * not contribute to the derived ``__keys__`` union).
     */
    template <typename S>
    [[nodiscard]] Port<S> no_key(const Port<S> &port)
    {
        return Port<S>{port.wiring(), port.erased().with_arg_tag(WiringPortRef::ArgTag::NoKey)};
    }

    /**
     * The ``map_`` / ``mesh_`` call configuration folded into the node's interning
     * identity: two calls with equal inputs but different function, key-arg
     * name, or argument tags must NOT dedup to one node.
     */
    struct MapCallConfig
    {
        WiredFn                   func{};
        Str                       key_arg{};
        Str                       mesh_name{};
        std::vector<std::uint8_t> arg_tags{};

        [[nodiscard]] bool operator==(const MapCallConfig &other) const
        {
            return func == other.func && key_arg == other.key_arg &&
                   mesh_name == other.mesh_name && arg_tags == other.arg_tags;
        }
    };

    /** Apply a child graph independently to every live keyed or list element.
        Multiplexed inputs supply one element per child while ordinary inputs broadcast
        whole. TSD children are created and destroyed with the effective key set; fixed
        lists expand at wiring time and dynamic lists allocate stable children by index.
        @param func Graph callable instantiated for each key or index.
        @param args Positional multiplexed or broadcast inputs.
        @param kwargs Named multiplexed or broadcast inputs.
        @param __keys__ Optional explicit key set controlling TSD child lifetime.
        @param __key_arg__ Name of the child key/index argument, or an empty string to omit it.
        @return A collection with one child result per active key or index.
        @par Python example
        @code{.py}
        notionals = hg.map_(multiply, prices, quantities)
        @endcode */
    struct map_ : Operator<"map_",
                           Scalar<"func", WiredFn>,
                           VarIn<"args", TsVar<"A">>,         // *args — multiplexed / broadcast inputs (positional)
                           VarKwIn<"kwargs">,                 // **kwargs — resolved onto func's named parameters
                           Out<TsVar<"O">>>
    {
    };

    /** Outputless keyed map. Resolves through the same ``map_`` registry name. */
    struct map_sink_ : Operator<"map_",
                                Scalar<"func", WiredFn>,
                                VarIn<"args", TsVar<"A">>,
                                VarKwIn<"kwargs">>
    {
    };

    /**
     * ``mesh_(func, *args, **kwargs)`` — like ``map_`` over a ``TSD``, but the
     * per-key instances may read each other's outputs (``mesh_(func)[k]``),
     * create instances on demand, and evaluate in dependency-rank order within a
     * cycle. The instantiation call surface mirrors ``map_``; cross-instance
     * access uses ``mesh_ref<OUT>(w, key)`` inside the mesh body. See the
     * developer guide *Mesh*. TSD argument classification, including generic
     * whole-time-series inputs and ``pass_through`` / ``no_key`` tags, follows
     * ``map_`` exactly; mesh does not implement map's TSL kernels.
     * @param func Per-key graph callable; it may access peers through the named mesh.
     * @param args Positional multiplexed or broadcast TSD inputs.
     * @param kwargs Named multiplexed or broadcast inputs.
     * @param __name__ Optional mesh name used by cross-instance references.
     * @param __keys__ Optional explicit key set controlling child lifetime.
     * @param __key_arg__ Name of the child key argument, or empty to omit it.
     * @return A keyed dictionary of per-instance outputs.
     * @par Python example
     * @code{.py}
     * ranked = hg.mesh_(rank_with_peers, scores, __name__="scores")
     * @endcode
     */
    struct mesh_ : Operator<"mesh_",
                            Scalar<"func", WiredFn>,
                            VarIn<"args", TsVar<"A">>,
                            VarKwIn<"kwargs">,
                            Out<TsVar<"O">>>
    {
    };
}  // namespace hgraph::stdlib

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<hgraph::stdlib::SwitchCases>
    {
        static constexpr std::string_view value{"switch_cases"};
    };
    template <>
    struct scalar_name<hgraph::stdlib::DispatchCases>
    {
        static constexpr std::string_view value{"dispatch_cases"};
    };
    template <>
    struct scalar_name<hgraph::stdlib::MapCallConfig>
    {
        static constexpr std::string_view value{"map_config"};
    };
    template <>
    struct scalar_name<hgraph::stdlib::TryExceptCallConfig>
    {
        static constexpr std::string_view value{"try_except_config"};
    };
}  // namespace hgraph::static_schema_detail

template <>
struct std::hash<hgraph::stdlib::MapCallConfig>
{
    [[nodiscard]] std::size_t operator()(const hgraph::stdlib::MapCallConfig &config) const noexcept
    {
        std::size_t h       = std::hash<hgraph::WiredFn>{}(config.func);
        const auto  combine = [&h](std::size_t v) { h ^= v + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2); };
        combine(std::hash<std::string>{}(config.key_arg));
        combine(std::hash<std::string>{}(config.mesh_name));
        for (const std::uint8_t tag : config.arg_tags) { combine(tag); }
        return h;
    }
};

template <>
struct std::hash<hgraph::stdlib::TryExceptCallConfig>
{
    [[nodiscard]] std::size_t operator()(const hgraph::stdlib::TryExceptCallConfig &config) const noexcept
    {
        std::size_t h       = std::hash<hgraph::WiredFn>{}(config.func);
        const auto  combine = [&h](std::size_t v) { h ^= v + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2); };
        combine(std::hash<std::size_t>{}(config.error_capture.trace_back_depth));
        combine(std::hash<bool>{}(config.error_capture.capture_values));
        return h;
    }
};

template <>
struct std::hash<hgraph::stdlib::SwitchCases>
{
    [[nodiscard]] std::size_t operator()(const hgraph::stdlib::SwitchCases &cases) const noexcept
    {
        std::size_t h = cases.cases.size();
        const auto  combine = [&h](std::size_t v) { h ^= v + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2); };
        for (const auto &entry : cases.cases)
        {
            combine(entry.key.has_value() ? entry.key.hash() : 0);
            combine(std::hash<hgraph::WiredFn>{}(entry.branch));
        }
        if (cases.default_branch.has_value()) { combine(std::hash<hgraph::WiredFn>{}(*cases.default_branch)); }
        combine(cases.reload_on_ticked ? 1 : 0);
        return h;
    }
};

template <>
struct std::hash<hgraph::stdlib::DispatchCases>
{
    [[nodiscard]] std::size_t operator()(const hgraph::stdlib::DispatchCases &cases) const noexcept
    {
        std::size_t h = cases.cases.size();
        const auto combine = [&h](std::size_t v) {
            h ^= v + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        };
        for (const auto &entry : cases.cases)
        {
            for (const auto *type : entry.types)
            {
                combine(std::hash<const void *>{}(type));
            }
            combine(std::hash<hgraph::WiredFn>{}(entry.branch));
        }
        for (const std::size_t index : cases.dispatch_args) { combine(index); }
        if (cases.default_branch.has_value())
        {
            combine(std::hash<hgraph::WiredFn>{}(*cases.default_branch));
        }
        return h;
    }
};

#endif  // HGRAPH_LIB_STD_OPERATORS_HIGHER_ORDER_H
