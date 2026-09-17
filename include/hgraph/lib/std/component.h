#ifndef HGRAPH_LIB_STD_COMPONENT_H
#define HGRAPH_LIB_STD_COMPONENT_H

#include <hgraph/lib/std/operators/io.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/manifest/schema_descriptor.h>
#include <hgraph/util/scope.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/record_replay.h>
#include <hgraph/types/wired_fn.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <functional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <typeindex>
#include <utility>

namespace hgraph::stdlib
{
    namespace component_detail
    {
        /** Checkpoint ownership boundary preserving the exact upstream storage.
         * A forwarding endpoint keeps keyed slot allocation and structural deltas
         * identical to an unconfigured component. Its source baseline belongs to
         * the coordinator; this node owns only the alias and its clocks.
         */
        struct checkpoint_input
        {
            static NodeCheckpointState capture(const NodeView &node, const CaptureGraphCheckpoint &)
            {
                NodeCheckpointState image;
                image.endpoints.push_back(node.output(node.graph().evaluation_time()).checkpoint_forwarding());
                return image;
            }

            static void restore(const NodeView &node, const NodeCheckpointState &image,
                                DateTime time, const RestoreGraphCheckpoint &)
            {
                if (image.payload.has_value() || !image.children.empty() || image.endpoints.size() != 1)
                {
                    throw std::invalid_argument("component checkpoint: invalid input boundary image");
                }
                const auto has_source = [](const auto &self, const TSCheckpointImage &endpoint) -> bool {
                    if (endpoint.payload.has_value()) { return endpoint.payload.view().checked_as<Bool>(); }
                    return std::any_of(endpoint.children.begin(), endpoint.children.end(),
                        [&](const auto &child) { return self(self, child); });
                };
                auto input = node.input(time);
                auto source = has_source(has_source, image.endpoints.front())
                    ? input.indexed_child_at(0).bound_output() : TSOutputView{};
                node.output(time).restore_checkpoint_forwarding(source, image.endpoints.front());
            }

            static const NodeCheckpointOps &checkpoint_ops() noexcept
            {
                static const NodeCheckpointOps ops{
                    .supported = true,
                    .captures_output = false,
                    .boundary_input = true,
                    .capture_impl = &capture,
                    .restore_impl = &restore,
                    .signature_impl = +[](const NodeBuilder &builder) {
                        manifest::CanonicalWriter writer;
                        writer.varint(3); // Bind value endpoints before first observation, including invalid values.
                        manifest::encode_manifest_scalar(writer, builder.scalars().view());
                        const auto &bytes = writer.bytes();
                        return std::string{reinterpret_cast<const char *>(bytes.data()), bytes.size()};
                    },
                };
                return ops;
            }

            static bool evaluate(const void *, const NodeView &node, DateTime time)
            {
                auto input = node.input(time);
                auto source_input = input.indexed_child_at(0);
                auto source = source_input.bound_output();
                auto output = node.output(time);
                const bool changed = bind_forwarding_output_tree_to_source(
                    output.borrowed_ref(), source, false, ForwardingSourceMode::PreserveEndpoint);
                // Initial binding and a source event admitted during start must
                // publish at this time. Ordinary later events already propagate
                // through the alias; repeated notification coalesces normally.
                if (source.valid() && (changed || source_input.modified()))
                {
                    output.begin_mutation(time).mark_modified();
                }
                return true;
            }

            static void start(const NodeView &node, DateTime time)
            {
                // An invalid value still has an endpoint identity. Bind before
                // consumers can capture a REF, so a later first tick is seen
                // through that same reference rather than an empty placeholder.
                const auto input = node.input(time).indexed_child_at(0).bound_output();
                (void)bind_forwarding_output_tree_to_source(
                    node.output(time), input, false, ForwardingSourceMode::PreserveEndpoint);
            }
        };

        [[nodiscard]] inline WiringPortRef checkpoint_boundary(Wiring &w, WiringPortRef source,
                                                               std::string_view input_name)
        {
            if (ts_checkpoint_schema_contains_reference(source.schema))
                throw std::invalid_argument("component checkpoint: inputs must expose dereferenced time-series values");
            const auto *input_schema = TypeRegistry::instance().un_named_tsb({{"ts", source.schema}});
            NodeTypeMetaData meta;
            meta.display_name = "component_checkpoint_input";
            meta.node_kind = NodeKind::Compute;
            meta.input_schema = input_schema;
            meta.output_schema = source.schema;
            meta.scalar_schema = scalar_descriptor<Str>::value_meta();
            meta.valid_inputs = std::vector<std::size_t>{};
            meta.output_endpoint_schema = forwarding_output_endpoint_schema(source.schema);
            NodeTypeDescriptor descriptor;
            descriptor.schema = std::move(meta);
            descriptor.callbacks.start = &checkpoint_input::start;
            descriptor.ops.evaluate_impl = &checkpoint_input::evaluate;
            descriptor.ops.checkpoint_ops = &checkpoint_input::checkpoint_ops();
            auto builder = NodeBuilder::from_descriptor(std::move(descriptor));
            const std::array inputs{source};
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(input_schema, inputs));
            return w.add_node(std::type_index(typeid(checkpoint_input)), std::move(builder), inputs,
                              Value{Str{input_name}});
        }
        /**
         * The RECOVER pass-through (P7, zero-cost form): a plain forwarding
         * node whose first scheduled evaluation resolves the last recorded
         * value at or before the start time and publishes it to its OWN output —
         * Python's ``merge(ts, replay_const(...))`` fused into one node.
         * Only recovering components pay for it; no graph-level state.
         */
        struct recovering_pass_through
        {
            static constexpr auto name = "recovering_pass_through";
            static constexpr bool schedule_on_start = true;

            static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                             Scalar<"fq_key", Str> fq_key, GlobalStateView gs,
                             DateTime now, State<Bool> initialized, Out<TsVar<"S">> out)
            {
                if (!initialized.get())
                {
                    const auto &erased = static_cast<const TSOutputView &>(out);
                    Value recovered = record_replay::recorded_seed_resolver(
                        gs, fq_key.value(), erased.schema(), now);
                    if (recovered.has_value()) { out.apply(recovered.view()); }
                    initialized.set(true);
                }
                if (ts.modified())
                {
                    const Value delta = capture_delta(ts.base());
                    apply_delta(out, delta.view());
                }
            }
        };

        /** The record/replay key for the I-th input: the graph's ``NamedPort`` name, else ``arg_<I>``. */
        template <typename Param, std::size_t I>
        [[nodiscard]] std::string input_key()
        {
            using P = static_node_detail::selector_of<Param>;
            if constexpr (requires { P::field_name; }) { return std::string{P::field_name.sv()}; }
            else { return "arg_" + std::to_string(I); }
        }

        [[nodiscard]] inline WiringArg ts_arg(WiringPortRef port)
        {
            WiringArg arg;
            arg.kind = WiringArg::Kind::TimeSeries;
            arg.port = std::move(port);
            return arg;
        }

        [[nodiscard]] inline WiringArg str_arg(std::string_view value, std::string_view name = {})
        {
            WiringArg arg;
            arg.kind         = WiringArg::Kind::Scalar;
            arg.scalar_value = Value{Str{value}};
            arg.scalar_meta  = scalar_descriptor<Str>::value_meta();
            arg.name         = name;
            return arg;
        }

        [[nodiscard]] inline WiringPortRef replay(Wiring &w, std::string_view key,
                                                  std::string_view fq,
                                                  const TSValueTypeMetaData *schema)
        {
            std::array args{
                str_arg(key),
                str_arg(fq, "recordable_id"),
            };
            return wire_operator(w, stdlib::replay::name, args, true, schema).output.erased();
        }

        inline void record(Wiring &w, const WiringPortRef &port,
                           std::string_view key, std::string_view fq)
        {
            std::array args{
                ts_arg(port),
                str_arg(key),
                str_arg(fq, "recordable_id"),
            };
            (void)wire_operator(w, stdlib::record::name, args, false);
        }

        inline void compare(Wiring &w, const WiringPortRef &actual,
                            const WiringPortRef &recorded, std::string_view fq)
        {
            std::array args{
                ts_arg(actual),
                ts_arg(recorded),
                str_arg(fq, "recordable_id"),
            };
            (void)wire_operator(w, stdlib::compare::name, args, false);
        }

        [[nodiscard]] inline WiringPortRef wrap_input(
            Wiring &w, WiringPortRef port, std::string_view key,
            const std::string &fq, record_replay::Mode mode)
        {
            using record_replay::has_mode;
            using record_replay::Mode;
            if (has_mode(mode, Mode::Replay) || has_mode(mode, Mode::Compare))
            {
                // The live input is REPLACED by the recorded one (Python parity).
                port = replay(w, key, fq, port.schema);
            }
            if (has_mode(mode, Mode::Recover))
            {
                // The RECOVER/P7 seed (no merge nodes — the ruling): the
                // recovering pass-through publishes the recorded value from
                // its own start, and live ticks flow through (and override)
                // thereafter. Costs only the components that recover.
                port = wire<recovering_pass_through>(
                           w, Port<void>{w, std::move(port)}, Str{fq + "." + std::string{key}})
                           .erased();
            }
            if (has_mode(mode, Mode::Record))
            {
                record(w, port, key, fq);
            }
            return port;
        }
    }  // namespace component_detail

    /**
     * ``component<G>(w, "id", inputs...)`` — Python's ``@component`` as a
     * wiring function (design record: *Record/replay, tables and const_fn*,
     * step 5). ``G`` is an ordinary graph struct; the component consults the
     * ambient ``record_replay::scope`` and, per its mode, wraps every input
     * and the output with name-resolved ``record``/``replay`` (whatever
     * implementation the active backend selects):
     *
     * - ``Record``   — each input and the output (``__out__``) is recorded.
     * - ``Replay`` / ``Compare`` — inputs are REPLACED by their recordings;
     *   ``Compare`` additionally recomputes the output and compares per-tick
     *   equality against the recorded output, publishing the core-neutral
     *   summary under ``fq.__compare__`` (read with
     *   ``record_replay::comparison_summary``).
     * - ``ReplayOutput`` — the output is replaced by its recording.
     * - ``None`` — no wrapping; the component is a plain ``wire<G>``.
     *
     * Input keys are the graph's ``NamedPort`` names (``arg_<I>`` for plain
     * ``Port`` params); the fully-qualified recordable id chains through
     * nested components via the mode scope
     * (``outer.inner``, replacing Python's trait copy-down at this level —
     * runtime graph traits still serve nodes inside compiled sub-graphs).
     * The consulted (mode, id) manifests structurally in the wiring, so
     * intern identity is respected per the P3 ruling.
     *
     * - ``Recover`` — each input routes through a component-owned
     *   recovering pass-through that publishes the last recorded value at
     *   or before the start time from its own ``start`` (the P7 seed — no
     *   merge nodes, no graph-level state); live ticks override thereafter.
     *   Combine with ``Record`` to continue recording the recovered stream.
     *
     */
    template <typename Compose>
        requires std::invocable<Compose &, std::span<const WiringPortRef>> &&
                 std::convertible_to<std::invoke_result_t<Compose &, std::span<const WiringPortRef>>, WiringPortRef>
    [[nodiscard]] WiringPortRef component(Wiring &w, std::string_view recordable_id,
                                          std::span<const WiringNamedPortRef> inputs,
                                          Compose &&compose)
    {
        using record_replay::has_mode;
        using record_replay::Mode;

        const auto &ambient = record_replay::current_scope();
        const Mode  mode    = ambient.mode;

        std::string fq;
        if (ambient.recordable_id.empty()) { fq = std::string{recordable_id}; }
        else if (recordable_id.empty()) { fq = ambient.recordable_id; }
        else { fq = ambient.recordable_id + "." + std::string{recordable_id}; }

        if (mode != Mode::None && fq.empty())
        {
            throw std::invalid_argument(
                "component: a recordable id is required under an active record/replay mode");
        }
        if (has_mode(mode, Mode::Recover) && (has_mode(mode, Mode::Replay) || has_mode(mode, Mode::ReplayOutput)))
        {
            throw std::invalid_argument("component: cannot recover and replay at the same time");
        }

        if (!fq.empty()) { w.claim_component_id(fq); }

        const bool checkpointed = component_recovery_selected(w.operator_state(), fq);
        if (checkpointed && mode != Mode::None)
        {
            throw std::invalid_argument("component checkpoint: legacy record/replay modes cannot be combined with recovery configuration");
        }
        const std::string previous_component = checkpointed ? w.checkpoint_component(fq) : std::string{};
        auto restore_component_scope = make_scope_exit([&] {
            if (checkpointed) { (void)w.checkpoint_component(previous_component); }
        });

        std::vector<WiringPortRef> wrapped;
        wrapped.reserve(inputs.size());
        for (const WiringNamedPortRef &input : inputs)
        {
            if (checkpointed)
            {
                wrapped.push_back(component_detail::checkpoint_boundary(w, input.source, input.name));
                continue;
            }
            wrapped.push_back(component_detail::wrap_input(
                w, input.source, input.name, fq, mode));
        }

        // Nested components chain their ids through the scope (mode carries).
        record_replay::scope nested{mode, fq};
        WiringPortRef out = std::invoke(
            compose, std::span<const WiringPortRef>{wrapped.data(), wrapped.size()});

        if (checkpointed && !out.is_unbound_source() && ts_checkpoint_schema_contains_reference(out.schema))
            throw std::invalid_argument("component checkpoint: references cannot escape the component output");
        if (checkpointed) { w.checkpoint_component_output(out); }

        if (!out.is_unbound_source())
        {
            if (has_mode(mode, Mode::ReplayOutput))
            {
                out = component_detail::replay(w, "__out__", fq, out.schema);
            }
            if (has_mode(mode, Mode::Record))
            {
                component_detail::record(w, out, "__out__", fq);
            }
            if (has_mode(mode, Mode::Compare))
            {
                // Backtesting regression: the recomputed output (from the
                // replayed inputs) against the recorded output; per-tick
                // equality lands in the store under ``fq.__compare__``.
                WiringPortRef recorded = component_detail::replay(
                    w, "__out__", fq, out.schema);
                component_detail::compare(w, out, recorded, fq);
            }
        }
        return out;
    }

    [[nodiscard]] inline WiringPortRef component(
        Wiring &w, std::string_view recordable_id,
        std::span<const WiringNamedPortRef> inputs, const WiredFn &compose)
    {
        return component(
            w, recordable_id, inputs,
            [&](std::span<const WiringPortRef> ports) { return compose.wire(w, ports); });
    }

    template <typename G, typename... S>
    [[nodiscard]] auto component(Wiring &w, std::string_view recordable_id, Port<S>... inputs)
    {
        using sig    = StaticGraphSignature<G>;
        using params = typename sig::param_types;
        static_assert(sig::scalar_count() == 0,
                      "component<G>: scalar compose parameters are not supported yet (time-series inputs only)");
        static_assert(sizeof...(S) == sig::input_count(),
                      "component<G>: pass exactly the graph's time-series inputs");

        auto input_tuple = std::make_tuple(std::move(inputs)...);
        auto named       = [&]<std::size_t... I>(std::index_sequence<I...>) {
            return std::array<WiringNamedPortRef, sizeof...(I)>{
                WiringNamedPortRef{
                    component_detail::input_key<std::tuple_element_t<I, params>, I>(),
                    std::get<I>(input_tuple).erased()}...};
        }(std::index_sequence_for<S...>{});

        WiringPortRef out = component(
            w, recordable_id,
            std::span<const WiringNamedPortRef>{named.data(), named.size()}, fn<G>());
        if constexpr (!std::is_void_v<std::remove_cvref_t<
                          typename StaticGraphSignature<G>::output_type>>)
        {
            using OutSchema = typename graph_wiring_detail::port_static_schema<
                typename StaticGraphSignature<G>::output_type>::type;
            return Port<OutSchema>{w, std::move(out)};
        }
    }
}  // namespace hgraph::stdlib

#endif  // HGRAPH_LIB_STD_COMPONENT_H
