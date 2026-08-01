/**
 * PyWiring/PyRun: the Python-facing wiring context and run handle. A
 * PyWiring is OWNED by default; a BORROWED one (PyWiring::borrow) aliases a
 * Wiring the C++ side owns (sub-graph compiles, overload trampolines) and
 * cannot be run/finished.
 */
#ifndef HGRAPH_PYTHON_PY_WIRING_H
#define HGRAPH_PYTHON_PY_WIRING_H

#include "py_runtime.h"

namespace hgraph::python_bridge
{
    [[nodiscard]] std::shared_ptr<spdlog::logger> make_python_run_logger(
        nb::object logger, int python_level, nb::object formatter);
    void add_python_lifecycle_observers(
        GraphExecutorBuilder &builder,
        std::vector<std::unique_ptr<LifecycleObserver>> &owned,
        nb::tuple observers);
    void configure_python_wiring_observers(
        Wiring &wiring,
        std::vector<nb::object> &borrowed,
        std::unique_ptr<WiringTracer> &tracer,
        nb::object trace_wiring,
        nb::tuple observers);

    /** Erased WiringArg assembly for the by-name wire path (defined in
        py_wiring.cpp). */
    [[nodiscard]] std::vector<WiringArg> build_args(nb::tuple args, nb::dict kwargs);

    struct PyRun
    {
        // Declared before the executor so destruction tears the executor down
        // while its registered observers are still alive.
        std::unique_ptr<EvaluationTrace> trace{};
        std::unique_ptr<EvaluationProfiler> profiler{};
        std::vector<std::unique_ptr<LifecycleObserver>> observers{};
        GraphExecutorValue executor;

        /** Recorded read-back. DENSE (default): per-cycle values, None = no
            tick. SPARSE (recordings made with sparse=True, the harness's
            __elide__): (cycle_offset, delta) pairs in time order. */
        [[nodiscard]] nb::list recorded(const std::string &key, bool sparse)
        {
            nb::list result;
            if (sparse)
            {
                for (const auto &[offset, delta] :
                     testing::get_recorded_sparse(executor.view().graph().global_state(), key))
                {
                    result.append(nb::make_tuple(offset, value_to_py(delta.view())));
                }
                return result;
            }
            for (const auto &delta : testing::get_recorded_deltas(executor.view().graph().global_state(), key))
            {
                result.append(delta.has_value() ? value_to_py(delta->view()) : nb::none());
            }
            return result;
        }
    };

    /** Keeps a failed, non-cleaned executor alive for the Python exception's lifetime. */
    class RetainedGraphRunError final : public std::runtime_error
    {
      public:
        RetainedGraphRunError(std::exception_ptr error,
                              std::shared_ptr<PyRun> run);

        [[nodiscard]] const std::shared_ptr<PyRun> &run() const noexcept
        {
            return run_;
        }

      private:
        std::shared_ptr<PyRun> run_;
    };

    void translate_retained_graph_run_error(const RetainedGraphRunError &error);

    struct PyWiring
    {
        // Owned by default; a BORROWED PyWiring (python graph callables run
        // against a Wiring the C++ side owns - e.g. a sub-graph compile)
        // aliases without ownership and cannot be run/finished.
        std::unique_ptr<GlobalContext> seed_context{};
        std::vector<nb::object>        borrowed_wiring_observers{};
        std::unique_ptr<WiringTracer> wiring_tracer{};
        std::unique_ptr<Wiring>        owned{};
        Wiring                        *raw{nullptr};
        GlobalState                   *python_state{nullptr};
        bool                           finished{false};

        PyWiring()
            : owned(std::make_unique<Wiring>()), raw(owned.get())
        {
            // The raw stateless wiring bridge IS the dense recording harness
            // (``Run.recorded`` reads cycle-aligned buffers): record/replay
            // resolve to the DENSE backend here. A seeded wiring (below)
            // inherits its GlobalState's model instead (default IN_MEMORY =
            // sparse ``:memory:`` recording for real graph runs / components).
            // Guarded so an ambient non-default model (e.g. DATA_FRAME) wins.
            const GlobalStateView state = raw->global_state();
            if (record_replay::model_is(state, record_replay::IN_MEMORY))
            {
                record_replay::Config config = record_replay::config(state);
                config.model                 = std::string{record_replay::IN_MEMORY_DENSE};
                record_replay::set_config(state, std::move(config));
            }
        }

        explicit PyWiring(GlobalState &state)
            : seed_context(std::make_unique<GlobalContext>(state)),
              owned(std::make_unique<Wiring>()),
              raw(owned.get()),
              python_state(&state)
        {
        }

        [[nodiscard]] static PyWiring borrow(Wiring &target)
        {
            PyWiring result;
            result.owned.reset();
            result.raw = &target;
            return result;
        }

        [[nodiscard]] Wiring &wiring_ref()
        {
            if (raw == nullptr) { throw std::logic_error("Wiring is no longer available"); }
            return *raw;
        }

        void configure_wiring_observers(nb::object trace_wiring,
                                        nb::tuple observers)
        {
            ensure_open();
            configure_python_wiring_observers(
                wiring_ref(), borrowed_wiring_observers, wiring_tracer,
                std::move(trace_wiring), std::move(observers));
        }

        [[nodiscard]] nb::list wiring_trace_lines() const
        {
            nb::list result;
            if (wiring_tracer != nullptr)
            {
                for (const std::string &line : wiring_tracer->lines())
                {
                    result.append(line);
                }
            }
            return result;
        }

        [[nodiscard]] WiringObservationScope graph_wiring_scope(
            std::string label)
        {
            Wiring &wiring = wiring_ref();
            if (!wiring.has_wiring_observers()) { return {}; }
            const WiringScopeKind kind = wiring.current_wiring_path().empty()
                                             ? WiringScopeKind::Graph
                                             : WiringScopeKind::NestedGraph;
            WiringScopeEvent event{.kind = kind};
            event.label = std::move(label);
            event.signature = event.label;
            return wiring.observation_scope(std::move(event));
        }

        [[nodiscard]] nb::object wire(const std::string &name, nb::tuple args, nb::dict kwargs,
                                      std::optional<PyTsType> output_type,
                                      std::optional<std::vector<std::size_t>> sizes = std::nullopt,
                                      PyResolutionScope *initial_resolution = nullptr)
        {
            ensure_open();
            // Target-directed scalar conversion: with an explicit output
            // type, a leading plain-python scalar converts AT the target's
            // value schema (const((1,2,3), tp=TS[tuple[int,...]]) builds
            // the variadic tuple, not a generic mutable list). Perform this
            // before generic argument conversion: an empty tuple/map has no
            // inferable element schema without the target.
            std::optional<Value> target_const;
            if (name == "const" && output_type.has_value() && output_type->meta != nullptr &&
                output_type->meta->value_schema != nullptr && nb::len(args) >= 1)
            {
                // Whole value first, then the DELTA form (a partial value -
                // dict over a TSL/TSD, set delta, ...) at the canonical delta
                // schema; if both fail, generic conversion below reports the
                // ordinary mismatch.
                target_const = fallback_on_exception(std::optional<Value>{}, [&] {
                    return std::optional<Value>{py_to_value_as(args[0], output_type->meta->value_schema)};
                });
                if (!target_const.has_value())
                {
                    target_const = fallback_on_exception(std::optional<Value>{}, [&] {
                        return std::optional<Value>{py_to_delta(args[0], output_type->meta)};
                    });
                }
            }
            std::vector<WiringArg> wiring_args;
            if (target_const.has_value())
            {
                nb::list converted_args;
                converted_args.append(nb::cast(PyScalarValue{std::move(*target_const)}));
                for (std::size_t index = 1; index < nb::len(args); ++index)
                {
                    converted_args.append(args[index]);
                }
                wiring_args = build_args(nb::tuple(converted_args), kwargs);
            }
            else { wiring_args = build_args(args, kwargs); }
            const std::vector<std::size_t> size_hints = sizes.value_or(std::vector<std::size_t>{});
            ResolvedOperatorCall resolved = OperatorRegistry::instance().resolve(
                name, std::span<const WiringArg>{wiring_args.data(), wiring_args.size()}, std::nullopt,
                output_type.has_value() ? output_type->meta : nullptr,
                std::span<const std::size_t>{size_hints.data(), size_hints.size()},
                wiring_ref().operator_state(), &wiring_ref(),
                initial_resolution != nullptr ? &initial_resolution->map : nullptr);
            OperatorWireResult result =
                resolved.impl->wire(wiring_ref(), resolved.map, resolved.args, resolved.kwargs);
            if (!result.has_output) { return nb::none(); }
            return nb::cast(PyPort{result.output.erased()});
        }

        /** hgraph's exception_time_series(port): activate error capture on
            the producing node. Ordinary nodes return TS[NodeError]; TSD map_
            nodes return TSD[K, TS[NodeError]]. */
        [[nodiscard]] nb::object exception_time_series(PyPort port,
                                                       std::int64_t trace_back_depth = 1,
                                                       bool capture_values = false)
        {
            ensure_open();
            if (trace_back_depth < 0)
            {
                throw std::invalid_argument("exception_time_series: trace-back depth must be non-negative");
            }
            wiring_ref().activate_error_capture(
                port.ref.peered_node(), node_error_ts_meta(),
                ErrorCaptureOptions{
                    .trace_back_depth = static_cast<std::size_t>(trace_back_depth),
                    .capture_values = capture_values,
                });
            WiringPortRef error = graph_wiring_detail::special_output_source(
                port.ref, GraphEdgeSourceKind::ErrorOutput, "error_output");
            return nb::cast(PyPort{error});
        }

        void set_replay(const std::string &key, nb::list values, std::optional<PyTsType> ts_type)
        {
            ensure_open();
            std::vector<std::optional<Value>> deltas;
            deltas.reserve(nb::len(values));
            for (nb::handle object : values)
            {
                if (object.is_none()) { deltas.emplace_back(std::nullopt); }
                else if (ts_type.has_value()) { deltas.emplace_back(py_to_delta(object, ts_type->meta)); }
                else { deltas.emplace_back(py_to_value(object)); }
            }
            testing::set_replay_deltas(wiring_ref().global_state(), key, deltas);
        }

        [[nodiscard]] std::unique_ptr<PyRun> run(
            std::optional<DateTime> start_time, std::optional<DateTime> end_time,
            bool realtime, const EvaluationTrace *trace,
            const EvaluationProfiler *profiler, nb::object logger,
            int logger_level, nb::object logger_formatter,
            nb::tuple observers, std::int64_t trace_back_depth,
            bool capture_values,
            bool cleanup_on_error,
            bool snapshot)
        {
            ensure_open();
            if (owned == nullptr) { throw std::logic_error("a borrowed Wiring cannot be run"); }
            if (trace_back_depth < 0)
            {
                throw std::invalid_argument(
                    "trace_back_depth must be non-negative");
            }
            // snapshot=true builds from the wiring AS IT STANDS and leaves it
            // open for further wiring (interactive/notebook sessions —
            // developer_guide/notebook.rst); the default consumes the wiring.
            GraphBuilder builder = snapshot
                                       ? owned->snapshot()
                                       : (finished = true, std::move(*owned).finish());
            const bool requires_phase_runner =
                builder.requires_phase_runner() || nb::len(observers) != 0;

            GraphExecutorBuilder eb;
            eb.graph_builder(std::move(builder))
                .start_time(start_time.value_or(MIN_ST))
                .end_time(end_time.value_or(MAX_ET))
                .mode(realtime ? GraphExecutorMode::RealTime : GraphExecutorMode::Simulation)
                .logger(make_python_run_logger(std::move(logger), logger_level,
                                               std::move(logger_formatter)))
                .error_capture_options(ErrorCaptureOptions{
                    .trace_back_depth = static_cast<std::size_t>(trace_back_depth),
                    .capture_values = capture_values,
                })
                .cleanup_on_error(cleanup_on_error);
            if (requires_phase_runner)
            {
                eb.phase_runner(&py_run_executor_phase);
            }
            auto run = std::make_unique<PyRun>();
            add_python_lifecycle_observers(eb, run->observers, observers);
            run->trace = trace != nullptr
                             ? std::make_unique<EvaluationTrace>(*trace)
                             : nullptr;
            if (run->trace != nullptr) { eb.add_lifecycle_observer(run->trace.get()); }
            run->profiler = profiler != nullptr
                                ? std::make_unique<EvaluationProfiler>(*profiler)
                                : nullptr;
            if (run->profiler != nullptr)
            {
                eb.add_lifecycle_observer(run->profiler.get());
            }
            run->executor = eb.make_executor();

            if (py_has_active_runtime_global_state())
            {
                throw std::logic_error("a runtime GlobalState is already active on this thread");
            }
            auto guard = std::make_shared<PyTsGuard>();
            nb::object runtime_state = nb::cast(PyRuntimeGlobalState{
                run->executor.view().graph().global_state(), guard});
            nb::object runtime = nb::module_::import_("hgraph._wiring._state");
            runtime.attr("_push_runtime_global_state")(runtime_state);
            py_active_runtime_global_state = runtime_state.ptr();
            py_active_runtime_guard() = guard;
            auto clear_runtime_state = UnwindCleanupGuard([&] {
                py_active_runtime_global_state = nullptr;
                py_active_runtime_guard().reset();
                guard->alive = false;
                runtime.attr("_pop_runtime_global_state")();
            });
            auto copy_runtime_state = UnwindCleanupGuard([&] {
                if (python_state != nullptr)
                {
                    python_state->view().copy_from(
                        run->executor.view().graph().global_state());
                }
            });
            annotate_on_exception(
                [&] {
                    // The native run loop releases the caller's GIL. Its phase
                    // runner reacquires one local nanobind guard for the
                    // complete start, evaluation-cycle, or stop phase, leaving
                    // the GIL free while a real-time run waits.
                    nb::gil_scoped_release release;
                    run->executor.view().run();
                },
                [&] {
                    if (!cleanup_on_error)
                    {
                        copy_runtime_state.complete();
                        throw RetainedGraphRunError{
                            std::current_exception(),
                            std::shared_ptr<PyRun>{std::move(run)}};
                    }
            });
            clear_runtime_state.complete();
            copy_runtime_state.complete();
            return run;
        }

        [[nodiscard]] nb::tuple push_source(PyTsType ts_type, bool conflate, nb::object on_start)
        {
            ensure_open();
            auto slot     = std::make_shared<PySenderSlot>();
            slot->schema  = ts_type.meta;
            auto policy   = conflate ? make_push_source_conflating_policy(*ts_type.meta->delta_value_schema)
                                     : make_push_source_queue_policy(*ts_type.meta->delta_value_schema);
            NodeBuilder builder = make_push_source_node(
                *ts_type.meta, std::move(policy), [slot, on_start](PushSourceSender sender) {
                    slot->sender = std::move(sender);
                    slot->type_realization = slot->sender.type_realization();
                    if (!on_start.is_none())
                    {
                        // hgraph's @push_queue contract: the wrapped function
                        // IS the start lifecycle hook, invoked with the sender.
                        on_start(nb::cast(PySender{slot}));
                    }
                }, true);
            struct py_push_source_tag
            {
            };
            WiringPortRef ref = wiring_ref().add_unique_node(std::type_index(typeid(py_push_source_tag)),
                                                             std::move(builder), std::span<const WiringPortRef>{},
                                                             Value{});
            return nb::make_tuple(PyPort{std::move(ref)}, PySender{std::move(slot)});
        }

        [[nodiscard]] PyFeedback feedback(PyTsType ts_type, nb::handle initial)
        {
            ensure_open();
            const auto *schema = stdlib::feedback_detail::require_feedback_schema(ts_type.meta);
            Value       initial_delta;
            const bool  has_initial = !initial.is_none();
            if (has_initial)
            {
                initial_delta = py_to_delta(initial, schema);
                stdlib::feedback_detail::validate_initial_delta(*schema, initial_delta);
            }
            NodeBuilder builder = make_feedback_source_node(*schema, has_initial);
            WiringPortRef ref   = wiring_ref().add_unique_node(
                std::type_index(typeid(stdlib::feedback_detail::feedback_source_node_tag)), std::move(builder),
                std::span<const WiringPortRef>{}, std::move(initial_delta));
            return PyFeedback{&wiring_ref(), std::move(ref), schema, false};
        }

        [[nodiscard]] PyDelayedBinding delayed_binding(PyTsType ts_type)
        {
            ensure_open();
            return PyDelayedBinding{ErasedDelayedBindingWiringPort{wiring_ref(), ts_type.meta}};
        }

        void delayed_binding_bind(PyDelayedBinding &delayed, const PyPort &port)
        {
            ensure_open();
            delayed.binding.bind(port.ref);
        }

        void feedback_bind(PyFeedback &fb, const PyPort &port)
        {
            ensure_open();
            if (fb.bound) { throw std::logic_error("feedback is already bound"); }
            WiringPortRef ts_source =
                graph_wiring_detail::adapt_source_for_input(wiring_ref(), fb.schema, port.ref);
            WiringPortRef self_source =
                graph_wiring_detail::adapt_source_for_input(wiring_ref(), fb.schema, fb.delegate);
            std::array<WiringPortRef, 2> sources{std::move(ts_source), std::move(self_source)};

            NodeBuilder builder = make_feedback_sink_node(*fb.schema);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema() != nullptr ? builder.type().schema()->input_schema : nullptr,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));
            (void)wiring_ref().add_node(
                std::type_index(typeid(stdlib::feedback_detail::feedback_sink_node_tag)), std::move(builder),
                std::span<const WiringPortRef>{sources.data(), sources.size()}, Value{});
            fb.bound = true;
        }

        void release_seed_context() noexcept { seed_context.reset(); }

      private:
        void ensure_open() const
        {
            if (finished || raw == nullptr) { throw std::logic_error("this Wiring has already been finished/run"); }
        }
    };
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_PYTHON_PY_WIRING_H
