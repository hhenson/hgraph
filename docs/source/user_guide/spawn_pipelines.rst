Spawned sink graphs and pipelines
=================================

.. note::

   This page describes the process-hosted implementation of
   :doc:`../rfc/rfc_0038_spawn_pipelines`. Every stage runs in its own worker
   process. Recovery of in-flight pipeline data is not supported.

A spawned graph consumes inputs independently while remaining on its owner's
logical clock. It can process an earlier cycle while the owner moves on, but
it cannot evaluate a future time before the owner has completed that time.
``spawn_`` accepts only sinks and returns no time-series output to the owner.
Use it when downstream processing can lag and does not feed results back into
the producing graph.

A single sink
-------------

Define an ordinary graph with an outputless signature, then choose the
execution boundary at its call site. In this schematic example,
``construct_portfolio`` and ``publish_portfolio`` are application graphs::

    from hgraph import TS, TSD, graph, spawn_

    @graph
    def portfolio_sink(signal: TS[float], prices: TSD[str, TS[float]]) -> None:
        portfolio = construct_portfolio(signal, prices)
        publish_portfolio(portfolio)

    @graph
    def application(signals: TS[float], prices: TSD[str, TS[float]]) -> None:
        spawn_(portfolio_sink, signal=signals, prices=prices)

The graph's own nodes, state and timers persist across incoming ticks. Its
ordinary internal graph compositions flatten as usual. A graph that returns a
time-series output cannot be passed directly to ``spawn_``. A sink with no
formal inputs is valid: its historical sources and timers can ask the owner
for permission to advance.

A pipeline
----------

``pipeline_`` describes an ordered sequence of stages. Every stage except the
last produces one time-series output; the last is a sink. ``bind_`` supplies
named arguments outside the main flow::

    from hgraph import bind_, pipeline_, spawn_

    spawn_(
        pipeline_([
            bind_(construct_portfolio, prices=prices, config=portfolio_config),
            bind_(generate_orders, limits=limits),
            publish_orders,
        ]),
        signal=signals,
    )

Here the first entry still needs ``signal``. The second entry has exactly one
unbound time-series argument, its portfolio input; the preceding output fills
that argument. The final sink receives the orders output. Both time-series
ports and scalar configuration can be bound by name.

Each listed stage executes in a separate process and keeps its own graph state.
``spawn_`` owns the complete pipeline lifecycle. It does not expose the
intermediate outputs as ordinary ports in the owning graph.

Reuse fragments by nesting pipeline descriptions. Nesting flattens the list
and adds no extra boundary. A fragment may end with an output, but the complete
plan passed to ``spawn_`` must end with a sink. Empty pipelines, intermediate
sinks, ambiguous remaining inputs and mismatched schemas are wiring errors.
An explicit pass-through graph is a valid stage. Structured time-series
outputs remain single connections; use an adapter graph for field extraction
or reshaping.

Time and additional inputs
--------------------------

The child processes the original input timestamps in order. Before handling
input at time ``T``, it runs any earlier scheduled work with the previous input
values. Input changes and timers at ``T`` are evaluated together.

When the queue is empty but a timer is pending, the child requests permission
to proceed. The owner grants immediately if it has already completed that
time, or schedules a notification when it reaches it. Permission arrives after
all inputs through the granted time. Advancing permission does not create a
fake tick on unchanged inputs.

In a pipeline, a stage cannot advance beyond its predecessor's completed time.
A stage with external bindings also waits for the owner to close those inputs
through the same time. For example, if ``limits`` has advanced to time 20 but
the portfolio stage is only complete through 12, the orders stage uses the
limits valid at each portfolio evaluation time. It does not read the latest
parent value simply because it runs later in wall-clock time.

A bound time-series input retains normal activation behavior: changing
``limits`` can trigger the orders stage even without a new portfolio tick.
Use the ordinary passive-input mechanism when sampling only on other active
inputs is intended. Idle stages forward requested progress without requiring
an output tick, so downstream timers can still run.

Data, capacity and shutdown
---------------------------

The boundary contract includes scalar series, signals, sets, dictionaries,
fixed/dynamic lists, bundles and windows, recursively. References are
materialized at the boundary; references internal to a child remain local.
Transfers preserve invalid members, removals, partial updates and window
history with original timestamps. Ordinary window updates transfer incremental
operations instead of repeatedly copying the complete window.

Set ``__capacity_frames__`` (default 256) and ``__capacity_bytes__`` (default
64 MiB) on ``spawn_`` to limit each channel's admitted frames, including frames
currently being processed. External inputs and preceding-stage outputs use
separate channels. Each producer may additionally construct one frame per
channel, bounded by the same byte maximum. Decoded graph state and temporary
codec memory are separate from the payload budget.

Queues have finite frame and byte limits. A slow consumer can eventually pause
its upstream producer; asynchronous execution does not imply unlimited
buffering. The default contract is lossless. Oversized frames and worker
failures fail the run explicitly. Enqueueing a frame does not mean a sink has
processed it or committed an external effect.

Normal shutdown stops admission, drains accepted work through the final
permitted time and joins all stage workers. It does not run future timers
forever. A child failure reaches the owning run and wakes other waiting stages.
No pipeline task remains detached after the run returns. A child calling
``request_stop`` early fails the owning run; it cannot silently abandon queued
input. Use normal owner completion to seal and drain the pipeline.

``__worker_timeout__`` (default 60 seconds, positive and at most 24 hours)
bounds worker bootstrap, each IPC evaluation exchange and shutdown. A crashed
or timed-out worker fails the run; unresponsive workers are terminated and
reaped. The timeout is a wall-clock limit, separate from graph logical time.

Native authoring and initial scope
----------------------------------

Python stages must be defined in an importable module, as with ``dmap_``.
Lambdas, local closures and functions defined only in ``__main__`` cannot be
reconstructed in a fresh interpreter. Keep the application entry point under
``if __name__ == "__main__":`` and import its stages from another module.
Scalar bindings use the existing value codec; code, live connections and parent
ports are never serialized. Each process owns its interpreter and GIL, allowing
CPU-bound Python stages to execute concurrently.

Native stages use registered worker factories. Include
``<hgraph/runtime/spawn.h>``. ``spawn_fn<G>`` binds native scalar configuration,
``bind_`` binds external ports, and ``process_stage`` supplies a factory name
and immutable bootstrap bytes. The application registers the same factory in
the worker program. For a sink ``Consume`` taking ``TS<Int>`` and a scalar
``path``, the registration and call-site wiring look like this::

    register_spawn_worker_recipe("consume", +[](std::string_view config) {
        auto stage = spawn_fn<Consume>(arg<"path">(Str{config}));
        const std::array inputs{schema_descriptor<TS<Int>>::ts_meta()};
        return prepare_spawn_worker(stage.function, inputs);
    });

    // Inside compose():
    auto sink = process_stage(spawn_fn<Consume>(arg<"path">(path)),
                              "consume", path);
    WiringArg input;
    input.port = value.erased();
    std::array arguments{input};
    wire_spawn(w, pipeline_({std::move(sink)}), arguments);

Call ``distributed::run_worker_if_requested(argc, argv)`` after registration
at the beginning of ``main`` and return if it serves a worker. By default the
launcher starts the current executable. ``SpawnConfig.worker_program`` and
``worker_arguments`` select a separate binary that registers the same recipes;
``worker_timeout`` sets the deadline. The worker verifies the complete boundary
schema before starting its graph. No local-thread hosting fallback exists.

Use ``NamedPort`` to expose native graph argument names to ``bind_``; ordinary
``Port`` arguments support positional wiring. The installed-SDK consumer in
``tests/install_consumer/spawn_probe.cpp`` compiles and runs a complete example.

The initial topology is a linear pipeline with additional inputs from its
owning graph. Independent live sources, arbitrary joins, feedback, nested spawn, worker
restart and recovery of in-flight pipeline data are outside this contract.
Historical pull sources and ordinary ``map_``, ``reduce`` and ``mesh_`` inside
a stage retain their local graph semantics. Hidden captures of parent ports
or live resources must not cross the execution boundary.

For per-cycle mapped results returned to the caller, see
:doc:`distributed_map`. For the complete timing, ownership and acceptance
requirements, see :doc:`../rfc/rfc_0038_spawn_pipelines`.
