/**
 * Runtime view structs handed to Python user nodes during evaluation:
 * the lazy TimeSeries/Output views, per-node python STATE, scheduler and
 * clock views, recordable state, and the guarded GlobalState view. See
 * docs/source/developer_guide/python_bridge.rst (GIL boundaries; transient
 * node/time-series views and RuntimeGlobalState are call-scoped).
 */
#ifndef HGRAPH_PYTHON_PY_RUNTIME_H
#define HGRAPH_PYTHON_PY_RUNTIME_H

#include "py_carriers.h"

#include <algorithm>

namespace hgraph::python_bridge
{
    /** One lexical GIL guard around a complete executor phase. */
    inline void py_run_executor_phase(GraphExecutorPhase,
                                      GraphExecutorPhaseAction action)
    {
        nb::gil_scoped_acquire gil;
        action();
    }

    /** Applies a python node's return value to its output (REF whole-move,
        TSS frozenset replace semantics, canonical-delta apply). Defined in
        py_nodes.cpp. */
    void apply_py_result(nb::handle result, Out<TsVar<"O">> &out);

    void py_call_push_queue_start(nb::handle fn, std::string_view config,
                                  const ValueView &scalars,
                                  const PySender &sender,
                                  const NodeView &node,
                                  DateTime evaluation_time);

    void py_call_push_queue_stop(nb::handle fn, bool enabled,
                                 std::string_view config,
                                 std::size_t scalar_offset,
                                 const ValueView &scalars,
                                 const NodeView &node);

    /** Copy a window's evaluation timestamps into the value layer's standard
        NumPy-compatible one-dimensional buffer.  The scalar binding owns the
        dtype conversion (DateTime -> datetime64[us]); the window remains the
        C++ source of ordering and contents. */
    [[nodiscard]] inline nb::object materialize_window_times(TSWDataView &window)
    {
        const ValueTypeRef binding = window.layout().time_binding;
        const ValueArraySource source{
            .owner = &window,
            .size = window.size(),
            .element_at = [](const void *owner, std::size_t index) -> const void * {
                return static_cast<const TSWDataView *>(owner)->time_value_at(index).data();
            },
        };
        return binding.ops_ref().to_python_buffer(binding, source);
    }

    /**
     * ONE compute/sink operator for ANY arity (Howard's review: per-arity
     * stubs do not scale): the argument ports pack into a STRUCTURAL
     * un-named TSB, wiring-time SCALARS ride a list-of-Any scalar, and the
     * LAYOUT string (part of node identity) maps the python call positions:
     * ``t`` = next ts field, ``s`` = next scalar, ``S`` = STATE namespace,
     * ``c`` = CLOCK, ``d`` = SCHEDULER, ``e`` = EvaluationEngineApi,
     * ``n`` = NODE. All ts
     * fields must hold values before the python function is called (the
     * all-valid gate).
     */
    struct PyCallLeaseState;

    struct PyStateRef
    {
        PyObject         *ns{nullptr};   ///< the lazily created per-node Python state value
        PyCallLeaseState *call_lease{nullptr};
        friend bool operator==(const PyStateRef &, const PyStateRef &) noexcept = default;
    };

    struct PyScheduler
    {
        NodeScheduler scheduler;
    };

    [[nodiscard]] inline nb::object py_state_value(PyStateRef &state,
                                                   nb::handle factory)
    {
        if (state.ns == nullptr)
        {
            nb::object value = nb::borrow<nb::object>(factory)();
            state.ns = value.release().ptr();
        }
        return nb::borrow(nb::handle(state.ns));
    }

    [[nodiscard]] inline nb::object py_state_value(State<PyStateRef> &state,
                                                   nb::handle factory)
    {
        PyStateRef ref = state.get();
        const bool created = ref.ns == nullptr;
        nb::object value = py_state_value(ref, factory);
        if (created) { state.set(ref); }
        return value;
    }

    [[nodiscard]] inline nb::object py_state_namespace(PyStateRef &state)
    {
        if (state.ns == nullptr)
        {
            nb::object factory = nb::module_::import_("hgraph._wiring._markers").attr("STATE");
            return py_state_value(state, factory);
        }
        return nb::borrow(nb::handle(state.ns));
    }

    [[nodiscard]] inline nb::object py_state_namespace(State<PyStateRef> &state)
    {
        PyStateRef ref = state.get();
        const bool created = ref.ns == nullptr;
        nb::object value = py_state_namespace(ref);
        if (created) { state.set(ref); }
        return value;
    }

    [[nodiscard]] inline nb::object py_typed_state(PyStateRef &state,
                                                    nb::handle factory)
    {
        return py_state_value(state, factory);
    }

    [[nodiscard]] inline nb::object py_typed_state(State<PyStateRef> &state,
                                                    nb::handle factory)
    {
        return py_state_value(state, factory);
    }

    /** Call-scope lifetime guard: python must not use a view after its eval. */
    struct PyTsGuard
    {
        bool          alive{true};
        std::uint64_t generation{0};
        std::size_t   deferred_users{0};
        bool          owner_released{false};
    };

    struct PyTsLease
    {
        std::shared_ptr<PyTsGuard> guard{};
        std::uint64_t              generation{0};
        bool                       owns_guard_lifetime{false};

        [[nodiscard]] bool alive() const noexcept
        {
            return guard != nullptr && guard->alive && guard->generation == generation;
        }

        void require_alive(const char *message) const
        {
            if (!alive()) { throw std::logic_error(message); }
        }

        void invalidate() const noexcept
        {
            if (guard == nullptr) { return; }
            if (guard->generation == generation) { ++guard->generation; }
            if (owns_guard_lifetime)
            {
                guard->owner_released = true;
                if (guard->deferred_users == 0) { guard->alive = false; }
            }
        }

        void retain_for_deferred_call() const noexcept
        {
            if (guard != nullptr) { ++guard->deferred_users; }
        }

        void release_from_deferred_call() const noexcept
        {
            if (guard == nullptr || guard->deferred_users == 0) { return; }
            if (--guard->deferred_users == 0 && guard->owner_released)
            {
                guard->alive = false;
            }
        }
    };

    /** Python projection over the native live evaluation clock.

        Generator arguments retain their lease for the generator lifetime, so
        each resumed access observes the current graph cycle. For compatibility
        with the formerly value-like wrapper, a retained clock falls back to
        the snapshot captured when it was injected after its lease expires. */
    struct PyEvalClock
    {
        EvaluationClockView clock{};
        PyTsLease           lease{};
        DateTime            evaluation_time_snapshot{};
        DateTime            now_snapshot{};
        TimeDelta           cycle_time_snapshot{};
        DateTime            next_cycle_evaluation_time_snapshot{};

        PyEvalClock(EvaluationClockView clock_, PyTsLease lease_) noexcept
            : clock(clock_)
            , lease(std::move(lease_))
            , evaluation_time_snapshot(clock_.evaluation_time())
            , now_snapshot(clock_.now())
            , cycle_time_snapshot(clock_.cycle_time())
            , next_cycle_evaluation_time_snapshot(clock_.next_cycle_evaluation_time())
        {
        }

        [[nodiscard]] DateTime evaluation_time() const noexcept
        {
            return lease.alive() ? clock.evaluation_time() : evaluation_time_snapshot;
        }

        [[nodiscard]] DateTime now() const noexcept
        {
            return lease.alive() ? clock.now() : now_snapshot;
        }

        [[nodiscard]] TimeDelta cycle_time() const noexcept
        {
            return lease.alive() ? clock.cycle_time() : cycle_time_snapshot;
        }

        [[nodiscard]] DateTime next_cycle_evaluation_time() const noexcept
        {
            return lease.alive() ? clock.next_cycle_evaluation_time()
                                 : next_cycle_evaluation_time_snapshot;
        }
    };

    struct PyRuntimeGlobalState;

    /** Node-owned guard and injectable cache for general compute and sink callbacks. */
    struct PyCallLeaseState
    {
        std::shared_ptr<PyTsGuard> guard{std::make_shared<PyTsGuard>()};
        PyObject                  *runtime_global_state{nullptr};
        PyRuntimeGlobalState      *runtime_global_state_view{nullptr};
    };

    [[nodiscard]] inline PyTsLease py_ts_lease_for_node(State<PyStateRef> &state)
    {
        PyStateRef ref = state.get();
        if (ref.call_lease == nullptr)
        {
            ref.call_lease = new PyCallLeaseState{};
            state.set(ref);
        }
        auto &guard = ref.call_lease->guard;
        return PyTsLease{
            .guard = guard,
            .generation = ++guard->generation,
            .owns_guard_lifetime = false,
        };
    }

    inline void py_release_call_lease(PyStateRef &state) noexcept
    {
        if (state.call_lease == nullptr) { return; }
        auto &guard = state.call_lease->guard;
        guard->owner_released = true;
        if (guard->deferred_users == 0) { guard->alive = false; }
        if (state.call_lease->runtime_global_state != nullptr)
        {
            nb::steal(nb::handle(state.call_lease->runtime_global_state));
        }
        delete state.call_lease;
        state.call_lease = nullptr;
    }

    inline void py_release_state(State<PyStateRef> &state)
    {
        PyStateRef ref = state.get();
        if (ref.ns != nullptr) { nb::steal(nb::handle(ref.ns)); }
        py_release_call_lease(ref);
        state.set(PyStateRef{});
    }

    inline void py_release_state(PyStateRef &state)
    {
        if (state.ns != nullptr) { nb::steal(nb::handle(state.ns)); }
        py_release_call_lease(state);
        state = PyStateRef{};
    }

    struct PyRuntimeGlobalState
    {
        GlobalStateView state;
        PyTsLease      lease;

        [[nodiscard]] GlobalStateView checked() const
        {
            lease.require_alive(
                "a GlobalState view was accessed outside its node's evaluation");
            return state;
        }
    };

    /** Callback-scoped Python projection over the owning graph's trait chain. */
    struct PyTraits
    {
        TraitsView traits;
        PyTsLease  lease;

        [[nodiscard]] TraitsView checked() const
        {
            lease.require_alive(
                "a Traits view was accessed outside its node's evaluation");
            return traits;
        }
    };

    /** Narrow, callback-scoped Python projection over the native run logger. */
    struct PyLogger
    {
        LoggerView logger;
        PyTsLease lease;

        [[nodiscard]] LoggerView checked() const
        {
            lease.require_alive(
                "a Logger view was accessed outside its node's evaluation");
            return logger;
        }
    };

    /** Read-only scheduler projection used by Node diagnostics. */
    struct PySchedulerState
    {
        NodeScheduler scheduler;
        PyTsLease     lease;

        [[nodiscard]] const NodeScheduler &checked() const
        {
            lease.require_alive(
                "a SchedulerState view was accessed outside its node's evaluation");
            return scheduler;
        }
    };

    [[nodiscard]] inline PyTsLease py_ts_lease_for_call()
    {
        auto guard = std::make_shared<PyTsGuard>();
        return PyTsLease{
            .guard = guard,
            .generation = ++guard->generation,
            .owns_guard_lifetime = true,
        };
    }

    [[nodiscard]] inline nb::object
    py_runtime_global_state_for_call(std::string_view layout,
                                     GlobalStateView state,
                                     const PyTsLease &lease,
                                     PyCallLeaseState *call_state = nullptr)
    {
        if (layout.find('g') == std::string_view::npos) { return nb::object{}; }
        if (call_state != nullptr)
        {
            if (call_state->runtime_global_state == nullptr)
            {
                nb::object wrapper = nb::cast(PyRuntimeGlobalState{state, lease});
                call_state->runtime_global_state_view =
                    nb::inst_ptr<PyRuntimeGlobalState>(wrapper.ptr());
                call_state->runtime_global_state = wrapper.release().ptr();
            }
            else
            {
                // The cached wrapper and its guard are both node-owned.  Only
                // the call generation changes between evaluations; replacing
                // the whole lease here would add shared_ptr reference-count
                // traffic to every injected GlobalState access.
                call_state->runtime_global_state_view->state = state;
                call_state->runtime_global_state_view->lease.generation = lease.generation;
            }
            return nb::borrow(nb::handle(call_state->runtime_global_state));
        }
        return nb::cast(PyRuntimeGlobalState{state, lease});
    }

    /** Call-scoped Python projection over the native engine-control view. */
    struct PyEvaluationEngineApi
    {
        EngineControlView engine;
        PyTsLease         lease;

        [[nodiscard]] EngineControlView checked() const
        {
            lease.require_alive(
                "an EvaluationEngineApi view was accessed outside its node's evaluation");
            if (!engine.valid())
            {
                throw std::logic_error("the active graph has no evaluation engine");
            }
            return engine;
        }
    };

    [[nodiscard]] inline std::vector<std::size_t> py_graph_id(GraphView graph)
    {
        std::vector<std::size_t> result;
        while (graph.valid() && graph.is_nested())
        {
            NodeView parent = graph.as_nested().parent_node();
            result.push_back(parent.node_index());
            graph = parent.graph();
        }
        std::ranges::reverse(result);
        return result;
    }

    /** Callback-scoped Python projection over a native graph. */
    struct PyGraph
    {
        GraphPtr  graph;
        PyTsLease lease;

        [[nodiscard]] GraphView checked() const
        {
            lease.require_alive("a Graph view was accessed outside its lifecycle callback");
            if (!graph.has_value()) { throw std::logic_error("the active graph is unavailable"); }
            return GraphView{graph};
        }

        [[nodiscard]] std::vector<std::size_t> graph_id() const
        {
            return py_graph_id(checked());
        }
    };

    /** Callback-scoped Python projection over the current native node. */
    struct PyNode
    {
        NodePtr       node;
        NodeScheduler scheduler;
        PyTsLease     lease;

        [[nodiscard]] NodeView checked() const
        {
            lease.require_alive("a Node view was accessed outside its node's evaluation");
            if (!node.has_value()) { throw std::logic_error("the active node is unavailable"); }
            return NodeView{node};
        }

        [[nodiscard]] std::vector<std::size_t> node_id() const
        {
            NodeView current = checked();
            std::vector<std::size_t> result = py_graph_id(current.graph());
            result.push_back(current.node_index());
            return result;
        }

        void notify_next_cycle() const
        {
            static_cast<void>(checked());
            scheduler.schedule(MIN_TD);
        }
    };

    [[nodiscard]] inline NodeScheduler
    py_scheduler_for_node(const NodeView &node, DateTime now)
    {
        if (!node.valid() || !node.has_scheduler()) { return {}; }
        const GraphExecutorView executor = node.graph().executor();
        const bool supports_wall_clock =
            executor.valid() &&
            executor.schema()->mode == GraphExecutorMode::RealTime;
        return NodeScheduler{
            node.scheduler_state(), node.graph_value(), node.node_index(), now,
            node.started(), node.evaluation_clock(), supports_wall_clock};
    }

    /**
     * The hgraph TimeSeries object handed to python user nodes: a LAZY,
     * C++-bound view over the node's live input - nothing converts unless
     * accessed. Kind-specific methods dispatch on the schema (TS/TSS/TSD/
     * TSL/TSB); child access returns child views sharing the same guard.
     */
    /** Mutable, call-scoped view of the node's own output (``_output``).

        All writes go through the native TSOutput mutation API. Child views
        share the callback guard, so Python cannot retain an output cursor
        beyond the evaluation that produced it. */
    struct PyOutput
    {
        TSOutputHandle handle;
        DateTime       now{};
        NodeScheduler  scheduler;
        PyTsLease      lease;
        /** Parent-relative identity retained for ``key_from_value`` when an
            invalid structural output child has no resolved data pointer. */
        nb::object     collection_key{};
        const void    *collection_identity{nullptr};

        [[nodiscard]] TSOutputView checked() const
        {
            lease.require_alive("an output view was accessed outside its node's evaluation");
            return handle.view(now);
        }

        [[nodiscard]] PyOutput collection_child(TSOutputHandle child,
                                                nb::object key) const
        {
            PyOutput result{
                std::move(child), now, scheduler, lease};
            result.collection_key      = std::move(key);
            result.collection_identity = checked().data_view().data();
            return result;
        }

        [[nodiscard]] bool valid() const
        {
            auto view = checked();
            return view.valid() && view.data_view().has_current_value();
        }

        [[nodiscard]] bool all_valid() const { return checked().all_valid(); }

        [[nodiscard]] DateTime last_modified_time() const
        {
            return checked().last_modified_time();
        }

        [[nodiscard]] bool is_reference() const { return checked().is_reference(); }

        [[nodiscard]] nb::object owning_node() const
        {
            const NodeView owner = checked().owner_node();
            return owner.valid()
                       ? nb::cast(PyNode{owner.pointer(), scheduler, lease})
                       : nb::none();
        }

        [[nodiscard]] nb::object owning_graph() const
        {
            const GraphView owner = checked().owner_graph();
            return owner.valid() ? nb::cast(PyGraph{owner.pointer(), lease}) : nb::none();
        }

        [[nodiscard]] nb::object value() const
        {
            auto view = checked();
            // Do not add TSTypeKind branches here. Every output storage
            // representation owns Python shaping through its TSDataOps table.
            return !view.valid() ? nb::none() : view.data_view().value_to_python();
        }

        void set_value(nb::object value) const
        {
            auto view = checked();
            if (value.is_none())
            {
                auto mutation = view.begin_mutation(now);
                static_cast<void>(mutation.invalidate());
                return;
            }
            Out<TsVar<"O">> out{std::move(view), now};
            apply_py_result(value, out);
        }

        [[nodiscard]] bool modified() const { return checked().modified(); }

        [[nodiscard]] nb::object delta_value() const
        {
            auto view = checked();
            nb::object delta = view.data_view().delta_value_to_python(now);
            nb::object &shape = delta_shaper_slot();
            return shape.is_valid() ? shape(delta) : delta;
        }

        [[nodiscard]] bool can_apply_result(nb::handle result) const
        {
            return result.is_none() || !checked().modified();
        }

        [[nodiscard]] PyOutput child(nb::handle key) const
        {
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    Value key_value = py_to_value_as(key, view.schema()->key_type());
                    auto  dict      = view.as_dict();
                    if (!dict.contains(key_value.view()))
                    {
                        throw nb::key_error("output key not found");
                    }
                    return collection_child(
                        dict.at(key_value.view()).handle(),
                        nb::borrow<nb::object>(key));
                }
                case TSTypeKind::TSL: {
                    auto list = view.as_list();
                    const auto index = nb::cast<std::size_t>(key);
                    return collection_child(list.at(index).handle(), nb::cast(index));
                }
                case TSTypeKind::TSB: {
                    auto bundle = view.as_bundle();
                    if (nb::isinstance<nb::str>(key))
                    {
                        return collection_child(
                            bundle.field(nb::cast<std::string>(key)).handle(),
                            nb::borrow<nb::object>(key));
                    }
                    const auto index = nb::cast<std::size_t>(key);
                    const auto &field = view.schema()->fields()[index];
                    return collection_child(
                        bundle.at(index).handle(), nb::str(field.name));
                }
                default: throw nb::type_error("this output kind has no children");
            }
        }

        [[nodiscard]] PyOutput get_or_create(nb::handle key) const
        {
            auto view = checked();
            if (view.schema()->kind != TSTypeKind::TSD)
            {
                throw nb::type_error("get_or_create: not a keyed output");
            }
            Value key_value = py_to_value_as(key, view.schema()->key_type());
            auto  mutation  = view.as_dict().begin_mutation(now);
            auto  child     = mutation.at(key_value.view());
            return collection_child(
                TSOutputHandle{view.output(), child}, nb::borrow<nb::object>(key));
        }

        void erase(nb::handle key) const
        {
            auto view = checked();
            if (view.schema()->kind != TSTypeKind::TSD)
            {
                throw nb::type_error("item deletion: not a keyed output");
            }
            Value key_value = py_to_value_as(key, view.schema()->key_type());
            static_cast<void>(view.as_dict().begin_mutation(now).erase(key_value.view()));
        }

        void set_child_value(nb::handle key, nb::object value) const
        {
            if (checked().schema()->kind == TSTypeKind::TSD)
            {
                get_or_create(key).set_value(std::move(value));
                return;
            }
            child(key).set_value(std::move(value));
        }

        [[nodiscard]] nb::object pop(nb::handle key) const
        {
            auto view = checked();
            if (view.schema()->kind != TSTypeKind::TSD)
            {
                throw nb::type_error("pop(): not a keyed output");
            }
            Value key_value = py_to_value_as(key, view.schema()->key_type());
            auto  dict      = view.as_dict();
            if (!dict.contains(key_value.view())) { return nb::none(); }
            PyOutput removed = collection_child(
                TSOutputHandle{dict.at(key_value.view())},
                nb::borrow<nb::object>(key));
            static_cast<void>(dict.begin_mutation(now).erase(key_value.view()));
            return nb::cast(std::move(removed));
        }

        void clear() const
        {
            auto view = checked();
            if (!view.data_view().clear_collection(now))
            {
                throw nb::type_error("clear: not a mutable collection output");
            }
        }

        void invalidate() const
        {
            auto mutation = checked().begin_mutation(now);
            static_cast<void>(mutation.invalidate());
        }

        [[nodiscard]] bool contains(nb::handle key) const
        {
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    Value key_value = py_to_value_as(key, view.schema()->key_type());
                    return view.as_dict().contains(key_value.view());
                }
                case TSTypeKind::TSS: {
                    Value element = py_to_value_as(key, view.schema()->value_schema->element_type);
                    return view.as_set().contains(element.view());
                }
                default: throw nb::type_error("contains: not a keyed collection output");
            }
        }

        [[nodiscard]] std::size_t size() const
        {
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSD: return view.as_dict().size();
                case TSTypeKind::TSS: return view.as_set().size();
                case TSTypeKind::TSL: return view.as_list().size();
                case TSTypeKind::TSB: return view.as_bundle().size();
                case TSTypeKind::TSW: return view.as_window().size();
                default: throw nb::type_error("this output kind has no size");
            }
        }

        [[nodiscard]] nb::object get(nb::handle key) const
        {
            auto view = checked();
            if (view.schema()->kind != TSTypeKind::TSD)
            {
                throw nb::type_error("get(): not a keyed output");
            }
            Value key_value = py_to_value_as(key, view.schema()->key_type());
            auto  dict      = view.as_dict();
            return dict.contains(key_value.view())
                       ? nb::cast(collection_child(
                             TSOutputHandle{dict.at(key_value.view())},
                             nb::borrow<nb::object>(key)))
                       : nb::none();
        }

        [[nodiscard]] PyOutput key_set() const
        {
            auto view = checked();
            if (view.schema()->kind != TSTypeKind::TSD)
            {
                throw nb::attribute_error("key_set");
            }
            return PyOutput{
                TSOutputHandle{view.as_dict().key_set()}, now, scheduler, lease};
        }

        [[nodiscard]] nb::list keys() const
        {
            nb::list result;
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = view.as_dict();
                    for (const ValueView &key : dict.keys())
                    {
                        result.append(value_to_py(key));
                    }
                    return result;
                }
                case TSTypeKind::TSL: {
                    const auto length = view.as_list().size();
                    for (std::size_t index = 0; index < length; ++index) { result.append(index); }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = view.as_bundle();
                    for (std::string_view key : bundle.keys())
                    {
                        result.append(nb::str(key.data(), key.size()));
                    }
                    return result;
                }
                default: throw nb::type_error("keys(): not an indexed output");
            }
        }

        [[nodiscard]] nb::list values() const
        {
            nb::list result;
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = view.as_dict();
                    for (auto &&[key, child] : dict.items())
                    {
                        result.append(collection_child(
                            TSOutputHandle{std::move(child)}, value_to_py(key)));
                    }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = view.as_list();
                    for (auto &&[index, child] : list.items())
                    {
                        result.append(collection_child(
                            TSOutputHandle{std::move(child)}, nb::cast(index)));
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = view.as_bundle();
                    for (auto &&[key, child] : bundle.items())
                    {
                        result.append(collection_child(
                            TSOutputHandle{std::move(child)},
                            nb::str(key.data(), key.size())));
                    }
                    return result;
                }
                case TSTypeKind::TSS: {
                    auto set = view.as_set();
                    for (const ValueView &element : set.values())
                    {
                        result.append(value_to_py(element));
                    }
                    return result;
                }
                default: throw nb::type_error("values(): not a collection output");
            }
        }

        [[nodiscard]] nb::list items() const
        {
            nb::list result;
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = view.as_dict();
                    for (auto &&[key, child] : dict.items())
                    {
                        nb::object py_key = value_to_py(key);
                        result.append(nb::make_tuple(
                            py_key,
                            collection_child(TSOutputHandle{std::move(child)}, py_key)));
                    }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = view.as_list();
                    for (auto &&[index, child] : list.items())
                    {
                        result.append(nb::make_tuple(
                            index,
                            collection_child(
                                TSOutputHandle{std::move(child)}, nb::cast(index))));
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = view.as_bundle();
                    for (auto &&[key, child] : bundle.items())
                    {
                        nb::object py_key = nb::str(key.data(), key.size());
                        result.append(nb::make_tuple(
                            py_key,
                            collection_child(TSOutputHandle{std::move(child)}, py_key)));
                    }
                    return result;
                }
                default: throw nb::type_error("items(): not an indexed output");
            }
        }

        [[nodiscard]] nb::list modified_keys() const
        {
            nb::list result;
            for (nb::handle item : modified_items()) { result.append(item[0]); }
            return result;
        }

        [[nodiscard]] nb::list modified_values() const
        {
            nb::list result;
            for (nb::handle item : modified_items()) { result.append(item[1]); }
            return result;
        }

        [[nodiscard]] nb::list modified_items() const
        {
            nb::list result;
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = view.as_dict();
                    for (auto &&[key, child] : dict.modified_items())
                    {
                        nb::object py_key = value_to_py(key);
                        result.append(nb::make_tuple(
                            py_key,
                            collection_child(TSOutputHandle{std::move(child)}, py_key)));
                    }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = view.as_list();
                    for (auto &&[index, child] : list.modified_items())
                    {
                        result.append(nb::make_tuple(
                            index,
                            collection_child(
                                TSOutputHandle{std::move(child)}, nb::cast(index))));
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = view.as_bundle();
                    for (auto &&[key, child] : bundle.modified_items())
                    {
                        nb::object py_key = nb::str(key.data(), key.size());
                        result.append(nb::make_tuple(
                            py_key,
                            collection_child(TSOutputHandle{std::move(child)}, py_key)));
                    }
                    return result;
                }
                default: throw nb::type_error("modified_items(): not an indexed output");
            }
        }

        [[nodiscard]] nb::list valid_keys() const
        {
            nb::list result;
            for (nb::handle item : valid_items()) { result.append(item[0]); }
            return result;
        }

        [[nodiscard]] nb::list valid_values() const
        {
            nb::list result;
            for (nb::handle item : valid_items()) { result.append(item[1]); }
            return result;
        }

        [[nodiscard]] nb::list valid_items() const
        {
            nb::list result;
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = view.as_dict();
                    for (auto &&[key, child] : dict.valid_items())
                    {
                        nb::object py_key = value_to_py(key);
                        result.append(nb::make_tuple(
                            py_key,
                            collection_child(TSOutputHandle{std::move(child)}, py_key)));
                    }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = view.as_list();
                    for (auto &&[index, child] : list.valid_items())
                    {
                        result.append(nb::make_tuple(
                            index,
                            collection_child(
                                TSOutputHandle{std::move(child)}, nb::cast(index))));
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = view.as_bundle();
                    for (auto &&[key, child] : bundle.valid_items())
                    {
                        nb::object py_key = nb::str(key.data(), key.size());
                        result.append(nb::make_tuple(
                            py_key,
                            collection_child(TSOutputHandle{std::move(child)}, py_key)));
                    }
                    return result;
                }
                default: throw nb::type_error("valid_items(): not an indexed output");
            }
        }

        /** True when this output is a dynamic ``TSL``, whose keys are indices
            and whose structural delta is tail truncation (RFC 0031). */
        [[nodiscard]] bool is_dynamic_list() const
        {
            const auto *schema = checked().schema();
            return schema != nullptr && schema->kind == TSTypeKind::TSL && schema->fixed_size() == 0;
        }

        [[nodiscard]] nb::list added_keys() const
        {
            nb::list result;
            auto view = checked();
            if (is_dynamic_list())
            {
                for (const std::size_t index : view.as_list().added_indices())
                {
                    result.append(nb::cast(index));
                }
                return result;
            }
            auto dict = view.as_dict();
            for (const ValueView &key : dict.added_keys()) { result.append(value_to_py(key)); }
            return result;
        }

        [[nodiscard]] nb::list added_values() const
        {
            nb::list result;
            auto view = checked();
            if (is_dynamic_list())
            {
                auto list = view.as_list();
                for (auto &&[index, child] : list.added_items())
                {
                    result.append(collection_child(
                        TSOutputHandle{std::move(child)}, nb::cast(index)));
                }
                return result;
            }
            auto dict = view.as_dict();
            for (auto &&[key, child] : dict.added_items())
            {
                result.append(collection_child(
                    TSOutputHandle{std::move(child)}, value_to_py(key)));
            }
            return result;
        }

        [[nodiscard]] nb::list added_items() const
        {
            nb::list result;
            auto view = checked();
            if (is_dynamic_list())
            {
                auto list = view.as_list();
                for (auto &&[index, child] : list.added_items())
                {
                    nb::object py_key = nb::cast(index);
                    result.append(nb::make_tuple(
                        py_key, collection_child(TSOutputHandle{std::move(child)}, py_key)));
                }
                return result;
            }
            auto dict = view.as_dict();
            for (auto &&[key, child] : dict.added_items())
            {
                nb::object py_key = value_to_py(key);
                result.append(nb::make_tuple(
                    py_key,
                    collection_child(TSOutputHandle{std::move(child)}, py_key)));
            }
            return result;
        }

        [[nodiscard]] nb::list removed_keys() const
        {
            nb::list result;
            auto     view = checked();
            if (is_dynamic_list())
            {
                for (const std::size_t index : view.as_list().removed_indices())
                {
                    result.append(nb::cast(index));
                }
                return result;
            }
            auto     dict = view.as_dict();
            for (const ValueView &key : dict.removed_keys()) { result.append(value_to_py(key)); }
            return result;
        }

        [[nodiscard]] nb::list removed_values() const
        {
            nb::list result;
            auto view = checked();
            if (is_dynamic_list())
            {
                auto list = view.as_list();
                for (auto &&[index, child] : list.removed_items())
                {
                    result.append(collection_child(
                        TSOutputHandle{std::move(child)}, nb::cast(index)));
                }
                return result;
            }
            auto dict = view.as_dict();
            for (auto &&[key, child] : dict.removed_items())
            {
                result.append(collection_child(
                    TSOutputHandle{std::move(child)}, value_to_py(key)));
            }
            return result;
        }

        [[nodiscard]] nb::list removed_items() const
        {
            nb::list result;
            auto view = checked();
            if (is_dynamic_list())
            {
                auto list = view.as_list();
                for (auto &&[index, child] : list.removed_items())
                {
                    nb::object py_key = nb::cast(index);
                    result.append(nb::make_tuple(
                        py_key, collection_child(TSOutputHandle{std::move(child)}, py_key)));
                }
                return result;
            }
            auto dict = view.as_dict();
            for (auto &&[key, child] : dict.removed_items())
            {
                nb::object py_key = value_to_py(key);
                result.append(nb::make_tuple(
                    py_key,
                    collection_child(TSOutputHandle{std::move(child)}, py_key)));
            }
            return result;
        }

        [[nodiscard]] nb::object set_added() const
        {
            nb::list items;
            auto view = checked();
            auto set  = view.as_set();
            for (const ValueView &element : set.added())
            {
                items.append(value_to_py(element));
            }
            return nb::steal(PyFrozenSet_New(items.ptr()));
        }

        [[nodiscard]] nb::object set_removed() const
        {
            nb::list items;
            auto view = checked();
            auto set  = view.as_set();
            for (const ValueView &element : set.removed())
            {
                items.append(value_to_py(element));
            }
            return nb::steal(PyFrozenSet_New(items.ptr()));
        }

        [[nodiscard]] bool was_added(nb::handle item) const
        {
            nb::object changes = set_added();
            const int result = PySequence_Contains(changes.ptr(), item.ptr());
            if (result < 0) { nb::raise_python_error(); }
            return result != 0;
        }

        [[nodiscard]] bool was_removed(nb::handle item) const
        {
            nb::object changes = set_removed();
            const int result = PySequence_Contains(changes.ptr(), item.ptr());
            if (result < 0) { nb::raise_python_error(); }
            return result != 0;
        }

        [[nodiscard]] nb::object key_from_value(const PyOutput &value) const
        {
            const auto identity = checked().data_view().data();
            if (value.collection_identity == identity &&
                value.collection_key.is_valid())
            {
                return nb::borrow<nb::object>(value.collection_key);
            }
            const auto target = value.checked().data_view().data();
            if (target == nullptr) { return nb::none(); }
            auto view = checked();
            if (view.schema()->kind == TSTypeKind::TSL)
            {
                auto list = view.as_list();
                for (std::size_t index = 0; index < list.size(); ++index)
                {
                    if (list[index].data_view().data() == target) { return nb::cast(index); }
                }
                return nb::none();
            }
            if (view.schema()->kind == TSTypeKind::TSB)
            {
                auto bundle = view.as_bundle();
                for (auto &&[key, child] : bundle.items())
                {
                    if (child.data_view().data() == target)
                    {
                        return nb::str(key.data(), key.size());
                    }
                }
                return nb::none();
            }
            if (view.schema()->kind == TSTypeKind::TSD)
            {
                auto dict = view.as_dict();
                for (auto &&[key, child] : dict.items())
                {
                    if (child.data_view().data() == target) { return value_to_py(key); }
                }
                return nb::none();
            }
            throw nb::type_error("key_from_value(): not an indexed output");
        }

        [[nodiscard]] nb::object window_size() const
        {
            auto view   = checked();
            if (view.schema()->kind != TSTypeKind::TSW)
            {
                throw nb::attribute_error("size");
            }
            auto window = view.as_window();
            return window.duration_based() ? nb::cast(window.time_range())
                                           : nb::cast(window.period());
        }

        [[nodiscard]] nb::object window_min_size() const
        {
            auto view   = checked();
            if (view.schema()->kind != TSTypeKind::TSW)
            {
                throw nb::attribute_error("min_size");
            }
            auto window = view.as_window();
            return window.duration_based() ? nb::cast(window.min_time_range())
                                           : nb::cast(window.min_period());
        }

        [[nodiscard]] nb::object value_times() const
        {
            auto view   = checked();
            if (view.schema()->kind != TSTypeKind::TSW)
            {
                throw nb::attribute_error("value_times");
            }
            auto window = view.as_window().data_view();
            return materialize_window_times(window);
        }

        [[nodiscard]] DateTime first_modified_time() const
        {
            auto view   = checked();
            if (view.schema()->kind != TSTypeKind::TSW)
            {
                throw nb::attribute_error("first_modified_time");
            }
            auto window = view.as_window();
            return window.first_modified_time();
        }

        [[nodiscard]] bool has_removed_value() const
        {
            auto view   = checked();
            if (view.schema()->kind != TSTypeKind::TSW)
            {
                throw nb::attribute_error("has_removed_value");
            }
            auto window = view.as_window().data_view();
            return window.has_removed_value(now);
        }

        [[nodiscard]] nb::object removed_value() const
        {
            auto view   = checked();
            if (view.schema()->kind != TSTypeKind::TSW)
            {
                throw nb::attribute_error("removed_value");
            }
            auto window = view.as_window().data_view();
            return window.has_removed_value(now) ? value_to_py(window.removed_value(now))
                                                 : nb::none();
        }

        [[nodiscard]] bool add(nb::handle value) const
        {
            auto view = checked();
            if (view.schema()->kind != TSTypeKind::TSS) { throw nb::type_error("add: not a set output"); }
            Value element = py_to_value_as(value, view.schema()->value_schema->element_type);
            return view.as_set().begin_mutation(now).add(element.view());
        }

        [[nodiscard]] bool remove(nb::handle value) const
        {
            auto view = checked();
            if (view.schema()->kind != TSTypeKind::TSS) { throw nb::type_error("remove: not a set output"); }
            Value element = py_to_value_as(value, view.schema()->value_schema->element_type);
            return view.as_set().begin_mutation(now).remove(element.view());
        }

    };

    /**
     * Read-only output endpoint projection used by input and node topology
     * diagnostics. Mutation remains available only through the explicit
     * ``_output``/``TS_OUT`` authoring views.
     */
    struct PyReadOnlyOutput
    {
        PyOutput output;

        [[nodiscard]] TSOutputView checked() const { return output.checked(); }
        [[nodiscard]] bool valid() const { return output.valid(); }
        [[nodiscard]] bool all_valid() const { return output.all_valid(); }
        [[nodiscard]] bool modified() const { return output.modified(); }
        [[nodiscard]] DateTime last_modified_time() const
        {
            return output.last_modified_time();
        }
        [[nodiscard]] bool is_reference() const { return output.is_reference(); }
        [[nodiscard]] nb::object owning_node() const { return output.owning_node(); }
        [[nodiscard]] nb::object owning_graph() const { return output.owning_graph(); }
        [[nodiscard]] nb::object value() const { return output.value(); }
        [[nodiscard]] nb::object delta_value() const { return output.delta_value(); }
        [[nodiscard]] nb::object window_size() const { return output.window_size(); }
        [[nodiscard]] nb::object window_min_size() const
        {
            return output.window_min_size();
        }
        [[nodiscard]] nb::object value_times() const { return output.value_times(); }
        [[nodiscard]] DateTime first_modified_time() const
        {
            return output.first_modified_time();
        }
        [[nodiscard]] bool has_removed_value() const
        {
            return output.has_removed_value();
        }
        [[nodiscard]] nb::object removed_value() const
        {
            return output.removed_value();
        }
        [[nodiscard]] bool contains(nb::handle key) const
        {
            return output.contains(key);
        }
        [[nodiscard]] std::size_t size() const { return output.size(); }
        [[nodiscard]] nb::list keys() const { return output.keys(); }

        [[nodiscard]] static PyReadOnlyOutput from_output(const PyOutput &source)
        {
            PyOutput copy{
                source.handle, source.now, source.scheduler, source.lease,
                source.collection_key, source.collection_identity};
            return PyReadOnlyOutput{std::move(copy)};
        }

        [[nodiscard]] PyReadOnlyOutput child(nb::handle key) const
        {
            return PyReadOnlyOutput{output.child(key)};
        }

        [[nodiscard]] nb::object get(nb::handle key) const
        {
            auto view = checked();
            if (view.schema()->kind != TSTypeKind::TSD)
            {
                throw nb::type_error("get(): not a keyed output");
            }
            Value key_value = py_to_value_as(key, view.schema()->key_type());
            if (!view.as_dict().contains(key_value.view())) { return nb::none(); }
            return nb::cast(child(key));
        }

        [[nodiscard]] nb::list values() const
        {
            nb::list result;
            for (nb::handle value : output.values())
            {
                if (nb::isinstance<PyOutput>(value))
                {
                    result.append(from_output(nb::cast<PyOutput &>(value)));
                }
                else { result.append(value); }
            }
            return result;
        }

        [[nodiscard]] static nb::list read_only_values(nb::list source)
        {
            nb::list result;
            for (nb::handle value : source)
            {
                if (nb::isinstance<PyOutput>(value))
                {
                    result.append(from_output(nb::cast<PyOutput &>(value)));
                }
                else { result.append(value); }
            }
            return result;
        }

        [[nodiscard]] static nb::list read_only_items(nb::list source)
        {
            nb::list result;
            for (nb::handle item : source)
            {
                nb::tuple pair = nb::borrow<nb::tuple>(item);
                result.append(nb::make_tuple(
                    nb::borrow<nb::object>(pair[0]),
                    from_output(nb::cast<PyOutput &>(pair[1]))));
            }
            return result;
        }

        [[nodiscard]] nb::list items() const
        {
            return read_only_items(output.items());
        }

        [[nodiscard]] nb::list modified_keys() const
        {
            return output.modified_keys();
        }
        [[nodiscard]] nb::list modified_values() const
        {
            return read_only_values(output.modified_values());
        }
        [[nodiscard]] nb::list modified_items() const
        {
            return read_only_items(output.modified_items());
        }
        [[nodiscard]] nb::list valid_keys() const { return output.valid_keys(); }
        [[nodiscard]] nb::list valid_values() const
        {
            return read_only_values(output.valid_values());
        }
        [[nodiscard]] nb::list valid_items() const
        {
            return read_only_items(output.valid_items());
        }
        [[nodiscard]] nb::list added_keys() const { return output.added_keys(); }
        [[nodiscard]] nb::list added_values() const
        {
            return read_only_values(output.added_values());
        }
        [[nodiscard]] nb::list added_items() const
        {
            return read_only_items(output.added_items());
        }
        [[nodiscard]] nb::list removed_keys() const
        {
            return output.removed_keys();
        }
        [[nodiscard]] nb::list removed_values() const
        {
            return read_only_values(output.removed_values());
        }
        [[nodiscard]] nb::list removed_items() const
        {
            return read_only_items(output.removed_items());
        }
        [[nodiscard]] nb::object set_added() const { return output.set_added(); }
        [[nodiscard]] nb::object set_removed() const { return output.set_removed(); }
        [[nodiscard]] bool was_added(nb::handle item) const
        {
            return output.was_added(item);
        }
        [[nodiscard]] bool was_removed(nb::handle item) const
        {
            return output.was_removed(item);
        }

        [[nodiscard]] PyReadOnlyOutput key_set() const
        {
            return PyReadOnlyOutput{output.key_set()};
        }

        [[nodiscard]] nb::object key_from_value(const PyReadOnlyOutput &value) const
        {
            return output.key_from_value(value.output);
        }
    };

    /** Mutable, call-scoped view over a node's C++ recordable-state output. */
    struct PyRecordableState
    {
        TSOutputHandle handle;
        DateTime       now{};
        PyTsLease      lease;

        [[nodiscard]] TSOutputView checked() const
        {
            lease.require_alive(
                "a recordable-state view was accessed outside its node's evaluation");
            return handle.view(now);
        }

        [[nodiscard]] bool valid() const
        {
            auto view = checked();
            return view.valid() && view.data_view().has_current_value();
        }

        [[nodiscard]] bool modified() const { return checked().modified(); }

        [[nodiscard]] nb::object value() const
        {
            auto view = checked();
            // Recordable-state TSBs use a conventional child named "value"
            // as their editable payload view. This is navigation, not Python
            // value shaping; all ordinary snapshots still use TSDataOps.
            if (view.schema()->kind == TSTypeKind::TSB)
            {
                auto bundle = view.as_bundle();
                if (bundle.has_field("value"))
                {
                    auto child = bundle.field("value");
                    return nb::cast(PyRecordableState{child.handle(), now, lease});
                }
            }
            return view.valid() ? view.data_view().value_to_python() : nb::none();
        }

        void set_value(nb::handle value) const
        {
            auto  view  = checked();
            if (view.schema()->kind == TSTypeKind::TSB)
            {
                auto bundle = view.as_bundle();
                if (bundle.has_field("value"))
                {
                    auto child = bundle.field("value");
                    PyRecordableState{child.handle(), now, lease}.set_value(value);
                    return;
                }
            }
            Value delta = py_to_delta(value, view.schema());
            apply_delta(view, delta.view());
        }

        [[nodiscard]] PyRecordableState child(nb::handle key) const
        {
            auto view = checked();
            switch (view.schema()->kind)
            {
                case TSTypeKind::TSB: {
                    auto bundle = view.as_bundle();
                    TSOutputView child = nb::isinstance<nb::str>(key)
                                             ? bundle.field(nb::cast<std::string>(key))
                                             : bundle.at(nb::cast<std::size_t>(key));
                    return PyRecordableState{child.handle(), now, lease};
                }
                case TSTypeKind::TSL: {
                    auto list = view.as_list();
                    TSOutputView child = list.at(nb::cast<std::size_t>(key));
                    return PyRecordableState{child.handle(), now, lease};
                }
                default:
                    throw nb::type_error(
                        "recordable-state value has no statically addressable children");
            }
        }
    };

    struct PyTimeSeries
    {
        TSInputView       view;
        PyTsLease         lease;
        TSDataStorageRef<> evaluation_data{};
        const TSDataOps   *python_value_ops{nullptr};
        /** ``TSD.key_set`` is a zero-copy observational projection over the
            parent input.  Keeping the parent TSInputView preserves ownership,
            sampled-transition and callback-lifetime behaviour while the
            public kind and set methods dispatch to its native key-set view. */
        bool               key_set_projection{false};
        /** Parent-relative identity retained for ``key_from_value`` even when
            a structural child is currently invalid and therefore has no
            resolved target-data pointer. */
        nb::object         collection_key{};
        const void        *collection_identity{nullptr};
        /** Public child depth, excluding the bridge's hidden packed input root. */
        std::size_t        topology_depth{0};

        void refresh_evaluation_data(TSDataStorageRef<> storage,
                                     bool has_current_value)
        {
            evaluation_data = storage;
            if (!has_current_value || !storage.has_value())
            {
                python_value_ops = nullptr;
                return;
            }
            const auto *ops = storage.type_ref().ops();
            python_value_ops = ops != nullptr && ops->kind == TSTypeKind::TS ? ops : nullptr;
        }

        /** Throws when the view outlived its node's evaluation. */
        void require_alive() const
        {
            lease.require_alive("a TimeSeries view was accessed outside its node's evaluation");
        }

        [[nodiscard]] const TSInputView &checked() const
        {
            require_alive();
            return view;
        }

        [[nodiscard]] TSTypeKind kind() const
        {
            return key_set_projection ? TSTypeKind::TSS : checked().schema()->kind;
        }

        [[nodiscard]] bool is_reference() const
        {
            return !key_set_projection && checked().is_reference();
        }

        [[nodiscard]] TSDInputView projected_dict() const
        {
            if (!key_set_projection)
            {
                throw nb::type_error("this time-series is not a dictionary key-set projection");
            }
            return checked().as_dict();
        }

        [[nodiscard]] static nb::object python_set(nb::list items, bool frozen)
        {
            return frozen ? nb::steal(PyFrozenSet_New(items.ptr()))
                          : nb::steal(PySet_New(items.ptr()));
        }

        [[nodiscard]] PyTimeSeries collection_child(TSInputView child,
                                                    nb::object key) const
        {
            PyTimeSeries result{std::move(child), lease};
            result.collection_key      = std::move(key);
            result.collection_identity = checked().data_view().data();
            result.topology_depth      = topology_depth + 1;
            return result;
        }

        [[nodiscard]] nb::object owning_node() const
        {
            const auto    &current = checked();
            const NodeView owner   = current.consumer_node();
            if (!owner.valid()) { return nb::none(); }
            NodeScheduler scheduler =
                py_scheduler_for_node(owner, current.evaluation_time());
            return nb::cast(PyNode{owner.pointer(), scheduler, lease});
        }

        [[nodiscard]] nb::object owning_graph() const
        {
            const GraphView owner = checked().consumer_graph();
            return owner.valid() ? nb::cast(PyGraph{owner.pointer(), lease}) : nb::none();
        }

        [[nodiscard]] bool has_parent_input() const
        {
            return topology_depth != 0 && checked().has_parent_input();
        }

        [[nodiscard]] nb::object parent_input() const
        {
            if (!has_parent_input()) { return nb::none(); }
            PyTimeSeries parent{checked().parent_input(), lease};
            parent.topology_depth = topology_depth - 1;
            return nb::cast(std::move(parent));
        }

        [[nodiscard]] bool bound() const { return checked().bound(); }

        [[nodiscard]] bool has_peer() const
        {
            const auto &current = checked();
            return current.is_bindable() && current.bound();
        }

        [[nodiscard]] nb::object bound_output() const
        {
            const auto &current = checked();
            if (!current.is_bindable() || !current.bound()) { return nb::none(); }
            TSOutputView target = current.bound_output();
            if (!target.bound()) { return nb::none(); }
            return nb::cast(PyReadOnlyOutput{PyOutput{
                target.handle(), current.evaluation_time(), NodeScheduler{}, lease}});
        }

        [[nodiscard]] nb::object value() const
        {
            if (key_set_projection)
            {
                return projected_dict().data_view().key_set().base().value_to_python();
            }
            if (python_value_ops != nullptr)
            {
                // The argument assembler already proved that this atomic TS
                // has a current value. Retain that fact for the callback so
                // its common ``ts.value`` read avoids both a schema lookup and
                // a duplicate has-current-value dispatch.
                require_alive();
                return python_value_ops->to_python_impl(
                    python_value_ops->context, evaluation_data.data());
            }
            const auto &v = checked();
            const auto *schema = v.schema();
            if (schema != nullptr && schema->kind == TSTypeKind::REF)
            {
                // A REF input's value is the REFERENCE - TSInputView::
                // reference() reads the to-REF alternative's populated value
                // (peered at the true upstream output).
                return nb::cast(python_bridge::PyOpaqueRef{Value{v.reference()}, v.evaluation_time()});
            }
            // Structural shape is entirely a TSDataOps concern. The facade
            // has one erased operation regardless of TSD/TSL/TSB nesting.
            return v.value_to_python();
        }

        [[nodiscard]] nb::object delta_value() const
        {
            if (key_set_projection)
            {
                nb::object canonical = projected_dict().data_view().key_set().base()
                                           .delta_value_to_python(checked().evaluation_time());
                nb::object &shape = delta_shaper_slot();
                return shape.is_valid() ? shape(canonical) : std::move(canonical);
            }
            const auto &ts = checked();
            if (ts.schema() != nullptr && ts.schema()->kind == TSTypeKind::REF)
            {
                // REF is a time-series of reference TOKENS. Its input storage
                // is a link to the referenced output, so the generic data
                // delta would expose the referenced value instead. Present the
                // same token as value(), matching REF's whole-value delta.
                return nb::cast(
                    python_bridge::PyOpaqueRef{Value{ts.reference()}, ts.evaluation_time()});
            }
            // Use the input view rather than reading the bound target's raw
            // data. The input view accounts for a sampled REF rebind: when an
            // already-valid target is bound this cycle, its current value is
            // the input delta even though the target itself last ticked in an
            // earlier cycle.
            // As with value(), representation-specific delta recursion is an
            // erased TSData operation owned below the Python facade.
            nb::object delta = ts.delta_value_to_python();
            nb::object &shape = delta_shaper_slot();
            return shape.is_valid() ? shape(delta) : delta;
        }

        [[nodiscard]] bool modified() const
        {
            return key_set_projection ? projected_dict().structure_modified()
                                      : checked().modified();
        }
        [[nodiscard]] bool valid() const
        {
            if (!key_set_projection) { return checked().valid(); }
            auto data = projected_dict().data_view().key_set().base();
            return data.has_current_value();
        }
        [[nodiscard]] bool all_valid() const
        {
            return key_set_projection ? valid() : checked().all_valid();
        }
        [[nodiscard]] DateTime last_modified_time() const
        {
            if (!key_set_projection) { return checked().last_modified_time(); }
            return projected_dict().data_view().key_set().base().last_modified_time();
        }

        // --- TSS ---
        [[nodiscard]] nb::object added() const
        {
            nb::list items;
            if (key_set_projection)
            {
                auto dict = projected_dict();
                for (const ValueView &element : dict.added_keys())
                {
                    items.append(value_to_py(element));
                }
            }
            else
            {
                auto set = checked().as_set();
                for (const ValueView &element : set.added()) { items.append(value_to_py(element)); }
            }
            return python_set(std::move(items), true);
        }

        [[nodiscard]] nb::object removed() const
        {
            nb::list items;
            if (key_set_projection)
            {
                auto dict = projected_dict();
                for (const ValueView &element : dict.removed_keys())
                {
                    items.append(value_to_py(element));
                }
            }
            else
            {
                auto set = checked().as_set();
                for (const ValueView &element : set.removed()) { items.append(value_to_py(element)); }
            }
            return python_set(std::move(items), true);
        }

        [[nodiscard]] bool was_added(nb::handle item) const
        {
            nb::object changes = added();
            const int result = PySequence_Contains(changes.ptr(), item.ptr());
            if (result < 0) { nb::raise_python_error(); }
            return result != 0;
        }

        [[nodiscard]] bool was_removed(nb::handle item) const
        {
            nb::object changes = removed();
            const int result = PySequence_Contains(changes.ptr(), item.ptr());
            if (result < 0) { nb::raise_python_error(); }
            return result != 0;
        }

        // --- TSD / TSL / TSB children (share the guard) ---
        [[nodiscard]] PyTimeSeries child_at(nb::handle key) const
        {
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    Value key_value = py_to_value_as(key, ts.schema()->key_type());
                    auto dict = ts.as_dict();
                    if (!dict.contains(key_value.view()))
                    {
                        throw nb::key_error("time-series key not found");
                    }
                    return collection_child(
                        dict.at(key_value.view()), nb::borrow<nb::object>(key));
                }
                case TSTypeKind::TSL: {
                    auto list = ts.as_list();
                    const auto index = nb::cast<std::size_t>(key);
                    return collection_child(list[index], nb::cast(index));
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    if (nb::isinstance<nb::str>(key))
                    {
                        return collection_child(
                            bundle.field(nb::cast<std::string>(key)),
                            nb::borrow<nb::object>(key));
                    }
                    const auto index = nb::cast<std::size_t>(key);
                    const auto &field = ts.schema()->fields()[index];
                    return collection_child(
                        bundle[index], nb::str(field.name));
                }
                default: throw nb::type_error("this time-series kind has no children");
            }
        }

        [[nodiscard]] nb::object get(nb::handle key) const
        {
            const auto &ts = checked();
            if (ts.schema()->kind != TSTypeKind::TSD)
            {
                throw nb::type_error("get(): not a keyed time-series");
            }
            Value key_value = py_to_value_as(key, ts.schema()->key_type());
            auto  dict      = ts.as_dict();
            return dict.contains(key_value.view())
                       ? nb::cast(collection_child(
                             dict.at(key_value.view()), nb::borrow<nb::object>(key)))
                       : nb::none();
        }

        [[nodiscard]] PyTimeSeries key_set() const
        {
            const auto &ts = checked();
            if (ts.schema()->kind != TSTypeKind::TSD)
            {
                throw nb::attribute_error("key_set");
            }
            return PyTimeSeries{ts.borrowed_ref(), lease, {}, nullptr, true};
        }

        [[nodiscard]] std::size_t size() const
        {
            if (key_set_projection) { return projected_dict().size(); }
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: return ts.as_dict().size();
                case TSTypeKind::TSL: return ts.as_list().size();
                case TSTypeKind::TSB: return ts.as_bundle().size();
                case TSTypeKind::TSS: return ts.as_set().size();
                case TSTypeKind::TSW: return ts.as_window().size();
                default: throw nb::type_error("this time-series kind has no size");
            }
        }

        [[nodiscard]] nb::list keys() const
        {
            nb::list result;
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = ts.as_dict();
                    for (const ValueView &key : dict.keys()) { result.append(value_to_py(key)); }
                    return result;
                }
                case TSTypeKind::TSL: {
                    const auto length = ts.as_list().size();
                    for (std::size_t index = 0; index < length; ++index) { result.append(index); }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    for (std::string_view key : bundle.keys()) { result.append(nb::str(key.data(), key.size())); }
                    return result;
                }
                default: throw nb::type_error("keys(): not an indexed time-series");
            }
        }

        [[nodiscard]] nb::list modified_keys() const
        {
            nb::list result;
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = ts.as_dict();
                    for (const ValueView &key : dict.modified_keys()) { result.append(value_to_py(key)); }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = ts.as_list();
                    for (auto &&[index, child] : list.modified_items())
                    {
                        static_cast<void>(child);
                        result.append(index);
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    for (auto &&[key, child] : bundle.modified_items())
                    {
                        static_cast<void>(child);
                        result.append(nb::str(key.data(), key.size()));
                    }
                    return result;
                }
                default: throw nb::type_error("modified_keys(): not an indexed time-series");
            }
        }

        [[nodiscard]] nb::list modified_items() const
        {
            nb::list result;
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = ts.as_dict();
                    for (auto &&[key, child] : dict.modified_items())
                    {
                        result.append(nb::make_tuple(
                            value_to_py(key),
                            collection_child(std::move(child), value_to_py(key))));
                    }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = ts.as_list();
                    for (auto &&[index, child] : list.modified_items())
                    {
                        result.append(nb::make_tuple(
                            index, collection_child(std::move(child), nb::cast(index))));
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    for (auto &&[key, child] : bundle.modified_items())
                    {
                        nb::object py_key = nb::str(key.data(), key.size());
                        result.append(nb::make_tuple(
                            py_key,
                            collection_child(std::move(child), py_key)));
                    }
                    return result;
                }
                default: throw nb::type_error("modified_items(): not an indexed time-series");
            }
        }

        [[nodiscard]] nb::list modified_values() const
        {
            nb::list result;
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = ts.as_dict();
                    for (auto &&[key, child] : dict.modified_items())
                    {
                        result.append(collection_child(
                            std::move(child), value_to_py(key)));
                    }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = ts.as_list();
                    for (auto &&[index, child] : list.modified_items())
                    {
                        result.append(collection_child(
                            std::move(child), nb::cast(index)));
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    for (auto &&[key, child] : bundle.modified_items())
                    {
                        result.append(collection_child(
                            std::move(child), nb::str(key.data(), key.size())));
                    }
                    return result;
                }
                default: throw nb::type_error("modified_values(): not an indexed time-series");
            }
        }

        /** Child views in order (TSB fields / TSD entries / TSL elements). */
        [[nodiscard]] nb::list values() const
        {
            nb::list    result;
            if (key_set_projection)
            {
                auto dict = projected_dict();
                for (const ValueView &key : dict.keys())
                {
                    result.append(value_to_py(key));
                }
                return result;
            }
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = ts.as_dict();
                    for (const ValueView &key : dict.keys())
                    {
                        result.append(collection_child(dict.at(key), value_to_py(key)));
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    for (std::size_t index = 0; index < bundle.size(); ++index)
                    {
                        result.append(collection_child(
                            bundle[index], nb::str(ts.schema()->fields()[index].name)));
                    }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = ts.as_list();
                    for (std::size_t index = 0; index < list.size(); ++index)
                    {
                        result.append(collection_child(list[index], nb::cast(index)));
                    }
                    return result;
                }
                case TSTypeKind::TSS: {
                    auto set = ts.as_set();
                    for (const ValueView &element : set.values())
                    {
                        result.append(value_to_py(element));
                    }
                    return result;
                }
                default: throw nb::type_error("values(): not a container time-series");
            }
        }

        [[nodiscard]] nb::list items() const
        {
            nb::list result;
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = ts.as_dict();
                    for (auto &&[key, child] : dict.items())
                    {
                        nb::object py_key = value_to_py(key);
                        result.append(nb::make_tuple(
                            py_key,
                            collection_child(std::move(child), py_key)));
                    }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = ts.as_list();
                    for (auto &&[index, child] : list.items())
                    {
                        result.append(nb::make_tuple(
                            index, collection_child(std::move(child), nb::cast(index))));
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    for (auto &&[key, child] : bundle.items())
                    {
                        nb::object py_key = nb::str(key.data(), key.size());
                        result.append(nb::make_tuple(
                            py_key,
                            collection_child(std::move(child), py_key)));
                    }
                    return result;
                }
                default: throw nb::type_error("items(): not an indexed time-series");
            }
        }

        [[nodiscard]] nb::list valid_keys() const
        {
            nb::list result;
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = ts.as_dict();
                    for (const ValueView &key : dict.valid_keys()) { result.append(value_to_py(key)); }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = ts.as_list();
                    for (auto &&[index, child] : list.valid_items())
                    {
                        static_cast<void>(child);
                        result.append(index);
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    for (auto &&[key, child] : bundle.valid_items())
                    {
                        static_cast<void>(child);
                        result.append(nb::str(key.data(), key.size()));
                    }
                    return result;
                }
                default: throw nb::type_error("valid_keys(): not an indexed time-series");
            }
        }

        [[nodiscard]] nb::list valid_values() const
        {
            nb::list result;
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = ts.as_dict();
                    for (auto &&[key, child] : dict.valid_items())
                    {
                        result.append(collection_child(
                            std::move(child), value_to_py(key)));
                    }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = ts.as_list();
                    for (auto &&[index, child] : list.valid_items())
                    {
                        result.append(collection_child(
                            std::move(child), nb::cast(index)));
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    for (auto &&[key, child] : bundle.valid_items())
                    {
                        result.append(collection_child(
                            std::move(child), nb::str(key.data(), key.size())));
                    }
                    return result;
                }
                default: throw nb::type_error("valid_values(): not an indexed time-series");
            }
        }

        [[nodiscard]] nb::list valid_items() const
        {
            nb::list result;
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = ts.as_dict();
                    for (auto &&[key, child] : dict.valid_items())
                    {
                        nb::object py_key = value_to_py(key);
                        result.append(nb::make_tuple(
                            py_key,
                            collection_child(std::move(child), py_key)));
                    }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = ts.as_list();
                    for (auto &&[index, child] : list.valid_items())
                    {
                        result.append(nb::make_tuple(
                            index, collection_child(std::move(child), nb::cast(index))));
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    for (auto &&[key, child] : bundle.valid_items())
                    {
                        nb::object py_key = nb::str(key.data(), key.size());
                        result.append(nb::make_tuple(
                            py_key,
                            collection_child(std::move(child), py_key)));
                    }
                    return result;
                }
                default: throw nb::type_error("valid_items(): not an indexed time-series");
            }
        }

        /** True when this input is a dynamic ``TSL``, whose keys are indices
            and whose structural delta is tail truncation (RFC 0031). */
        [[nodiscard]] bool is_dynamic_list() const
        {
            const auto *schema = checked().schema();
            return schema != nullptr && schema->kind == TSTypeKind::TSL && schema->fixed_size() == 0;
        }

        [[nodiscard]] nb::list added_keys() const
        {
            nb::list result;
            if (is_dynamic_list())
            {
                auto list = checked().as_list();
                for (const std::size_t index : list.added_indices()) { result.append(nb::cast(index)); }
                return result;
            }
            auto dict = checked().as_dict();
            for (const ValueView &key : dict.added_keys()) { result.append(value_to_py(key)); }
            return result;
        }

        [[nodiscard]] nb::list added_values() const
        {
            nb::list result;
            if (is_dynamic_list())
            {
                auto list = checked().as_list();
                for (auto &&[index, child] : list.added_items())
                {
                    result.append(collection_child(std::move(child), nb::cast(index)));
                }
                return result;
            }
            auto dict = checked().as_dict();
            for (auto &&[key, child] : dict.added_items())
            {
                result.append(collection_child(
                    std::move(child), value_to_py(key)));
            }
            return result;
        }

        [[nodiscard]] nb::list added_items() const
        {
            nb::list result;
            if (is_dynamic_list())
            {
                auto list = checked().as_list();
                for (auto &&[index, child] : list.added_items())
                {
                    nb::object py_key = nb::cast(index);
                    result.append(nb::make_tuple(
                        py_key, collection_child(std::move(child), py_key)));
                }
                return result;
            }
            auto dict = checked().as_dict();
            for (auto &&[key, child] : dict.added_items())
            {
                nb::object py_key = value_to_py(key);
                result.append(nb::make_tuple(
                    py_key, collection_child(std::move(child), py_key)));
            }
            return result;
        }

        [[nodiscard]] nb::list removed_keys() const
        {
            nb::list result;
            if (is_dynamic_list())
            {
                auto list = checked().as_list();
                for (const std::size_t index : list.removed_indices()) { result.append(nb::cast(index)); }
                return result;
            }
            auto dict = checked().as_dict();
            for (const ValueView &key : dict.removed_keys()) { result.append(value_to_py(key)); }
            return result;
        }

        [[nodiscard]] nb::list removed_values() const
        {
            nb::list result;
            if (is_dynamic_list())
            {
                auto list = checked().as_list();
                for (auto &&[index, child] : list.removed_items())
                {
                    result.append(collection_child(std::move(child), nb::cast(index)));
                }
                return result;
            }
            auto dict = checked().as_dict();
            for (auto &&[key, child] : dict.removed_items())
            {
                result.append(collection_child(
                    std::move(child), value_to_py(key)));
            }
            return result;
        }

        [[nodiscard]] nb::list removed_items() const
        {
            nb::list result;
            if (is_dynamic_list())
            {
                auto list = checked().as_list();
                for (auto &&[index, child] : list.removed_items())
                {
                    nb::object py_key = nb::cast(index);
                    result.append(nb::make_tuple(
                        py_key, collection_child(std::move(child), py_key)));
                }
                return result;
            }
            auto dict = checked().as_dict();
            for (auto &&[key, child] : dict.removed_items())
            {
                nb::object py_key = value_to_py(key);
                result.append(nb::make_tuple(
                    py_key, collection_child(std::move(child), py_key)));
            }
            return result;
        }

        [[nodiscard]] nb::object key_from_value(const PyTimeSeries &value) const
        {
            const auto identity = checked().data_view().data();
            if (value.collection_identity == identity &&
                value.collection_key.is_valid())
            {
                return nb::borrow<nb::object>(value.collection_key);
            }
            const auto target = value.checked().data_view().data();
            if (target == nullptr) { return nb::none(); }
            const auto &ts = checked();
            if (ts.schema()->kind == TSTypeKind::TSL)
            {
                auto list = ts.as_list();
                for (std::size_t index = 0; index < list.size(); ++index)
                {
                    if (list[index].data_view().data() == target) { return nb::cast(index); }
                }
                return nb::none();
            }
            if (ts.schema()->kind == TSTypeKind::TSB)
            {
                auto bundle = ts.as_bundle();
                for (auto &&[key, child] : bundle.items())
                {
                    if (child.data_view().data() == target)
                    {
                        return nb::str(key.data(), key.size());
                    }
                }
                return nb::none();
            }
            if (ts.schema()->kind == TSTypeKind::TSD)
            {
                auto dict = ts.as_dict();
                for (auto &&[key, child] : dict.items())
                {
                    if (child.data_view().data() == target) { return value_to_py(key); }
                }
                return nb::none();
            }
            throw nb::type_error("key_from_value(): not an indexed time-series");
        }

        [[nodiscard]] nb::object window_size() const
        {
            const auto &ts = checked();
            if (ts.schema()->kind != TSTypeKind::TSW)
            {
                throw nb::attribute_error("size");
            }
            auto window = ts.as_window();
            return window.duration_based() ? nb::cast(window.time_range())
                                           : nb::cast(window.period());
        }

        [[nodiscard]] nb::object window_min_size() const
        {
            const auto &ts = checked();
            if (ts.schema()->kind != TSTypeKind::TSW)
            {
                throw nb::attribute_error("min_size");
            }
            auto window = ts.as_window();
            return window.duration_based() ? nb::cast(window.min_time_range())
                                           : nb::cast(window.min_period());
        }

        [[nodiscard]] nb::object value_times() const
        {
            const auto &ts = checked();
            if (ts.schema()->kind != TSTypeKind::TSW)
            {
                throw nb::attribute_error("value_times");
            }
            auto window = ts.as_window().data_view();
            return materialize_window_times(window);
        }

        [[nodiscard]] DateTime first_modified_time() const
        {
            const auto &ts = checked();
            if (ts.schema()->kind != TSTypeKind::TSW)
            {
                throw nb::attribute_error("first_modified_time");
            }
            return ts.as_window().first_modified_time();
        }

        [[nodiscard]] bool has_removed_value() const
        {
            const auto &ts = checked();
            if (ts.schema()->kind != TSTypeKind::TSW)
            {
                throw nb::attribute_error("has_removed_value");
            }
            return ts.as_window().has_removed_value();
        }

        [[nodiscard]] nb::object removed_value() const
        {
            const auto &ts = checked();
            if (ts.schema()->kind != TSTypeKind::TSW)
            {
                throw nb::attribute_error("removed_value");
            }
            auto window = ts.as_window();
            return window.has_removed_value() ? value_to_py(window.removed_value()) : nb::none();
        }

        [[nodiscard]] bool contains(nb::handle key) const
        {
            if (key_set_projection)
            {
                Value key_value = py_to_value_as(key, checked().schema()->key_type());
                return projected_dict().contains(key_value.view());
            }
            const auto &ts = checked();
            if (ts.schema()->kind == TSTypeKind::TSD)
            {
                Value key_value = py_to_value_as(key, ts.schema()->key_type());
                return ts.as_dict().contains(key_value.view());
            }
            if (ts.schema()->kind == TSTypeKind::TSS)
            {
                Value element = py_to_value_as(key, ts.schema()->value_schema->element_type);
                return ts.as_set().contains(element.view());
            }
            throw nb::type_error("contains: not a keyed time-series");
        }

        /** The python REMOVED sentinel, registered by the hgraph package at import. */
        [[nodiscard]] static nb::object &removed_slot() { return removed_sentinel_slot(); }

        [[nodiscard]] static nb::object removed_sentinel()
        {
            nb::object &slot = removed_slot();
            return slot.is_valid() ? slot : nb::none();
        }
    };
}  // namespace hgraph::python_bridge

template <>
struct std::hash<hgraph::python_bridge::PyStateRef>
{
    [[nodiscard]] std::size_t operator()(const hgraph::python_bridge::PyStateRef &ref) const noexcept
    {
        return std::hash<const void *>{}(ref.ns);
    }
};

namespace hgraph::static_schema_detail
{
    using python_bridge::PyStateRef;

    template <>
    struct scalar_name<PyStateRef>
    {
        static constexpr std::string_view value{"py_state"};
    };

}  // namespace hgraph::static_schema_detail

#endif  // HGRAPH_PYTHON_PY_RUNTIME_H
