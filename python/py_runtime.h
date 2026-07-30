/**
 * Runtime view structs handed to Python user nodes during evaluation:
 * the lazy TimeSeries/Output views, per-node python STATE, scheduler and
 * clock views, recordable state, and the guarded GlobalState view. See
 * docs/source/developer_guide/python_bridge.rst (GIL boundaries; transient
 * node/time-series views are call-scoped, while RuntimeGlobalState is scoped
 * to the complete graph execution).
 */
#ifndef HGRAPH_PYTHON_PY_RUNTIME_H
#define HGRAPH_PYTHON_PY_RUNTIME_H

#include "py_carriers.h"
#include "py_value_mirror.h"

#include <algorithm>

namespace hgraph::python_bridge
{
    /** Applies a python node's return value to its output (REF whole-move,
        TSS frozenset replace semantics, canonical-delta apply). Defined in
        py_nodes.cpp. */
    void apply_py_result(nb::handle result, Out<TsVar<"O">> &out);

    [[nodiscard]] inline nb::object materialize_tsb_value(
        const TSValueTypeMetaData *schema,
        nb::dict value)
    {
        const auto mapped = tsb_compound_value_registry().find(schema);
        if (mapped == tsb_compound_value_registry().end()) { return value; }

        const auto info = bundle_class_info_registry().find(mapped->second);
        if (info == bundle_class_info_registry().end() || !info->second.type.is_valid())
        {
            throw std::logic_error("TSB CompoundScalar class registration is incomplete");
        }

        const auto &class_info = info->second;
        nb::dict constructor_arguments;
        nb::object result;
        if (!class_info.requires_constructor)
        {
            result = nb::steal(class_info.allocator(
                reinterpret_cast<PyTypeObject *>(class_info.type.ptr()), 0));
            if (!result.is_valid()) { nb::raise_python_error(); }
        }
        for (std::size_t index = 0; index < class_info.field_names.size(); ++index)
        {
            const nb::handle key = class_info.field_names[index];
            nb::object field_value = nb::borrow<nb::object>(value[key]);
            if (class_info.requires_constructor)
            {
                if (class_info.constructor_fields[index])
                {
                    constructor_arguments[key] = field_value;
                }
            }
            else if (class_info.field_overrides[index].is_valid())
            {
                class_info.field_overrides[index](result, field_value);
            }
            else if (PyObject_GenericSetAttr(result.ptr(), key.ptr(), field_value.ptr()) != 0)
            {
                nb::raise_python_error();
            }
        }
        if (class_info.requires_constructor)
        {
            nb::tuple positional = nb::steal<nb::tuple>(PyTuple_New(0));
            result = nb::steal(PyObject_Call(
                class_info.type.ptr(), positional.ptr(), constructor_arguments.ptr()));
            if (!result.is_valid()) { nb::raise_python_error(); }
        }
        return result;
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
    struct PyStateRef
    {
        PyObject *ns{nullptr};   ///< a SimpleNamespace, lazily created (per-node python STATE)
        friend bool operator==(const PyStateRef &, const PyStateRef &) noexcept = default;
    };

    struct PyEvalClock
    {
        DateTime evaluation_time{};
        DateTime now{};
        TimeDelta cycle_time{};
        DateTime next_cycle_evaluation_time{};

        explicit PyEvalClock(EvaluationClockView clock) noexcept
            : evaluation_time(clock.evaluation_time()), now(clock.now()), cycle_time(clock.cycle_time()),
              next_cycle_evaluation_time(clock.next_cycle_evaluation_time())
        {
        }
    };

    struct PyScheduler
    {
        NodeScheduler scheduler;
    };

    [[nodiscard]] inline nb::object py_state_namespace(State<PyStateRef> &state)
    {
        PyStateRef ref = state.get();
        if (ref.ns == nullptr)
        {
            nb::object ns = nb::module_::import_("types").attr("SimpleNamespace")();
            ref.ns        = ns.release().ptr();
            state.set(ref);
        }
        return nb::borrow(nb::handle(ref.ns));
    }

    [[nodiscard]] inline nb::object py_state_namespace(PyStateRef &state)
    {
        if (state.ns == nullptr)
        {
            nb::object ns = nb::module_::import_("types").attr("SimpleNamespace")();
            state.ns = ns.release().ptr();
        }
        return nb::borrow(nb::handle(state.ns));
    }

    [[nodiscard]] inline nb::object py_typed_state(PyStateRef &state,
                                                    nb::handle factory)
    {
        if (state.ns == nullptr)
        {
            nb::object value = nb::borrow<nb::object>(factory)();
            state.ns = value.release().ptr();
        }
        return nb::borrow(nb::handle(state.ns));
    }

    [[nodiscard]] inline nb::object py_typed_state(State<PyStateRef> &state,
                                                    nb::handle factory)
    {
        PyStateRef ref = state.get();
        nb::object value = py_typed_state(ref, factory);
        state.set(ref);
        return value;
    }

    inline void py_release_state(State<PyStateRef> &state)
    {
        PyStateRef ref = state.get();
        if (ref.ns != nullptr)
        {
            nb::gil_scoped_acquire gil;
            nb::steal(nb::handle(ref.ns));   // drop the held reference
            state.set(PyStateRef{});
        }
    }

    inline void py_release_state(PyStateRef &state)
    {
        if (state.ns != nullptr)
        {
            nb::gil_scoped_acquire gil;
            nb::steal(nb::handle(state.ns));
            state = PyStateRef{};
        }
    }

    /** Call-scope lifetime guard: python must not use a view after its eval. */
    struct PyTsGuard
    {
        bool          alive{true};
        std::uint64_t generation{0};
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
            if (owns_guard_lifetime) { guard->alive = false; }
        }
    };

    struct PyRuntimeGlobalState
    {
        GlobalStateView            state;
        std::shared_ptr<PyTsGuard> guard;

        [[nodiscard]] GlobalStateView checked() const
        {
            if (guard == nullptr || !guard->alive)
            {
                throw std::logic_error("a GlobalState view was accessed outside its graph execution");
            }
            return state;
        }
    };

    /**
     * The Python projection of GlobalState is run-scoped, matching the native
     * GlobalState lifetime. PyWiring installs the exact Python object here
     * before releasing the GIL; user-node calls borrow it instead of creating
     * and publishing a new wrapper on every evaluation.
     */
    inline thread_local PyObject *py_active_runtime_global_state{nullptr};

    [[nodiscard]] inline std::shared_ptr<PyTsGuard> &py_active_runtime_guard()
    {
        static thread_local std::shared_ptr<PyTsGuard> guard{};
        return guard;
    }

    [[nodiscard]] inline bool py_has_active_runtime_global_state() noexcept
    {
        return py_active_runtime_global_state != nullptr;
    }

    [[nodiscard]] inline PyTsLease py_ts_lease_for_call()
    {
        auto &active_guard = py_active_runtime_guard();
        if (active_guard != nullptr)
        {
            return PyTsLease{
                .guard = active_guard,
                .generation = ++active_guard->generation,
                .owns_guard_lifetime = false,
            };
        }
        auto guard = std::make_shared<PyTsGuard>();
        return PyTsLease{
            .guard = guard,
            .generation = ++guard->generation,
            .owns_guard_lifetime = true,
        };
    }

    [[nodiscard]] inline nb::object
    py_runtime_global_state_for_call(GlobalStateView state,
                                     const std::shared_ptr<PyTsGuard> &fallback_guard)
    {
        if (py_active_runtime_global_state != nullptr)
        {
            return nb::borrow(nb::handle(py_active_runtime_global_state));
        }
        return nb::cast(PyRuntimeGlobalState{state, fallback_guard});
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

        void notify() const
        {
            static_cast<void>(checked());
            scheduler.schedule(scheduler.now());
        }

        void notify_next_cycle() const
        {
            static_cast<void>(checked());
            scheduler.schedule(MIN_TD);
        }
    };

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
        PyTsLease      lease;

        [[nodiscard]] TSOutputView checked() const
        {
            lease.require_alive("an output view was accessed outside its node's evaluation");
            return handle.view(now);
        }

        [[nodiscard]] bool valid() const
        {
            auto view = checked();
            return view.valid() && view.data_view().has_current_value();
        }

        [[nodiscard]] nb::object value() const
        {
            auto view = checked();
            if (!view.valid() || !view.data_view().has_current_value()) { return nb::none(); }
            if (view.schema() != nullptr && view.schema()->kind == TSTypeKind::TSB)
            {
                nb::dict value;
                auto bundle = view.as_bundle();
                for (std::size_t index = 0; index < view.schema()->field_count(); ++index)
                {
                    const auto &field = view.schema()->fields()[index];
                    auto child = bundle.at(index);
                    value[nb::str{field.name}] =
                        PyOutput{TSOutputHandle{child}, now, lease}.value();
                }
                return materialize_tsb_value(view.schema(), std::move(value));
            }
            return value_to_py(view.data_view().value());
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
                    return PyOutput{dict.at(key_value.view()).handle(), now, lease};
                }
                case TSTypeKind::TSL: {
                    auto list = view.as_list();
                    return PyOutput{list.at(nb::cast<std::size_t>(key)).handle(), now, lease};
                }
                case TSTypeKind::TSB: {
                    auto bundle = view.as_bundle();
                    TSOutputView result = nb::isinstance<nb::str>(key)
                                              ? bundle.field(nb::cast<std::string>(key))
                                              : bundle.at(nb::cast<std::size_t>(key));
                    return PyOutput{result.handle(), now, lease};
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
            return PyOutput{TSOutputHandle{view.output(), child}, now, lease};
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
                default: throw nb::type_error("this output kind has no size");
            }
        }

        [[nodiscard]] nb::list removed_keys() const
        {
            nb::list result;
            auto     view = checked();
            auto     dict = view.as_dict();
            for (const ValueView &key : dict.removed_keys()) { result.append(value_to_py(key)); }
            return result;
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
            if (view.schema()->kind == TSTypeKind::TSB)
            {
                auto bundle = view.as_bundle();
                if (bundle.has_field("value"))
                {
                    auto child = bundle.field("value");
                    return nb::cast(PyRecordableState{child.handle(), now, lease});
                }
            }
            return view.valid() && view.data_view().has_current_value()
                       ? value_to_py(view.data_view().value())
                       : nb::none();
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

        [[nodiscard]] TSTypeKind kind() const { return checked().schema()->kind; }

        [[nodiscard]] nb::object value() const
        {
            const auto &v = checked();
            const auto *schema = v.schema();
            if (schema != nullptr && schema->kind == TSTypeKind::TS)
            {
                // HOT PATH: atomic scalars dominate python-node reads.
                // value_to_python() folds the has_current_value check into
                // one ops dispatch; the invalid/None read falls out of it.
                TSDataView data = evaluation_data.has_value()
                                      ? TSDataView{evaluation_data}
                                      : v.data_view().borrowed_ref();
                if (!data.valid()) { return nb::none(); }
                // Python-value mirror (issue #204): a python-written output
                // read by python skips conversion — an lmt-matched entry IS
                // the current value (single writer per output). A miss marks
                // the output wanted so future python writes mirror it.
                if (auto *mirror = py_active_value_mirror;
                    mirror != nullptr && py_mirror_eligible(schema->value_schema))
                {
                    if (PyObject *hit = mirror->probe(data.data(), data.last_modified_time()))
                    {
                        return nb::steal<nb::object>(hit);
                    }
                }
                nb::object result = data.value_to_python();
                if (PySet_CheckExact(result.ptr()))
                {
                    // hgraph parity: a scalar set is a FROZENSET (TSS values
                    // stay mutable sets) - returning it to a TSS output means
                    // replace.
                    return nb::steal(PyFrozenSet_New(result.ptr()));
                }
                return result;
            }
            TSDataView data = evaluation_data.has_value()
                                  ? TSDataView{evaluation_data}
                                  : v.data_view().borrowed_ref();
            if (!data.valid() || !data.has_current_value())
            {
                return nb::none();   // hgraph: invalid reads as None
            }
            if (schema != nullptr && schema->kind == TSTypeKind::TSL)
            {
                // hgraph parity: a TSL's value is a tuple of child values
                // (invalid children read as None).
                auto list = const_cast<TSInputView &>(v).as_list();
                nb::list out;
                for (auto &&[index, child] : list.items())
                {
                    static_cast<void>(index);
                    out.append(child.valid() ? value_to_py(child.value()) : nb::none());
                }
                return nb::tuple(out);
            }
            if (schema != nullptr && schema->kind == TSTypeKind::REF)
            {
                // A REF input's value is the REFERENCE - TSInputView::
                // reference() reads the to-REF alternative's populated value
                // (peered at the true upstream output).
                return nb::cast(python_bridge::PyOpaqueRef{Value{v.reference()}, v.evaluation_time()});
            }
            if (schema != nullptr && schema->kind == TSTypeKind::TSB)
            {
                nb::dict result;
                auto bundle = const_cast<TSInputView &>(v).as_bundle();
                for (std::size_t index = 0; index < schema->field_count(); ++index)
                {
                    const auto &field = schema->fields()[index];
                    auto child = bundle.at(index);
                    result[nb::str{field.name}] =
                        PyTimeSeries{std::move(child), lease}.value();
                }
                return materialize_tsb_value(schema, std::move(result));
            }
            return data.value_to_python();   // TS handled on the hot path above
        }

        [[nodiscard]] nb::object delta_value() const
        {
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
            nb::object delta = value_to_py(ts.delta_value());
            nb::object &shape = delta_shaper_slot();
            return shape.is_valid() ? shape(delta) : delta;
        }

        [[nodiscard]] bool modified() const { return checked().modified(); }
        [[nodiscard]] bool valid() const { return checked().valid(); }
        [[nodiscard]] bool all_valid() const { return checked().all_valid(); }
        [[nodiscard]] DateTime last_modified_time() const { return checked().last_modified_time(); }

        // --- TSS ---
        [[nodiscard]] nb::object added() const
        {
            nb::list items;
            auto set = checked().as_set();
            for (const ValueView &element : set.added()) { items.append(value_to_py(element)); }
            return nb::steal(PyFrozenSet_New(nb::list(items).ptr()));
        }

        [[nodiscard]] nb::object removed() const
        {
            nb::list items;
            auto set = checked().as_set();
            for (const ValueView &element : set.removed()) { items.append(value_to_py(element)); }
            return nb::steal(PyFrozenSet_New(nb::list(items).ptr()));
        }

        // --- TSD / TSL / TSB children (share the guard) ---
        [[nodiscard]] PyTimeSeries child_at(nb::handle key) const
        {
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    Value key_value = py_to_value_as(key, ts.schema()->key_type());
                    return PyTimeSeries{ts.as_dict().at(key_value.view()), lease};
                }
                case TSTypeKind::TSL: {
                    auto list = ts.as_list();
                    return PyTimeSeries{list[nb::cast<std::size_t>(key)], lease};
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    if (nb::isinstance<nb::str>(key))
                    {
                        return PyTimeSeries{bundle.field(nb::cast<std::string>(key)), lease};
                    }
                    return PyTimeSeries{bundle[nb::cast<std::size_t>(key)], lease};
                }
                default: throw nb::type_error("this time-series kind has no children");
            }
        }

        [[nodiscard]] std::size_t size() const
        {
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: return ts.as_dict().size();
                case TSTypeKind::TSL: return ts.as_list().size();
                case TSTypeKind::TSB: return ts.as_bundle().size();
                case TSTypeKind::TSS: return ts.as_set().size();
                default: throw nb::type_error("this time-series kind has no size");
            }
        }

        [[nodiscard]] nb::list keys() const
        {
            nb::list result;
            auto dict = checked().as_dict();
            for (const ValueView &key : dict.keys()) { result.append(value_to_py(key)); }
            return result;
        }

        [[nodiscard]] nb::list modified_keys() const
        {
            nb::list result;
            auto dict = checked().as_dict();
            for (const auto &[key, child] : dict.modified_items()) { result.append(value_to_py(key)); }
            return result;
        }

        [[nodiscard]] nb::list modified_items() const
        {
            nb::list result;
            auto dict = checked().as_dict();
            for (auto &&[key, child] : dict.modified_items())
            {
                result.append(nb::make_tuple(value_to_py(key), PyTimeSeries{std::move(child), lease}));
            }
            return result;
        }

        [[nodiscard]] nb::list modified_values() const
        {
            nb::list result;
            auto dict = checked().as_dict();
            for (auto &&[key, child] : dict.modified_items())
            {
                static_cast<void>(key);
                result.append(PyTimeSeries{std::move(child), lease});
            }
            return result;
        }

        /** Child views in order (TSB fields / TSD entries / TSL elements). */
        [[nodiscard]] nb::list values() const
        {
            nb::list    result;
            const auto &ts = checked();
            switch (ts.schema()->kind)
            {
                case TSTypeKind::TSD: {
                    auto dict = ts.as_dict();
                    for (const ValueView &key : dict.keys())
                    {
                        result.append(nb::cast(PyTimeSeries{dict.at(key), lease}));
                    }
                    return result;
                }
                case TSTypeKind::TSB: {
                    auto bundle = ts.as_bundle();
                    for (std::size_t index = 0; index < bundle.size(); ++index)
                    {
                        result.append(nb::cast(PyTimeSeries{bundle[index], lease}));
                    }
                    return result;
                }
                case TSTypeKind::TSL: {
                    auto list = ts.as_list();
                    for (std::size_t index = 0; index < list.size(); ++index)
                    {
                        result.append(nb::cast(PyTimeSeries{list[index], lease}));
                    }
                    return result;
                }
                default: throw nb::type_error("values(): not a container time-series");
            }
        }

        [[nodiscard]] nb::list removed_keys() const
        {
            nb::list result;
            auto dict = checked().as_dict();
            for (const ValueView &key : dict.removed_keys()) { result.append(value_to_py(key)); }
            return result;
        }

        [[nodiscard]] bool contains(nb::handle key) const
        {
            const auto &ts = checked();
            if (ts.schema()->kind == TSTypeKind::TSD)
            {
                Value key_value = py_to_value_as(key, ts.schema()->key_type());
                return ts.as_dict().contains(key_value.view());
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
