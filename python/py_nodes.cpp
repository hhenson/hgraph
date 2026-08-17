/**
 * Python user nodes (@compute_node / @generator / @sink_node).
 * Ruling (refined 2026-08-01): graph-thread only, both modes; the GIL is
 * RELEASED on entering the native run loop. The Python bridge's executor
 * phase runner holds one ordinary ``nb::gil_scoped_acquire`` across the
 * complete root start phase, each complete evaluation cycle, and the complete
 * stop phase. These node trampolines therefore run under that phase guard and
 * do not acquire independently. Values cross the boundary through the module
 * converters.
 *
 * Everything here is TU-local by design (the node/op structs' typeid IS
 * node identity, but registration happens only in this file through
 * register_python_overloads()).
 */
#include "py_bindings.h"
#include "py_runtime.h"

#include <hgraph/lib/std/operators/impl/record_replay_memory_impl.h>
#include <hgraph/python/ts_data_conversion.h>

namespace nb = nanobind;
using namespace hgraph;
using namespace hgraph::python_bridge;

namespace hgraph::python_bridge {
/** Reserves scheduler state for fast Python nodes without eagerly building a
 *  full NodeScheduler on every callback.  Time-series ownership metadata
 *  materializes that view only when Python requests ``owning_node``. */
struct PyOwnerSchedulerSupport {};
} // namespace hgraph::python_bridge

namespace hgraph::static_node_detail {
template <>
struct is_scheduler_selector<python_bridge::PyOwnerSchedulerSupport>
    : std::true_type {};

template <> struct arg_provider<python_bridge::PyOwnerSchedulerSupport> {
  static python_bridge::PyOwnerSchedulerSupport get(const NodeView &, DateTime) {
    return {};
  }
};
} // namespace hgraph::static_node_detail

namespace hgraph::python_bridge {
void apply_py_result(nb::handle result, Out<TsVar<"O">> &out) {
  apply_python_result(static_cast<const TSOutputView &>(out), result);
}
} // namespace hgraph::python_bridge

namespace {
template <typename Fn> decltype(auto) translate_python_error(Fn &&fn) {
  try {
    return std::forward<Fn>(fn)();
  } catch (const nb::python_error &error) {
    // python_error::what() formats the active traceback and therefore
    // must be consumed before the surrounding GIL guard unwinds.
    throw std::runtime_error(error.what());
  }
}

/**
 * Python inputs share one packed structural port, so per-child activity is
 * applied before acquiring the GIL and invoking user code. Uppercase TS
 * layout markers are passive. Runtime scheduler events remain independent
 * of input activity, including for ``active=()`` nodes.
 */
/**
 * The node CONFIG string: the layout markers, optionally followed by
 * ``|name,name,...`` — the trailing layout entries called BY NAME
 * (python params after ``*args``: keyword-only, injectables declared
 * after the tail, and ``**kwargs`` expansions).
 */
struct PyCallShape {
  std::string_view layout;
  std::vector<std::string_view> kw_names;
  std::vector<std::string_view> input_names;
};

[[nodiscard]] PyCallShape parse_py_call_shape(std::string_view config) {
  PyCallShape shape;
  const auto metadata_separator = config.find(';');
  if (metadata_separator != std::string_view::npos) {
    std::string_view names = config.substr(metadata_separator + 1);
    config = config.substr(0, metadata_separator);
    while (!names.empty()) {
      const auto comma = names.find(',');
      shape.input_names.push_back(names.substr(0, comma));
      if (comma == std::string_view::npos) {
        break;
      }
      names.remove_prefix(comma + 1);
    }
  }
  const auto separator = config.find('|');
  if (separator == std::string_view::npos) {
    shape.layout = config;
    return shape;
  }
  shape.layout = config.substr(0, separator);
  std::string_view names = config.substr(separator + 1);
  while (!names.empty()) {
    const auto comma = names.find(',');
    shape.kw_names.push_back(names.substr(0, comma));
    if (comma == std::string_view::npos) {
      break;
    }
    names.remove_prefix(comma + 1);
  }
  return shape;
}

/**
 * Make python-node input activity REAL at the per-child link level.
 *
 * The packed ``args`` port is declaratively passive. At start we activate
 * each child per its layout marker, so per-child activity — including
 * runtime changes from python code — is the single subscription model.
 * Activity only controls subscription; it never schedules an evaluation.
 */
void py_apply_input_activity(std::string_view layout,
                             const TSInputView &args_view) {
  auto &args = const_cast<TSInputView &>(args_view);
  auto bundle = args.as_bundle();
  std::size_t ts_index = 0;
  for (const char kind : layout) {
    switch (kind) {
    case 't':
    case 'u':
    case 'a':
    case 'C':
      bundle[ts_index++].make_active();
      break;
    case 'T':
    case 'U':
    case 'A':
    case 'P':
      bundle[ts_index++].make_passive();
      break;
    default:
      break;
    }
  }
  args.make_passive(); // retain the invariant if a caller changed the root link
}

/**
 * A REF carries binding topology rather than target-value ticks. A directly
 * bound active REF can therefore be valid before graph start without ever
 * notifying its consumer. Request one explicit startup sample for that
 * case; this is independent of make_active() and does not mark the input
 * modified. Required invalid inputs remain guarded by py_assemble_args().
 */
void py_schedule_initial_reference_sample(std::string_view layout,
                                          const TSInputView &args,
                                          SingleShotScheduler scheduler) {
  auto bundle = args.as_bundle();
  std::size_t ts_index = 0;
  for (const char kind : layout) {
    switch (kind) {
    case 't':
    case 'u':
    case 'a':
    case 'C': {
      auto child = bundle[ts_index++];
      if (child.valid() && TypeRegistry::contains_ref(child.schema())) {
        scheduler.schedule_now();
        return;
      }
      break;
    }
    case 'T':
    case 'U':
    case 'A':
    case 'P':
      ++ts_index;
      break;
    default:
      break;
    }
  }
}

/** The stop-side mirror: every child link goes passive (including any the
    python code re-activated at runtime). */
void py_clear_input_activity(std::string_view layout,
                             const TSInputView &args_view) {
  auto &args = const_cast<TSInputView &>(args_view);
  auto bundle = args.as_bundle();
  std::size_t ts_index = 0;
  for (const char kind : layout) {
    switch (kind) {
    case 't':
    case 'u':
    case 'a':
    case 'C':
    case 'T':
    case 'U':
    case 'A':
    case 'P':
      bundle[ts_index++].make_passive();
      break;
    default:
      break;
    }
  }
  args.make_passive();
}

struct PyInvocationState {
  State<PyStateRef> *local{nullptr};
  TSOutputView *recordable{nullptr};
};

struct PyFastComputeCache {
  PyFastComputeCache(const PyNodeRecord *record_, PyCallShape shape_,
                     TSInputView input_, ValueView scalars_,
                     TSOutputHandle output_)
      : record(record_), shape(std::move(shape_)), input(std::move(input_)),
        scalars(std::move(scalars_)), output(std::move(output_)) {
    call_lease.guard = std::make_shared<PyTsGuard>();
  }

  const PyNodeRecord *record{nullptr};
  PyCallShape shape{};
  TSInputView input{};
  ValueView scalars{};
  TSOutputHandle output{};
  // The guard is allocated once for the node lifetime. Per-evaluation
  // code advances only the generation, avoiding shared_ptr reference-count
  // traffic at every Python callback while preserving retained-view safety.
  PyTsLease call_lease{};
  PyObject *input_object{nullptr};
  PyTimeSeries *input_wrapper{nullptr};
  std::array<PyObject *, 2> pair_objects{};
  std::array<PyTimeSeries *, 2> pair_wrappers{};
  // Per-ts-arg prepared routes below the args root (issue #203): acquired
  // once at start, non-owning (trie handles update in place; the read-side
  // trust condition re-checks per use). Indexed by ts-arg position.
  std::vector<hgraph::detail::PreparedInputSlotRoute> arg_routes{};

  /** Project ts-arg ``slot`` from ``args_root`` through the prepared route
      when ready; the plain per-tick projection otherwise. */
  [[nodiscard]] TSInputView arg_at(const TSInputView &args_root,
                                   TSBInputView &bundle,
                                   std::size_t slot) const {
    if (slot < arg_routes.size() && arg_routes[slot].ready()) {
      return args_root.child_from_prepared(arg_routes[slot]);
    }
    return bundle[slot];
  }

  [[nodiscard]] bool direct() const noexcept {
    return shape.kw_names.empty() && shape.layout.size() == 1 &&
           (shape.layout.front() == 't' || shape.layout.front() == 'u' ||
            shape.layout.front() == 'T' || shape.layout.front() == 'U' ||
            shape.layout.front() == 'a' || shape.layout.front() == 'A');
  }

  [[nodiscard]] bool direct_pair() const noexcept {
    if (!shape.kw_names.empty() || shape.layout.size() != 2) {
      return false;
    }
    const auto is_ts = [](char kind) {
      return kind == 't' || kind == 'u' || kind == 'T' || kind == 'U' ||
             kind == 'a' || kind == 'A';
    };
    return is_ts(shape.layout[0]) && is_ts(shape.layout[1]);
  }
};

struct PyFastComputeStateRef {
  PyFastComputeCache *cache{nullptr};
  friend bool operator==(const PyFastComputeStateRef &,
                         const PyFastComputeStateRef &) noexcept = default;
};

[[nodiscard]] bool py_make_ts_arg(char kind, TSInputView child,
                                  const PyTsLease &lease, nb::object &result) {
  const auto &evaluation_data = child.data_view();
  const bool has_current_value =
      evaluation_data.valid() && evaluation_data.has_current_value();
  // 'u'/'U' = UNCHECKED (hgraph's valid=(...) opt-out): the
  // python fn sees the view and guards itself.
  if (kind != 'u' && kind != 'U' &&
      !has_current_value) {
    return false;
  }
  if ((kind == 'a' || kind == 'A') && !evaluation_data.all_valid()) {
    return false;
  }
  // The LAZY C++ TimeSeries view: nothing converts unless the python
  // code touches it. The lease expires after the callback.
  const auto evaluation_storage = evaluation_data.valid()
                                      ? evaluation_data.storage_ref()
                                      : TSDataStorageRef<>{};
  PyTimeSeries wrapped{std::move(child), lease, evaluation_storage};
  wrapped.refresh_evaluation_data(evaluation_storage, has_current_value);
  result = nb::cast(std::move(wrapped));
  return true;
}

[[nodiscard]] bool py_make_direct_ts_arg(PyFastComputeCache &cache,
                                         DateTime now, const PyTsLease &lease,
                                         nb::object &result) {
  TSInputView child = cache.input.borrowed_ref(now);
  const auto &evaluation_data = child.data_view();
  const char kind = cache.shape.layout.front();
  const bool has_current_value =
      evaluation_data.valid() && evaluation_data.has_current_value();
  if (kind != 'u' && kind != 'U' &&
      !has_current_value) {
    return false;
  }
  if ((kind == 'a' || kind == 'A') && !evaluation_data.all_valid()) {
    return false;
  }
  const auto evaluation_storage = evaluation_data.valid()
                                      ? evaluation_data.storage_ref()
                                      : TSDataStorageRef<>{};

  // Repoint only the cache's sole reference. If Python retained the
  // previous argument, leave that expired object untouched and replace
  // the cache entry with a fresh wrapper.
  if (cache.input_object != nullptr && Py_REFCNT(cache.input_object) == 1) {
    cache.input_wrapper->view = std::move(child);
    cache.input_wrapper->lease.generation = lease.generation;
    cache.input_wrapper->refresh_evaluation_data(evaluation_storage,
                                                 has_current_value);
    result = nb::borrow<nb::object>(nb::handle(cache.input_object));
    return true;
  }

  if (cache.input_object != nullptr) {
    nb::handle(cache.input_object).dec_ref();
    cache.input_object = nullptr;
    cache.input_wrapper = nullptr;
  }
  PyTimeSeries wrapped{std::move(child), lease, evaluation_storage};
  wrapped.refresh_evaluation_data(evaluation_storage, has_current_value);
  result = nb::cast(std::move(wrapped));
  cache.input_object = result.ptr();
  cache.input_wrapper = std::addressof(nb::cast<PyTimeSeries &>(result));
  nb::handle(cache.input_object).inc_ref();
  return true;
}

[[nodiscard]] bool py_make_cached_pair_ts_arg(PyFastComputeCache &cache,
                                              std::size_t slot, char kind,
                                              TSInputView child,
                                              const PyTsLease &lease,
                                              nb::object &result) {
  const auto &evaluation_data = child.data_view();
  const bool has_current_value =
      evaluation_data.valid() && evaluation_data.has_current_value();
  if (kind != 'u' && kind != 'U' &&
      !has_current_value) {
    return false;
  }
  if ((kind == 'a' || kind == 'A') && !evaluation_data.all_valid()) {
    return false;
  }
  const auto evaluation_storage = evaluation_data.valid()
                                      ? evaluation_data.storage_ref()
                                      : TSDataStorageRef<>{};

  PyObject *&cached_object = cache.pair_objects.at(slot);
  PyTimeSeries *&cached_wrapper = cache.pair_wrappers.at(slot);
  if (cached_object != nullptr && Py_REFCNT(cached_object) == 1) {
    cached_wrapper->view = std::move(child);
    cached_wrapper->lease.generation = lease.generation;
    cached_wrapper->refresh_evaluation_data(evaluation_storage,
                                            has_current_value);
    result = nb::borrow<nb::object>(nb::handle(cached_object));
    return true;
  }

  if (cached_object != nullptr) {
    nb::handle(cached_object).dec_ref();
    cached_object = nullptr;
    cached_wrapper = nullptr;
  }
  PyTimeSeries wrapped{std::move(child), lease, evaluation_storage};
  wrapped.refresh_evaluation_data(evaluation_storage, has_current_value);
  result = nb::cast(std::move(wrapped));
  cached_object = result.ptr();
  cached_wrapper = std::addressof(nb::cast<PyTimeSeries &>(result));
  nb::handle(cached_object).inc_ref();
  return true;
}

/** Assemble the python call args per the layout; false = a ts arg is not yet
 * valid. */
[[nodiscard]] bool py_assemble_args(
    std::string_view layout, const TSInputView &args, const ValueView &scalars,
    PyInvocationState state, NodeScheduler scheduler, DateTime now,
    nb::list &call_args, std::optional<nb::list> &context_values,
    const PyTsLease &lease, const nb::object &runtime_global_state,
    EngineControlView engine, const NodeView &node,
    const TSOutputView *output = nullptr) // borrowed for the call only
{
  auto bundle = args.as_bundle();
  std::size_t ts_index = 0;
  std::size_t scalar_index = 0;
  auto scalar_list =
      scalars.valid() ? std::optional{scalars.as_list()} : std::nullopt;
  for (const char kind : layout) {
    switch (kind) {
    case 't':
    case 'u':
    case 'a':
    case 'C':
    case 'T':
    case 'U':
    case 'A':
    case 'P': {
      auto child = bundle[ts_index++];
      nb::object ts_obj;
      if (!py_make_ts_arg(kind, std::move(child), lease, ts_obj)) {
        return false;
      }
      call_args.append(ts_obj);
      if (kind == 'C' || kind == 'P') {
        // A context input is ALSO entered (python
        // context-manager protocol) around the call - the
        // value converts here because entering needs it.
        if (!context_values.has_value()) {
          context_values.emplace();
        }
        context_values->append(nb::cast<PyTimeSeries &>(ts_obj).value());
      }
      break;
    }
    case 's': {
      if (!scalar_list.has_value()) {
        throw std::logic_error("python node: missing scalars value");
      }
      call_args.append(
          value_to_py((*scalar_list)[scalar_index++].as_any().get()));
      break;
    }
    case 'S':
      if (state.local == nullptr) {
        throw std::logic_error("python node: local STATE is unavailable on a "
                               "recordable-state node");
      }
      call_args.append(py_state_namespace(*state.local));
      break;
    case 'Q': {
      if (state.local == nullptr || !scalar_list.has_value()) {
        throw std::logic_error("python node: typed STATE is unavailable");
      }
      nb::object factory =
          value_to_py((*scalar_list)[scalar_index++].as_any().get());
      call_args.append(py_typed_state(*state.local, factory));
      break;
    }
    case 'R':
      if (state.recordable == nullptr) {
        throw std::logic_error(
            "python node: RECORDABLE_STATE is unavailable on this node");
      }
      call_args.append(
          nb::cast(PyRecordableState{state.recordable->handle(), now, lease}));
      break;
    case 'o': {
      if (output == nullptr) {
        throw std::logic_error("_output injection requires a compute node");
      }
      call_args.append(
          nb::cast(PyOutput{output->handle(), now, scheduler, lease}));
      break;
    }
    case 'c':
      call_args.append(nb::cast(PyEvalClock{engine.evaluation_clock(), lease}));
      break;
    case 'd':
      call_args.append(nb::cast(PyScheduler{scheduler}));
      break;
    case 'e':
      call_args.append(nb::cast(PyEvaluationEngineApi{engine, lease}));
      break;
    case 'g':
      call_args.append(runtime_global_state);
      break;
    case 'h': {
      GraphView graph = node.graph();
      call_args.append(nb::cast(PyTraits{TraitsView{graph.pointer()}, lease}));
      break;
    }
    case 'l':
      call_args.append(nb::cast(PyLogger{
          LoggerView{node.graph().logger(), node.pointer(), node.graph().logger_ops()}, lease}));
      break;
    case 'n':
      call_args.append(nb::cast(PyNode{node.pointer(), scheduler, lease}));
      break;
    default:
      throw std::logic_error("python node: unknown layout marker");
    }
  }
  return true;
}

[[nodiscard]] bool py_fast_compute_eligible(OperatorCallContext context) {
  const auto *config = context.scalar_as<Str>("config");
  const auto *start_enabled = context.scalar_as<Bool>("start_enabled");
  const auto *stop_enabled = context.scalar_as<Bool>("stop_enabled");
  if (config == nullptr || start_enabled == nullptr ||
      stop_enabled == nullptr || *start_enabled || *stop_enabled) {
    return false;
  }
  for (const char kind : parse_py_call_shape(*config).layout) {
    switch (kind) {
    case 't':
    case 'u':
    case 'T':
    case 'U':
    case 'a':
    case 'A':
    case 's':
      break;
    default:
      return false;
    }
  }
  return true;
}

[[nodiscard]] bool py_assemble_fast_args(const PyFastComputeCache &cache,
                                         std::string_view layout,
                                         const TSInputView &args,
                                         const ValueView &scalars,
                                         const PyTsLease &lease,
                                         nb::list &call_args) {
  auto bundle = args.as_bundle();
  std::size_t ts_index = 0;
  std::size_t scalar_index = 0;
  auto scalar_list =
      scalars.valid() ? std::optional{scalars.as_list()} : std::nullopt;
  for (const char kind : layout) {
    switch (kind) {
    case 't':
    case 'u':
    case 'T':
    case 'U':
    case 'a':
    case 'A': {
      nb::object ts_obj;
      if (!py_make_ts_arg(kind, cache.arg_at(args, bundle, ts_index++), lease,
                          ts_obj)) {
        return false;
      }
      call_args.append(ts_obj);
      break;
    }
    case 's': {
      if (!scalar_list.has_value()) {
        throw std::logic_error("fast python node: missing scalars value");
      }
      call_args.append(
          value_to_py((*scalar_list)[scalar_index++].as_any().get()));
      break;
    }
    default:
      throw std::logic_error("fast python node: unsupported layout marker");
    }
  }
  return true;
}

template <typename TState>
void py_assemble_lifecycle_args(std::string_view layout,
                                const ValueView &scalars, TState *state,
                                TSOutputView *recordable_state,
                                DateTime now,
                                NodeScheduler scheduler,
                                const nb::object &runtime_global_state,
                                EngineControlView engine,
                                const PyTsLease &lease, const NodeView &node,
                                nb::list &call_args,
                                const TSInputView *inputs = nullptr) {
  std::size_t scalar_index = 0;
  auto scalar_list =
      scalars.valid() ? std::optional{scalars.as_list()} : std::nullopt;
  for (const char kind : layout) {
    switch (kind) {
    case 'i': {
      if (inputs == nullptr || !scalar_list.has_value()) {
        throw std::logic_error(
            "python lifecycle callback: missing stop input bundle");
      }
      const auto index = static_cast<std::size_t>(
          (*scalar_list)[scalar_index++].as_any().get().checked_as<Int>());
      auto bundle = inputs->as_bundle();
      if (index >= bundle.size()) {
        throw std::out_of_range(
            "python lifecycle callback: stop input index is out of range");
      }
      nb::object ts_object;
      static_cast<void>(
          py_make_ts_arg('U', bundle[index], lease, ts_object));
      call_args.append(ts_object);
      break;
    }
    case 's':
      if (!scalar_list.has_value()) {
        throw std::logic_error(
            "python lifecycle callback: missing scalars value");
      }
      call_args.append(
          value_to_py((*scalar_list)[scalar_index++].as_any().get()));
      break;
    case 'S':
      if (state == nullptr) {
        throw std::logic_error(
            "python lifecycle callback: local STATE is unavailable");
      }
      call_args.append(py_state_namespace(*state));
      break;
    case 'Q': {
      if (state == nullptr || !scalar_list.has_value()) {
        throw std::logic_error(
            "python lifecycle callback: missing typed STATE factory");
      }
      nb::object factory =
          value_to_py((*scalar_list)[scalar_index++].as_any().get());
      call_args.append(py_typed_state(*state, factory));
      break;
    }
    case 'R':
      if (recordable_state == nullptr) {
        throw std::logic_error(
            "python lifecycle callback: RECORDABLE_STATE is unavailable");
      }
      call_args.append(nb::cast(PyRecordableState{
          recordable_state->handle(), now, lease}));
      break;
    case 'c':
      call_args.append(nb::cast(PyEvalClock{engine.evaluation_clock(), lease}));
      break;
    case 'd':
      call_args.append(nb::cast(PyScheduler{scheduler}));
      break;
    case 'e':
      call_args.append(nb::cast(PyEvaluationEngineApi{engine, lease}));
      break;
    case 'g':
      call_args.append(runtime_global_state);
      break;
    case 'h': {
      GraphView graph = node.graph();
      call_args.append(nb::cast(PyTraits{TraitsView{graph.pointer()}, lease}));
      break;
    }
    case 'l':
      call_args.append(nb::cast(PyLogger{
          LoggerView{node.graph().logger(), node.pointer(), node.graph().logger_ops()}, lease}));
      break;
    case 'n':
      call_args.append(nb::cast(PyNode{node.pointer(), scheduler, lease}));
      break;
    default:
      throw std::logic_error(
          "python lifecycle callback: unsupported layout marker");
    }
  }
}

/** Peel the trailing keyword-called entries off ``call_args`` (python
    params after ``*args`` fill BY NAME). */
[[nodiscard]] std::optional<nb::dict>
py_peel_kwargs(nb::list &call_args,
               std::span<const std::string_view> kw_names) {
  if (kw_names.empty()) {
    return std::nullopt;
  }
  nb::dict kwargs;
  const std::size_t total = nb::len(call_args);
  if (total < kw_names.size()) {
    throw std::logic_error("python node: call shape shorter than its kw names");
  }
  const std::size_t first = total - kw_names.size();
  for (std::size_t index = 0; index < kw_names.size(); ++index) {
    kwargs[nb::str(std::string{kw_names[index]}.c_str())] =
        call_args[first + index];
  }
  nb::list positional;
  for (std::size_t index = 0; index < first; ++index) {
    positional.append(call_args[index]);
  }
  call_args = std::move(positional);
  return kwargs;
}

[[nodiscard]] nb::object py_invoke(const nb::object &fn,
                                   const nb::list &call_args,
                                   const std::optional<nb::dict> &call_kwargs) {
  if (call_kwargs.has_value()) {
    return fn(*nb::tuple(call_args), **call_kwargs.value());
  }
  switch (nb::len(call_args)) {
  case 0:
    return fn();
  case 1:
    return fn(call_args[0]);
  default:
    return fn(*nb::tuple(call_args));
  }
}

/** Enter context-manager values (hgraph's context semantics), call, exit in
 * reverse. */
[[nodiscard]] nb::object
py_call_with_contexts(const nb::object &fn, nb::list &call_args,
                      const std::optional<nb::list> &context_values,
                      std::optional<nb::dict> call_kwargs = std::nullopt) {
  if (!context_values.has_value()) {
    return py_invoke(fn, call_args, call_kwargs);
  }
  std::vector<nb::object> entered;
  entered.reserve(context_values.has_value() ? nb::len(*context_values) : 0);
  auto unwind = UnwindCleanupGuard([&] {
    for (auto it = entered.rbegin(); it != entered.rend(); ++it) {
      (*it).attr("__exit__")(nb::none(), nb::none(), nb::none());
    }
  });
  if (context_values.has_value()) {
    for (nb::handle value : *context_values) {
      if (nb::hasattr(value, "__enter__")) {
        nb::object holder = nb::borrow(value);
        holder.attr("__enter__")();
        entered.push_back(std::move(holder));
      }
    }
  }
  nb::object result = py_invoke(fn, call_args, call_kwargs);
  while (!entered.empty()) {
    nb::object holder = std::move(entered.back());
    entered.pop_back();
    holder.attr("__exit__")(nb::none(), nb::none(), nb::none());
  }
  unwind.release();
  return result;
}

void py_call_lifecycle(const PyNodeRef &fn, bool enabled,
                       std::string_view config, const ValueView &scalars,
                       State<PyStateRef> &state, NodeScheduler scheduler,
                       GlobalStateView global_state, EngineControlView engine,
                       const NodeView &node,
                       const TSInputView *inputs = nullptr) {
  if (!enabled) {
    return;
  }
  translate_python_error([&] {
    nb::list call_args;
    std::optional<nb::list> context_values;
    auto lease = py_ts_lease_for_node(state);
    auto invalid = UnwindCleanupGuard([&] { lease.invalidate(); });
    nb::object runtime_state =
        py_runtime_global_state_for_call(config, global_state, lease,
                                         state.get().call_lease);
    py_assemble_lifecycle_args(config, scalars, &state, nullptr,
                               engine.evaluation_clock().evaluation_time(),
                               scheduler, runtime_state, engine, lease, node,
                               call_args, inputs);
    (void)py_call_with_contexts(fn.record->fn, call_args, context_values);
    invalid.release();
    lease.invalidate();
  });
}

struct py_compute_node {
  static constexpr auto name = "__py_compute";
  static constexpr std::string_view implementation_label =
      "hgraph.python.compute";
  static constexpr bool uses_python_values = true;
  static constexpr bool requires_phase_runner = true;
  using signature_args = std::tuple<
      In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>,
      Scalar<"fn", PyNodeRef>, Scalar<"config", Str>,
      Scalar<"scalars", ScalarVar<"SV">>, Scalar<"start_fn", PyNodeRef>,
      Scalar<"start_enabled", Bool>, Scalar<"start_config", Str>,
      Scalar<"start_scalars", ScalarVar<"SSV">>, Scalar<"stop_fn", PyNodeRef>,
      Scalar<"stop_enabled", Bool>, Scalar<"stop_config", Str>,
      Scalar<"stop_scalars", ScalarVar<"XSV">>, State<PyStateRef>,
      NodeScheduler, DateTime, GlobalStateView, EngineControlView, NodeView,
      Out<TsVar<"O">>>;

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    return !py_fast_compute_eligible(context);
  }

  static void
  start(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>
            args,
        Scalar<"config", Str> eval_config, Scalar<"start_fn", PyNodeRef> fn,
        Scalar<"start_enabled", Bool> enabled,
        Scalar<"start_config", Str> config,
        Scalar<"start_scalars", ScalarVar<"SSV">> scalars,
        State<PyStateRef> state, NodeScheduler scheduler,
        SingleShotScheduler initial_sample, GlobalStateView global_state,
        EngineControlView engine, NodeView node) {
    const auto layout = parse_py_call_shape(eval_config.value()).layout;
    py_apply_input_activity(layout, args.base());
    py_schedule_initial_reference_sample(layout, args.base(), initial_sample);
    py_call_lifecycle(fn.value(), enabled.value(), config.value(),
                      scalars.value(), state, scheduler, global_state, engine,
                      node);
  }

  static void
  eval(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>
           args,
       Scalar<"fn", PyNodeRef> fn, Scalar<"config", Str> config,
       Scalar<"scalars", ScalarVar<"SV">> scalars, State<PyStateRef> state,
       NodeScheduler scheduler, DateTime now, GlobalStateView global_state,
       EngineControlView engine, NodeView node, Out<TsVar<"O">> out) {
    const PyCallShape shape = parse_py_call_shape(config.value());
    translate_python_error([&] {
      nb::list call_args;
      std::optional<nb::list> context_values;
      auto lease = py_ts_lease_for_node(state);
      auto invalid = UnwindCleanupGuard([&] { lease.invalidate(); });
      const auto &out_view = static_cast<const TSOutputView &>(out);
      nb::object runtime_state =
          py_runtime_global_state_for_call(shape.layout, global_state, lease,
                                           state.get().call_lease);
      if (!py_assemble_args(shape.layout, args.base(), scalars.value(),
                            PyInvocationState{.local = &state}, scheduler, now,
                            call_args, context_values, lease, runtime_state,
                            engine, node, &out_view)) {
        return;
      }
      auto call_kwargs = py_peel_kwargs(call_args, shape.kw_names);
      apply_py_result(py_call_with_contexts(fn.value().record->fn, call_args,
                                            context_values,
                                            std::move(call_kwargs)),
                      out);
      invalid.release();
      lease.invalidate();
    });
  }

  static void
  stop(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>
           args,
       Scalar<"config", Str> eval_config, Scalar<"stop_fn", PyNodeRef> fn,
       Scalar<"stop_enabled", Bool> enabled, Scalar<"stop_config", Str> config,
       Scalar<"stop_scalars", ScalarVar<"XSV">> scalars,
       State<PyStateRef> state, NodeScheduler scheduler,
       GlobalStateView global_state, EngineControlView engine, NodeView node) {
    auto cleanup = UnwindCleanupGuard([&] {
      py_clear_input_activity(
          parse_py_call_shape(eval_config.value()).layout, args.base());
      py_release_state(state);
    });
    const auto input_view = args.base().borrowed_ref();
    py_call_lifecycle(fn.value(), enabled.value(), config.value(),
                      scalars.value(), state, scheduler, global_state, engine,
                      node, &input_view);
    cleanup.release();
    py_clear_input_activity(parse_py_call_shape(eval_config.value()).layout,
                            args.base());
    py_release_state(state);
  }
};

struct py_fast_compute_node {
  static constexpr auto name = "__py_compute";
  static constexpr std::string_view implementation_label =
      "hgraph.python.compute.fast";
  static constexpr bool uses_python_values = true;
  static constexpr bool requires_phase_runner = true;
  using signature_args = std::tuple<
      In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>,
      Scalar<"fn", PyNodeRef>, Scalar<"config", Str>,
      Scalar<"scalars", ScalarVar<"SV">>, Scalar<"start_fn", PyNodeRef>,
      Scalar<"start_enabled", Bool>, Scalar<"start_config", Str>,
      Scalar<"start_scalars", ScalarVar<"SSV">>, Scalar<"stop_fn", PyNodeRef>,
      Scalar<"stop_enabled", Bool>, Scalar<"stop_config", Str>,
      Scalar<"stop_scalars", ScalarVar<"XSV">>, State<PyFastComputeStateRef>,
      PyOwnerSchedulerSupport, Out<TsVar<"O">>>;

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    return py_fast_compute_eligible(context);
  }

  static void
  start(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>
            args,
        Scalar<"fn", PyNodeRef> fn, Scalar<"config", Str> config,
        Scalar<"scalars", ScalarVar<"SV">> scalars,
        State<PyFastComputeStateRef> state, SingleShotScheduler initial_sample,
        Out<TsVar<"O">> out) {
    PyCallShape shape = parse_py_call_shape(config.value());
    py_apply_input_activity(shape.layout, args.base());
    py_schedule_initial_reference_sample(shape.layout, args.base(),
                                         initial_sample);

    const bool direct =
        shape.kw_names.empty() && shape.layout.size() == 1 &&
        (shape.layout.front() == 't' || shape.layout.front() == 'u' ||
         shape.layout.front() == 'T' || shape.layout.front() == 'U' ||
         shape.layout.front() == 'a' || shape.layout.front() == 'A');
    TSInputView cached_input = args.base().borrowed_ref();
    // Acquire the level-2 prepared routes (issue #203): the args root is
    // owned child-carrying storage, so each ts-arg slot's route is stable
    // for the node's lifetime and the retained/per-tick views built from it
    // resolve reads through the trie handle.
    std::vector<hgraph::detail::PreparedInputSlotRoute> arg_routes;
    if (detail::has_input_children(cached_input.data_view())) {
      std::size_t ts_count = 0;
      for (const char kind : shape.layout) {
        if (kind != 's') { ++ts_count; }
      }
      arg_routes.reserve(ts_count);
      for (std::size_t slot = 0; slot < ts_count; ++slot) {
        arg_routes.push_back(cached_input.prepare_child_route(slot));
      }
    }
    if (direct) {
      auto bundle = cached_input.as_bundle();
      cached_input = !arg_routes.empty() && arg_routes[0].ready()
                         ? cached_input.child_from_prepared(arg_routes[0])
                         : bundle[0];
    }
    auto cache = std::make_unique<PyFastComputeCache>(
        fn.value().record, std::move(shape), std::move(cached_input),
        scalars.value(), out.handle());
    cache->arg_routes = std::move(arg_routes);
    state.set(PyFastComputeStateRef{cache.get()});
    static_cast<void>(cache.release());
  }

  static void eval(State<PyFastComputeStateRef> state,
                   PyOwnerSchedulerSupport, DateTime now) {
    PyFastComputeCache *cache = state.get().cache;
    if (cache == nullptr) {
      throw std::logic_error("fast python node has no runtime cache");
    }

    translate_python_error([&] {
      PyTsLease &lease = cache->call_lease;
      lease.generation = ++lease.guard->generation;
      auto invalid = UnwindCleanupGuard([&] { lease.invalidate(); });
      nb::object result;
      if (cache->direct()) {
        nb::object ts_obj;
        if (!py_make_direct_ts_arg(*cache, now, lease, ts_obj)) {
          return;
        }
        result = cache->record->fn(ts_obj);
      } else if (cache->direct_pair()) {
        TSInputView input = cache->input.borrowed_ref(now);
        auto bundle = input.as_bundle();
        nb::object lhs;
        nb::object rhs;
        if (!py_make_cached_pair_ts_arg(*cache, 0, cache->shape.layout[0],
                                        cache->arg_at(input, bundle, 0),
                                        lease, lhs) ||
            !py_make_cached_pair_ts_arg(*cache, 1, cache->shape.layout[1],
                                        cache->arg_at(input, bundle, 1),
                                        lease, rhs)) {
          return;
        }
        result = cache->record->fn(lhs, rhs);
      } else {
        TSInputView input = cache->input.borrowed_ref(now);
        nb::list call_args;
        if (!py_assemble_fast_args(*cache, cache->shape.layout, input,
                                   cache->scalars, lease, call_args)) {
          return;
        }
        auto call_kwargs = py_peel_kwargs(call_args, cache->shape.kw_names);
        result = py_invoke(cache->record->fn, call_args,
                           std::move(call_kwargs));
      }

      auto output_view = cache->output.view(now);
      Out<TsVar<"O">> out{std::move(output_view), now};
      apply_py_result(result, out);
      invalid.release();
      lease.invalidate();
    });
  }

  static void
  stop(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>
           args,
       Scalar<"config", Str> config, State<PyFastComputeStateRef> state) {
    std::unique_ptr<PyFastComputeCache> cache{state.get().cache};
    state.set(PyFastComputeStateRef{});
    if (cache != nullptr && cache->input_object != nullptr) {
      nb::handle(cache->input_object).dec_ref();
      cache->input_object = nullptr;
      cache->input_wrapper = nullptr;
    }
    if (cache != nullptr) {
      cache->call_lease.guard->alive = false;
      for (std::size_t slot = 0; slot < cache->pair_objects.size(); ++slot) {
        if (cache->pair_objects[slot] == nullptr) {
          continue;
        }
        nb::handle(cache->pair_objects[slot]).dec_ref();
        cache->pair_objects[slot] = nullptr;
        cache->pair_wrappers[slot] = nullptr;
      }
    }
    py_clear_input_activity(parse_py_call_shape(config.value()).layout,
                            args.base());
  }
};

struct py_compute_recordable_node {
  static constexpr auto name = "__py_compute_recordable";
  static constexpr std::string_view implementation_label =
      "hgraph.python.compute_recordable";
  static constexpr bool uses_python_values = true;
  static constexpr bool requires_phase_runner = true;
  using signature_args = std::tuple<
      In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>,
      Scalar<"fn", PyNodeRef>, Scalar<"config", Str>,
      Scalar<"scalars", ScalarVar<"SV">>,
      Scalar<"recordable_state_schema", PyTsMetaRef>,
      Scalar<"start_fn", PyNodeRef>, Scalar<"start_enabled", Bool>,
      Scalar<"start_config", Str>, Scalar<"start_scalars", ScalarVar<"SSV">>,
      Scalar<"stop_fn", PyNodeRef>, Scalar<"stop_enabled", Bool>,
      Scalar<"stop_config", Str>, Scalar<"stop_scalars", ScalarVar<"XSV">>,
      RecordableState<TsVar<"RS">>, NodeScheduler, DateTime, GlobalStateView,
      EngineControlView, NodeView, Out<TsVar<"O">>>;

  static void resolve_default_types(ResolutionMap &resolution,
                                    OperatorCallContext context) {
    const auto *schema =
        context.scalar_as<PyTsMetaRef>("recordable_state_schema");
    if (schema == nullptr || schema->meta == nullptr) {
      throw std::invalid_argument(
          "python recordable-state node requires a concrete state schema");
    }
    resolution.bind_ts("RS", schema->meta);
  }

  static void
  start(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>
            args,
        Scalar<"config", Str> eval_config,
        Scalar<"start_fn", PyNodeRef> fn,
        Scalar<"start_enabled", Bool> enabled,
        Scalar<"start_config", Str> config,
        Scalar<"start_scalars", ScalarVar<"SSV">> scalars,
        RecordableState<TsVar<"RS">> state, NodeScheduler scheduler,
        SingleShotScheduler initial_sample, GlobalStateView global_state,
        EngineControlView engine, NodeView node) {
    const auto layout = parse_py_call_shape(eval_config.value()).layout;
    py_apply_input_activity(layout, args.base());
    py_schedule_initial_reference_sample(layout, args.base(), initial_sample);
    if (!enabled.value()) {
      return;
    }
    translate_python_error([&] {
      nb::list call_args;
      auto lease = py_ts_lease_for_call();
      auto invalid = UnwindCleanupGuard([&] { lease.invalidate(); });
      TSOutputView state_view =
          static_cast<const TSOutputView &>(state).borrowed_ref();
      nb::object runtime_state =
          py_runtime_global_state_for_call(config.value(), global_state, lease);
      py_assemble_lifecycle_args(
          config.value(), scalars.value(),
          static_cast<PyStateRef *>(nullptr), &state_view,
          state.evaluation_time(), scheduler, runtime_state, engine, lease,
          node, call_args);
      (void)py_call_with_contexts(fn.value().record->fn, call_args,
                                  std::nullopt);
      invalid.release();
      lease.invalidate();
    });
  }

  static void
  eval(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>
           args,
       Scalar<"fn", PyNodeRef> fn, Scalar<"config", Str> config,
       Scalar<"scalars", ScalarVar<"SV">> scalars,
       RecordableState<TsVar<"RS">> state, NodeScheduler scheduler,
       DateTime now, GlobalStateView global_state, EngineControlView engine,
       NodeView node, Out<TsVar<"O">> out) {
    const PyCallShape shape = parse_py_call_shape(config.value());
    translate_python_error([&] {
      nb::list call_args;
      std::optional<nb::list> context_values;
      auto lease = py_ts_lease_for_call();
      auto invalid = UnwindCleanupGuard([&] { lease.invalidate(); });
      const auto &out_view = static_cast<const TSOutputView &>(out);
      TSOutputView state_view =
          static_cast<const TSOutputView &>(state).borrowed_ref();
      nb::object runtime_state =
          py_runtime_global_state_for_call(shape.layout, global_state, lease);
      if (!py_assemble_args(shape.layout, args.base(), scalars.value(),
                            PyInvocationState{.recordable = &state_view},
                            scheduler, now, call_args, context_values, lease,
                            runtime_state, engine, node, &out_view)) {
        return;
      }
      auto call_kwargs = py_peel_kwargs(call_args, shape.kw_names);
      apply_py_result(py_call_with_contexts(fn.value().record->fn, call_args,
                                            context_values,
                                            std::move(call_kwargs)),
                      out);
      invalid.release();
      lease.invalidate();
    });
  }

  static void
  stop(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>
           args,
       Scalar<"config", Str> eval_config,
       Scalar<"stop_fn", PyNodeRef> fn,
       Scalar<"stop_enabled", Bool> enabled,
       Scalar<"stop_config", Str> config,
       Scalar<"stop_scalars", ScalarVar<"XSV">> scalars,
       RecordableState<TsVar<"RS">> state, NodeScheduler scheduler,
       GlobalStateView global_state, EngineControlView engine, NodeView node) {
    auto cleanup = UnwindCleanupGuard([&] {
      py_clear_input_activity(
          parse_py_call_shape(eval_config.value()).layout, args.base());
    });
    if (!enabled.value()) { return; }
    translate_python_error([&] {
      nb::list call_args;
      auto lease = py_ts_lease_for_call();
      auto invalid = UnwindCleanupGuard([&] { lease.invalidate(); });
      TSOutputView state_view =
          static_cast<const TSOutputView &>(state).borrowed_ref();
      nb::object runtime_state =
          py_runtime_global_state_for_call(config.value(), global_state, lease);
      py_assemble_lifecycle_args(
          config.value(), scalars.value(),
          static_cast<PyStateRef *>(nullptr), &state_view,
          state.evaluation_time(), scheduler, runtime_state, engine, lease,
          node, call_args, &args.base());
      (void)py_call_with_contexts(fn.value().record->fn, call_args,
                                  std::nullopt);
      invalid.release();
      lease.invalidate();
    });
    cleanup.complete();
  }
};

struct py_sink_node {
  static constexpr auto name = "__py_sink";
  static constexpr std::string_view implementation_label = "hgraph.python.sink";
  static constexpr bool uses_python_values = true;
  static constexpr bool requires_phase_runner = true;
  using signature_args = std::tuple<
      In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>,
      Scalar<"fn", PyNodeRef>, Scalar<"config", Str>,
      Scalar<"scalars", ScalarVar<"SV">>, Scalar<"start_fn", PyNodeRef>,
      Scalar<"start_enabled", Bool>, Scalar<"start_config", Str>,
      Scalar<"start_scalars", ScalarVar<"SSV">>, Scalar<"stop_fn", PyNodeRef>,
      Scalar<"stop_enabled", Bool>, Scalar<"stop_config", Str>,
      Scalar<"stop_scalars", ScalarVar<"XSV">>, State<PyStateRef>,
      NodeScheduler, DateTime, GlobalStateView, EngineControlView, NodeView>;

  static void
  start(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>
            args,
        Scalar<"config", Str> eval_config, Scalar<"start_fn", PyNodeRef> fn,
        Scalar<"start_enabled", Bool> enabled,
        Scalar<"start_config", Str> config,
        Scalar<"start_scalars", ScalarVar<"SSV">> scalars,
        State<PyStateRef> state, NodeScheduler scheduler,
        SingleShotScheduler initial_sample, GlobalStateView global_state,
        EngineControlView engine, NodeView node) {
    const auto layout = parse_py_call_shape(eval_config.value()).layout;
    py_apply_input_activity(layout, args.base());
    py_schedule_initial_reference_sample(layout, args.base(), initial_sample);
    py_call_lifecycle(fn.value(), enabled.value(), config.value(),
                      scalars.value(), state, scheduler, global_state, engine,
                      node);
  }

  static void
  eval(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>
           args,
       Scalar<"fn", PyNodeRef> fn, Scalar<"config", Str> config,
       Scalar<"scalars", ScalarVar<"SV">> scalars, State<PyStateRef> state,
       NodeScheduler scheduler, DateTime now, GlobalStateView global_state,
       EngineControlView engine, NodeView node) {
    const PyCallShape shape = parse_py_call_shape(config.value());
    translate_python_error([&] {
      nb::list call_args;
      std::optional<nb::list> context_values;
      auto lease = py_ts_lease_for_node(state);
      auto invalid = UnwindCleanupGuard([&] { lease.invalidate(); });
      nb::object runtime_state =
          py_runtime_global_state_for_call(shape.layout, global_state, lease,
                                           state.get().call_lease);
      if (!py_assemble_args(shape.layout, args.base(), scalars.value(),
                            PyInvocationState{.local = &state}, scheduler, now,
                            call_args, context_values, lease, runtime_state,
                            engine, node)) {
        return;
      }
      auto call_kwargs = py_peel_kwargs(call_args, shape.kw_names);
      (void)py_call_with_contexts(fn.value().record->fn, call_args,
                                  context_values,
                                  std::move(call_kwargs));
      invalid.release();
      lease.invalidate();
    });
  }

  static void
  stop(In<"args", TsVar<"A">, InputValidity::Unchecked, InputActivity::Passive>
           args,
       Scalar<"config", Str> eval_config, Scalar<"stop_fn", PyNodeRef> fn,
       Scalar<"stop_enabled", Bool> enabled, Scalar<"stop_config", Str> config,
       Scalar<"stop_scalars", ScalarVar<"XSV">> scalars,
       State<PyStateRef> state, NodeScheduler scheduler,
       GlobalStateView global_state, EngineControlView engine, NodeView node) {
    auto cleanup = UnwindCleanupGuard([&] {
      py_clear_input_activity(
          parse_py_call_shape(eval_config.value()).layout, args.base());
      py_release_state(state);
    });
    const auto input_view = args.base().borrowed_ref();
    py_call_lifecycle(fn.value(), enabled.value(), config.value(),
                      scalars.value(), state, scheduler, global_state, engine,
                      node, &input_view);
    cleanup.release();
    py_clear_input_activity(parse_py_call_shape(eval_config.value()).layout,
                            args.base());
    py_release_state(state);
  }
};

/** Heap iterator state (pointer-in-State, the frame-backend pattern). */
struct PyGenHandle {
  nb::object iterator;
  nb::object pending; ///< the value yielded for the SCHEDULED time
  std::optional<DateTime> last_time{};
  bool exhausted{false};
  PyTsLease lease{};
  PyStateRef local_state{};
};

struct PyGenStateRef {
  PyGenHandle *handle{nullptr};
  friend bool operator==(const PyGenStateRef &,
                         const PyGenStateRef &) noexcept = default;
};

/** Pull the next (datetime, value) pair; schedules it or marks exhaustion. */
template <typename Scheduler>
void py_gen_advance(PyGenHandle &handle, Scheduler &sched) {
  nb::object next = nb::steal(PyIter_Next(handle.iterator.ptr()));
  if (!next.is_valid()) {
    if (PyErr_Occurred() != nullptr) {
      throw nb::python_error();
    }
    handle.exhausted = true;
    handle.pending = nb::object{};
    return;
  }
  auto pair = nb::cast<nb::tuple>(next);
  if (nb::len(pair) != 2) {
    throw nb::value_error(
        "a Python generator must yield (datetime, value) pairs");
  }
  DateTime when;
  if (!nb::try_cast<DateTime>(pair[0], when)) {
    TimeDelta delay;
    if (!nb::try_cast<TimeDelta>(pair[0], delay)) {
      throw nb::type_error(
          "a Python generator time must be a datetime or timedelta");
    }
    when = sched.now() + delay;
  }
  if (handle.last_time.has_value() && when <= *handle.last_time) {
    throw std::invalid_argument(
        "Python generator output times must be strictly increasing");
  }
  handle.last_time = when;
  handle.pending = nb::object(pair[1]);
  sched.schedule(when);
}

struct py_generator_node {
  static constexpr auto name = "__py_generator";
  static constexpr std::string_view implementation_label =
      "hgraph.python.generator";
  static constexpr bool uses_python_values = true;
  static constexpr bool requires_phase_runner = true;

  static void start(Scalar<"fn", PyNodeRef> fn, Scalar<"config", Str> config,
                    Scalar<"scalars", ScalarVar<"SV">> scalars,
                    State<PyGenStateRef> state, NodeScheduler sched,
                    GlobalStateView global_state, EngineControlView engine,
                    NodeView node) {
    translate_python_error([&] {
      auto handle = std::make_unique<PyGenHandle>();
      auto guard = std::make_shared<PyTsGuard>();
      handle->lease = PyTsLease{
          .guard = guard,
          .generation = ++guard->generation,
          .owns_guard_lifetime = true,
      };
      auto invalidate = UnwindCleanupGuard([&] { handle->lease.invalidate(); });
      const auto shape = parse_py_call_shape(config.value());
      nb::list call_args;
      nb::object runtime_state =
          py_runtime_global_state_for_call(shape.layout, global_state,
                                           handle->lease);
      py_assemble_lifecycle_args(
          shape.layout, scalars.value(), &handle->local_state, nullptr,
          engine.evaluation_clock().evaluation_time(), sched, runtime_state,
          engine, handle->lease, node, call_args);
      auto call_kwargs = py_peel_kwargs(call_args, shape.kw_names);
      nb::object iterable =
          py_call_with_contexts(fn.value().record->fn, call_args, std::nullopt,
                                std::move(call_kwargs));
      handle->iterator = nb::steal(PyObject_GetIter(iterable.ptr()));
      if (!handle->iterator.is_valid()) {
        throw nb::python_error();
      }
      py_gen_advance(*handle, sched);
      state.set(
          PyGenStateRef{handle.release()}); // owned by node State until stop
      invalidate.release();
    });
  }

  static void eval(Scalar<"fn", PyNodeRef> fn, Scalar<"config", Str> config,
                   Scalar<"scalars", ScalarVar<"SV">> scalars,
                   Scalar<"stop_fn", PyNodeRef> stop_fn,
                   Scalar<"stop_enabled", Bool> stop_enabled,
                   Scalar<"stop_config", Str> stop_config,
                   Scalar<"stop_scalars", ScalarVar<"XSV">> stop_scalars,
                   State<PyGenStateRef> state, NodeScheduler sched,
                   Out<TsVar<"O">> out) {
    static_cast<void>(fn);
    static_cast<void>(config);
    static_cast<void>(scalars);
    static_cast<void>(stop_fn);
    static_cast<void>(stop_enabled);
    static_cast<void>(stop_config);
    static_cast<void>(stop_scalars);
    translate_python_error([&] {
      PyGenHandle *handle = state.get().handle;
      if (handle == nullptr || handle->exhausted) {
        return;
      }
      apply_py_result(handle->pending, out);
      py_gen_advance(*handle, sched);
    });
  }

  static void stop(State<PyGenStateRef> state,
                   Scalar<"stop_fn", PyNodeRef> fn,
                   Scalar<"stop_enabled", Bool> enabled,
                   Scalar<"stop_config", Str> config,
                   Scalar<"stop_scalars", ScalarVar<"XSV">> scalars,
                   NodeScheduler scheduler, GlobalStateView global_state,
                   EngineControlView engine, NodeView node) {
    std::unique_ptr<PyGenHandle> handle{state.get().handle};
    state.set(PyGenStateRef{});
    if (handle != nullptr) {
      auto release = UnwindCleanupGuard([&] {
        handle->iterator = nb::object{};
        handle->pending = nb::object{};
        handle->lease.invalidate();
        py_release_state(handle->local_state);
      });
      if (enabled.value()) {
        translate_python_error([&] {
          nb::list call_args;
          nb::object runtime_state = py_runtime_global_state_for_call(
              config.value(), global_state, handle->lease);
          py_assemble_lifecycle_args(
              config.value(), scalars.value(), &handle->local_state,
              static_cast<TSOutputView *>(nullptr),
              engine.evaluation_clock().evaluation_time(), scheduler,
              runtime_state, engine, handle->lease, node, call_args);
          (void)py_call_with_contexts(fn.value().record->fn, call_args,
                                      std::nullopt);
        });
      }
      release.release();
      handle->iterator = nb::object{};
      handle->pending = nb::object{};
      handle->lease.invalidate();
      py_release_state(handle->local_state);
    }
  }
};

struct op_py_compute
    : Operator<"__py_compute", In<"args", TsVar<"A">>, Scalar<"fn", PyNodeRef>,
               Scalar<"config", Str>, Scalar<"scalars", ScalarVar<"SV">>,
               Scalar<"start_fn", PyNodeRef>, Scalar<"start_enabled", Bool>,
               Scalar<"start_config", Str>,
               Scalar<"start_scalars", ScalarVar<"SSV">>,
               Scalar<"stop_fn", PyNodeRef>, Scalar<"stop_enabled", Bool>,
               Scalar<"stop_config", Str>,
               Scalar<"stop_scalars", ScalarVar<"XSV">>, Out<TsVar<"O">>> {};
struct op_py_compute_recordable
    : Operator<"__py_compute_recordable", In<"args", TsVar<"A">>,
               Scalar<"fn", PyNodeRef>, Scalar<"config", Str>,
               Scalar<"scalars", ScalarVar<"SV">>,
               Scalar<"recordable_state_schema", PyTsMetaRef>,
               Scalar<"start_fn", PyNodeRef>, Scalar<"start_enabled", Bool>,
               Scalar<"start_config", Str>,
               Scalar<"start_scalars", ScalarVar<"SSV">>,
               Scalar<"stop_fn", PyNodeRef>, Scalar<"stop_enabled", Bool>,
               Scalar<"stop_config", Str>,
               Scalar<"stop_scalars", ScalarVar<"XSV">>, Out<TsVar<"O">>> {};
struct op_py_sink
    : Operator<"__py_sink", In<"args", TsVar<"A">>, Scalar<"fn", PyNodeRef>,
               Scalar<"config", Str>, Scalar<"scalars", ScalarVar<"SV">>,
               Scalar<"start_fn", PyNodeRef>, Scalar<"start_enabled", Bool>,
               Scalar<"start_config", Str>,
               Scalar<"start_scalars", ScalarVar<"SSV">>,
               Scalar<"stop_fn", PyNodeRef>, Scalar<"stop_enabled", Bool>,
               Scalar<"stop_config", Str>,
               Scalar<"stop_scalars", ScalarVar<"XSV">>> {};
struct op_py_generator
    : Operator<"__py_generator", Scalar<"fn", PyNodeRef>, Scalar<"config", Str>,
               Scalar<"scalars", ScalarVar<"SV">>,
               Scalar<"stop_fn", PyNodeRef>, Scalar<"stop_enabled", Bool>,
               Scalar<"stop_config", Str>,
               Scalar<"stop_scalars", ScalarVar<"XSV">>, Out<TsVar<"O">>> {};
struct harness_replay {
  static constexpr auto name = "__harness_replay";
  static constexpr bool schedule_on_start = true;

  // The eval_node harness always replays a seeded plain-key (dense) buffer;
  // the empty recordable_id selects the unified replay's dense path.
  static auto defaults() { return std::tuple{arg<"recordable_id">(Str{""})}; }

  static void start(Scalar<"key", std::string> key,
                    Scalar<"recordable_id", Str> recordable_id, TraitsView traits,
                    State<stdlib::record_replay_memory_detail::ReplayCursorState> cursor) {
    stdlib::replay_impl::start(std::move(key), std::move(recordable_id),
                               Scalar<"model", Str>{Str{}}, std::move(traits),
                               std::move(cursor));
  }

  static void eval(Scalar<"key", std::string> key,
                   Scalar<"recordable_id", Str> recordable_id,
                   TraitsView traits, GlobalStateView gs, NodeScheduler sched,
                   State<stdlib::record_replay_memory_detail::ReplayCursorState> cursor,
                   DateTime now, Out<TsVar<"S">> out) {
    // The harness never selects a backend: it is the dense in-memory path by
    // construction, so it forwards an empty model ("use the graph's").
    stdlib::replay_impl::eval(
        std::move(key), std::move(recordable_id), Scalar<"model", Str>{Str{}},
        std::move(traits), std::move(gs), std::move(sched), std::move(cursor), now,
        std::move(out));
  }
};

struct harness_record {
  static constexpr auto name = "__harness_record";

  static auto defaults() { return std::tuple{arg<"sparse">(Bool{false})}; }

  static void start(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                    Scalar<"key", std::string> key,
                    Scalar<"sparse", Bool> sparse, GlobalStateView gs,
                    State<stdlib::ResolvedBindings> bindings) {
    stdlib::dense_record_impl::start(std::move(ts), std::move(key), std::move(sparse),
                                     Scalar<"model", Str>{Str{}}, std::move(gs),
                                     std::move(bindings));
  }

  static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts,
                   Scalar<"key", std::string> key,
                   Scalar<"sparse", Bool> sparse, GlobalStateView gs,
                   State<stdlib::ResolvedBindings> bindings,
                   DateTime now) {
    stdlib::dense_record_impl::eval(std::move(ts), std::move(key),
                                    std::move(sparse), Scalar<"model", Str>{Str{}},
                                    std::move(gs), std::move(bindings), now);
  }
};

struct op_harness_replay
    : Operator<"__harness_replay", Scalar<"key", Str>, Out<TsVar<"S">>> {};
struct op_harness_record
    : Operator<"__harness_record", In<"ts", TsVar<"S">>, Scalar<"key", Str>,
               Scalar<"sparse", Bool>> {};

/** Materialize a STRUCTURAL port through a real node output (child
    sub-graph outputs must be node outputs - a python function returning
    combine[TSB[...]](...) produces a structural source). Canonical
    delta capture/apply keeps every kind's granularity. */
struct materialize_node {
  static constexpr auto name = "__materialize";

  static void eval(In<"ts", TsVar<"S">> ts, Out<TsVar<"S">> out) {
    const Value delta = capture_delta(ts.base());
    apply_delta(static_cast<const TSOutputView &>(out), delta.view());
  }
};

struct op_materialize
    : Operator<"__materialize", In<"ts", TsVar<"S">>, Out<TsVar<"S">>> {};

/** ``type_(ts)`` — the python TYPE of each tick's value. The type object
    remains an opaque PyObj contained by the native Any output. */
struct type_py_node {
  static constexpr auto name = "type_py";
  static constexpr bool uses_python_values = true;
  static constexpr bool requires_phase_runner = true;

  static void eval(In<"ts", TsVar<"S">> ts, Out<TS<AnyValue>> out) {
    translate_python_error([&] {
      // Projected structures may use non-owning ValueOps; the input's TSData
      // conversion contract is the authoritative Python value surface.
      nb::object value = ts.base().value_to_python();
      out.set(Value{PyObj{nb::borrow(value.type())}});
    });
  }
};

/** ``getattr_(TS[type], "name" | "__name__")`` — the type's __name__
    (upstream's getattr_type_name). */
struct getattr_type_name_node {
  static constexpr auto name = "getattr_type_name";
  static constexpr bool uses_python_values = true;
  static constexpr bool requires_phase_runner = true;

  static bool requires_(const ResolutionMap &, OperatorCallContext context) {
    using namespace hgraph::operator_type_resolution;
    const auto *schema = time_series_schema_at(context, 0);
    const Str *attr = context.scalar_as<Str>("attr");
    return schema != nullptr && schema->kind == TSTypeKind::TS &&
           attr != nullptr &&
           schema->value_schema ==
               TypeRegistry::instance().value_type("object") &&
           (*attr == "name" || *attr == "__name__");
  }

  static void eval(In<"ts", TS<AnyValue>> ts, Scalar<"attr", Str> attr,
                   Out<TS<Str>> out) {
    static_cast<void>(attr);
    translate_python_error([&] {
      const auto *value = ts.contained_value().try_as<PyObj>();
      if (value == nullptr) {
        throw std::invalid_argument(
            "getattr_ type input is not a Python type object");
      }
      out.set(Str{nb::cast<std::string>(value->get().attr("__name__"))});
    });
  }
};

struct op_recover_pt
    : Operator<"__recovering_pass_through", In<"ts", TsVar<"S">>,
               Scalar<"fq_key", Str>, Out<TsVar<"S">>> {};

} // namespace

template <> struct std::hash<PyGenStateRef> {
  [[nodiscard]] std::size_t
  operator()(const PyGenStateRef &ref) const noexcept {
    return std::hash<const void *>{}(ref.handle);
  }
};

template <> struct std::hash<PyFastComputeStateRef> {
  [[nodiscard]] std::size_t
  operator()(const PyFastComputeStateRef &ref) const noexcept {
    return std::hash<const void *>{}(ref.cache);
  }
};

namespace hgraph::static_schema_detail {
template <> struct scalar_name<PyGenStateRef> {
  static constexpr std::string_view value{"py_gen_state"};
};

template <> struct scalar_name<PyFastComputeStateRef> {
  static constexpr std::string_view value{"py_fast_compute_state"};
};
} // namespace hgraph::static_schema_detail

namespace hgraph::python_bridge {
/** The python-node operator registrations, shared by NB_MODULE init and
    reset_registries (module.cpp) - keep this the ONLY copy. */
void register_python_overloads() {
  (void)
      scalar_descriptor<PyObj>::value_meta(); // opaque fallback held inside Any
  TypeRegistry::instance().register_value_type_alias(
      "object", TypeRegistry::instance().any());
  register_overload<op_materialize, materialize_node>();
  register_overload<op_py_compute, py_fast_compute_node>();
  register_overload<op_py_compute, py_compute_node>();
  register_overload<op_py_compute_recordable, py_compute_recordable_node>();
  register_overload<op_py_sink, py_sink_node>();
  register_overload<op_py_generator, py_generator_node>();
  register_overload<op_recover_pt,
                    stdlib::component_detail::recovering_pass_through>();
  register_overload<op_harness_replay, harness_replay>();
  register_overload<op_harness_record, harness_record>();
  register_overload<stdlib::type_, type_py_node>();
  register_overload<stdlib::getattr_, getattr_type_name_node>();
}
} // namespace hgraph::python_bridge
