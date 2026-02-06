#include <hgraph/api/python/wrapper_factory.h>

#include <hgraph/nodes/base_python_node.h>
#include <hgraph/runtime/evaluation_engine.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input_view.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output_view.h>
#include <hgraph/util/date_time.h>
#include <iostream>

namespace hgraph
{
    BasePythonNode::BasePythonNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::s_ptr signature,
                                   nb::dict scalars, nb::callable eval_fn, nb::callable start_fn, nb::callable stop_fn,
                                   const TSMeta* input_meta, const TSMeta* output_meta,
                                   const TSMeta* error_output_meta, const TSMeta* recordable_state_meta)
        : Node(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars),
               input_meta, output_meta, error_output_meta, recordable_state_meta),
          _eval_fn{std::move(eval_fn)}, _start_fn{std::move(start_fn)}, _stop_fn{std::move(stop_fn)} {}

    void BasePythonNode::_cache_view_pointers(const nb::object& wrapped) {
        if (wrapped.is_none()) return;

        // All time-series wrappers inherit from PyTimeSeriesType, which holds the base TSView
        if (nb::isinstance<PyTimeSeriesType>(wrapped)) {
            auto& py_ts = nb::cast<PyTimeSeriesType&>(wrapped);
            _cached_views.push_back(&py_ts.view());
        }
        // Input wrappers also have an internal TSInputView with its own TSView
        if (nb::isinstance<PyTimeSeriesInput>(wrapped)) {
            auto& py_input = nb::cast<PyTimeSeriesInput&>(wrapped);
            _cached_views.push_back(&py_input.input_view().ts_view());
        }
        // Output wrappers also have an internal TSOutputView with its own TSView
        else if (nb::isinstance<PyTimeSeriesOutput>(wrapped)) {
            auto& py_output = nb::cast<PyTimeSeriesOutput&>(wrapped);
            _cached_views.push_back(&py_output.output_view().ts_view());
        }
    }

    void BasePythonNode::_update_cached_view_times() {
        engine_time_t current = graph()->evaluation_time();
        for (auto* view : _cached_views) {
            view->set_current_time(current);
        }
    }

    void BasePythonNode::_initialise_kwargs() {
        // Assuming Injector and related types are properly defined, and scalars is a map-like container
        _kwargs = {};
        _cached_views.clear();

        bool  has_injectables{signature().injectables != 0};
        auto *injectable_map = has_injectables ? &(*signature().injectable_inputs) : nullptr;
        auto g = graph();
        auto &signature_args = signature().args;

        // Pre-compute the number of TSView pointers we'll cache (2 per wrapped TS object)
        size_t ts_wrapper_count = 0;
        if (injectable_map) {
            for (const auto& [key, inj] : *injectable_map) {
                if (std::ranges::find(signature_args, key) == std::ranges::end(signature_args)) continue;
                if ((inj & InjectableTypesEnum::OUTPUT) != InjectableTypesEnum::NONE ||
                    (inj & InjectableTypesEnum::RECORDABLE_STATE) != InjectableTypesEnum::NONE) {
                    ts_wrapper_count++;
                }
            }
        }
        if (has_input() && ts_input()->meta()) {
            const TSMeta* meta = ts_input()->meta();
            if (meta->kind == TSKind::TSB) {
                for (size_t i = 0; i < meta->field_count; ++i) {
                    if (std::ranges::find(signature_args, std::string(meta->fields[i].name)) != std::ranges::end(signature_args)) {
                        ts_wrapper_count++;
                    }
                }
            } else if (signature().time_series_inputs.has_value()) {
                ts_wrapper_count++;
            }
        }
        _cached_views.reserve(ts_wrapper_count * 2);

        nb::object node_wrapper{};
        auto       get_node_wrapper = [&]() -> nb::object {
            if (!node_wrapper) { node_wrapper = wrap_node(shared_from_this()); }
            return node_wrapper;
        };
        for (const auto &[key_, value] : scalars()) {
            std::string key{nb::cast<std::string>(key_)};
            // Only include scalars that are in signature.args (same as Python implementation)
            if (std::ranges::find(signature_args, key) == std::ranges::end(signature_args)) { continue; }
            try {
                if (injectable_map && injectable_map->contains(key)) {
                    auto       injectable = injectable_map->at(key);
                    nb::object wrapped_value;

                    if ((injectable & InjectableTypesEnum::NODE) != InjectableTypesEnum::NONE) {
                        wrapped_value = get_node_wrapper();
                    } else if ((injectable & InjectableTypesEnum::OUTPUT) != InjectableTypesEnum::NONE) {
                        if (has_output()) {
                            auto view = ts_output()->view(graph()->evaluation_time());
                            wrapped_value = wrap_output_view(view);
                        } else {
                            wrapped_value = nb::none();
                        }
                    } else if ((injectable & InjectableTypesEnum::SCHEDULER) != InjectableTypesEnum::NONE) {
                        auto sched    = scheduler();
                        wrapped_value = wrap_node_scheduler(sched);
                    } else if ((injectable & InjectableTypesEnum::ENGINE_API) != InjectableTypesEnum::NONE) {
                        if (g) {
                            auto engine_api = g->evaluation_engine_api();
                            if (engine_api) {
                                wrapped_value = wrap_evaluation_engine_api(engine_api);
                            } else {
                                wrapped_value = nb::none();
                            }
                        } else {
                            wrapped_value = nb::none();
                        }
                    } else if ((injectable & InjectableTypesEnum::CLOCK) != InjectableTypesEnum::NONE) {
                        if (g) {
                            auto clock = g->evaluation_clock();
                            if (clock) {
                                wrapped_value = wrap_evaluation_clock(clock);
                            } else {
                                wrapped_value = nb::none();
                            }
                        } else {
                            wrapped_value = nb::none();
                        }
                    } else if ((injectable & InjectableTypesEnum::TRAIT) != InjectableTypesEnum::NONE) {
                        wrapped_value = g ? wrap_traits(&g->traits(), g->shared_from_this()) : nb::none();
                    } else if ((injectable & InjectableTypesEnum::RECORDABLE_STATE) != InjectableTypesEnum::NONE) {
                        if (!has_recordable_state()) { throw std::runtime_error("Recordable state not set"); }
                        auto view = ts_recordable_state()->view(graph()->evaluation_time());
                        wrapped_value = wrap_output_view(view);
                    } else {
                        // Fallback: call injector with this node (same behaviour as python impl)
                        wrapped_value = value(get_node_wrapper());
                    }

                    _cache_view_pointers(wrapped_value);
                    _kwargs[key_] = wrapped_value;
                    continue;
                }
                _kwargs[key_] = value;
            } catch (const nb::python_error &e) {
                throw NodeException::capture_error(e, *this, std::string("Initialising kwargs for '" + key + "'"));
            } catch (const std::exception &e) {
                throw NodeException::capture_error(e, *this, std::string("Initialising kwargs for '" + key + "'"));
            }
        }
        try {
            _initialise_kwarg_inputs();
        } catch (const nb::python_error &e) {
            throw NodeException::capture_error(e, *this, "Initialising kwargs");
        } catch (const std::exception &e) { throw NodeException::capture_error(e, *this, "Initialising kwargs"); }
    }

    void BasePythonNode::_initialise_kwarg_inputs() {
        // This can be called during wiring in the current flow, would be worth looking into that to clean up, but for now protect
        if (graph() == nullptr) { return; }

        // Check if we have TSInput (new path) or legacy input
        if (has_input()) {
            // TSInput path: wrap TSInputView for each field
            auto &signature_args = signature().args;
            const TSMeta* meta = ts_input()->meta();
            if (!meta) return;

            if (meta->kind == TSKind::TSB) {
                // Bundle input: each field is a separate kwarg
                for (size_t i = 0; i < meta->field_count; ++i) {
                    std::string key = meta->fields[i].name;
                    if (std::ranges::find(signature_args, key) != std::ranges::end(signature_args)) {
                        // Get TSInputView for this field and wrap it for Python using the correct wrapper
                        TSInputView field_view = ts_input()->view(graph()->evaluation_time())[i];
                        nb::object wrapped = wrap_input_view(field_view);
                        if (wrapped.is_none()) {
                            throw std::runtime_error(
                                std::string("BasePythonNode::_initialise_kwarg_inputs: Failed to wrap TSInputView for '") +
                                key + "'.");
                        }
                        _cache_view_pointers(wrapped);
                        _kwargs[key.c_str()] = wrapped;
                    }
                }
            } else {
                // Non-bundle (scalar) input: the entire input is a single kwarg
                // Find the parameter name from time_series_inputs
                if (signature().time_series_inputs.has_value()) {
                    for (const auto& [key, _] : *signature().time_series_inputs) {
                        if (std::ranges::find(signature_args, key) != std::ranges::end(signature_args)) {
                            // Wrap the entire TSInputView
                            TSInputView input_view = ts_input()->view(graph()->evaluation_time());
                            nb::object wrapped = wrap_input_view(input_view);
                            if (wrapped.is_none()) {
                                throw std::runtime_error(
                                    std::string("BasePythonNode::_initialise_kwarg_inputs: Failed to wrap TSInputView for '") +
                                    key + "'.");
                            }
                            _cache_view_pointers(wrapped);
                            _kwargs[key.c_str()] = wrapped;
                            break;  // Only one scalar input expected for non-TSB
                        }
                    }
                }
            }
        }
    }

    void BasePythonNode::_initialise_state() {
        if (!has_recordable_state()) { return; }

        // Get RecordReplayContext and check if RECOVER mode is active
        nb::object context_cls = get_record_replay_context();
        nb::object context     = context_cls.attr("instance")();
        nb::object mode        = context.attr("mode");

        // Get RecordReplayEnum to check for RECOVER flag
        nb::object enum_cls     = get_record_replay_enum();
        nb::object recover_flag = enum_cls.attr("RECOVER");

        // Check if RECOVER is in mode (using bitwise 'in' operation via Python __contains__)
        nb::object anded{mode.attr("__and__")(recover_flag)};
        bool       is_recover_mode = nb::cast<bool>(nb::bool_(anded));

        if (!is_recover_mode) { return; }

        // Get the evaluation clock as a Python object
        nb::object clock = wrap_evaluation_clock(graph()->evaluation_clock());

        // Get the fully qualified recordable ID
        nb::object  fq_recordable_id_fn = get_fq_recordable_id_fn();
        nb::object  traits_obj       = wrap_traits(&graph()->traits(), graph()->shared_from_this());
        std::string record_replay_id = signature().record_replay_id.value_or("");
        nb::object  recordable_id    = fq_recordable_id_fn(traits_obj, nb::str(record_replay_id.c_str()));

        // Get evaluation time minus MIN_TD
        engine_time_t eval_time = graph()->evaluation_time();
        engine_time_t tm        = eval_time - MIN_TD;

        // Get as_of time
        nb::object get_as_of = get_as_of_fn();
        nb::object as_of     = get_as_of(clock);

        // Get the recordable_state type from the Python signature object
        // Cast the C++ signature to a Python object to access the recordable_state property
        nb::object py_signature          = nb::cast(&signature());
        nb::object recordable_state_type = py_signature.attr("recordable_state");
        nb::object tsb_type              = recordable_state_type.attr("tsb_type").attr("py_type");

        // Call replay_const to restore state
        nb::object replay_const   = get_replay_const_fn();
        nb::object restored_state = replay_const(nb::str("__state__"), tsb_type, nb::arg("recordable_id") = recordable_id,
                                                 nb::arg("tm") = nb::cast(tm), nb::arg("as_of") = as_of);

        // Set the value on recordable_state via TSOutput
        if (ts_recordable_state()) {
            auto view = ts_recordable_state()->view(graph()->evaluation_time());
            view.from_python(restored_state.attr("value"));
        }
    }

    class ContextManager
    {
      public:
        explicit ContextManager(BasePythonNode &node) {
            if (node.signature().context_inputs.has_value() && !node.signature().context_inputs->empty()) {
                contexts_.reserve(node.signature().context_inputs->size());
                auto* ts_input = node.ts_input();
                if (ts_input) {
                    auto input_view = ts_input->view(node.graph()->evaluation_time());
                    for (const auto &context_key : *node.signature().context_inputs) {
                        auto field_view = input_view.field(context_key);
                        if (field_view.valid()) {
                            nb::object context_value = field_view.to_python();
                            context_value.attr("__enter__")();
                            contexts_.push_back(context_value);
                        }
                    }
                }
            }
        }

        ~ContextManager() {
            std::exception_ptr first_error;
            for (auto it = contexts_.rbegin(); it != contexts_.rend(); ++it) {
                try {
                    it->attr("__exit__")(nb::none(), nb::none(), nb::none());
                } catch (const nb::python_error &e) {
                    if (!first_error) { first_error = std::current_exception(); }
                } catch (const std::exception &e) {
                    if (!first_error) { first_error = std::current_exception(); }
                }
            }
            if (first_error) { std::rethrow_exception(first_error); }
        }

      private:
        std::vector<nb::object> contexts_;
        std::exception_ptr      cached_error_;
    };

    void BasePythonNode::do_eval() {
        _update_cached_view_times();
        ContextManager context_manager(*this);
        try {
            auto out{_eval_fn(**_kwargs)};
            if (!out.is_none()) {
                if (has_output()) {
                    auto view = ts_output()->view(graph()->evaluation_time());
                    view.from_python(out);
                }
            }
        } catch (nb::python_error &e) {
            throw NodeException::capture_error(e, *this, "During Python node evaluation");
        } catch (std::exception &e) {
            throw NodeException::capture_error(e, *this, "During Python node evaluation");
        }
    }

    void BasePythonNode::do_start() {
        if (_start_fn.is_valid() && !_start_fn.is_none()) {
            // Get the callable signature parameters using inspect.signature
            // This matches Python's approach: signature(self.start_fn).parameters.keys()
            // Using __code__.co_varnames includes local variables, not just parameters
            auto inspect = nb::module_::import_("inspect");
            auto sig     = inspect.attr("signature")(_start_fn);
            auto params  = sig.attr("parameters").attr("keys")();

            // Filter kwargs to only include parameters in start_fn signature
            nb::dict filtered_kwargs;
            for (auto k : params) {
                if (_kwargs.contains(k)) { filtered_kwargs[k] = _kwargs[k]; }
            }
            // Call start_fn with filtered kwargs
            _start_fn(**filtered_kwargs);
        }
    }

    void BasePythonNode::do_stop() {
        if (_stop_fn.is_valid() and !_stop_fn.is_none()) {
            // Get the callable signature parameters using inspect.signature
            // This matches Python's approach: signature(self.stop_fn).parameters.keys()
            // Using __code__.co_varnames includes local variables, not just parameters
            auto inspect = nb::module_::import_("inspect");
            auto sig     = inspect.attr("signature")(_stop_fn);
            auto params  = sig.attr("parameters").attr("keys")();

            // Filter kwargs to only include parameters in stop_fn signature
            nb::dict filtered_kwargs;
            for (auto k : params) {
                if (_kwargs.contains(k)) { filtered_kwargs[k] = _kwargs[k]; }
            }

            // Call stop_fn with filtered kwargs
            _stop_fn(**filtered_kwargs);
        }
    }

    void BasePythonNode::initialise() {}

    void BasePythonNode::start() {
        if (graph() == nullptr) { throw std::runtime_error("BasePythonNode::start: missing owning graph"); }
        _initialise_kwargs();
        _initialise_inputs();
        _initialise_state();
        Node::start();
    }

    void BasePythonNode::dispose() {
        _cached_views.clear();
        _kwargs.clear();
    }
}  // namespace hgraph