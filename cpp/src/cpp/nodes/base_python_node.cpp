#include <hgraph/nodes/base_python_node.h>
#include <hgraph/types/constants.h>
#include <hgraph/types/error_type.h>
#include <hgraph/types/graph.h>
#include <hgraph/types/traits.h>
#include <hgraph/types/tsb.h>
#include <hgraph/util/date_time.h>

namespace hgraph {
    BasePythonNode::BasePythonNode(int64_t node_ndx, std::vector<int64_t> owning_graph_id, NodeSignature::ptr signature,
                                   nb::dict scalars, nb::callable eval_fn, nb::callable start_fn, nb::callable stop_fn)
        : Node(node_ndx, std::move(owning_graph_id), std::move(signature), std::move(scalars)),
          _eval_fn{std::move(eval_fn)},
          _start_fn{std::move(start_fn)}, _stop_fn{std::move(stop_fn)} {
    }

    void BasePythonNode::_initialise_kwargs() {
        // Assuming Injector and related types are properly defined, and scalars is a map-like container
        _kwargs = {};

        bool has_injectables{signature().injectables != 0};
        for (const auto &[key_, value]: scalars()) {
            std::string key{nb::cast<std::string>(key_)};
            try {
                if (has_injectables && signature().injectable_inputs->contains(key)) {
                    // TODO: This may be better extracted directly, but for now use the python function calls.
                    nb::object node{nb::cast(this)};
                    nb::object key_handle{value(node)};
                    _kwargs[key_] = key_handle; // Assuming this call applies the Injector properly
                } else {
                    _kwargs[key_] = value;
                }
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
        } catch (const std::exception &e) {
            throw NodeException::capture_error(e, *this, "Initialising kwargs");
        }
    }

    void BasePythonNode::_initialise_kwarg_inputs() {
        auto &signature_args = signature().args;
        for (size_t i = 0, l = signature().time_series_inputs.has_value() ? signature().time_series_inputs->size() : 0;
             i < l;
             ++i) {
            // Apple does not yet support ranges::contains :(
            auto key{input()->schema().keys()[i]};
            if (std::ranges::find(signature_args, key) != std::ranges::end(signature_args)) {
                // Expose inputs as base TimeSeriesInput using nb::ref to preserve lifetime semantics.
                // Avoid casting to raw pointer, which bypasses intrusive ref counting and can cause
                // dangling references when Python holds onto the object (e.g., during iteration).
                _kwargs[key.c_str()] = nb::cast((*input())[i]);
            }
        }
    }

    void BasePythonNode::_initialise_state() {
        if (!has_recordable_state()) { return; }

        // Get RecordReplayContext and check if RECOVER mode is active
        nb::object context_cls = get_record_replay_context();
        nb::object context = context_cls.attr("instance")();
        nb::object mode = context.attr("mode");

        // Get RecordReplayEnum to check for RECOVER flag
        nb::object enum_cls = get_record_replay_enum();
        nb::object recover_flag = enum_cls.attr("RECOVER");

        // Check if RECOVER is in mode (using bitwise 'in' operation via Python __contains__)
        nb::object anded{mode.attr("__and__")(recover_flag)};
        bool is_recover_mode = nb::cast<bool>(nb::bool_(anded));

        if (!is_recover_mode) { return; }

        // Get the evaluation clock as a Python object
        nb::object clock = nb::cast(graph()->evaluation_clock());

        // Get the fully qualified recordable ID
        nb::object fq_recordable_id_fn = get_fq_recordable_id_fn();
        nb::object traits_obj = nb::cast(&(graph()->traits()));
        std::string record_replay_id = signature().record_replay_id.value_or("");
        nb::object recordable_id = fq_recordable_id_fn(traits_obj, nb::str(record_replay_id.c_str()));

        // Get evaluation time minus MIN_TD
        engine_time_t eval_time = graph()->evaluation_clock()->evaluation_time();
        engine_time_t tm = eval_time - MIN_TD;

        // Get as_of time
        nb::object get_as_of = get_as_of_fn();
        nb::object as_of = get_as_of(clock);

        // Get the recordable_state type from the Python signature object
        // Cast the C++ signature to a Python object to access the recordable_state property
        nb::object py_signature = nb::cast(&signature());
        nb::object recordable_state_type = py_signature.attr("recordable_state");
        nb::object tsb_type = recordable_state_type.attr("tsb_type").attr("py_type");

        // Call replay_const to restore state
        nb::object replay_const = get_replay_const_fn();
        nb::object restored_state = replay_const(nb::str("__state__"), tsb_type,
                                                 nb::arg("recordable_id") = recordable_id,
                                                 nb::arg("tm") = nb::cast(tm), nb::arg("as_of") = as_of);

        // Set the value on recordable_state
        recordable_state()->apply_result(restored_state.attr("value"));
    }

    void BasePythonNode::reset_input(time_series_bundle_input_ptr value) {
        Node::reset_input(value);
        _initialise_kwarg_inputs();
    }

    class ContextManager {
    public:
        explicit ContextManager(BasePythonNode &node) {
            if (node.signature().context_inputs.has_value() && !node.signature().context_inputs->empty()) {
                contexts_.reserve(node.signature().context_inputs->size());
                for (const auto &context_key: *node.signature().context_inputs) {
                    if ((*node.input())[context_key]->valid()) {
                        nb::object context_value = (*node.input())[context_key]->py_value();
                        context_value.attr("__enter__")();
                        contexts_.push_back(context_value);
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
        std::exception_ptr cached_error_;
    };

    void BasePythonNode::do_eval() {
        ContextManager context_manager(*this);
        try {
            auto out{_eval_fn(**_kwargs)};
            if (!out.is_none()) { output()->apply_result(out); }
        } catch (nb::python_error &e) { throw NodeException::capture_error(e, *this, "During Python node evaluation"); }
    }

    void BasePythonNode::do_start() {
        if (_start_fn.is_valid() && !_start_fn.is_none()) {
            // Get the callable signature parameters using inspect.signature
            // This matches Python's approach: signature(self.start_fn).parameters.keys()
            // Using __code__.co_varnames includes local variables, not just parameters
            auto inspect = nb::module_::import_("inspect");
            auto sig = inspect.attr("signature")(_start_fn);
            auto params = sig.attr("parameters").attr("keys")();

            // Filter kwargs to only include parameters in start_fn signature
            nb::dict filtered_kwargs;
            for (auto k: params) {
                if (_kwargs.contains(k)) { filtered_kwargs[k] = _kwargs[k]; }
            }
            // Call start_fn with filtered kwargs
            _start_fn(**filtered_kwargs);
        }
    }

    void BasePythonNode::do_stop() {
        if (_stop_fn.is_valid() and
        !_stop_fn.is_none()
        ) {
            // Get the callable signature parameters using inspect.signature
            // This matches Python's approach: signature(self.stop_fn).parameters.keys()
            // Using __code__.co_varnames includes local variables, not just parameters
            auto inspect = nb::module_::import_("inspect");
            auto sig = inspect.attr("signature")(_stop_fn);
            auto params = sig.attr("parameters").attr("keys")();

            // Filter kwargs to only include parameters in stop_fn signature
            nb::dict filtered_kwargs;
            for (auto k: params) {
                if (_kwargs.contains(k)) { filtered_kwargs[k] = _kwargs[k]; }
            }

            // Call stop_fn with filtered kwargs
            _stop_fn(**filtered_kwargs);
        }
    }

    void BasePythonNode::initialise() {
    }

    void BasePythonNode::start() {
        _initialise_kwargs();
        _initialise_inputs();
        _initialise_state();
        // Now call parent class
        Node::start();
    }

    void BasePythonNode::dispose() { _kwargs.clear(); }
} // namespace hgraph