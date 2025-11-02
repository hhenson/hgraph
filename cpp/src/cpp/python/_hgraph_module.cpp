/*
 * The entry point into the python _hgraph module exposing the C++ types to python.
 *
 * Note that as a pattern, we will return alias shared pointers for objects that have their life-times managed by an outer object
 * such as ExecutionGraph, where the life-time of the objects contained within are all directly managed by the outer object.
 * This reduces the number of shared pointers that need to be constructed inside the graph and thus provides a small improvement
 * on memory and general performance.
 *
 */
#include <hgraph/hgraph_base.h>
#include <hgraph/types/error_type.h>
#include <hgraph/util/stack_trace.h>

#include <nanobind/intrusive/counter.inl>

void export_runtime(nb::module_ &);

void export_builders(nb::module_ &);

void export_types(nb::module_ &);

void export_utils(nb::module_ &);

void export_nodes(nb::module_ &);

NB_MODULE(_hgraph, m) {
    nb::set_leak_warnings(false);
    m.doc() = "The HGraph C++ runtime engine";
    nb::intrusive_init(
        [](PyObject *o) noexcept {
            nb::gil_scoped_acquire guard;
            Py_INCREF(o);
        },
        [](PyObject *o) noexcept {
            nb::gil_scoped_acquire guard;
            Py_DECREF(o);
        });

    // Translate hgraph::NodeException into the Python hgraph.NodeException to match Python error shape
    nb::register_exception_translator([](const std::exception_ptr &p, void *) {
        try {
            if (p) std::rethrow_exception(p);
        } catch (const hgraph::NodeException &e) {
            try {
                // Import Python hgraph.NodeException class
                nb::object hgraph_mod = nb::module_::import_("hgraph");
                nb::object py_node_exc_cls = hgraph_mod.attr("NodeException");
                // Raise Python NodeException by setting the exception type and constructor args
                nb::tuple args = nb::make_tuple(
                    nb::cast(e.signature_name),
                    nb::cast(e.label),
                    nb::cast(e.wiring_path),
                    nb::cast(e.error_msg),
                    nb::cast(e.stack_trace),
                    nb::cast(e.activation_back_trace),
                    nb::cast(e.additional_context)
                );
                PyErr_SetObject(py_node_exc_cls.ptr(), args.ptr());
            } catch (...) {
                // Fallback to RuntimeError if anything goes wrong to avoid swallowing the error
                PyErr_SetString(PyExc_RuntimeError, e.what());
            }
            throw nb::python_error();
        }
    }, nullptr);

    nb::class_<nb::intrusive_base>(
        m, "intrusive_base",
        nb::intrusive_ptr<nb::intrusive_base>(
            [](nb::intrusive_base *o, PyObject *po) noexcept { o->set_self_py(po); }));

    // Install crash handlers for automatic stack traces on crashes
    hgraph::install_crash_handlers();

    // Expose stack trace functions to Python
    m.def("get_stack_trace", &hgraph::get_stack_trace, "Get current C++ stack trace as a string");
    m.def("print_stack_trace", &hgraph::print_stack_trace, "Print current C++ stack trace to stderr");

    export_utils(m);
    export_types(m);
    export_builders(m);
    export_runtime(m);
    export_nodes(m);
}

/*
 nb::class_<Object>(
   m, "Object",
   nb::intrusive_ptr<Object>(
       [](Object *o, PyObject *po) noexcept { o->set_self_py(po); }));
*/