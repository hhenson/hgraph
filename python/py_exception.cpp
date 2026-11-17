#include "py_exception.h"

#include <hgraph/util/scope.h>

namespace hgraph::python_bridge
{
    namespace
    {
        /** Release under the GIL: the last copy may be destroyed without it. */
        void release_python_exception(PyObject *value) noexcept
        {
            nb::gil_scoped_acquire gil;
            Py_DECREF(value);
        }
    } // namespace

    PythonCallbackError::PythonCallbackError(const nb::python_error &error)
        : std::runtime_error(error.what()),
          value_(error.value().inc_ref().ptr(), &release_python_exception)
    {
    }

    nb::object PythonCallbackError::value() const
    {
        return nb::borrow<nb::object>(nb::handle(value_.get()));
    }

    nb::object python_cause(const std::exception_ptr &error) noexcept
    {
        if (error == nullptr) { return {}; }
        nb::object cause;
        // A failure that is no ``std::exception`` carries no Python exception
        // this walk could reach, and leaves the cause unset.
        static_cast<void>(fallback_on_exception(false, [&]() -> bool {
            try
            {
                std::rethrow_exception(error);
            }
            catch (const std::exception &caught)
            {
                return find_nested_exception<PythonCallbackError>(
                    caught, [&](const PythonCallbackError &python) noexcept { cause = python.value(); });
            }
        }));
        return cause;
    }

    nb::object make_runtime_error(const char *message, nb::object cause) noexcept
    {
        PyObject *text = PyUnicode_FromString(message);
        PyObject *instance = text != nullptr
                                 ? PyObject_CallFunctionObjArgs(PyExc_RuntimeError, text, nullptr)
                                 : nullptr;
        Py_XDECREF(text);
        if (instance == nullptr) { return {}; }
        // PyException_SetCause steals the reference, and suppresses the
        // implicit context as ``raise ... from`` does.
        if (cause.is_valid()) { PyException_SetCause(instance, cause.release().ptr()); }
        return nb::steal<nb::object>(instance);
    }
} // namespace hgraph::python_bridge
