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

        [[nodiscard]] nb::object cause_of(const std::exception &error)
        {
            if (const auto *python = dynamic_cast<const PythonCallbackError *>(&error))
            {
                return python->value();
            }
            // A nested failure that is no ``std::exception`` has no Python
            // exception beneath it that this walk could reach. It leaves
            // through ``python_cause``, which answers with no cause.
            try
            {
                std::rethrow_if_nested(error);
            }
            catch (const std::exception &nested)
            {
                return cause_of(nested);
            }
            return {};
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
        return fallback_on_exception(nb::object{}, [&]() -> nb::object {
            try
            {
                std::rethrow_exception(error);
            }
            catch (const std::exception &caught)
            {
                return cause_of(caught);
            }
        });
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
