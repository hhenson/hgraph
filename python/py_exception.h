#ifndef HGRAPH_PYTHON_PY_EXCEPTION_H
#define HGRAPH_PYTHON_PY_EXCEPTION_H

#include <nanobind/nanobind.h>

#include <exception>
#include <memory>
#include <stdexcept>

namespace hgraph::python_bridge
{
    namespace nb = nanobind;

    /**
     * A Python exception that left a user callback, as the native runtime
     * carries it.
     *
     * The runtime knows a failure by its ``what()``: per-node capture,
     * ``try_except_`` and the root annotation all read that text, which is
     * the traceback nanobind formats. The exception object travels beside
     * the text, so that the Python caller of a run can be given it as the
     * ``__cause__`` of the ``RuntimeError`` it receives.
     *
     * The object is released under the GIL, wherever the last copy of this
     * exception is destroyed.
     */
    class PythonCallbackError final : public std::runtime_error
    {
      public:
        /**
         * From the error a callback raised. Requires the GIL: nanobind
         * formats ``what()`` from the live traceback.
         */
        explicit PythonCallbackError(const nb::python_error &error);

        /** A new reference to the Python exception. Requires the GIL. */
        [[nodiscard]] nb::object value() const;

      private:
        std::shared_ptr<PyObject> value_;
    };

    /**
     * The Python exception a native failure began as: the one ``error``
     * carries, or one nested beneath it by ``std::throw_with_nested``, as the
     * root annotation nests the failure it annotates. An invalid object when
     * there is none. Requires the GIL.
     */
    [[nodiscard]] nb::object python_cause(const std::exception_ptr &error) noexcept;

    /**
     * A new ``RuntimeError(message)``, not yet raised; ``cause``, when valid,
     * becomes its ``__cause__``. An invalid object when the instance could not
     * be made: the Python error of that failure is then the active one.
     */
    [[nodiscard]] nb::object make_runtime_error(const char *message, nb::object cause) noexcept;
} // namespace hgraph::python_bridge

#endif // HGRAPH_PYTHON_PY_EXCEPTION_H
