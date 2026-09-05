#include <hgraph/python/object_semantics.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <nanobind/nanobind.h>

#include <compare>
#include <cstddef>
#include <functional>
#include <stdexcept>
#include <string>

namespace nb = nanobind;

namespace hgraph::python_bridge {
std::size_t object_hash(PyObject *object) {
  if (object == nullptr) {
    throw std::logic_error("cannot hash an empty Python object");
  }
  nb::gil_scoped_acquire gil;
  const Py_hash_t result = PyObject_Hash(object);
  if (result == -1) {
    PyErr_Clear();
    return std::hash<const void *>{}(object);
  }
  return static_cast<std::size_t>(result);
}

bool object_equals(PyObject *lhs, PyObject *rhs) {
  if (lhs == rhs) {
    return true;
  }
  if (lhs == nullptr || rhs == nullptr) {
    return false;
  }
  nb::gil_scoped_acquire gil;
  const int result = PyObject_RichCompareBool(lhs, rhs, Py_EQ);
  if (result < 0) {
    throw nb::python_error();
  }
  if (result == 0) {
    return false;
  }
  // Semantic equality succeeded. If either side is unhashable its hash is
  // its address, so two distinct objects must not compare equal or equal
  // values would carry different hashes. Equality ran first so a Python
  // __eq__ exception keeps its public behaviour.
  if (PyObject_Hash(lhs) == -1) {
    PyErr_Clear();
    return false;
  }
  if (PyObject_Hash(rhs) == -1) {
    PyErr_Clear();
    return false;
  }
  return true;
}

std::partial_ordering object_compare(PyObject *lhs, PyObject *rhs) noexcept {
  nb::gil_scoped_acquire gil;
  try {
    if (lhs == rhs) {
      return std::partial_ordering::equivalent;
    }
    if (lhs == nullptr || rhs == nullptr) {
      return lhs == nullptr ? std::partial_ordering::less
                            : std::partial_ordering::greater;
    }
    const int less = PyObject_RichCompareBool(lhs, rhs, Py_LT);
    if (less < 0) {
      PyErr_Clear();
      return std::partial_ordering::unordered;
    }
    if (less == 1) {
      return std::partial_ordering::less;
    }
    const int greater = PyObject_RichCompareBool(lhs, rhs, Py_GT);
    if (greater < 0) {
      PyErr_Clear();
      return std::partial_ordering::unordered;
    }
    return greater == 1 ? std::partial_ordering::greater
                        : std::partial_ordering::equivalent;
  } catch (...) {
    PyErr_Clear();
    return std::partial_ordering::unordered;
  }
}

namespace {
[[nodiscard]] std::string python_utf8(nb::handle value) {
  Py_ssize_t size{};
  const char *data = PyUnicode_AsUTF8AndSize(value.ptr(), &size);
  if (data == nullptr) {
    throw nb::python_error();
  }
  return {data, static_cast<std::size_t>(size)};
}
}  // namespace

std::string object_str(PyObject *object) {
  nb::gil_scoped_acquire gil;
  nb::object text = nb::steal(PyObject_Str(object));
  if (!text.is_valid()) {
    throw nb::python_error();
  }
  return python_utf8(text);
}
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES
