//
// Created by Howard Henson on 27/06/2025.
//

#ifndef NB_TYPES_EXT_H
#define NB_TYPES_EXT_H

#include <nanobind/nanobind.h>

NAMESPACE_BEGIN (NB_NAMESPACE)

NAMESPACE_BEGIN (detail)

PyObject *frozenset_from_obj(PyObject *o) {
    PyObject *result = PyFrozenSet_New(o);
    if (!result)
        raise_python_error();
    return result;
}

#if defined(Py_LIMITED_API)
#  if PY_VERSION_HEX < 0x030C0000 || defined(PYPY_VERSION)
#    error "nanobind can target Python's limited API, but this requires CPython >= 3.12"
#  endif
#  define NB_FROZENSETSET_GET_SIZE PySet_Size
#else
#  define NB_FROZENSET_GET_SIZE PySet_GET_SIZE
#endif
NAMESPACE_END (detail)


class frozenset : public set {
    NB_OBJECT(frozenset, object, "frozenset", PyFrozenSet_Check)

    frozenset() : object(PyFrozenSet_New(nullptr), detail::steal_t()) {
    }

    explicit frozenset(handle h)
        : object(detail::frozenset_from_obj(h.ptr()), detail::steal_t{}) {
    }

    size_t size() const { return (size_t) NB_SET_GET_SIZE(m_ptr); }

    template<typename T>
    bool contains(T &&key) const;

    template<typename T>
    void add(T &&value);

    void clear() {
        if (PySet_Clear(m_ptr))
            raise_python_error();
    }

    template<typename T>
    bool discard(T &&value);
};

NAMESPACE_END (NB_NAMESPACE)

#endif //NB_TYPES_EXT_H