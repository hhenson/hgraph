//
// Created by Howard Henson on 08/05/2025.
//

#ifndef REFERENCE_WRAPPER_H
#define REFERENCE_WRAPPER_H

#include <nanobind/nanobind.h>
#include <string>
#include <functional>

NAMESPACE_BEGIN (NB_NAMESPACE)
NAMESPACE_BEGIN (detail)

using string_reference_wrapper = std::reference_wrapper<const std::string>;

template<>
struct type_caster<string_reference_wrapper> {
    NB_TYPE_CASTER(string_reference_wrapper, const_name ("str")

    )

    bool from_python(handle src, uint8_t, cleanup_list *) noexcept {
        return false;
    }

    static handle from_cpp(string_reference_wrapper value, rv_policy,
                           cleanup_list *) noexcept {
        auto v{value.get()};
        return PyUnicode_FromStringAndSize(v.data(), v.size());
    }
};

NAMESPACE_END (detail)
NAMESPACE_END (NB_NAMESPACE)

#endif //REFERENCE_WRAPPER_H