/*
 * The core imports for hgraph. Use this to ensure the correct import order can be maintained.
 * This is especially required when using the nanobind bindings as we otherwise get strange behaviour when mapping
 * between Python and C++ objects.
 */

#ifndef HGRAPH_BASE_H
#define HGRAPH_BASE_H

#include <nanobind/nanobind.h>
#include <nanobind/trampoline.h>
#include <nanobind/make_iterator.h>
#include <nanobind/ndarray.h>

#include <nanobind/intrusive/counter.h>
#include <nanobind/intrusive/ref.h>

#include <hgraph/python/chrono.h>
#include <hgraph/python/format.h>
#include <hgraph/python/hashable.h>
#include <hgraph/python/reference_wrapper.h>

#include <nanobind/stl/bind_map.h>
#include <nanobind/stl/bind_vector.h>
#include <nanobind/stl/function.h>
#include <nanobind/stl/map.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/set.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/string_view.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/unique_ptr.h>
#include <nanobind/stl/unordered_map.h>
#include <nanobind/stl/unordered_set.h>
#include <nanobind/stl/variant.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/wstring.h>

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include <hgraph/hgraph_export.h>
#include <hgraph/hgraph_forward_declarations.h>
#include <hgraph/util/date_time.h>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph {
    // ONLY use then when you need to return a casted ptr reference, otherwise unpack and use as a raw pointer or reference.
    template<typename T, typename T_>
    nb::ref<T> dynamic_cast_ref(nb::ref<T_> ptr) {
        auto v = dynamic_cast<T *>(ptr.get());
        if (v != nullptr) {
            return nb::ref<T>(v);
        } else {
            return nb::ref<T>();
        }
    }
} // namespace hgraph

#endif //HGRAPH_BASE_H