//
// PyTraits Implementation
//

#include <hgraph/api/python/py_traits.h>
#include <hgraph/types/traits.h>

namespace hgraph::api {
    
    PyTraits::PyTraits(Traits* impl, control_block_ptr control_block)
        : _impl(impl, std::move(control_block)) {
    }
    
    nb::object PyTraits::get_trait(const std::string& trait_name) const {
        return _impl->get_trait(trait_name);
    }
    
    void PyTraits::set_traits(nb::kwargs traits) {
        _impl->set_traits(traits);
    }
    
    nb::object PyTraits::get_trait_or(const std::string& trait_name, nb::object def_value) const {
        return _impl->get_trait_or(trait_name, def_value);
    }
    
    nb::object PyTraits::copy() const {
        // Return the raw Traits object from copy
        auto copied = _impl->copy();
        return nb::cast(copied);
    }
    
    void PyTraits::register_with_nanobind(nb::module_& m) {
        nb::class_<PyTraits>(m, "Traits",
            "Python wrapper for Traits - provides access to graph traits")
            .def("get_trait", &PyTraits::get_trait, "trait_name"_a)
            .def("set_traits", &PyTraits::set_traits)
            .def("get_trait_or", &PyTraits::get_trait_or, "trait_name"_a, "def_value"_a = nb::none())
            .def("copy", &PyTraits::copy)
            .def("is_valid", &PyTraits::is_valid,
                "Check if this wrapper is valid (graph is alive and wrapper has value)");
    }
    
} // namespace hgraph::api

