#include <hgraph/types/traits.h>

namespace hgraph {
    Traits::Traits(Traits::ptr parent_traits) : _parent_traits{std::move(parent_traits)} {
    }

    void Traits::set_traits(nb::kwargs traits) { _traits.update(traits); }

    void Traits::set_trait(const std::string &trait_name, nb::object value) const {
        const_cast<nb::dict &>(_traits)[trait_name.c_str()] = value;
    }

    nb::object Traits::get_trait(const std::string &trait_name) const { return _traits[trait_name.c_str()]; }

    nb::object Traits::get_trait_or(const std::string &trait_name, nb::object def_value) const {
        return _traits.contains(trait_name.c_str()) ? _traits[trait_name.c_str()] : std::move(def_value);
    }

    Traits::ptr Traits::copy() const {
        auto new_traits{_parent_traits.has_value() ? new Traits(_parent_traits.value()) : new Traits()};
        new_traits->set_traits(nb::cast<nb::kwargs>(_traits));
        return new_traits;
    }

    void Traits::register_with_nanobind(nb::module_ &m) {
        nb::class_ < Traits, nb::intrusive_base > (m, "Traits")
                .def("get_trait", &Traits::get_trait, "trait_name"_a)
                .def("set_traits", &Traits::set_traits)
                .def("get_trait_or", &Traits::get_trait_or, "trait_name"_a, "def_value"_a = nb::none())
                .def("copy", &Traits::copy);
    }
} // namespace hgraph