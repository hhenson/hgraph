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

} // namespace hgraph