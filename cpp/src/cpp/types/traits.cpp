#include <hgraph/types/traits.h>

namespace hgraph {
    Traits::Traits(const Traits* parent_traits) : _parent_traits{parent_traits} {
    }

    void Traits::set_traits(nb::kwargs traits) { _traits.update(traits); }

    void Traits::set_trait(const std::string &trait_name, nb::object value) const {
        const_cast<nb::dict &>(_traits)[trait_name.c_str()] = value;
    }

    nb::object Traits::get_trait(const std::string &trait_name) const {
        if (_traits.contains(trait_name.c_str())) {
            return _traits[trait_name.c_str()];
        }
        if (_parent_traits != nullptr) {
            return _parent_traits->get_trait(trait_name);
        }
        throw std::runtime_error("Trait " + trait_name + " not found");
    }

    nb::object Traits::get_trait_or(const std::string &trait_name, nb::object def_value) const {
        return _traits.contains(trait_name.c_str()) ? _traits[trait_name.c_str()] : std::move(def_value);
    }

    Traits Traits::copy(Traits* new_parent_traits) const {
        Traits new_traits{new_parent_traits != nullptr ? new_parent_traits : _parent_traits};
        new_traits.set_traits(nb::cast<nb::kwargs>(_traits));
        return new_traits;
    }

} // namespace hgraph