//
// Created by Howard Henson on 18/12/2024.
//

#ifndef TRAITS_H
#define TRAITS_H

#include <hgraph/hgraph_base.h>

namespace hgraph {
    struct Traits : nb::intrusive_base {
        using ptr = nb::ref<Traits>;

        Traits() = default;

        explicit Traits(Traits::ptr parent_traits);

        void set_traits(nb::kwargs traits);

        void set_trait(const std::string &trait_name, nb::object value) const;

        [[nodiscard]] nb::object get_trait(const std::string &trait_name) const;

        [[nodiscard]] nb::object get_trait_or(const std::string &trait_name, nb::object def_value) const;

        [[nodiscard]] Traits::ptr copy() const;

        static void register_with_nanobind(nb::module_ &m);

    private:
        std::optional<ptr> _parent_traits;
        nb::dict _traits;
    };
} // namespace hgraph

#endif  // TRAITS_H