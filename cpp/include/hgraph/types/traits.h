//
// Created by Howard Henson on 18/12/2024.
//

#ifndef TRAITS_H
#define TRAITS_H

#include <hgraph/hgraph_base.h>
#include <memory>

namespace hgraph
{
    struct Traits
    {
        using ptr = std::shared_ptr<Traits>;

        Traits() = default;

        explicit Traits(const Traits *parent_traits = nullptr);

        void set_traits(nb::kwargs traits);

        void set_trait(const std::string &trait_name, nb::object value) const;

        [[nodiscard]] nb::object get_trait(const std::string &trait_name) const;

        [[nodiscard]] nb::object get_trait_or(const std::string &trait_name, nb::object def_value) const;

        /**
         * Copy this Traits object. If parent_traits is provided, it will be used as the parent.
         * Otherwise, the current parent (if any) will be copied.
         */
        [[nodiscard]] Traits copy(Traits *new_parent_traits = nullptr) const;

        /**
         * Get the traits dictionary (for internal use, e.g., copying traits)
         */
        [[nodiscard]] const nb::dict &get_traits_dict() const { return _traits; }

    private:
        const Traits *_parent_traits{nullptr}; // Raw pointer - parent Graph outlives this Graph
        nb::dict      _traits;
    };
} // namespace hgraph

#endif  // TRAITS_H