//
// Created by Howard Henson on 26/12/2024.
//

#ifndef HGRAPH_FORWARD_DECLARATIONS_H
#define HGRAPH_FORWARD_DECLARATIONS_H

#include <nanobind/intrusive/ref.h>
#include <string>
#include <functional>
#include <memory>

namespace hgraph {
    struct Traits;
    using traits_ptr = Traits*;
    using const_traits_ptr = const Traits*;
    using traits_s_ptr = std::shared_ptr<Traits>;

    using c_string_ref = std::reference_wrapper<const std::string>;
}

#endif  // HGRAPH_FORWARD_DECLARATIONS_H
