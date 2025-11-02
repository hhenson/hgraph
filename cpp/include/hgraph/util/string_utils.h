//
// Created by Howard Henson on 06/09/2025.
//

#ifndef HGRAPH_CPP_ENGINE_STRING_UTILS_H
#define HGRAPH_CPP_ENGINE_STRING_UTILS_H

#include <hgraph/hgraph_base.h>


namespace hgraph {
    template<typename T>
    std::string to_string(const T &value);

    template<>
    std::string to_string(const bool &value);

    template<>
    std::string to_string(const int64_t &value);

    template<>
    std::string to_string(const double &value);

    template<>
    std::string to_string(const engine_time_t &value);

    template<>
    std::string to_string(const engine_date_t &value);

    template<>
    std::string to_string(const engine_time_delta_t &value);

    template<>
    std::string to_string(const nb::object &value);
} // namespace hgraph

#endif  // HGRAPH_CPP_ENGINE_STRING_UTILS_H