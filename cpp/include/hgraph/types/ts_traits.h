//
// Created by Howard Henson on 02/11/2025.
//

#ifndef HGRAPH_CPP_ROOT_TS_TRAITS_H
#define HGRAPH_CPP_ROOT_TS_TRAITS_H
#include "hgraph/util/date_time.h"

namespace hgraph
{
    struct Notifiable
    {
        virtual      ~Notifiable() = default;
        virtual void notify(engine_time_t et) = 0;
    };

    struct CurrentTimeProvider
    {
        virtual                             ~CurrentTimeProvider() = default;
        [[nodiscard]] virtual engine_time_t current_engine_time() const = 0;
    };
}

#endif //HGRAPH_CPP_ROOT_TS_TRAITS_H
