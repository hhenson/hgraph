//
// Created by Howard Henson on 06/05/2024.
//

#ifndef TS_TYPE_H
#define TS_TYPE_H
#include <hgraph/types/time_series_type.h>

namespace hgraph {

    struct TimeSeriesValueOutput : TimeSeriesOutput {

    };

    template<typename T>
    struct TimeSeriesValueOutputT : TimeSeriesValueOutput {

    };

    struct TimeSeriesValueInput : TimeSeriesInput {
    };

    template<typename T>
    struct TimeSeriesValueInputT : TimeSeriesValueInput {

    };

}

#endif //TS_TYPE_H
