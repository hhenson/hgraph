//
// Created by Howard Henson on 13/03/2021.
//

#ifndef HGRAPH_SCHEMA_H
#define HGRAPH_SCHEMA_H

#include <functional>
#include <memory>
#include <vector>

#include <hgraph/python/pyb.h>

namespace hgraph
{

    using type_enum_base = uint8_t;

    enum class SchemaElementTypeEnum : type_enum_base {
        BOOL,
        BYTE,
        INT,
        FLOAT,
        STRING,
        STRUCT,
        LIST,
        SET,
        DICT,
        TUPLE,
        PYTHON,
        TS,
        TSS,
        TSL,
        TSD,
        TSB
    };

    void py_register_schema_element_type_enum(py::module &m);

}  // namespace hgraph


#endif  // HGRAPH_SCHEMA_H
