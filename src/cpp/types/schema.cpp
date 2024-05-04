#include <hgraph/types/schema.h>
#include <unordered_set>

namespace hgraph
{

    void py_register_schema_element_type_enum(py::module &m) {
        py::enum_<SchemaElementTypeEnum>(m, "SchemaElementTypeEnum")
            .value("BOOL", SchemaElementTypeEnum::BOOL)
            .value("BYTE", SchemaElementTypeEnum::BYTE)
            .value("INT", SchemaElementTypeEnum::INT)
            .value("FLOAT", SchemaElementTypeEnum::FLOAT)
            .value("STRING", SchemaElementTypeEnum::STRING)
            .value("STRUCT", SchemaElementTypeEnum::STRUCT)
            .value("PYTHON", SchemaElementTypeEnum::PYTHON)
            .value("TUPLE", SchemaElementTypeEnum::TUPLE)
            .value("LIST", SchemaElementTypeEnum::LIST)
            .value("SET", SchemaElementTypeEnum::SET)
            .value("DICT", SchemaElementTypeEnum::DICT)
            .value("TS", SchemaElementTypeEnum::TS)
            .value("TS", SchemaElementTypeEnum::TSS)
            .value("TSL", SchemaElementTypeEnum::TSL)
            .value("TSB", SchemaElementTypeEnum::TSB)
            .value("TSD", SchemaElementTypeEnum::TSD)
            .export_values();
    }

}  // namespace hgraph

