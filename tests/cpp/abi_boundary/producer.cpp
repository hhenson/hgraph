#include "fixture_api.h"

#include <hgraph/types/metadata/type_record_registry.h>

#include <cstddef>

namespace
{
    constexpr std::uint32_t FIXTURE_OPS_MARKER = 0x48474142u;

    int value_payload = 41;
    int graph_payload = 17;
    int ts_input_payload = 23;

    int read_value(const void *data)
    {
        return *static_cast<const int *>(data);
    }

    void write_value(void *data, int value)
    {
        *static_cast<int *>(data) = value;
    }

    HGraphAbiFixtureOps value_ops{FIXTURE_OPS_MARKER, &read_value, &write_value};
    HGraphAbiFixtureOps graph_ops{FIXTURE_OPS_MARKER + 1u, &read_value, nullptr};
    HGraphAbiFixtureOps ts_input_ops{FIXTURE_OPS_MARKER + 2u, &read_value, nullptr};

    hgraph::SchemaHeader value_schema{hgraph::TypeFamily::Value, 41, "abi.fixture.value"};
    hgraph::SchemaHeader graph_schema{hgraph::TypeFamily::Graph, 42, "abi.fixture.graph"};
    hgraph::SchemaHeader ts_schema{hgraph::TypeFamily::TimeSeries, 43, "abi.fixture.ts"};

    const hgraph::TypeRecord &value_record()
    {
        return hgraph::TypeRecordRegistry::instance().intern(
            {{&value_schema, hgraph::TypeRole::Instance, &hgraph::MemoryUtils::plan_for<int>(), &value_ops, nullptr},
             1,
             hgraph::TypeCapabilities::Mutable | hgraph::TypeCapabilities::Viewable,
             "abi.fixture.value.native"});
    }

    const hgraph::TypeRecord &graph_record()
    {
        return hgraph::TypeRecordRegistry::instance().intern(
            {{&graph_schema, hgraph::TypeRole::Runtime, &hgraph::MemoryUtils::plan_for<int>(), &graph_ops, nullptr},
             1,
             hgraph::TypeCapabilities::Viewable,
             "abi.fixture.graph.native"});
    }

    const hgraph::TypeRecord &ts_input_record()
    {
        return hgraph::TypeRecordRegistry::instance().intern(
            {{&ts_schema, hgraph::TypeRole::Input, &hgraph::MemoryUtils::plan_for<int>(), &ts_input_ops, nullptr},
             1,
             hgraph::TypeCapabilities::Viewable,
             "abi.fixture.ts.input"});
    }
}

HGraphAbiLayoutSnapshot hgraph_abi_fixture_layout() noexcept
{
    return {
        .fixture_abi_version = 1,
        .pointer_size = sizeof(void *),
        .schema_size = sizeof(hgraph::SchemaHeader),
        .schema_alignment = alignof(hgraph::SchemaHeader),
        .type_record_size = sizeof(hgraph::TypeRecord),
        .type_record_alignment = alignof(hgraph::TypeRecord),
        .type_record_schema_offset = offsetof(hgraph::TypeRecord, schema),
        .type_record_plan_offset = offsetof(hgraph::TypeRecord, plan),
        .type_record_ops_offset = offsetof(hgraph::TypeRecord, ops),
        .any_ptr_size = sizeof(hgraph::AnyPtr),
        .any_ptr_alignment = alignof(hgraph::AnyPtr),
        .any_ptr_data_offset = hgraph::detail::TypePointerLayoutAccess::data_offset,
    };
}

hgraph::AnyPtr hgraph_abi_fixture_read_only()
{
    return hgraph::AnyPtr::read_only(value_record(), &value_payload);
}

hgraph::AnyPtr hgraph_abi_fixture_writable()
{
    return hgraph::AnyPtr::writable(value_record(), &value_payload);
}

hgraph::AnyPtr hgraph_abi_fixture_typed_null()
{
    return hgraph::AnyPtr::typed_null(value_record());
}

hgraph::AnyPtr hgraph_abi_fixture_graph_pointer()
{
    return hgraph::AnyPtr::read_only(graph_record(), &graph_payload);
}

hgraph::AnyPtr hgraph_abi_fixture_ts_input_pointer()
{
    return hgraph::AnyPtr::read_only(ts_input_record(), &ts_input_payload);
}

const hgraph::TypeRecord *hgraph_abi_fixture_bad_record() noexcept
{
    static const hgraph::TypeRecord invalid = [] {
        hgraph::TypeRecord copy = value_record();
        copy.abi_version = hgraph::TYPE_RECORD_ABI_VERSION + 1;
        return copy;
    }();
    return &invalid;
}

int hgraph_abi_fixture_payload() noexcept
{
    return value_payload;
}

void hgraph_abi_fixture_reset() noexcept
{
    value_payload = 41;
}
