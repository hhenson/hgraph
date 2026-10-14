#include "fixture_api.h"

#include <hgraph/types/metadata/type_record.h>

#include <cstddef>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

namespace
{
    void require(bool condition, std::string_view message)
    {
        if (!condition) { throw std::runtime_error(std::string{message}); }
    }

    template <typename Exception, typename Operation>
    void require_throws(Operation &&operation, std::string_view message)
    {
        try
        {
            operation();
        }
        catch (const Exception &)
        {
            return;
        }
        throw std::runtime_error(std::string{message});
    }
}

int main()
{
    using namespace hgraph;

    static_assert(std::is_standard_layout_v<SchemaHeader>);
    static_assert(std::is_trivially_copyable_v<SchemaHeader>);
    static_assert(std::is_standard_layout_v<TypeRecord>);
    static_assert(std::is_trivially_copyable_v<TypeRecord>);
    static_assert(std::is_standard_layout_v<AnyPtr>);
    static_assert(std::is_trivially_copyable_v<AnyPtr>);

    try
    {
        const HGraphAbiLayoutSnapshot layout = hgraph_abi_fixture_layout();
        require(layout.fixture_abi_version == 1, "fixture ABI version mismatch");
        require(layout.pointer_size == sizeof(void *), "pointer size mismatch");
        require(layout.schema_size == sizeof(SchemaHeader), "SchemaHeader size mismatch");
        require(layout.schema_alignment == alignof(SchemaHeader), "SchemaHeader alignment mismatch");
        require(layout.type_record_size == sizeof(TypeRecord), "TypeRecord size mismatch");
        require(layout.type_record_alignment == alignof(TypeRecord), "TypeRecord alignment mismatch");
        require(layout.type_record_schema_offset == offsetof(TypeRecord, schema), "TypeRecord schema offset mismatch");
        require(layout.type_record_plan_offset == offsetof(TypeRecord, plan), "TypeRecord plan offset mismatch");
        require(layout.type_record_ops_offset == offsetof(TypeRecord, ops), "TypeRecord ops offset mismatch");
        require(layout.any_ptr_size == sizeof(AnyPtr), "AnyPtr size mismatch");
        require(layout.any_ptr_alignment == alignof(AnyPtr), "AnyPtr alignment mismatch");
        require(layout.any_ptr_data_offset == detail::TypePointerLayoutAccess::data_offset,
                "AnyPtr data offset mismatch");

        hgraph_abi_fixture_reset();
        const AnyPtr read_only = hgraph_abi_fixture_read_only();
        require(read_only.valid(), "cross-library readonly pointer is invalid");
        require(read_only.read_only_access(), "readonly access tag was not preserved");
        require(read_only.family() == TypeFamily::Value, "readonly family mismatch");
        require(read_only.role() == TypeRole::Instance, "readonly role mismatch");
        require(read_only.semantic_name() == "abi.fixture.value", "semantic label mismatch");
        require(read_only.implementation_name() == "abi.fixture.value.native", "implementation label mismatch");
        require(read_only.plan() != nullptr && read_only.plan()->layout.size == sizeof(int), "storage plan mismatch");

        const auto &ops = *static_cast<const HGraphAbiFixtureOps *>(read_only.ops());
        require(ops.marker == 0x48474142u, "ops marker mismatch");
        require(ops.read != nullptr && ops.read(read_only.data()) == 41, "cross-library ops read failed");

        const ValuePtr value = ValuePtr::checked(read_only);
        require(value.record() == read_only.record(), "typed value pointer changed the record");
        require_throws<std::invalid_argument>([&] { (void)GraphPtr::checked(read_only); },
                                              "wrong-family pointer narrowing succeeded");

        AnyPtr writable = hgraph_abi_fixture_writable();
        require(writable.writable_access(), "writable access tag was not preserved");
        AnyPtr mutation = writable.begin_mutation();
        require(mutation.mutation_access(), "mutation access tag was not preserved");
        ops.write(mutation.mutable_data(), 73);
        writable = mutation.end_mutation();
        require(writable.writable_access() && hgraph_abi_fixture_payload() == 73,
                "cross-library writable ops failed");

        const AnyPtr typed_null = hgraph_abi_fixture_typed_null();
        require(typed_null.bound() && typed_null.is_typed_null() && !typed_null.valid(),
                "typed-null state was not preserved");
        require(ValuePtr::checked(typed_null).is_typed_null(), "typed-null narrowing failed");

        const AnyPtr graph = hgraph_abi_fixture_graph_pointer();
        require(GraphPtr::checked(graph).valid(), "graph pointer narrowing failed");
        require_throws<std::invalid_argument>([&] { (void)ValuePtr::checked(graph); },
                                              "graph-to-value narrowing succeeded");

        const AnyPtr ts_input = hgraph_abi_fixture_ts_input_pointer();
        require(TSInputPtr::checked(ts_input).valid(), "TS input pointer narrowing failed");
        require_throws<std::invalid_argument>([&] { (void)TSDataPtr::checked(ts_input); },
                                              "wrong-role pointer narrowing succeeded");

        const TypeRecord *bad_record = hgraph_abi_fixture_bad_record();
        require(bad_record != nullptr && !bad_record->valid(), "incompatible record was accepted");
        require_throws<std::invalid_argument>([&] { (void)AnyPtr::read_only(*bad_record, read_only.data()); },
                                              "incompatible record constructed a pointer");
    }
    catch (const std::exception &error)
    {
        std::cerr << "ABI boundary test failed: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
