#ifndef HGRAPH_TESTS_CPP_ABI_BOUNDARY_FIXTURE_API_H
#define HGRAPH_TESTS_CPP_ABI_BOUNDARY_FIXTURE_API_H

#include <hgraph/types/type_pointer.h>

#include <cstddef>
#include <cstdint>

#if defined(_WIN32)
#if defined(HGRAPH_ABI_FIXTURE_BUILD)
#define HGRAPH_ABI_FIXTURE_API __declspec(dllexport)
#else
#define HGRAPH_ABI_FIXTURE_API __declspec(dllimport)
#endif
#else
#define HGRAPH_ABI_FIXTURE_API __attribute__((visibility("default")))
#endif

struct HGraphAbiFixtureOps
{
    std::uint32_t marker;
    int (*read)(const void *);
    void (*write)(void *, int);
};

struct HGraphAbiLayoutSnapshot
{
    std::uint32_t fixture_abi_version;
    std::uint32_t pointer_size;
    std::size_t schema_size;
    std::size_t schema_alignment;
    std::size_t type_record_size;
    std::size_t type_record_alignment;
    std::size_t type_record_schema_offset;
    std::size_t type_record_plan_offset;
    std::size_t type_record_ops_offset;
    std::size_t any_ptr_size;
    std::size_t any_ptr_alignment;
    std::size_t any_ptr_data_offset;
};

HGRAPH_ABI_FIXTURE_API HGraphAbiLayoutSnapshot hgraph_abi_fixture_layout() noexcept;
HGRAPH_ABI_FIXTURE_API hgraph::AnyPtr hgraph_abi_fixture_read_only();
HGRAPH_ABI_FIXTURE_API hgraph::AnyPtr hgraph_abi_fixture_writable();
HGRAPH_ABI_FIXTURE_API hgraph::AnyPtr hgraph_abi_fixture_typed_null();
HGRAPH_ABI_FIXTURE_API hgraph::AnyPtr hgraph_abi_fixture_graph_pointer();
HGRAPH_ABI_FIXTURE_API hgraph::AnyPtr hgraph_abi_fixture_ts_input_pointer();
HGRAPH_ABI_FIXTURE_API const hgraph::TypeRecord *hgraph_abi_fixture_bad_record() noexcept;
HGRAPH_ABI_FIXTURE_API int hgraph_abi_fixture_payload() noexcept;
HGRAPH_ABI_FIXTURE_API void hgraph_abi_fixture_reset() noexcept;

#endif // HGRAPH_TESTS_CPP_ABI_BOUNDARY_FIXTURE_API_H
