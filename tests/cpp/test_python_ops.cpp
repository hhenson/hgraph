// RFC 0035: the type layer's Python slots resolve through the registered
// PythonOps table via Python-free forwarders. These tests run without any
// interpreter: the "objects" are sentinel pointers, which is all the type
// layer ever sees of them.
#include <hgraph/types/python_ops.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/value/any_ops.h>
#include <hgraph/types/value/value_ops.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <cstdint>
#include <string>
#include <typeindex>

namespace
{
    struct ProviderReset
    {
        const hgraph::PythonOps *previous{hgraph::python_ops()};
        ~ProviderReset() { hgraph::set_python_ops(previous); }
    };

    ::_object *sentinel(std::uintptr_t tag) { return reinterpret_cast<::_object *>(tag); }

    hgraph::PyNewRef fake_int_to_python(const void *, const void *memory)
    {
        return hgraph::PyNewRef{sentinel(0x1000 + static_cast<std::uintptr_t>(*static_cast<const int *>(memory)))};
    }

    void fake_int_from_python(const void *, const hgraph::ValueTypeRef &, void *memory, hgraph::PyRef source)
    {
        *static_cast<int *>(memory) = static_cast<int>(reinterpret_cast<std::uintptr_t>(source.ptr) - 0x1000);
    }

    const hgraph::PythonScalarSlots &int_slots()
    {
        static const hgraph::PythonScalarSlots slots{
            .to_python = &fake_int_to_python, .from_python = &fake_int_from_python, .to_python_buffer = nullptr};
        return slots;
    }

    const hgraph::PythonScalarSlots *conversion_for(const std::type_info &type)
    {
        return std::type_index{type} == std::type_index{typeid(int)} ? &int_slots() : nullptr;
    }

    hgraph::PyNewRef fake_any_to_python(const void *, const void *) { return hgraph::PyNewRef{sentinel(0x2000)}; }

    const hgraph::PythonOps &fake_provider()
    {
        static const hgraph::PythonOps ops = [] {
            hgraph::PythonOps table;
            table.scalars.conversion_for = &conversion_for;
            table.any.to_python          = &fake_any_to_python;
            return table;
        }();
        return ops;
    }
}  // namespace

TEST_CASE("scalar slots resolve through the registered PythonOps table", "[python_ops][rfc0035]")
{
    ProviderReset reset;
    const auto   &ops = hgraph::ops_for<int>();
    REQUIRE(ops.to_python_impl != nullptr);
    REQUIRE(ops.from_python_impl != nullptr);

    hgraph::set_python_ops(&fake_provider());
    int value = 7;
    const auto result = ops.to_python_impl(ops.context, &value);
    CHECK(result.ptr == sentinel(0x1007));

    int written = 0;
    ops.from_python_impl(ops.context, hgraph::ValueTypeRef{}, &written, hgraph::PyRef{sentinel(0x1003)});
    CHECK(written == 3);
}

TEST_CASE("a scalar the provider does not know throws the named error", "[python_ops][rfc0035]")
{
    ProviderReset reset;
    hgraph::set_python_ops(&fake_provider());
    const auto &ops = hgraph::ops_for<double>();
    double      value = 1.5;
    CHECK_THROWS_WITH(ops.to_python_impl(ops.context, &value),
                      Catch::Matchers::ContainsSubstring("no Python conversion is registered for the scalar type"));
}

TEST_CASE("family forwarders read the table when called, so registration order is free",
          "[python_ops][rfc0035]")
{
    ProviderReset reset;
    hgraph::set_python_ops(nullptr);
    const auto &any = hgraph::any_ops();  // the table exists before any provider
    CHECK_THROWS_WITH(any.to_python_impl(any.context, nullptr),
                      Catch::Matchers::ContainsSubstring("no Python conversion is registered for Any"));

    hgraph::set_python_ops(&fake_provider());
    CHECK(any.to_python_impl(any.context, nullptr).ptr == sentinel(0x2000));

    // An entry the provider left null is reported the same way.
    CHECK_THROWS_WITH(any.from_python_impl(any.context, hgraph::ValueTypeRef{}, nullptr, hgraph::PyRef{}),
                      Catch::Matchers::ContainsSubstring("no Python conversion is registered for Any"));
}

TEST_CASE("a TSData table without a Python-authoring family holds the throwing table, never null",
          "[python_ops][rfc0035]")
{
    const hgraph::TSDataOps ops{};
    REQUIRE(ops.python_ops != nullptr);
    CHECK(ops.python_ops == &hgraph::ts_data_detail::missing_python_ts_data_ops());
    CHECK_THROWS_WITH(ops.python_ops->requires_authored_delta_impl(hgraph::TSRoleTypeRef{}, hgraph::PyRef{}),
                      Catch::Matchers::ContainsSubstring("requires authored delta"));
    CHECK_THROWS_WITH(ops.to_python_impl(ops.context, nullptr), Catch::Matchers::ContainsSubstring("to Python"));
}
