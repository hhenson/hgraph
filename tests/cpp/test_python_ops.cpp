// RFC 0035: the type layer's Python slots resolve through the registered
// PythonOps table via Python-free forwarders. These tests run without any
// interpreter: the "objects" are sentinel pointers, which is all the type
// layer ever sees of them.
#include <hgraph/types/python_ops.h>
#include <hgraph/types/time_series/endpoint_schema.h>
#include <hgraph/types/time_series/ts_data/ops.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/value/any_ops.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value.h>

#include "../../src/hgraph/types/metadata/detail/realized_value_seams.h"
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
    hgraph::PyNewRef fake_set_to_python(const void *, const void *) { return hgraph::PyNewRef{sentinel(0x3000)}; }
    hgraph::PyNewRef fake_input_bundle_to_python(const void *, const void *)
    {
        return hgraph::PyNewRef{sentinel(0x4000)};
    }
    hgraph::PyNewRef fake_input_bundle_delta_to_python(const void *, const void *, hgraph::DateTime)
    {
        return hgraph::PyNewRef{sentinel(0x4001)};
    }
    hgraph::PyNewRef fake_target_link_to_python(const void *, const void *) { return hgraph::PyNewRef{sentinel(0x5000)}; }

    const hgraph::PythonOps &fake_provider()
    {
        static const hgraph::PythonOps ops = [] {
            hgraph::PythonOps table;
            table.scalars.conversion_for = &conversion_for;
            table.any.to_python          = &fake_any_to_python;
            table.compact.set_to_python  = &fake_set_to_python;
            table.ts_data.input_bundle_to_python       = &fake_input_bundle_to_python;
            table.ts_data.input_bundle_delta_to_python = &fake_input_bundle_delta_to_python;
            table.ts_data.target_link_to_python        = &fake_target_link_to_python;
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

TEST_CASE("a TSData table records no Python-authoring family by default and its slots throw the missing-op error",
          "[python_ops][rfc0035]")
{
    const hgraph::TSDataOps ops{};
    CHECK(ops.python_family == hgraph::PythonTSDataFamily::none);
    // The canonical throwing table the bridge answers for ``none``.
    const auto &missing = hgraph::ts_data_detail::missing_python_ts_data_ops();
    CHECK_THROWS_WITH(missing.requires_authored_delta_impl(hgraph::TSRoleTypeRef{}, hgraph::PyRef{}),
                      Catch::Matchers::ContainsSubstring("requires authored delta"));
    CHECK_THROWS_WITH(ops.to_python_impl(ops.context, nullptr), Catch::Matchers::ContainsSubstring("to Python"));
}

TEST_CASE("compact container slots are provider forwarders selected by family", "[python_ops][rfc0035]")
{
    ProviderReset reset;
    const hgraph::ValueOps &set_ops = hgraph::compact_set_ops();
    hgraph::set_python_ops(nullptr);
    CHECK_THROWS_WITH(set_ops.to_python_impl(set_ops.context, nullptr),
                      Catch::Matchers::ContainsSubstring("no Python conversion is registered for compact container"));
    hgraph::set_python_ops(&fake_provider());
    CHECK(set_ops.to_python_impl(set_ops.context, nullptr).ptr == sentinel(0x3000));
    // The read-only key-set adapter never gains a from_python slot (null is the one idiom for that).
    CHECK(hgraph::compact_map_key_set_ops().from_python_impl == nullptr);
}

TEST_CASE("a realized composite's slots are provider forwarders over its private context",
          "[python_ops][rfc0035]")
{
    ProviderReset reset;
    auto       &registry   = hgraph::TypeRegistry::instance();
    const auto *int_meta   = registry.register_scalar<hgraph::Int>("int");
    const auto *str_meta   = registry.register_scalar<std::string>("str");
    const auto *tuple_meta = registry.tuple({int_meta, str_meta});
    const auto  binding    = hgraph::ValuePlanFactory::instance().type_for(tuple_meta);
    hgraph::Value value{binding};
    const auto &ops = binding.ops_ref();

    // The slot is the Realized forwarder: named error without a provider.
    hgraph::set_python_ops(nullptr);
    CHECK_THROWS_WITH(ops.to_python_impl(ops.context, value.view().data()),
                      Catch::Matchers::ContainsSubstring("no Python conversion is registered for realized value"));

    // The context the bridge reads through the seams is the composite's own
    // (plain data: no exported symbol is needed to look at it, which keeps
    // this test linkable against a shared runtime whose seams are private).
    const auto *state = static_cast<const hgraph::realized_detail::CompositeIndexedContext *>(ops.context);
    REQUIRE(state != nullptr);
    CHECK(state->schema == tuple_meta);
    CHECK(state->child_bindings.size() == 2);
    CHECK(state->offsets.size() == 2);
}

TEST_CASE("TS input facades dispatch their Python slots through the endpoint table to provider forwarders",
          "[python_ops][rfc0035]")
{
    using namespace hgraph;
    ProviderReset reset;
    auto       &registry   = TypeRegistry::instance();
    const auto *integer    = registry.register_scalar<std::int32_t>("int32");
    const auto *scalar     = registry.ts(integer);
    const auto *fixed_list = registry.tsl(scalar, 2);
    const auto *bundle     = registry.tsb("PythonOpsInputBundle", {{"value", scalar}, {"items", fixed_list}});

    // A non-peered bundle's own slots are Python-free dispatch to the TSB
    // endpoint table, whose slots are the provider's input_bundle entries.
    const auto list_endpoint   = TSEndpointSchema::non_peered_list(fixed_list, TSEndpointSchema::peered(scalar));
    const auto bundle_endpoint = TSEndpointSchema::non_peered(bundle, {TSEndpointSchema::peered(scalar), list_endpoint});
    TSInput    root{TSInputBuilderFactory::checked_builder_for(*bundle, bundle_endpoint)};
    const auto  root_view = root.view();
    const auto &root_data = root_view.data_view();
    const auto &root_ops  = root_data.ops();
    REQUIRE(root_ops.is_input_binding);
    CHECK(root_ops.python_family == PythonTSDataFamily::none);
    set_python_ops(nullptr);
    CHECK_THROWS_WITH(root_ops.to_python_impl(root_ops.context, root_data.data()),
                      Catch::Matchers::ContainsSubstring("no Python conversion is registered for TSData"));
    set_python_ops(&fake_provider());
    CHECK(root_ops.to_python_impl(root_ops.context, root_data.data()).ptr == sentinel(0x4000));
    CHECK(root_ops.delta_to_python_impl(root_ops.context, root_data.data(), MIN_ST).ptr == sentinel(0x4001));

    // Its peered child is a target link: the slots convert the bound target
    // on the bridge, and authoring goes through the recorded family.
    const auto  leaf     = detail::input_child_projection(root_data, 0).target_link;
    REQUIRE(leaf.valid());
    const auto &leaf_ops = leaf.ops();
    REQUIRE(leaf_ops.is_target_link);
    CHECK(leaf_ops.python_family == PythonTSDataFamily::target_link);
    CHECK(leaf_ops.to_python_impl(leaf_ops.context, leaf.data()).ptr == sentinel(0x5000));
    set_python_ops(nullptr);
    CHECK_THROWS_WITH(leaf_ops.to_python_impl(leaf_ops.context, leaf.data()),
                      Catch::Matchers::ContainsSubstring("no Python conversion is registered for TSData"));
}
