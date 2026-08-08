// Tests for the value-layer mutable (structurally-mutable) Set — a growable,
// slot-store-backed set distinct from the immutable compact set. Covers the
// Mutable schema axis, distinct interning, add/remove/contains/clear,
// order-independent equality, and copy independence.

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <stdexcept>
#include <string>

#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value.h>

namespace
{
    using namespace hgraph;

    Value make_mutable_set(const ValueTypeMetaData *element_meta)
    {
        const auto *schema  = TypeRegistry::instance().mutable_set(element_meta);
        const auto binding = ValuePlanFactory::instance().type_for(schema);
        REQUIRE(binding != nullptr);
        REQUIRE(binding.ops_ref().kind == ValueOpsKind::MutableSet);
        return Value{binding};
    }

    template <typename BreakHook, typename Invoke>
    void require_missing_set_hook(Value &set, BreakHook break_hook, Invoke invoke)
    {
        MutableSetValueOps ops = *checked_value_ops<MutableSetValueOps>(set.binding(), "mutable set hook test");
        break_hook(ops);
        const ValueTypeRef binding = intern_value_type(*set.schema(), *set.binding().plan(), ops);
        auto view = ValueView{binding, const_cast<void *>(set.view().data())}.as_set().begin_mutation();
        REQUIRE_THROWS_AS(invoke(view), std::logic_error);
    }
}  // namespace

TEST_CASE("mutable set: the Mutable schema axis interns distinctly from the immutable set")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    const ValueTypeMetaData *immutable = registry.set(int_meta);
    const ValueTypeMetaData *mutable_  = registry.mutable_set(int_meta);

    REQUIRE(mutable_ != nullptr);
    CHECK(mutable_->value_kind() == ValueTypeKind::Set);
    CHECK(mutable_->is_mutable());
    CHECK_FALSE(immutable->is_mutable());
    CHECK(mutable_ != immutable);
    CHECK(registry.mutable_set(int_meta) == mutable_);  // interned singleton per element
}

TEST_CASE("mutable set: add, contains, remove, clear")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    Value set = make_mutable_set(int_meta);
    CHECK(set.view().is_set());
    CHECK(set.as_set().size() == 0);

    {
        auto m = set.as_set().begin_mutation();
        CHECK(m.add(Value{std::int32_t{1}}.view()));
        CHECK(m.add(Value{std::int32_t{2}}.view()));
        CHECK_FALSE(m.add(Value{std::int32_t{1}}.view()));  // duplicate: no change
    }
    {
        auto view = set.as_set();
        CHECK(view.size() == 2);
        CHECK(view.contains(Value{std::int32_t{1}}.view()));
        CHECK_FALSE(view.contains(Value{std::int32_t{3}}.view()));
    }
    {
        auto m = set.as_set().begin_mutation();
        CHECK(m.remove(Value{std::int32_t{1}}.view()));
        CHECK_FALSE(m.remove(Value{std::int32_t{9}}.view()));  // absent: no change
    }
    CHECK(set.as_set().size() == 1);
    CHECK(set.as_set().contains(Value{std::int32_t{2}}.view()));

    set.as_set().begin_mutation().clear();
    CHECK(set.as_set().size() == 0);
}

TEST_CASE("mutable set: storage metrics include key payloads and retained capacity", "[memory]")
{
    using namespace hgraph;
    auto &registry = TypeRegistry::instance();
    const auto *str_meta = registry.register_scalar<std::string>("str");
    Value set = make_mutable_set(str_meta);
    const std::string key(256, 'k');

    REQUIRE(set.as_set().begin_mutation().add(Value{key}.view()));
    const auto populated = set.view().dynamic_storage_metrics();
    CHECK(populated.live_bytes >= key.size() + 1);
    CHECK(populated.reserved_bytes >= populated.live_bytes);

    set.as_set().begin_mutation().clear();
    const auto cleared = set.view().dynamic_storage_metrics();
    CHECK(cleared.live_bytes < populated.live_bytes);
    CHECK(cleared.reserved_bytes >= cleared.live_bytes);
}

TEST_CASE("mutable set: equality is order-independent and a copy is independent")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");

    Value a = make_mutable_set(int_meta);
    {
        auto m = a.as_set().begin_mutation();
        (void)m.add(Value{std::int32_t{1}}.view());
        (void)m.add(Value{std::int32_t{2}}.view());
        (void)m.add(Value{std::int32_t{3}}.view());
    }
    Value b = make_mutable_set(int_meta);
    {
        auto m = b.as_set().begin_mutation();
        (void)m.add(Value{std::int32_t{3}}.view());  // different insertion order
        (void)m.add(Value{std::int32_t{1}}.view());
        (void)m.add(Value{std::int32_t{2}}.view());
    }
    CHECK(a.equals(b));  // order-independent

    Value c = a;  // deep copy
    c.as_set().begin_mutation().clear();
    CHECK(c.as_set().size() == 0);
    CHECK(a.as_set().size() == 3);  // original unaffected
}

TEST_CASE("mutable set: every missing mutation hook fails before invocation")
{
    using namespace hgraph;
    auto       &registry = TypeRegistry::instance();
    const auto *int_meta = registry.register_scalar<std::int32_t>("int32");
    Value       set      = make_mutable_set(int_meta);
    Value       key{std::int32_t{7}};

    SECTION("add")
    {
        require_missing_set_hook(set, [](auto &ops) { ops.add = nullptr; },
                                 [&](auto &view) { (void)view.add(key.view()); });
    }
    SECTION("remove")
    {
        require_missing_set_hook(set, [](auto &ops) { ops.remove = nullptr; },
                                 [&](auto &view) { (void)view.remove(key.view()); });
    }
    SECTION("clear")
    {
        require_missing_set_hook(set, [](auto &ops) { ops.clear = nullptr; },
                                 [](auto &view) { view.clear(); });
    }
}
