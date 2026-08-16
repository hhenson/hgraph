// The no-value sentinel contract (access-path audit, 2026-08-16): an
// UNBOUND typed TSData view dispatches into its family's sentinel table and
// every read answers the empty/never-ticked value — no caller-side validity
// branch required. Mutations through a sentinel still throw, and a BOUND
// view of the wrong kind still fails kind validation.

#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/time_series/ts_data.h>

namespace
{
    using namespace hgraph;

    [[nodiscard]] TSDataView unbound() { return TSDataView{}; }
}  // namespace

TEST_CASE("sentinel: an unbound base view answers the never-ticked value")
{
    auto view = unbound();
    CHECK_FALSE(view.valid());
    CHECK_FALSE(view.has_current_value());
    CHECK_FALSE(view.all_valid());
    CHECK(view.last_modified_time() == MIN_DT);
    CHECK_FALSE(view.modified(MIN_ST));
    CHECK_FALSE(view.value().has_value());
    CHECK_FALSE(view.delta_value(MIN_ST).has_value());
}

TEST_CASE("sentinel: an unbound set view answers empty")
{
    auto view = unbound();
    auto set  = view.as_set();
    CHECK(set.size() == 0);
    CHECK(set.slot_capacity() == 0);
    CHECK_FALSE(set.slot_occupied(0));
    CHECK_FALSE(set.slot_live(0));
    CHECK_FALSE(set.slot_added(0));
    CHECK_FALSE(set.slot_removed(0));
    std::size_t seen = 0;
    for (const auto value : set.values())
    {
        static_cast<void>(value);
        ++seen;
    }
    CHECK(seen == 0);
}

TEST_CASE("sentinel: an unbound dict view answers empty")
{
    auto view = unbound();
    auto dict = view.as_dict();
    CHECK(dict.size() == 0);
    CHECK(dict.slot_capacity() == 0);
    CHECK_FALSE(dict.slot_occupied(0));
    CHECK_FALSE(dict.slot_modified(0));
    CHECK_FALSE(dict.structural_delta_current(MIN_ST));
    std::size_t seen = 0;
    for (const auto &value : dict.values())
    {
        static_cast<void>(value);
        ++seen;
    }
    CHECK(seen == 0);
}

TEST_CASE("sentinel: an unbound window view answers empty")
{
    auto view   = unbound();
    auto window = view.as_window();
    CHECK(window.size() == 0);
    CHECK(window.capacity() == 0);
    CHECK_FALSE(window.full());
}

TEST_CASE("sentinel: mutation through an unbound view still throws")
{
    auto view = unbound();
    CHECK_THROWS_AS(view.begin_mutation(MIN_ST), std::logic_error);
}
