#include <catch2/catch_test_macros.hpp>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/time_series/output_mutation.h>

namespace {
using namespace hgraph;
using namespace hgraph::testing;
using Book = TSD<Str, TS<Int>>;

// MAP-CONN-2/3: create a real but unreadable child, invalidate/revalidate it,
// restore it during removal, then remove it through an engine boundary.
struct BookLifecycle {
    static void eval(In<"step", TS<Int>> step, Out<Book> out) {
        const Str x{"X"};
        switch (step.value()) {
            case 1:
                static_cast<void>(out[x]);
                REQUIRE(out.contains(x));
                break;
            case 2: upsert(out, Str{"Y"}, Int{7}); break;
            case 3: update(out, x, Int{7}); break;
            case 4:
                invalidate(out, x);
                REQUIRE(out.contains(x));
                break;
            case 5: update(out, x, Int{7}); break;
            case 6:
                remove(out, x);
                upsert(out, x, Int{8});
                break;
            case 7: remove(out, x); break;
            case 9: upsert(out, x, Int{8}); break;
            default: break;
        }
    }
};

struct ObserveSelection {
    static void eval(In<"designation", REF<TS<Int>>, InputValidity::Unchecked> designation,
                     In<"value", TS<Int>, InputValidity::Unchecked> value,
                     In<"step", TS<Int>> step, Out<TS<Int>> out) {
        static_cast<void>(step);
        REQUIRE(designation.valid());
        if (!value.valid()) {
            CHECK(value.last_modified_time() == MIN_DT);
            CHECK_FALSE(value.delta_value().has_value());
        }
        out.set(16 + (designation.modified() ? 8 : 0) +
                (designation.value().is_empty() ? 4 : 0) +
                (value.valid() ? 2 : 0) + (value.modified() ? 1 : 0));
    }
};
struct LatchFirstReadable {
    static void eval(In<"designation", REF<TS<Int>>> designation, In<"readable", TS<Int>>,
                     State<Bool> captured, Out<REF<TS<Int>>> out) {
        if (!captured.get()) {
            captured.set(true);
            out.set(designation.value());
        }
    }
};
struct SampleHeld {
    static void eval(In<"value", TS<Int>, InputValidity::Unchecked> value,
                     In<"step", TS<Int>> step, Out<TS<Int>> out) {
        static_cast<void>(step);
        if (value.valid()) { out.set(value.value()); }
    }
};
struct SelectionMetadata {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> step) {
        auto book = wire<BookLifecycle>(w, step);
        auto selected = wire<stdlib::getitem_>(w, book, Str{"X"}).as<REF<TS<Int>>>();
        return wire<ObserveSelection>(w, selected, selected, step);
    }
};
struct HeldChild {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> step) {
        auto book = wire<BookLifecycle>(w, step);
        auto selected = wire<stdlib::getitem_>(w, book, Str{"X"}).as<REF<TS<Int>>>();
        auto held = wire<LatchFirstReadable>(w, selected, selected);
        return wire<SampleHeld>(w, held, step);
    }
};
struct ObserveSamplingTime {
    static void eval(In<"value", TS<Int>> value, In<"book", Book, InputActivity::Passive> book,
                     In<"key", TS<Str>> key, DateTime time, Out<TS<Bool>> out) {
        const auto child = book.at_slot(book.find_slot(key.value()));
        out.set(value.last_modified_time() == time && child.last_modified_time() == MIN_ST);
    }
};
struct SampleRetainedChild {
    static Port<TS<Bool>> compose(Wiring &w, Port<Book> book, Port<TS<Str>> key) {
        auto selected = wire<stdlib::getitem_>(w, book, key);
        return wire<ObserveSamplingTime>(w, selected, book, key);
    }
};
}

TEST_CASE("map selection distinguishes absent keys and invalid children without designation ticks",
          "[map][reference][metadata]") {
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<SelectionMetadata>(values<Int>(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)),
                 values<Int>(28, 24, 16, 19, 16, 19, 19, 28, 20, 27));
}

TEST_CASE("held map child survives same-cycle restoration and removal then expires permanently",
          "[map][reference][lifetime]") {
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<HeldChild>(values<Int>(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)),
                 values<Int>(none, none, none, 7, none, 7, 8, 8, none, none));
}

TEST_CASE("map key retarget samples a retained equal child without changing its producer timestamp",
          "[map][reference][sampling]") {
    stdlib::register_standard_operators();
    CHECK_OUTPUT(eval_node<SampleRetainedChild>(
        values<Value>(dict_delta<Str, TS<Int>>({{"X", 7}, {"Y", 7}})), values<Str>("X", "Y")),
        values<Bool>(true, true));
}
