#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/native_bundle.h>

#include <catch2/catch_test_macros.hpp>

namespace
{
    using namespace hgraph;

    // Different native and schema order makes a storage reinterpretation observable.
    struct Quote {
        Int ask;
        Int bid;
        bool operator==(const Quote &) const = default;
    };
    struct Book {
        Quote top;
        Str symbol;
        bool operator==(const Book &) const = default;
    };
}

template <> struct hgraph::native_bundle<Quote> {
    using schema = NominalBundle<"test.native", "Quote", false, BundleParents<>, BundleArguments<>,
                                 Field<"bid", Int>, Field<"ask", Int>>;
    static constexpr auto members = std::tuple{&Quote::bid, &Quote::ask};
};

template <> struct hgraph::native_bundle<Book> {
    using schema = NominalBundle<"test.native", "Book", false, BundleParents<>, BundleArguments<>,
                                 Field<"top", native_bundle<Quote>::schema>, Field<"symbol", Str>>;
    static constexpr auto members = std::tuple{&Book::top, &Book::symbol};
};

namespace
{
    struct PublishBook {
        static void eval(In<"bid", TS<Int>> bid, In<"ask", TS<Int>> ask, Out<NativeTSB<Book>> out)
        {
            out.set(Book{Quote{ask.value(), bid.value()}, "ABC"});
        }
    };

    struct ReadBook {
        static void eval(In<"book", NativeTSB<Book>, InputValidity::AllValid> book, Out<TS<Int>> out)
        {
            const auto value = book.value();
            REQUIRE(book.field<"top">().field<"bid">().modified());
            REQUIRE(book.field<"top">().field<"ask">().modified());
            REQUIRE(book.field<"symbol">().modified());
            REQUIRE(value.symbol == "ABC");
            out.set(value.top.ask - value.top.bid);
        }
    };

    struct Width {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> bid, Port<TS<Int>> ask) {
            return wire<ReadBook>(w, wire<PublishBook>(w, bid, ask));
        }
    };

    struct StoreQuote {
        static void eval(In<"bid", TS<Int>> bid, Scalar<"spread", Quote> spread,
                         State<Quote> state, Out<TS<Int>> out) {
            state.set(Quote{bid.value() + spread.value().ask, bid.value()});
            const Quote read = state.get();
            out.set(read.ask - read.bid);
        }
    };

    struct PublishBid {
        static void eval(In<"bid", TS<Int>> bid, Out<NativeTSB<Quote>> out) {
            out.field<"bid">().set(bid.value());
        }
    };

    struct PartialReader {
        static void eval(In<"q", NativeTSB<Quote>> q, Out<TS<Int>> out) {
            REQUIRE_FALSE(q.all_valid());
            REQUIRE_THROWS_AS(q.value(), std::logic_error);
            out.set(q.field<"bid">().value());
        }
    };

    struct PartialGraph {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> bid) {
            return wire<PartialReader>(w, wire<PublishBid>(w, bid));
        }
    };
}

TEST_CASE("a mapped aggregate round trips through its existing nominal value schema", "[native-bundle]")
{
    using namespace hgraph;
    const Book book{Quote{42, 10}, "XYZ"};
    const Value stored{book};
    REQUIRE(stored.schema() == value_schema_descriptor<native_bundle<Book>::schema>::value_meta());
    using QuoteTS = NominalTSB<native_bundle<Quote>::schema, Field<"bid", TS<Int>>, Field<"ask", TS<Int>>>;
    REQUIRE(schema_descriptor<NativeTSB<Quote>>::ts_meta() == schema_descriptor<QuoteTS>::ts_meta());
    CHECK(stored.view().as_bundle().at("top").as_bundle().at(0).checked_as<Int>() == 10);
    CHECK(stored.view().checked_as<Book>() == book);
    GlobalState store;
    store.view().set("book", stored);
    CHECK(store.view().get_as<Book>("book") == book);
    CHECK_THROWS_AS(stored.view().checked_as<Quote>(), std::logic_error);

    BundleBuilder partial{ValuePlanFactory::instance().type_for(scalar_descriptor<Quote>::value_meta())};
    partial.set("bid", Value{Int{1}});
    CHECK_THROWS_AS(partial.build().view().checked_as<Quote>(), std::logic_error);
}

TEST_CASE("whole native bundle publication ticks each field and supports recursive reads", "[native-bundle]")
{
    using namespace hgraph;
    using namespace hgraph::testing;
    CHECK_OUTPUT(eval_node<Width>(values<Int>(1, none, 2), values<Int>(4, 6, none)), {3, 5, 4});
    CHECK_OUTPUT(eval_node<PartialGraph>({1, 2}), {1, 2});
    CHECK_OUTPUT(eval_node<StoreQuote>({1, 2}, Quote{3, 0}), {3, 3});
}
