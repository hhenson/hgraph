#include <hgraph/types/generated.h>
#include <hgraph/types/native_bundle.h>

#include <optional>

namespace {
using namespace hgraph;

struct Price {
    Int ask;
    Int bid;
    bool operator==(const Price &) const = default;
};

struct Quote {
    Int seq;
    std::optional<Str> note;
    std::optional<Price> price;
    bool operator==(const Quote &) const = default;
};
}

template <> struct hgraph::native_bundle<Price> {
    using schema = NominalBundle<"consumer", "Price", false, BundleParents<>, BundleArguments<>,
                                 Field<"bid", Int>, Field<"ask", Int>>;
    static constexpr auto members = std::tuple{&Price::bid, &Price::ask};
};

template <> struct hgraph::native_bundle<Quote> {
    using schema = NominalBundle<"consumer", "Quote", false, BundleParents<>, BundleArguments<>,
                                 Field<"seq", Int>, Field<"note", Optional<Str>>,
                                 Field<"price", Optional<native_bundle<Price>::schema>>>;
    static constexpr auto members = std::tuple{&Quote::seq, &Quote::note, &Quote::price};
};

namespace {
using namespace hgraph;

Quote quote(Int seq)
{
    if (seq == 1) { return {seq, {}, {}}; }
    return {seq, "note " + std::to_string(seq), Price{seq + 10, seq}};
}

struct Write {
    static constexpr bool schedule_on_start = true;
    static void eval(NodeScheduler scheduler, State<Int> cursor, Out<NativeTSB<Quote>> out)
    {
        const Int seq = cursor.get();
        if (seq == 3) {
            auto note = out.field<"note">();
            static_cast<void>(note.begin_mutation(note.evaluation_time()).invalidate());
            out.field<"seq">().set(seq);
        } else {
            out.set(quote(seq));
        }
        if (seq < 4) { cursor.set(seq + 1); scheduler.schedule(MIN_TD); }
    }
};

struct Read {
    static inline std::vector<Int> seen;
    static void eval(In<"quote", NativeTSB<Quote>> input)
    {
        const Int seq = input.field<"seq">().value();
        const auto note = input.field<"note">();
        if (seq == 3) {
            if (!note.valid() && !input.all_valid()) { seen.push_back(seq); }
            return;
        }
        if (input.all_valid() && note.valid() && note.modified() &&
            note.value().has_value() == (seq != 1) && input.value() == quote(seq)) {
            seen.push_back(seq);
        }
    }
};
}

int main()
{
    using namespace hgraph;
    for (Int seq = 0; seq != 3; ++seq) {
        const auto expected = quote(seq);
        Value boxed{expected};
        if (boxed.view().checked_as<Quote>() != expected) { return 1; }
        const auto price = boxed.view().as_bundle().at("price").as_optional();
        if (price.has_value() != expected.price.has_value()) { return 2; }
        if (price.has_value() && price.value().as_bundle().at("bid").checked_as<Int>() != seq) { return 3; }
    }

    Wiring wiring;
    wire<Read>(wiring, wire<Write>(wiring));
    GraphExecutorBuilder builder;
    builder.graph_builder(std::move(wiring).finish()).start_time(MIN_ST).end_time(MIN_ST + 5 * MIN_TD);
    auto executor = builder.make_executor();
    executor.view().run();
    return Read::seen == std::vector<Int>{0, 1, 2, 3, 4} ? 0 : 4;
}
