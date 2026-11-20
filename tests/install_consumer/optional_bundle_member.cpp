#include <hgraph/types/generated.h>
#include <hgraph/types/native_bundle.h>

#ifndef HGRAPH_NO_ARROW
#include <hgraph/lib/testing/eval_node.h>
#endif

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
    static void eval(In<"command", TS<Int>> command, Out<NativeTSB<Quote>> out)
    {
        const Int seq = command.value();
        if (seq == 3) {
            auto note = out.field<"note">();
            static_cast<void>(note.begin_mutation(note.evaluation_time()).invalidate());
            out.field<"seq">().set(seq);
        } else {
            out.set(quote(seq));
        }
    }
};

struct Read {
    static void eval(In<"quote", NativeTSB<Quote>> input, Out<TS<Int>> out)
    {
        const Int seq = input.field<"seq">().value();
        const auto note = input.field<"note">();
        if (seq == 3) {
            if (!note.valid() && !note.modified() && !input.all_valid()) { out.set(seq); }
            return;
        }
        if (input.all_valid() && note.valid() && note.modified() &&
            note.value().has_value() == (seq != 1) && input.value() == quote(seq)) {
            out.set(seq);
        }
    }
};

struct OptionalFields {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> command)
    {
        return wire<Read>(w, wire<Write>(w, command));
    }
};

#ifdef HGRAPH_NO_ARROW
struct Input {
    static constexpr bool schedule_on_start = true;
    static void eval(Out<TS<Int>> out) { out.set(Int{0}); }
};
struct Sink {
    static void eval(In<"value", TS<Int>>) {}
};
#endif
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

#ifdef HGRAPH_NO_ARROW
    // The lean SDK ships runtime and wiring. Build the generated graph here
    // without the standard library's replay and record nodes used by eval_node.
    Wiring wiring;
    wire<Sink>(wiring, OptionalFields::compose(wiring, wire<Input>(wiring)));
    return std::move(wiring).finish().node_count() > 0 ? 0 : 4;
#else
    const auto observed = testing::eval_node<OptionalFields>({0, 1, 2, 3, 4});
    return observed == std::vector<std::optional<Int>>{0, 1, 2, 3, 4} ? 0 : 4;
#endif
}
