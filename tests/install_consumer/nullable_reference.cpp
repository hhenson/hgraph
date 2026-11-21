// A generated application uses only the light SDK. Keep one designation
// sampled while another reference switches between two plain scalar outputs.
#include <hgraph/types/generated.h>

#include <array>
#include <optional>
#include <vector>

namespace {
using namespace hgraph;
using Plain = TS<Int>;
using Nullable = TS<Optional<Int>>;
using Reference = REF<Nullable>;

struct Clock {
    static constexpr bool schedule_on_start = true;
    static void eval(NodeScheduler scheduler, State<Int> position, Out<Plain> out) {
        const auto tick = position.get();
        out.set(tick);
        position.set(tick + 1);
        if (tick < 7) { scheduler.schedule(MIN_TD); }
    }
};

struct Prices {
    static void eval(In<"clock", Plain> clock, Out<Plain> out) {
        switch (clock.value()) {
            case 0: out.set(Int{5}); break;
            case 1: out.set(Int{6}); break;
            case 2: static_cast<void>(out.begin_mutation(out.evaluation_time()).invalidate()); break;
            case 3: out.set(Int{7}); break;
            case 5: out.set(Int{8}); break;
        }
    }
};

struct Other {
    static void eval(In<"clock", Plain> clock, Out<Plain> out) {
        if (clock.value() == 0) { out.set(Int{70}); }
        if (clock.value() == 5) { out.set(Int{80}); }
    }
};

using PassiveRef = In<"source", Reference, InputValidity::Unchecked, InputActivity::Passive>;
struct Select {
    static void eval(In<"clock", Plain> clock, PassiveRef first,
                     In<"second", Reference, InputValidity::Unchecked, InputActivity::Passive> second,
                     Out<Reference> out) {
        switch (clock.value()) {
            case 0: case 7: out.set(first.value()); break;
            case 4: out.set(second.value()); break;
            case 6: static_cast<void>(out.begin_mutation(out.evaluation_time()).invalidate()); break;
        }
    }
};

struct Save {
    static void eval(In<"clock", Plain> clock, PassiveRef selected, Out<Reference> out) {
        if (clock.value() == 0) { out.set(selected.value()); }
    }
};

struct Observe {
    using Sample = std::array<std::optional<Int>, 2>;
    static inline std::vector<Sample> rows;
    static void eval(In<"clock", Plain>,
                     In<"selected", Nullable, InputValidity::Unchecked, InputActivity::Passive> selected,
                     In<"saved", Nullable, InputValidity::Unchecked, InputActivity::Passive> saved) {
        const auto read = [](const auto& input) -> std::optional<Int> {
            if (!input.valid()) { return std::nullopt; }
            // Every valid value from a plain source must be present.
            return input.value().value().template checked_as<Int>();
        };
        rows.push_back({read(selected), read(saved)});
    }
};
}  // namespace

int main() {
    Wiring wiring;
    const auto clock = wire<Clock>(wiring);
    const auto first = wire<Prices>(wiring, clock);
    const auto second = wire<Other>(wiring, clock);
    const auto selected = wire<Select>(wiring, clock, first.as<Nullable>(), second);
    const auto saved = wire<Save>(wiring, clock, selected);
    wire<Observe>(wiring, clock, selected, saved);

    GraphExecutorBuilder builder;
    builder.graph_builder(std::move(wiring).finish()).start_time(MIN_ST).end_time(MIN_ST + 8 * MIN_TD);
    auto executor = builder.make_executor();
    executor.view().run();

    const std::vector<Observe::Sample> expected{
        {5, 5}, {6, 6}, {std::nullopt, std::nullopt}, {7, 7}, {70, 7}, {80, 8}, {std::nullopt, 8}, {8, 8}};
    return Observe::rows == expected ? 0 : 1;
}
