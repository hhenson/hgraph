#include <hgraph/types/generated.h>

namespace {
using namespace hgraph;
using Number = TS<Int>;
using Canonical = UnNamedTSB<Field<"value", Number>, Field<"error", TS<CanonicalError>>>;
using Diagnostic = UnNamedTSB<Field<"exception", TS<NodeError>>, Field<"out", Number>>;

struct Source {
    static constexpr bool schedule_on_start = true;
    static void eval(NodeScheduler scheduler, State<Int> cursor, Out<Number> out) {
        constexpr std::array<Int, 3> values{5, -3, 7};
        const auto index = static_cast<std::size_t>(cursor.get());
        out.set(values.at(index));
        cursor.set(static_cast<Int>(index + 1));
        if (index + 1 < values.size()) { scheduler.schedule(MIN_TD); }
    }
};

struct Checked {
    static constexpr auto name = "checked";
    static void eval(In<"x", Number> x, Out<Number> out) {
        if (x.value() < 0) {
            throw ExecutionFailure{CanonicalError{.code = "numeric.out_of_range", .message = "negative", .context = {}}};
        }
        out.set(x.value());
    }
};

struct Child {
    static Port<Number> compose(Wiring &w, Port<Number> x) { return wire<Checked>(w, x); }
};

struct ObserveCanonical {
    static inline std::vector<Str> events;
    static void eval(In<"capture", Canonical, InputValidity::Unchecked> capture) {
        auto value = capture.field<"value">();
        auto error = capture.field<"error">();
        if (value.modified()) { events.push_back(std::to_string(value.value())); }
        if (error.modified()) {
            const auto &e = error.value();
            if (e.context && e.context->phase == "evaluate" && e.message == "negative") {
                events.push_back(e.code.value_or("missing code"));
            }
        }
    }
};

struct ObserveDiagnostic {
    static inline std::vector<Str> events;
    static void eval(In<"capture", Diagnostic, InputValidity::Unchecked> capture) {
        auto value = capture.field<"out">();
        auto error = capture.field<"exception">();
        if (value.modified()) { events.push_back(std::to_string(value.value())); }
        if (error.modified()) {
            const auto trace = error.base().value().as_bundle().at("activation_back_trace");
            events.push_back(trace.checked_as<Str>().contains("*x*: value=-3, delta=-3") ? "detailed" : "no values");
        }
    }
};
}

int main()
{
    using namespace hgraph;
    Wiring wiring;
    const std::array inputs{wire<Source>(wiring).erased()};
    auto canonical = compiled::try_except_(wiring, inputs, compile_subgraph<Child>(wiring), {.canonical_errors = true});
    wire<ObserveCanonical>(wiring, Port<void>{wiring, std::move(canonical)}.as<Canonical>());
    auto diagnostic = compiled::try_except_(wiring, inputs, compile_subgraph<Child>(wiring),
                                             {.trace_back_depth = 2, .capture_values = true});
    wire<ObserveDiagnostic>(wiring, Port<void>{wiring, std::move(diagnostic)}.as<Diagnostic>());

    GraphExecutorBuilder builder;
    builder.graph_builder(std::move(wiring).finish()).start_time(MIN_ST).end_time(MIN_ST + 3 * MIN_TD);
    auto executor = builder.make_executor();
    executor.view().run();
    return ObserveCanonical::events == std::vector<Str>{"5", "numeric.out_of_range", "7"} &&
           ObserveDiagnostic::events == std::vector<Str>{"5", "detailed", "7"} ? 0 : 1;
}
