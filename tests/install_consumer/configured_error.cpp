#include <hgraph/types/generated.h>
#include <hgraph/types/graph_wiring.h>

#include <algorithm>
#include <iostream>
#include <vector>

namespace {
using namespace hgraph;

std::vector<CanonicalError> observed;
std::size_t evaluations{};

struct Configured {
    static constexpr bool schedule_on_start = true;
    static void eval(Scalar<"error", CanonicalError> error, Out<TS<CanonicalError>> out) {
        ++evaluations;
        out.set(error.value());
    }
};
struct Nested {
    static constexpr bool schedule_on_start = true;
    static void eval(Scalar<"option", ScalarVar<"E">> option, Out<TS<CanonicalError>> out) {
        ++evaluations;
        out.set(option.value().as_optional().value().checked_as<CanonicalError>());
    }
};
struct Observe {
    static void eval(In<"error", TS<CanonicalError>> error) { observed.push_back(error.value()); }
};

Value configured_optional(const CanonicalError &error) {
    const auto *schema = scalar_descriptor<Optional<CanonicalError>>::value_meta();
    Value held{ValuePlanFactory::instance().type_for(schema)};
    held.view().as_optional().begin_mutation().emplace(Value{error}.view());
    return held;
}

bool verify(std::vector<CanonicalError> errors, bool nested) {
    Wiring wiring;
    observed.clear();
    evaluations = 0;
    for (const auto &error : errors) {
        auto port = nested ? wire<Nested>(wiring, configured_optional(error)).as<TS<CanonicalError>>()
                           : wire<Configured>(wiring, error);
        wire<Observe>(wiring, port);
    }
    GraphExecutorBuilder builder;
    builder.graph_builder(std::move(wiring).finish()).start_time(MIN_ST).end_time(MIN_ST + MIN_TD);
    auto executor = builder.make_executor();
    executor.view().run();
    const bool exact = std::ranges::equal(errors, observed, [](const auto &a, const auto &b) {
        return a == b && a.context == b.context;
    });
    return exact && evaluations == errors.size() - 1;
}
}

int main() {
    using namespace hgraph;
    static_cast<void>(canonical_error_value_meta());
    const CanonicalError first{.code = "failure", .message = "bad", .context = ErrorContext{"left", "eval", MIN_ST}};
    auto changed_node = first, changed_phase = first, changed_time = first;
    changed_node.context->node = "right";
    changed_phase.context->phase = "start";
    changed_time.context->evaluation_time += MIN_TD;
    auto changed_message = first;
    changed_message.message = "other";
    if (!(first == changed_node) || Value{first}.hash() != Value{changed_node}.hash()) { return 1; }

    std::vector<CanonicalError> errors{first, first, changed_node, changed_phase, changed_time, changed_message,
                                       CanonicalError{first.code, first.message, {}},
                                       CanonicalError{{}, first.message, {}}, CanonicalError{"", first.message, {}}};
    for (bool nested : {false, true}) {
        if (!verify(errors, nested)) { return 2; }
        std::ranges::reverse(errors);
        if (!verify(errors, nested)) { return 3; }
        std::ranges::reverse(errors);
    }
    std::cout << "Configured diagnostic provenance and sharing verified\n";
}
