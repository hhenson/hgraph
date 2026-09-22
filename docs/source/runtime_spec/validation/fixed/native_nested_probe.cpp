// Public C++ wiring for the eight nested whole-invalidation recipes.
#include "native_build_identity.h"
#include "native_loaded_libraries.h"
#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/json_codec.h>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
using namespace hgraph;
using namespace hgraph::testing;

template<bool Bundle, typename Child>
using Pair = std::conditional_t<Bundle,
    UnNamedTSB<Field<"left", Child>, Field<"right", Child>>, TSL<Child, 2>>;

template<bool Bundle, typename Child>
Port<Pair<Bundle, Child>> assemble(Wiring &w, Port<Child> left, Port<Child> right) {
    if constexpr (Bundle) return stdlib::to_tsb<Pair<Bundle, Child>>(w, left, right);
    else return stdlib::to_tsl<Pair<Bundle, Child>>(w, left, right).template as<Pair<Bundle, Child>>();
}

TSInputView child(const TSInputView &view, bool bundle, std::size_t index) {
    if (bundle) { auto children = view.as_bundle(); return children.at(index); }
    auto children = view.as_list(); return children.at(index);
}
TSOutputView child(const TSOutputView &view, bool bundle, std::size_t index) {
    if (bundle) { auto children = view.as_bundle(); return children.at(index); }
    auto children = view.as_list(); return children.at(index);
}

// Serialize the aggregate ValueView itself, preserving invalid slots as null.
// This is observation formatting, not a reconstruction from time-series children.
std::string json_value(const ValueView &value) {
    if (!value.valid()) return "null";
    if (value.is_bundle() || value.is_list() || value.is_tuple()) {
        const auto items = value.as_indexed_view();
        const bool bundle = value.is_bundle();
        std::string result = bundle ? "{" : "[";
        for (std::size_t i = 0; i < items.size(); ++i) {
            if (i) result += ',';
            if (bundle) result += '"' + std::string(value.schema()->fields[i].name) + "\":";
            result += json_value(items.at(i));
        }
        return result + (bundle ? "}" : "]");
    }
    if (value.is_map()) {
        std::string result = "{";
        const auto items = value.as_map();
        bool first = true;
        for (const auto &[key, item] : items.items()) {
            if (!first) result += ',';
            first = false;
            auto label = to_json_string(key);
            if (label.front() != '"') label = '"' + label + '"';
            result += label + ':' + json_value(item);
        }
        return result + '}';
    }
    return to_json_string(value);
}

std::string snapshot(const TSInputView &view, int depth, bool outer, bool inner) {
    const auto time = view.last_modified_time();
    std::string result = "{\"valid\":" + std::string(view.valid() ? "true" : "false") +
        ",\"modified\":" + (view.modified() ? "true" : "false") + ",\"last\":" +
        (time == MIN_DT ? "\"never\"" : std::to_string((time - MIN_ST) / MIN_TD)) +
        ",\"value\":" + (view.valid() ? json_value(view.value()) : "null") +
        ",\"delta\":" + (view.modified() ? json_value(view.delta_value()) : "null");
    if (depth) {
        bool bundle = depth == 2 ? outer : inner;
        result += ",\"all_valid\":" + std::string(view.all_valid() ? "true" : "false") +
            ",\"peer\":" + (view.is_bindable() && view.bound() ? "true" : "false") + ",\"children\":{";
        for (std::size_t i = 0; i < 2; ++i) {
            if (i) result += ',';
            result += '"' + std::string(bundle ? (i ? "right" : "left") : (i ? "1" : "0")) + "\":";
            result += snapshot(child(view, bundle, i), depth - 1, outer, inner);
        }
        result += '}';
    }
    return result + '}';
}

std::optional<Int> action(Int tick, Int index) {
    if (tick == 1 && index == 0) return 7;
    if (tick == 2 && index == 2) return 9;
    if (tick == 3 && index == 1) return 8;
    if (tick == 5 && index == 3) return 10;
    if (tick == 6 && index == 0) return 11;
    return {};
}

struct LeafSource {
    static constexpr auto name = "fixed_native_leaf";
    static void eval(In<"step", TS<Int>> step, Scalar<"index", Int> index, Out<TS<Int>> out) {
        if (step.value() == 7) {
            static_cast<void>(out.begin_mutation(out.evaluation_time()).invalidate());
        } else if (auto value = action(step.value(), index.value())) out.set(*value);
    }
};

template<bool Outer, bool Inner>
struct OwnedSource {
    using Shape = Pair<Outer, Pair<Inner, TS<Int>>>;
    static constexpr auto name = "fixed_native_owned";
    static void eval(In<"step", TS<Int>> step, Out<Shape> out) {
        if (step.value() == 7) {
            static_cast<void>(out.begin_mutation(out.evaluation_time()).invalidate());
        } else {
            for (Int i = 0; i < 4; ++i) {
                if (auto value = action(step.value(), i)) {
                    auto parent = child(out.base(), Outer, i / 2);
                    auto leaf = child(parent, Inner, i % 2);
                    Out<TS<Int>>{std::move(leaf), out.evaluation_time()}.set(*value);
                }
            }
        }
    }
};

template<bool Outer, bool Inner>
struct Observe {
    using Shape = Pair<Outer, Pair<Inner, TS<Int>>>;
    static constexpr auto name = "fixed_native_observe";
    static void eval(In<"step", TS<Int>>, In<"ts", Shape, InputValidity::Unchecked, InputActivity::Passive> ts,
                     Out<TS<Str>> out) {
        const auto first = snapshot(ts.base(), 2, Outer, Inner);
        if (first != snapshot(ts.base(), 2, Outer, Inner)) throw std::runtime_error("Reading changed collection state");
        out.set(first);
    }
};

template<bool Outer, bool Inner, bool Assembled>
struct Graph {
    static constexpr auto name = "fixed_native_graph";
    static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> step) {
        if constexpr (Assembled) {
            auto a = wire<LeafSource>(w, step, Int{0});
            auto b = wire<LeafSource>(w, step, Int{1});
            auto c = wire<LeafSource>(w, step, Int{2});
            auto d = wire<LeafSource>(w, step, Int{3});
            auto left = assemble<Inner>(w, a, b);
            auto right = assemble<Inner>(w, c, d);
            return wire<Observe<Outer, Inner>>(w, step, assemble<Outer>(w, left, right));
        } else return wire<Observe<Outer, Inner>>(w, step, wire<OwnedSource<Outer, Inner>>(w, step));
    }
};

template<bool Outer, bool Inner, bool Assembled>
void run() {
    const std::vector<std::optional<Int>> steps{0, 1, 2, 3, 4, 5, 6, 7, 8};
    const auto result = eval_node<Graph<Outer, Inner, Assembled>>(steps);
    if (result.size() != 9) throw std::runtime_error("Missing native observation cycles");
    std::cout << '"' << (Outer ? "tsb" : "tsl") << '_' << (Inner ? "tsb" : "tsl")
              << (Assembled ? "_assembled_invalidate" : "_owned_invalidate") << "\":{\"ticks\":[";
    for (std::size_t i = 0; i < result.size(); ++i) {
        if (!result[i]) throw std::runtime_error("Missing native observation");
        if (i) std::cout << ',';
        std::cout << *result[i];
    }
    std::cout << "]}";
}

int main() {
    try {
        std::cout << "{\"cases\":{";
        run<false, false, false>(); std::cout << ',';
        run<false, false, true>(); std::cout << ',';
        run<false, true, false>(); std::cout << ',';
        run<false, true, true>(); std::cout << ',';
        run<true, false, false>(); std::cout << ',';
        run<true, false, true>(); std::cout << ',';
        run<true, true, false>(); std::cout << ',';
        run<true, true, true>();
        std::cout << "},\"build\":{\"source_sha256\":\"" << probe_source_sha256
                  << "\",\"loader_sha256\":\"" << probe_loader_sha256
                  << "\",\"headers_sha256\":\"" << probe_headers_sha256 << "\"},\"libraries\":[";
        bool first = true;
        for (const auto &path : loaded_libraries()) {
            if (!first) std::cout << ',';
            first = false;
            Value name{Str{path}};
            std::cout << to_json_string(name.view());
        }
        std::cout << "]}\n";
    } catch (const std::exception &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
