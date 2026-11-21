#include <hgraph/types/generated.h>

#include <algorithm>
#include <array>
#include <optional>
#include <vector>

using namespace hgraph;

namespace {
using Number = TS<Int>;
using Book = TSD<Int, Number>;

struct Clock {
    static constexpr bool schedule_on_start = true;
    static void eval(DateTime now, NodeScheduler scheduler, Out<Number> out) {
        auto step = (now - MIN_ST).count();
        out.set(static_cast<Int>(step));
        if (step < 5) { scheduler.schedule(now + MIN_TD); }
    }
};

struct Numbers {
    static void eval(In<"tick", Number> tick, Out<Book> out) {
        switch (tick.value()) {
        case 0: out[Int{0}].set(Int{4}); break;
        case 1: out[Int{1}].set(Int{7}); break;
        case 2: out[Int{0}].set(Int{6}); break;
        case 3: static_cast<void>(out.erase(Int{1})); break;
        case 4: static_cast<void>(out.erase(Int{0})); break;
        case 5: out[Int{0}].set(Int{8}); out[Int{1}].set(Int{3}); break;
        }
    }
};

struct Right {
    static void eval(In<"tick", Number> tick, Out<Book> out) {
        switch (tick.value()) {
        case 0: out[Int{0}].set(Int{100}); break;
        case 1: out[Int{1}].set(Int{200}); break;
        case 4: static_cast<void>(out.erase(Int{0})); break;
        case 5: out[Int{0}].set(Int{300}); break;
        }
    }
};

struct Keys {
    static void eval(In<"tick", Number> tick, Out<TSS<Int>> out) {
        switch (tick.value()) {
        case 0: static_cast<void>(out.add(Int{0})); break;
        case 1: static_cast<void>(out.add(Int{1})); break;
        case 3: static_cast<void>(out.remove(Int{1})); break;
        case 4: static_cast<void>(out.remove(Int{0})); break;
        case 5: static_cast<void>(out.add(Int{0})); static_cast<void>(out.add(Int{1})); break;
        }
    }
};

struct Seed {
    static constexpr bool schedule_on_start = true;
    static void eval(Out<Number> out) { out.set(Int{10}); }
};

struct Add {
    static void eval(In<"left", Number> left, In<"right", Number> right, Out<Number> out) {
        out.set(left.value() + right.value());
    }
};

struct Subtract {
    static void eval(In<"left", Number> left, In<"right", Number> right, Out<Number> out) {
        out.set(left.value() - right.value());
    }
};

template <typename Operator> struct Combine {
    static Port<Number> compose(Wiring &w, Port<Number> left, Port<Number> right) {
        return wire<Operator>(w, left, right);
    }
};

struct Previous {
    static void eval(In<"key", Number> key, Out<Number> out) {
        if (key.value() > 0) { out.set(key.value() - 1); }
    }
};

struct Total {
    static void eval(In<"local", Number> local, In<"seed", Number> seed,
                     In<"peer", Number, InputValidity::Unchecked> peer, Out<Number> out) {
        out.set(local.value() + seed.value() + (peer.valid() ? peer.value() : 0));
    }
};

struct MeshChild {
    static Port<Number> compose(Wiring &w, Port<Number> key, Port<Number> value, Port<Number> seed) {
        auto previous = wire<Previous>(w, key);
        auto peer = compiled::mesh_peer(w, previous.erased(), schema_descriptor<Book>::ts_meta());
        return wire<Total>(w, value, seed, Port<Number>{w, std::move(peer)});
    }
};

struct Both {
    static Port<Number> compose(Wiring &w, Port<Number> left, Port<Number> offset, Port<Number> right) {
        return wire<Add>(w, wire<Add>(w, left, right), offset);
    }
};

struct Visit {
    static inline std::vector<std::pair<Int, Int>> items;
    static void eval(In<"key", Number> key, In<"value", Number> value) {
        items.emplace_back(key.value(), value.value());
    }
};
struct Sink {
    static void compose(Wiring &w, Port<Number> key, Port<Number> left, Port<Number> offset, Port<Number> right) {
        wire<Visit>(w, key, Both::compose(w, left, offset, right));
    }
};

using Maybe = std::optional<Int>;
using Row = std::array<Maybe, 4>;
struct Observe {
    static inline std::vector<Row> rows;
    static void eval(In<"tick", Number>, In<"sum", Number, InputActivity::Passive, InputValidity::Unchecked> sum,
                     In<"fold", Number, InputActivity::Passive, InputValidity::Unchecked> fold,
                     In<"mesh", Book, InputActivity::Passive, InputValidity::Unchecked> mesh) {
        auto read = [&](Int key) -> Maybe {
            if (mesh.contains(key) && mesh.at(key).valid()) { return mesh.at(key).value(); }
            return {};
        };
        rows.push_back({sum.valid() ? Maybe{sum.value()} : Maybe{}, fold.valid() ? Maybe{fold.value()} : Maybe{},
                        read(0), read(1)});
    }
};
struct ObservePair {
    static inline std::vector<Row> rows;
    static void eval(In<"tick", Number>, In<"map", Book, InputActivity::Passive, InputValidity::Unchecked> map,
                     In<"mesh", Book, InputActivity::Passive, InputValidity::Unchecked> mesh) {
        auto read = [](const auto &book, Int key) -> Maybe {
            if (book.contains(key) && book.at(key).valid()) { return book.at(key).value(); }
            return {};
        };
        rows.push_back({read(map, 0), read(map, 1), read(mesh, 0), read(mesh, 1)});
    }
};
}

int main() {
    Wiring w;
    auto tick = wire<Clock>(w);
    auto book = wire<Numbers>(w, tick);
    auto seed = wire<Seed>(w);
    auto sum = compiled::reduce_(w, book.erased(), compile_subgraph<Combine<Add>>(w));
    auto fold = compiled::reduce_(w, book.erased(), compile_subgraph<Combine<Subtract>>(w),
                                  seed.erased(), compiled::ReduceOrder::Ordered);
    auto mesh = compiled::mesh_(w, book.erased(), std::array{seed.erased()},
                                compile_subgraph<MeshChild>(w), MapKeyArg::First);
    wire<Observe>(w, tick, Port<Number>{w, std::move(sum)}, Port<Number>{w, std::move(fold)},
                  Port<Book>{w, std::move(mesh)});
    const auto right = wire<Right>(w, tick);
    const auto keys = wire<Keys>(w, tick).erased();
    const std::array inputs{book.erased(), seed.erased(), right.erased()};
    constexpr std::array<std::size_t, 2> per_key{0, 2};
    auto pairs = compiled::map_(w, inputs, per_key, compile_subgraph<Both>(w), MapKeyArg::None, keys);
    auto mesh_pairs = compiled::mesh_(w, inputs, per_key, compile_subgraph<Both>(w), MapKeyArg::None, keys);
    compiled::map_sink_(w, inputs, per_key, compile_subgraph<Sink>(w), MapKeyArg::First, keys);
    wire<ObservePair>(w, tick, Port<Book>{w, std::move(pairs)}, Port<Book>{w, std::move(mesh_pairs)});
    GraphExecutorBuilder graph;
    graph.graph_builder(std::move(w).finish()).start_time(MIN_ST).end_time(MIN_ST + 6 * MIN_TD);
    auto executor = graph.make_executor();
    executor.view().run();
    const std::vector<Row> expected{{4, 6, 14, {}}, {11, -1, 14, 31}, {13, -3, 16, 33},
                                    {6, 4, 16, {}}, {std::nullopt, 10, {}, {}}, {11, -1, 18, 31}};
    const std::vector<Row> pairs_expected{{114, {}, 114, {}}, {114, 217, 114, 217}, {116, 217, 116, 217},
                                          {116, {}, 116, {}}, {std::nullopt, {}, {}, {}}, {318, 213, 318, 213}};
    std::sort(Visit::items.begin(), Visit::items.end());
    const std::vector<std::pair<Int, Int>> visits{{0, 114}, {0, 116}, {0, 318}, {1, 213}, {1, 217}};
    return Observe::rows == expected && ObservePair::rows == pairs_expected && Visit::items == visits ? 0 : 1;
}
