#include <hgraph/types/generated.h>

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
    GraphExecutorBuilder graph;
    graph.graph_builder(std::move(w).finish()).start_time(MIN_ST).end_time(MIN_ST + 6 * MIN_TD);
    auto executor = graph.make_executor();
    executor.view().run();
    const std::vector<Row> expected{{4, 6, 14, {}}, {11, -1, 14, 31}, {13, -3, 16, 33},
                                    {6, 4, 16, {}}, {std::nullopt, 10, {}, {}}, {11, -1, 18, 31}};
    return Observe::rows == expected ? 0 : 1;
}
