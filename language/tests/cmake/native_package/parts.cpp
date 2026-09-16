#include <hgraph/lib/testing/eval_node.h>
#include <native.h>
#include <standard.h>

#include <iostream>

int main() {
    const auto provider = hgraph_::std_::register_operators();
    if (!provider.active()) {
        std::cerr << "rebuilt standard module did not register\n";
        return 2;
    }
    using namespace hgraph;
    const auto result = testing::eval_node<hgraph_::std_::operators::drop>(std::vector<std::optional<Int>>{1, 2, 3}, Int{1});
    if (result.size() != 3 || result[0] || !result[1] || result[1]->view().checked_as<Int>() != 2) {
        std::cerr << "rebuilt drop returned the wrong ticks\n";
        return 3;
    }
    namespace native = hgraph_::native::native;
    if (native::len(hgraph::Str{"hgraph"}) != 6 || !native::contains("hgraph", "graph") || !native::starts_with("hgraph", "hg") ||
        !native::ends_with("hgraph", "graph")) {
        std::cerr << "rebuilt installed native source parts returned the wrong value\n";
        return 1;
    }
    if (native::days(TimeDelta{-1}) != -1 || native::seconds(TimeDelta{-1}) != 86399 || native::as_int(Bool{true}) != 1) {
        std::cerr << "rebuilt native value projections returned the wrong result\n";
        return 4;
    }
}
