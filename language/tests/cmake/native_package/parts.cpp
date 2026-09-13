#include <native.h>

#include <iostream>

int main() {
    namespace native = hgraph_::native::native;
    if (native::len(hgraph::Str{"hgraph"}) != 6 || !native::contains("hgraph", "graph") || !native::starts_with("hgraph", "hg") ||
        !native::ends_with("hgraph", "graph")) {
        std::cerr << "rebuilt installed native source parts returned the wrong value\n";
        return 1;
    }
}
