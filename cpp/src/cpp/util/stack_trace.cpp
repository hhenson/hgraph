#include <hgraph/util/stack_trace.h>

//#define BACKWARD_HAS_BACKTRACE 1
#include <backward.hpp>

namespace hgraph {

std::string get_stack_trace() {
    backward::StackTrace st;
    st.load_here(32); // Capture up to 32 frames
    backward::Printer p;
    p.object = true;
    p.color_mode = backward::ColorMode::never; // No colors for string output
    p.address = true;

    std::ostringstream oss;
    p.print(st, oss);
    return oss.str();
}

void print_stack_trace() {
    backward::StackTrace st;
    st.load_here(32);
    backward::Printer p;
    p.object = true;
    p.color_mode = backward::ColorMode::automatic;
    p.address = true;

    p.print(st, std::cerr);
}

void install_crash_handlers() {
    static backward::SignalHandling sh;
}

} // namespace hgraph
