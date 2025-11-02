#include <hgraph/util/stack_trace.h>
#include <sstream>
#include <iostream>

#if HGRAPH_WITH_BACKWARD
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

#else

namespace hgraph {
    std::string get_stack_trace() {
        // backward-cpp disabled: return empty string (no-op)
        return std::string();
    }

    void print_stack_trace() {
        // backward-cpp disabled: no-op
    }

    void install_crash_handlers() {
        // backward-cpp disabled: no-op
    }
} // namespace hgraph

#endif