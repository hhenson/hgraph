#pragma once

#include <string>
#include <iostream>

namespace hgraph {
    /**
 * Get a stack trace as a string.
 * Uses backward-cpp for detailed stack traces with symbols.
 */
    std::string get_stack_trace();

    /**
 * Print a stack trace to stderr.
 */
    void print_stack_trace();

    /**
 * Install signal handlers to print stack traces on crashes (SIGSEGV, SIGABRT, etc).
 * Call this once at program startup for automatic crash reporting.
 */
    void install_crash_handlers();
} // namespace hgraph