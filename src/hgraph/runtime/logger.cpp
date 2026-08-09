#include <hgraph/runtime/logger.h>

#include <spdlog/sinks/stdout_color_sinks.h>

#include <mutex>
#include <utility>

namespace hgraph::log
{
    namespace
    {
        // Configuration-time state (a shared_ptr here is sanctioned: the
        // per-tick path only ever borrows the raw pointer via LoggerView).
        std::shared_ptr<spdlog::logger> g_logger;

        std::shared_ptr<spdlog::logger> make_default_logger()
        {
            if (auto existing = spdlog::get("hgraph")) { return existing; }
            return spdlog::stdout_color_mt("hgraph");
        }
    }  // namespace

    spdlog::logger &logger()
    {
        return *shared_logger();
    }

    std::shared_ptr<spdlog::logger> shared_logger()
    {
        if (g_logger == nullptr) { g_logger = make_default_logger(); }
        return g_logger;
    }

    void set_logger(std::shared_ptr<spdlog::logger> logger)
    {
        g_logger = logger != nullptr ? std::move(logger) : make_default_logger();
    }

    void reset_logger()
    {
        // Drop from the spdlog registry too — make_default_logger() would
        // otherwise hand back the same instance with its stale sinks.
        spdlog::drop("hgraph");
        g_logger = nullptr;
    }
}  // namespace hgraph::log
