#include <hgraph/runtime/distributed_worker.h>

#include <hgraph/runtime/distributed_child.h>

#include <ankerl/unordered_dense.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string_view>

namespace hgraph::distributed
{
    namespace
    {
        using RecipeTable = ankerl::unordered_dense::map<std::string, WorkerRecipe>;

        RecipeTable &recipes()
        {
            // Program-lifetime, and deliberately NOT cleared by a registry
            // reset: a recipe is usually registered at static initialisation,
            // which will not run again, so clearing it would leave a process
            // unable to serve anything it could serve a moment earlier.
            static RecipeTable table;
            return table;
        }

        [[nodiscard]] bool same(const WorkerRecipe &left, const WorkerRecipe &right) noexcept
        {
            return left.build == right.build && left.boundary == right.boundary;
        }

        /** ``--flag=value`` -> value, or nullopt when this is a different flag. */
        [[nodiscard]] std::optional<std::string_view> flag_value(std::string_view argument,
                                                                 std::string_view flag)
        {
            if (!argument.starts_with(flag)) { return std::nullopt; }
            return argument.substr(flag.size());
        }

        [[nodiscard]] std::int64_t as_number(std::string_view text, std::string_view what)
        {
            std::int64_t value = 0;
            const auto [end, code] =
                std::from_chars(text.data(), text.data() + text.size(), value);
            if (code != std::errc{} || end != text.data() + text.size())
            {
                throw std::invalid_argument(
                    fmt::format("distributed worker: {} is not a number: '{}'", what, text));
            }
            return value;
        }
    }  // namespace

    void register_worker_recipe(std::string key, WorkerRecipe recipe)
    {
        if (!recipe.valid())
        {
            throw std::invalid_argument(
                fmt::format("distributed worker: the recipe for '{}' is incomplete", key));
        }
        const auto found = recipes().find(key);
        if (found != recipes().end())
        {
            if (same(found->second, recipe)) { return; }
            throw std::invalid_argument(fmt::format(
                "distributed worker: '{}' is already registered as a different recipe. The name is "
                "what the two processes agree on, so one name must mean one child graph.",
                key));
        }
        recipes().emplace(std::move(key), recipe);
    }

    const WorkerRecipe *worker_recipe(std::string_view key)
    {
        // Looked up once per worker start, never per cycle: the temporary
        // string is cheaper than a transparent hash nobody else needs.
        const auto found = recipes().find(std::string{key});
        return found == recipes().end() ? nullptr : &found->second;
    }

    std::vector<std::string> registered_worker_recipes()
    {
        std::vector<std::string> keys;
        keys.reserve(recipes().size());
        for (const auto &entry : recipes()) { keys.push_back(entry.first); }
        std::sort(keys.begin(), keys.end());
        return keys;
    }

    void serve_worker(PipeEndpoint &channel, const WorkerRecipe &recipe, DateTime start_time,
                      DateTime end_time)
    {
        serve_worker(channel, recipe.build(), recipe.boundary(), start_time, end_time);
    }

    void serve_worker(PipeEndpoint &channel, GraphBuilder child, const BoundarySlots &slots,
                      DateTime start_time, DateTime end_time, GraphExecutorPhaseRunner phase_runner)
    {
        DistributedChildHost host{std::move(child), end_time, std::move(phase_runner)};
        host.start(start_time);

        std::string payload;
        while (channel.receive(payload))
        {
            // Decode failures are reported, not thrown: the caller is in
            // another process and can only learn of them through a reply.
            CycleReply reply;
            try
            {
                reply = serve_cycle(host, slots, decode_request(slots, payload));
            }
            catch (const std::exception &error)
            {
                reply       = CycleReply{};
                reply.error = fmt::format("distributed worker: {}", error.what());
            }
            channel.send(encode_reply(slots, reply));
        }
        host.stop();
    }

    bool run_worker_if_requested(int argc, char **argv)
    {
        std::string_view key{};
        std::int64_t     read_handle  = -1;
        std::int64_t     write_handle = -1;
        std::int64_t     start_micros = MIN_ST.time_since_epoch().count();
        std::int64_t     end_micros   = MAX_ET.time_since_epoch().count();
        bool             requested    = false;

        for (int i = 1; i < argc; ++i)
        {
            const std::string_view argument{argv[i]};
            if (const auto recipe_value = flag_value(argument, worker_recipe_flag))
            {
                key       = *recipe_value;
                requested = true;
            }
            else if (const auto read_value = flag_value(argument, worker_read_flag))
            {
                read_handle = as_number(*read_value, "the read handle");
            }
            else if (const auto write_value = flag_value(argument, worker_write_flag))
            {
                write_handle = as_number(*write_value, "the write handle");
            }
            else if (const auto start_value = flag_value(argument, worker_start_flag))
            {
                start_micros = as_number(*start_value, "the start time");
            }
            else if (const auto end_value = flag_value(argument, worker_end_flag))
            {
                end_micros = as_number(*end_value, "the end time");
            }
        }

        if (!requested) { return false; }

        const WorkerRecipe *recipe = worker_recipe(key);
        if (recipe == nullptr)
        {
            throw std::invalid_argument(fmt::format(
                "distributed worker: no recipe named '{}' is registered in this program. The "
                "caller and the worker must link the same registration; this program knows: [{}]",
                key, fmt::join(registered_worker_recipes(), ", ")));
        }
        if (read_handle < 0 || write_handle < 0)
        {
            throw std::invalid_argument(
                "distributed worker: launched without a channel to serve on");
        }

        PipeEndpoint channel = PipeEndpoint::adopt(read_handle, write_handle);
        serve_worker(channel, *recipe, DateTime{TimeDelta{start_micros}},
                     DateTime{TimeDelta{end_micros}});
        return true;
    }
}  // namespace hgraph::distributed
