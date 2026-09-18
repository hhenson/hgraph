#include <hgraph/runtime/distributed_worker.h>
#include <hgraph/runtime/spawn.h>

#include <hgraph/runtime/distributed_child.h>

#include <ankerl/unordered_dense.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <optional>
#include <map>
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

        using PreparedRecipeTable = std::map<std::string, PreparedWorkerRecipe, std::less<>>;

        PreparedRecipeTable &prepared_recipes()
        {
            static PreparedRecipeTable table;
            return table;
        }

        constexpr std::string_view prepared_prefix{"@hgraph-prepared:1:"};

        struct PreparedSelection
        {
            std::string_view name;
            std::size_t group;
            std::size_t groups;
        };

        std::size_t partition_number(std::string_view text)
        {
            std::size_t value{};
            const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
            if (text.empty() || error != std::errc{} || end != text.data() + text.size())
                throw std::invalid_argument("distributed worker: invalid prepared recipe partition");
            return value;
        }

        std::optional<PreparedSelection> prepared_selection(std::string_view key)
        {
            if (!key.starts_with(prepared_prefix)) return {};
            key.remove_prefix(prepared_prefix.size());
            auto separator = key.find(':');
            if (separator == key.npos)
                throw std::invalid_argument("distributed worker: malformed prepared recipe");
            const auto length = partition_number(key.substr(0, separator));
            key.remove_prefix(separator + 1);
            if (length == 0 || length >= key.size() || key[length] != ':')
                throw std::invalid_argument("distributed worker: malformed prepared recipe name");
            const auto name = key.substr(0, length);
            key.remove_prefix(length + 1);
            separator = key.find(':');
            if (separator == key.npos)
                throw std::invalid_argument("distributed worker: malformed prepared recipe partition");
            const auto group = partition_number(key.substr(0, separator));
            const auto groups = partition_number(key.substr(separator + 1));
            if (groups == 0 || group >= groups)
                throw std::invalid_argument("distributed worker: invalid prepared recipe partition");
            return PreparedSelection{name, group, groups};
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

    void register_prepared_worker_recipe(std::string key, PreparedWorkerRecipe recipe)
    {
        if (key.empty() || !recipe.valid())
            throw std::invalid_argument("distributed worker: prepared recipe needs a name and factory");
        const auto [entry, inserted] = prepared_recipes().emplace(std::move(key), recipe);
        if (!inserted && entry->second.build != recipe.build)
            throw std::invalid_argument("distributed worker: prepared recipe name already has a different factory");
    }

    const PreparedWorkerRecipe *prepared_worker_recipe(std::string_view key)
    {
        const auto entry = prepared_recipes().find(key);
        return entry == prepared_recipes().end() ? nullptr : &entry->second;
    }

    std::string prepared_worker_recipe_key(std::string_view name, std::size_t group, std::size_t groups)
    {
        if (name.empty() || groups == 0 || group >= groups)
            throw std::invalid_argument("distributed worker: invalid prepared recipe name or partition");
        return fmt::format("{}{}:{}:{}:{}", prepared_prefix, name.size(), name, group, groups);
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

        // The first frame decides how the graph starts (RFC 0039), so it is
        // read before the start. Nothing else about the ordinary path moves: a
        // first frame that is not a restore starts the graph fresh and is then
        // served as the cycle it is, and a channel that closes without a frame
        // still starts and stops the graph, so its hooks run when they did.
        bool        started = false;
        std::string payload;
        while (channel.receive(payload))
        {
            // Failures are reported, not thrown: the caller is in another
            // process and can only learn of them through a reply.
            if (const auto image = restore_frame_image(payload))
            {
                CycleReply reply;
                try
                {
                    if (started) { throw std::logic_error("a restore must be the first frame"); }
                    reply.next_scheduled_time = start_worker_restored(host, start_time, *image);
                    started                   = true;
                }
                catch (const std::exception &error)
                {
                    reply       = CycleReply{};
                    reply.error = fmt::format("distributed worker: {}", error.what());
                }
                channel.send(encode_reply(slots, reply));
                // A refused image leaves nothing to serve: a partially
                // restored worker is never started fresh instead.
                if (!started) { return; }
                continue;
            }
            if (!started)
            {
                host.start(start_time);
                started = true;
            }
            if (payload == checkpoint_frame)
            {
                std::string reply;
                try { reply = encode_checkpoint_reply(capture_worker_image(host)); }
                catch (const std::exception &error)
                {
                    reply = encode_checkpoint_error(fmt::format("distributed worker: {}", error.what()));
                }
                channel.send(reply);
                continue;
            }
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
        if (!started) { host.start(start_time); }
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

        if (key.starts_with(spawn_worker_prefix))
        {
            if (read_handle < 0 || write_handle < 0)
                throw std::invalid_argument("spawn_: launched without a channel");
            auto channel = PipeEndpoint::adopt(read_handle, write_handle);
            serve_registered_spawn_worker(channel, key.substr(spawn_worker_prefix.size()),
                DateTime{TimeDelta{start_micros}}, DateTime{TimeDelta{end_micros}});
            return true;
        }

        const WorkerRecipe *recipe = worker_recipe(key);
        const auto selection = recipe == nullptr ? prepared_selection(key) : std::nullopt;
        const auto *prepared = selection ? prepared_worker_recipe(selection->name) : nullptr;
        if (recipe == nullptr && prepared == nullptr)
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
        if (prepared != nullptr)
        {
            auto plan = prepared->build(selection->group, selection->groups);
            serve_worker(channel, std::move(plan.child), plan.slots,
                         DateTime{TimeDelta{start_micros}}, DateTime{TimeDelta{end_micros}});
        }
        else
        {
            serve_worker(channel, *recipe, DateTime{TimeDelta{start_micros}},
                         DateTime{TimeDelta{end_micros}});
        }
        return true;
    }
}  // namespace hgraph::distributed
