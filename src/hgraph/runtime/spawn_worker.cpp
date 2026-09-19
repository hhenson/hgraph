#include <hgraph/runtime/spawn.h>
#include <hgraph/runtime/distributed_child.h>
#include <hgraph/util/scope.h>

#include <map>

// Worker serving belongs to the runtime: the distributed argv entry point
// must remain linkable without depending back on higher-level spawn wiring.
namespace hgraph
{
    namespace
    {
        auto &spawn_recipes()
        {
            static std::map<std::string, SpawnWorkerFactory, std::less<>> recipes;
            return recipes;
        }
    }
    void register_spawn_worker_recipe(std::string name, SpawnWorkerFactory factory)
    {
        if (name.empty() || factory == nullptr)
            throw std::invalid_argument("spawn_: worker recipe needs a name and factory");
        const auto [entry, inserted] = spawn_recipes().emplace(std::move(name), factory);
        if (!inserted && entry->second != factory)
            throw std::invalid_argument("spawn_: worker recipe name already has a different factory");
    }

    void serve_spawn_worker(distributed::PipeEndpoint &channel, SpawnWorkerPlan plan,
                            DateTime start, DateTime end, GraphExecutorPhaseRunner phase_runner)
    {
        using namespace distributed;
        DistributedChildHost host{std::move(plan.graph), end, std::move(phase_runner)};
        bool stopped = false;
        static_cast<void>(fallback_on_exception(false, [&] {
            std::string identity;
            if (!channel.receive(identity)) throw std::runtime_error("spawn_: missing boundary identity");
            if (identity != plan.boundary_identity)
                throw std::runtime_error("spawn_: worker boundary differs from caller");
            // The owner says how this stage starts (RFC 0039): fresh, or
            // from the image it held at the last completed day.
            std::string opening;
            if (!channel.receive(opening)) throw std::runtime_error("spawn_: missing start frame");
            if (const auto restore = decode_restore_frame(opening))
                static_cast<void>(start_worker_restored(host, start, restore->image, restore->component));
            else if (opening == start_frame) host.start(start);
            else throw std::runtime_error("spawn_: unknown start frame");
            if (host.graph().executor().stop_requested())
                throw std::runtime_error("child requested stop during start");
            channel.send(encode_reply(plan.slots, CycleReply{host.next_scheduled_time(), {}, {}}));
            std::string payload;
            while (channel.receive(payload))
            {
                if (payload == "stop")
                {
                    stopped = true;
                    host.stop();
                    channel.send(encode_reply(plan.slots, CycleReply{}));
                    return true;
                }
                if (const auto component = checkpoint_frame_component(payload))
                {
                    // A stage that cannot capture says so and carries on: the
                    // refusal fails its owner's capture, not this graph.
                    channel.send(answer_checkpoint(host, *component));
                    continue;
                }
                auto reply = serve_cycle(host, plan.slots, decode_request(plan.slots, payload));
                if (!reply.error.empty()) throw std::runtime_error(reply.error);
                if (host.graph().executor().stop_requested())
                    throw std::runtime_error("child requested stop before its owner sealed the input");
                channel.send(encode_reply(plan.slots, reply));
            }
            stopped = true;
            host.stop();
            return true;
        }, [&](const char *message) {
            const std::string error{message};
            if (!stopped) static_cast<void>(fallback_on_exception(false, [&] { host.stop(); return true; }));
            static_cast<void>(fallback_on_exception(false, [&] {
                channel.send(encode_reply(plan.slots, CycleReply{MAX_DT, {}, error}));
                return true;
            }));
        }));
    }

    void serve_registered_spawn_worker(distributed::PipeEndpoint &channel, std::string_view recipe,
                                       DateTime start, DateTime end)
    {
        static_cast<void>(fallback_on_exception(false, [&] {
            const auto factory = spawn_recipes().find(recipe);
            if (factory == spawn_recipes().end()) throw std::invalid_argument("spawn_: worker recipe is not registered");
            std::string bootstrap;
            if (!channel.receive(bootstrap)) throw std::runtime_error("spawn_: missing bootstrap");
            auto plan = factory->second(bootstrap);
            serve_spawn_worker(channel, std::move(plan), start, end);
            return true;
        }, [&](const char *message) {
            channel.send(distributed::encode_reply({}, distributed::CycleReply{MAX_DT, {}, message}));
        }));
    }

}
