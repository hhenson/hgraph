#include <hgraph/runtime/shared_output_node.h>

#include <cassert>

#include <hgraph/runtime/graph.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_input/bundle_view.h>
#include <hgraph/types/time_series/ts_input/base_view.h>
#include <hgraph/types/time_series/ts_input/target_link.h>
#include <hgraph/types/time_series/ts_output/base_view.h>
#include <hgraph/types/time_series_reference.h>

#include <algorithm>
#include <array>
#include <deque>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph
{
    namespace
    {
        constexpr std::string_view shared_output_capture_storage_field{"shared_output_capture"};

        struct SharedOutputCaptureStorage
        {
            NodePtr        source{};
            TSOutputHandle target{};
            TSInputView    input{};
            bool           target_known{false};
        };

        struct SharedOutputConfig
        {
            std::string                 key{};
            const TSValueTypeMetaData  *target_schema{nullptr};
            std::size_t                  storage_offset{0};
            bool                        strict{true};
        };

        [[nodiscard]] const SharedOutputConfig &register_shared_output_config(
            std::string key,
            const TSValueTypeMetaData &target_schema,
            bool strict,
            std::size_t storage_offset = 0)
        {
            if (key.empty()) { throw std::invalid_argument("shared output key must not be empty"); }
            static auto *configs = new std::deque<SharedOutputConfig>;
            const auto existing = std::ranges::find_if(
                *configs,
                [&](const SharedOutputConfig &config) {
                    return config.key == key &&
                           config.target_schema == &target_schema &&
                           config.storage_offset == storage_offset &&
                           config.strict == strict;
                });
            if (existing != configs->end()) { return *existing; }
            // Node callback tables are interned, so builder config storage must
            // outlive every graph instance created from the builder.
            configs->push_back(SharedOutputConfig{
                .key = std::move(key),
                .target_schema = &target_schema,
                .storage_offset = storage_offset,
                .strict = strict,
            });
            return configs->back();
        }

        [[nodiscard]] TimeSeriesReference normalize_shared_output_reference(
            const SharedOutputConfig &config,
            TimeSeriesReference reference)
        {
            const auto *actual = reference.target_schema();
            if (actual == nullptr)
            {
                if (reference.is_empty()) { return TimeSeriesReference::empty(config.target_schema); }
                throw std::invalid_argument("shared output reference has no target schema");
            }

            auto &registry = TypeRegistry::instance();
            if (!time_series_schema_equivalent(registry.dereference(actual),
                                               registry.dereference(config.target_schema)))
            {
                throw std::invalid_argument("shared output reference target schema does not match output schema");
            }
            return reference;
        }

        [[nodiscard]] SharedOutputCaptureStorage &capture_storage_of(
            const NodeView &view,
            const SharedOutputConfig &config)
        {
            return *MemoryUtils::cast<SharedOutputCaptureStorage>(
                MemoryUtils::advance(view.data(), config.storage_offset));
        }

        [[nodiscard]] bool input_target_is_stable(const TSInputView &input)
        {
            if (!input.is_bindable() || !input.bound()) { return false; }

            auto target_view = input.bound_output();
            const auto *target_schema = target_view.schema();
            if (target_schema == nullptr || target_schema->kind == TSTypeKind::REF) { return false; }

            return detail::target_link_storage(target_view.data_view()) == nullptr;
        }

        [[nodiscard]] bool input_target_unchanged(
            const TSInputView &input,
            SharedOutputCaptureStorage &storage)
        {
            if (!storage.target_known || !storage.target.bound() || !input_target_is_stable(input))
            {
                return false;
            }
            return storage.target.same_as(input.bound_output().handle());
        }

        void remember_input_target(const TSInputView &input, SharedOutputCaptureStorage &storage)
        {
            if (!input.is_bindable())
            {
                storage.target.reset();
                storage.target_known = false;
                return;
            }

            storage.target = input.bound_output().handle();
            storage.target_known = true;
        }

        void initialize_shared_output_capture(
            const NodeView &view,
            DateTime evaluation_time,
            SharedOutputCaptureStorage &storage)
        {
            if (storage.source && storage.input.schema() != nullptr) { return; }

            auto input       = view.input(evaluation_time);
            auto bundle      = input.as_bundle();
            // Retained view: carry the prepared route (issue #203).
            storage.input    = input.routed_child_at(0);
            auto source_input = bundle.at(1);

            NodeView source = source_input.bound_output().owner_node();
            if (!source.valid())
            {
                throw std::logic_error("shared output capture could not recover the shared output source node");
            }
            if (!source.has_state())
            {
                throw std::logic_error("shared output capture target node has no reference state");
            }
            storage.source = source.pointer();
        }

        void capture_shared_output_reference(
            const SharedOutputConfig &config,
            const NodeView &view,
            DateTime evaluation_time,
            bool start_phase)
        {
            auto &storage = capture_storage_of(view, config);
            initialize_shared_output_capture(view, evaluation_time, storage);
            auto target = storage.input.borrowed_ref(evaluation_time);
            if (!start_phase && input_target_unchanged(target, storage)) { return; }

            auto reference = normalize_shared_output_reference(config, TimeSeriesReference{target});
            remember_input_target(target, storage);
            // Capture nodes begin active because a plain TS can still be backed
            // by a retargetable link (for example, a chained adaptor). Once the
            // runtime proves that the bound endpoint itself is stable, value
            // ticks can flow directly through the published reference and this
            // relay no longer needs to observe them.
            if (input_target_is_stable(target) && storage.input.active())
            {
                storage.input.make_passive();
            }
            NodeView source_node{storage.source};

            const auto  state = source_node.state();
            const auto &previous = state.checked_as<TimeSeriesReference>();
            if (previous == reference) { return; }

            source_node.replace_state(Value{std::move(reference)});

            GraphValue *graph = source_node.graph_value();
            if (graph == nullptr)
            {
                throw std::logic_error("shared output source node is not attached to a graph");
            }
            // Shared-output relays are rank-correct BY WIRING-TIME PROOF: the
            // pair is declared with Wiring::add_same_cycle_pair, which both
            // rank-constrains the source after every capture and has
            // Wiring::finish VALIDATE the final order once all captures are
            // known. The runtime therefore trusts it — the relay is SAME-cycle
            // with no hot-path checks (wiring-time validation over run-time
            // cost). Next-cycle forwarding is the sanctioned design only for
            // subscription/request-reply request stubs — service_node.cpp.
            assert(graph == view.graph_value() &&
                   "shared output capture and its paired source must live in the same graph");
            assert((start_phase || source_node.node_index() > view.node_index()) &&
                   "shared output capture must rank before its paired source (validated at wiring)");
            static_cast<void>(start_phase);
            graph->schedule_node(source_node.node_index(), evaluation_time);
        }

        bool shared_output_capture_evaluate_impl(const void *, const NodeView &view, DateTime evaluation_time)
        {
            if (!view.started()) { return true; }
            const auto *config = static_cast<const SharedOutputConfig *>(
                view.type().ops_ref().extended_view_context);
            capture_shared_output_reference(*config, view, evaluation_time, false);
            return true;
        }

        void shared_output_capture_stop(const NodeView &view, DateTime)
        {
            const auto *config = static_cast<const SharedOutputConfig *>(
                view.type().ops_ref().extended_view_context);
            auto &storage = capture_storage_of(view, *config);
            storage.source = NodePtr{};
            storage.target.reset();
            storage.input = TSInputView{};
            storage.target_known = false;
        }
    }  // namespace

    std::string output_key(std::string_view path)
    {
        if (path.empty()) { throw std::invalid_argument("shared output path must not be empty"); }
        return std::string{path};
    }

    NodeBuilder make_shared_output_source_node(
        std::string path,
        const TSValueTypeMetaData &target_schema,
        bool strict)
    {
        const auto *config = &register_shared_output_config(output_key(path), target_schema, strict);
        auto       &registry = TypeRegistry::instance();
        const auto *output_schema = registry.ref(&target_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema.display_name      = "shared_output_source";
        descriptor.schema.output_schema     = output_schema;
        descriptor.schema.state_schema      = output_schema->value_schema;
        descriptor.schema.node_kind         = NodeKind::PullSource;
        descriptor.schema.schedule_on_start = true;

        descriptor.callbacks.evaluate = [config](const NodeView &view, DateTime evaluation_time) {
            const auto  state = view.state();
            const auto &reference = state.checked_as<TimeSeriesReference>();
            if (reference.is_empty() && reference.target_schema() == nullptr)
            {
                if (config->strict) { throw std::runtime_error("missing shared output: " + config->key); }
                return;
            }

            Value value{normalize_shared_output_reference(*config, reference)};
            auto  mutation = view.output(evaluation_time).begin_mutation(evaluation_time);
            if (!mutation.move_value_from(std::move(value)))
            {
                throw std::logic_error("shared output source failed to publish the captured reference");
            }
        };
        descriptor.callbacks.stop = [](const NodeView &view, DateTime) {
            view.replace_state(Value{TimeSeriesReference{}});
        };

        NodeBuilder builder = NodeBuilder::from_canonical_descriptor(
            std::move(descriptor), config);
        builder.label(std::string{"shared_output_source:"} + config->key);
        return builder;
    }

    NodeBuilder make_shared_output_capture_node(std::string path, const TSValueTypeMetaData &target_schema)
    {
        auto       &registry  = TypeRegistry::instance();
        const auto *ref_schema = registry.ref(&target_schema);

        NodeTypeDescriptor descriptor;
        descriptor.schema.display_name = "shared_output_capture";
        descriptor.schema.input_schema = registry.un_named_tsb({
            {"ts", &target_schema},
            {"shared_output", ref_schema},
        });
        descriptor.schema.node_kind     = NodeKind::Sink;
        descriptor.schema.active_inputs = std::vector<std::size_t>{0};
        descriptor.schema.valid_inputs  = std::vector<std::size_t>{};

        const std::array fields{NodeStorageField{
            .name = shared_output_capture_storage_field,
            .plan = &MemoryUtils::plan_for<SharedOutputCaptureStorage>(),
        }};
        descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);

        const auto *config = &register_shared_output_config(
            output_key(path), target_schema, true,
            descriptor.storage_plan->component(shared_output_capture_storage_field).offset);

        descriptor.callbacks.start = [config](const NodeView &view, DateTime evaluation_time) {
            capture_shared_output_reference(*config, view, evaluation_time, true);
        };
        descriptor.callbacks.stop            = &shared_output_capture_stop;
        descriptor.ops.evaluate_impl         = &shared_output_capture_evaluate_impl;
        descriptor.ops.extended_view_context = config;

        NodeBuilder builder = NodeBuilder::from_canonical_descriptor(
            std::move(descriptor), config);
        builder.label(std::string{"shared_output_capture:"} + config->key);
        return builder;
    }
}  // namespace hgraph
