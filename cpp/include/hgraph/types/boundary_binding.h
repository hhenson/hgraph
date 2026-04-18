#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/node.h>

#include <cstdint>
#include <string>
#include <vector>

namespace hgraph
{
    struct Graph;

    // ---- Binding mode enums ----

    enum class InputBindingMode : uint8_t
    {
        BIND_DIRECT,                // Non-REF, non-multiplexed direct binding
        CLONE_REF_BINDING,          // REF input bound to same upstream as parent
        BIND_MULTIPLEXED_ELEMENT,   // Keyed/list element selected by operator
        BIND_KEY_VALUE,             // Key input from operator-owned value
        DETACH_RESTORE_BLANK,       // Detach + restore inert input on child removal
    };

    enum class OutputBindingMode : uint8_t
    {
        ALIAS_CHILD_OUTPUT,         // Expose child output as parent output
        ALIAS_PARENT_INPUT,         // Expose one of the parent's inputs as parent output
        ALIAS_KEY_VALUE,           // Expose the operator-owned key value as parent output
    };

    enum class ContextBindingMode : uint8_t
    {
        CONTEXT_IMPORT,             // Import context from parent graph boundary
        CONTEXT_EXPORT,             // Export child-owned context
    };

    // ---- Binding specs ----

    struct InputBindingSpec
    {
        std::string arg_name;
        InputBindingMode mode{InputBindingMode::BIND_DIRECT};
        int64_t child_node_index{-1};       // Target node in compiled child graph
        Path parent_input_path;             // Path within the parent arg source
        Path child_input_path;              // Path within that node's input
        const TSMeta *ts_schema{nullptr};
    };

    struct OutputBindingSpec
    {
        OutputBindingMode mode{OutputBindingMode::ALIAS_CHILD_OUTPUT};
        std::string parent_arg_name;
        int64_t child_node_index{-1};
        Path child_output_path;
        Path parent_output_path;        // Empty = root output
    };

    struct ContextBindingSpec
    {
        ContextBindingMode mode{ContextBindingMode::CONTEXT_IMPORT};
        std::string context_key;
        int64_t child_node_index{-1};
        Path child_path;
    };

    // ---- Boundary plan ----

    /**
     * Compile-time plan describing how a child graph connects to its parent.
     *
     * Produced once during ChildGraphTemplate compilation from wiring-layer stubs.
     * Shared immutable data — never modified at runtime.
     */
    struct HGRAPH_EXPORT BoundaryBindingPlan
    {
        std::vector<InputBindingSpec> inputs;
        std::vector<OutputBindingSpec> outputs;
        std::vector<ContextBindingSpec> context_bindings;
    };

    // ---- Boundary runtime ----

    /**
     * Runtime executor for a BoundaryBindingPlan.
     *
     * Performs the actual input/output binding against live graph nodes.
     * This is a stateless helper — all state lives in the plan and child graph.
     */
    struct HGRAPH_EXPORT BoundaryBindingRuntime
    {
        /**
         * Initial bind: connect all child graph boundary inputs/outputs
         * to the parent's corresponding time-series.
         *
         * For BIND_DIRECT, navigates the parent node's input fields via
         * arg_name, resolves the upstream bound output, and binds the child
         * input to that output.
         *
         * @param plan         The boundary binding plan
         * @param child        The child graph whose inputs/outputs are being bound
         * @param parent       The parent node owning this nested graph
         * @param eval_time    Current evaluation time
         */
        static void bind(const BoundaryBindingPlan &plan,
                         Graph &child,
                         Node &parent,
                         engine_time_t eval_time);

        /**
         * Keyed bind: for multiplexed arguments, bind a specific key's element.
         * Used by map_/mesh_ when a new key is activated.
         */
        static void bind_keyed(const BoundaryBindingPlan &plan,
                               Graph &child,
                               Node &parent,
                               const value::View &key,
                               engine_time_t eval_time);

        /**
         * Unbind: detach child inputs and restore inert state.
         * Used when a child graph is being removed.
         */
        static void unbind(const BoundaryBindingPlan &plan,
                           Graph &child);

        /**
         * Rebind: update bindings when a parent input target changes.
         * Used for REF rebinding when the upstream reference changes.
         */
        static void rebind(const BoundaryBindingPlan &plan,
                           Graph &child,
                           Node &parent,
                           std::string_view arg_name,
                           engine_time_t eval_time);
    };

    HGRAPH_EXPORT const char *boundary_output_binding_mode_name(OutputBindingMode mode);

}  // namespace hgraph
