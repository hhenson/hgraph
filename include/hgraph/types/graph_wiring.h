#ifndef HGRAPH_CPP_ROOT_GRAPH_WIRING_H
#define HGRAPH_CPP_ROOT_GRAPH_WIRING_H

#include <hgraph/runtime/graph.h>                       // GraphBuilder, GraphEdge
#include <hgraph/runtime/node.h>                        // NodeBuilder, NodeTypeRef
#include <hgraph/types/call_args.h>                     // NamedArg / arg<"name">(...)
#include <hgraph/types/metadata/type_realization.h>     // graph-scoped closed-union value bindings
#include <hgraph/types/metadata/value_plan_factory.h>   // ValuePlanFactory (scalar bundle binding)
#include <hgraph/types/static_node.h>                   // StaticNodeSignature, In/Out/State/Scalar markers
#include <hgraph/runtime/shared_output_node.h>
#include <hgraph/types/static_schema.h>                 // schema_descriptor
#include <hgraph/types/time_series/endpoint_schema.h>   // time_series_schema_equivalent
#include <hgraph/types/type_resolution.h>               // ResolutionMap, ts_resolver, unifiers, ts_type
#include <hgraph/types/value/value.h>                   // Value (scalar configuration)
#include <hgraph/types/wiring_observer.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <functional>
#include <initializer_list>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace hgraph
{
    class Wiring;
    /**
     * C++ graph wiring (slice 1: top-level node wiring, no scalars yet).
     *
     * A graph is authored as a struct with a static ``compose(Wiring &)`` body that
     * calls ``wire<NodeType>(w, ports...)`` to add nodes; each call returns a typed
     * ``Port`` to the node's output, and passing ports as inputs records edges.
     * Nodes are **interned** (identical node + inputs → one node) and the graph is
     * **topologically sorted and ranked** when built. The runtime ``GraphBuilder``
     * it produces is consumed exactly as a hand-built one.
     *
     * See the developer guide *Graph Wiring* for the full design (including the
     * planned scalar inputs, sub-graph composition and the Python-shared core).
     */

    struct WiringInstance;
    struct WiringDelayedBindingState;
    struct WiringDelayedBindingControl;

    /**
     * Erased wiring-time handle to a time-series source.
     *
     * A peered source references a producing node, a source root (ordinary
     * output, error output, or recordable-state output), and optional ``path``
     * within that root. A structural source has no producing node of its own; its
     * children describe the peered or structural sources for each fixed child
     * slot. Target information belongs on ``WiringInputRef`` only.
     */
    struct WiringPortRef
    {
        struct PeeredSource
        {
            const WiringInstance     *node{nullptr};
            std::vector<std::size_t>  path{};
            GraphEdgeSourceKind       output_kind{GraphEdgeSourceKind::Output};
        };

        struct StructuralSource
        {
            std::vector<WiringPortRef> children{};
        };

        /**
         * A sub-graph boundary placeholder: the source is either the
         * ``arg_index``-th declared input or a local captured-input index
         * (``path`` walks within it). Boundary sources exist only while compiling a sub-graph
         * (``compile_subgraph<G>``); ``Wiring::finish_subgraph`` converts them
         * into nested-graph input bindings — no stub node is ever created.
         */
        struct BoundarySource
        {
            std::size_t              arg_index{0};
            std::vector<std::size_t> path{};
            bool                     captured{false};
        };

        /** A typed wiring source whose producer is supplied later in compose(). */
        struct DelayedSource
        {
            std::shared_ptr<WiringDelayedBindingState> state{};
            std::vector<std::size_t>                   path{};
        };

        enum class SourceKind
        {
            Unbound,
            Null,
            Peered,
            Structural,
            Boundary,
            Delayed,
        };

        const TSValueTypeMetaData *schema{nullptr};

        [[nodiscard]] static WiringPortRef peered_source(const WiringInstance *node,
                                                         std::vector<std::size_t> path,
                                                         const TSValueTypeMetaData *schema,
                                                         GraphEdgeSourceKind output_kind = GraphEdgeSourceKind::Output)
        {
            if (node == nullptr) { throw std::logic_error("WiringPortRef::peered_source requires a node"); }
            WiringPortRef ref;
            ref.schema  = schema;
            ref.source_ = PeeredSource{node, std::move(path), output_kind};
            return ref;
        }

        /**
         * Wiring-time **argument adornment** (Python's ``pass_through()`` /
         * ``no_key()`` wrappers): consumed by operator implementations during
         * classification; never part of graph structure (edges, bindings and
         * source interning ignore it — operator configs that depend on it
         * must fold it into their own interning identity).
         */
        enum class ArgTag : std::uint8_t
        {
            None        = 0,
            PassThrough = 1,   ///< do not demultiplex — pass the input whole
            NoKey       = 2,   ///< demultiplex, but exclude from key inference
            /** The consuming node's input becomes PASSIVE (removed from its
                active list; Python's ``passive(ts)`` marker). Applies to the
                tagged USAGE only — tag a copy, the original port is untouched. */
            Passive     = 3,
        };

        ArgTag arg_tag{ArgTag::None};

        [[nodiscard]] WiringPortRef with_arg_tag(ArgTag tag) const
        {
            WiringPortRef tagged{*this};
            tagged.arg_tag = tag;
            return tagged;
        }

        [[nodiscard]] static WiringPortRef structural_source(const TSValueTypeMetaData *schema,
                                                             std::vector<WiringPortRef> children)
        {
            WiringPortRef ref;
            ref.schema  = schema;
            ref.source_ = StructuralSource{std::move(children)};
            return ref;
        }

        [[nodiscard]] static WiringPortRef null_source(const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr) { throw std::logic_error("WiringPortRef::null_source requires a schema"); }
            WiringPortRef ref;
            ref.schema = schema;
            return ref;
        }

        [[nodiscard]] static WiringPortRef boundary_source(std::size_t arg_index,
                                                           std::vector<std::size_t> path,
                                                           const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr) { throw std::logic_error("WiringPortRef::boundary_source requires a schema"); }
            WiringPortRef ref;
            ref.schema  = schema;
            ref.source_ = BoundarySource{arg_index, std::move(path), false};
            return ref;
        }

        /**
         * A placeholder for an enclosing-wiring source captured by a compiled
         * child. ``finish_subgraph`` appends the real source after the child's
         * declared arguments and resolves this local capture index to that
         * final boundary ordinal.
         */
        [[nodiscard]] static WiringPortRef captured_boundary_source(
            std::size_t capture_index,
            std::vector<std::size_t> path,
            const TSValueTypeMetaData *schema)
        {
            if (schema == nullptr)
            {
                throw std::logic_error("WiringPortRef::captured_boundary_source requires a schema");
            }
            WiringPortRef ref;
            ref.schema  = schema;
            ref.source_ = BoundarySource{capture_index, std::move(path), true};
            return ref;
        }

        [[nodiscard]] static WiringPortRef delayed_source(
            std::shared_ptr<WiringDelayedBindingState> state,
            std::vector<std::size_t> path,
            const TSValueTypeMetaData *schema)
        {
            if (state == nullptr) { throw std::logic_error("WiringPortRef::delayed_source requires state"); }
            if (schema == nullptr) { throw std::logic_error("WiringPortRef::delayed_source requires a schema"); }
            WiringPortRef ref;
            ref.schema  = schema;
            ref.source_ = DelayedSource{std::move(state), std::move(path)};
            return ref;
        }

        [[nodiscard]] SourceKind source_kind() const noexcept
        {
            if (std::holds_alternative<PeeredSource>(source_)) { return SourceKind::Peered; }
            if (std::holds_alternative<StructuralSource>(source_)) { return SourceKind::Structural; }
            if (std::holds_alternative<BoundarySource>(source_)) { return SourceKind::Boundary; }
            if (std::holds_alternative<DelayedSource>(source_)) { return SourceKind::Delayed; }
            return schema != nullptr ? SourceKind::Null : SourceKind::Unbound;
        }

        [[nodiscard]] bool is_peered_source() const noexcept { return source_kind() == SourceKind::Peered; }
        [[nodiscard]] bool is_structural_source() const noexcept { return source_kind() == SourceKind::Structural; }
        [[nodiscard]] bool is_null_source() const noexcept { return source_kind() == SourceKind::Null; }
        [[nodiscard]] bool is_unbound_source() const noexcept { return source_kind() == SourceKind::Unbound; }
        [[nodiscard]] bool is_boundary_source() const noexcept { return source_kind() == SourceKind::Boundary; }
        [[nodiscard]] bool is_delayed_source() const noexcept { return source_kind() == SourceKind::Delayed; }
        [[nodiscard]] bool is_captured_boundary_source() const noexcept
        {
            const auto *source = std::get_if<BoundarySource>(&source_);
            return source != nullptr && source->captured;
        }

        [[nodiscard]] std::size_t boundary_arg_index() const
        {
            const auto *source = std::get_if<BoundarySource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not a sub-graph boundary"); }
            if (source->captured)
            {
                throw std::logic_error("WiringPortRef source is a captured sub-graph boundary");
            }
            return source->arg_index;
        }

        [[nodiscard]] std::size_t boundary_capture_index() const
        {
            const auto *source = std::get_if<BoundarySource>(&source_);
            if (source == nullptr || !source->captured)
            {
                throw std::logic_error("WiringPortRef source is not a captured sub-graph boundary");
            }
            return source->arg_index;
        }

        [[nodiscard]] const std::vector<std::size_t> &boundary_path() const
        {
            const auto *source = std::get_if<BoundarySource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not a sub-graph boundary"); }
            return source->path;
        }

        [[nodiscard]] WiringPortRef projected_boundary_source(
            std::vector<std::size_t> path,
            const TSValueTypeMetaData *projected_schema) const
        {
            const auto *source = std::get_if<BoundarySource>(&source_);
            if (source == nullptr)
            {
                throw std::logic_error("WiringPortRef source is not a sub-graph boundary");
            }
            return source->captured
                       ? captured_boundary_source(source->arg_index, std::move(path), projected_schema)
                       : boundary_source(source->arg_index, std::move(path), projected_schema);
        }

        [[nodiscard]] const std::shared_ptr<WiringDelayedBindingState> &delayed_state() const
        {
            const auto *source = std::get_if<DelayedSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not delayed"); }
            return source->state;
        }

        [[nodiscard]] const std::vector<std::size_t> &delayed_path() const
        {
            const auto *source = std::get_if<DelayedSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not delayed"); }
            return source->path;
        }

        [[nodiscard]] WiringPortRef projected_delayed_source(
            std::vector<std::size_t> path,
            const TSValueTypeMetaData *projected_schema) const
        {
            return delayed_source(delayed_state(), std::move(path), projected_schema);
        }

        [[nodiscard]] const WiringInstance *peered_node() const
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not peered"); }
            return source->node;
        }

        [[nodiscard]] const std::vector<std::size_t> &peered_path() const
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not peered"); }
            return source->path;
        }

        [[nodiscard]] GraphEdgeSourceKind peered_output_kind() const
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not peered"); }
            return source->output_kind;
        }

        [[nodiscard]] const std::vector<WiringPortRef> &structural_children() const
        {
            const auto *source = std::get_if<StructuralSource>(&source_);
            if (source == nullptr) { throw std::logic_error("WiringPortRef source is not structural"); }
            return source->children;
        }

        [[nodiscard]] const WiringInstance *peered_node_or_null() const noexcept
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            return source != nullptr ? source->node : nullptr;
        }

        [[nodiscard]] const std::vector<std::size_t> &peered_path_or_empty() const noexcept
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            if (source != nullptr) { return source->path; }
            static const std::vector<std::size_t> empty_path;
            return empty_path;
        }

        [[nodiscard]] GraphEdgeSourceKind peered_output_kind_or_default() const noexcept
        {
            const auto *source = std::get_if<PeeredSource>(&source_);
            return source != nullptr ? source->output_kind : GraphEdgeSourceKind::Output;
        }

        /** Source identity used for capture deduplication; argument tags are usage metadata. */
        [[nodiscard]] bool same_source_as(const WiringPortRef &other) const noexcept
        {
            if (schema != other.schema || source_kind() != other.source_kind()) { return false; }
            switch (source_kind())
            {
                case SourceKind::Unbound:
                case SourceKind::Null:
                    return true;
                case SourceKind::Peered:
                    return peered_node() == other.peered_node() &&
                           peered_path() == other.peered_path() &&
                           peered_output_kind() == other.peered_output_kind();
                case SourceKind::Boundary:
                    return is_captured_boundary_source() == other.is_captured_boundary_source() &&
                           (is_captured_boundary_source()
                                ? boundary_capture_index() == other.boundary_capture_index()
                                : boundary_arg_index() == other.boundary_arg_index()) &&
                           boundary_path() == other.boundary_path();
                case SourceKind::Delayed:
                    return delayed_state() == other.delayed_state() &&
                           delayed_path() == other.delayed_path();
                case SourceKind::Structural:
                {
                    const auto &left  = structural_children();
                    const auto &right = other.structural_children();
                    if (left.size() != right.size()) { return false; }
                    for (std::size_t index = 0; index < left.size(); ++index)
                    {
                        if (!left[index].same_source_as(right[index])) { return false; }
                    }
                    return true;
                }
            }
            return false;
        }

      private:
        std::variant<std::monostate, PeeredSource, StructuralSource, BoundarySource, DelayedSource> source_{};
    };

    /** Shared state behind one leaf of a delayed binding. */
    struct WiringDelayedBindingState
    {
        Wiring                        *wiring{nullptr};
        const TSValueTypeMetaData     *schema{nullptr};
        std::optional<WiringPortRef>   source{};
    };

    /** Shared handle state for a delayed binding and all copies of its facade. */
    struct WiringDelayedBindingControl
    {
        Wiring                    *wiring{nullptr};
        const TSValueTypeMetaData *schema{nullptr};
        WiringPortRef              port{};
        std::vector<std::shared_ptr<WiringDelayedBindingState>>
            leaves{};
        bool bound{false};
    };

    /** Consumer-side wiring input: source port plus optional target path on the consuming node. */
    struct WiringInputRef
    {
        WiringPortRef             source{};
        std::vector<std::size_t>  target_path{};
        bool                      rank_dependency{true};
    };

    /**
     * Erased structural wiring argument captured from brace syntax such as
     * ``wire<Node>(w, {a, b})``. The consuming ``In<>`` schema decides whether the
     * children form a TSL or TSB and provides the structural source schema.
     */
    struct WiringStructuralSourceArg
    {
        std::vector<WiringPortRef> children{};

        WiringStructuralSourceArg() = default;
        WiringStructuralSourceArg(std::initializer_list<WiringPortRef> refs) : children(refs) {}
        explicit WiringStructuralSourceArg(std::vector<WiringPortRef> refs) : children(std::move(refs)) {}
    };

    struct WiringNamedPortRef
    {
        std::string   name{};
        WiringPortRef source{};

        WiringNamedPortRef(std::string_view field_name, WiringPortRef field_source)
            : name(field_name),
              source(std::move(field_source))
        {
        }
    };

    struct WiringNamedStructuralSourceArg
    {
        std::vector<WiringNamedPortRef> fields{};

        WiringNamedStructuralSourceArg() = default;
        WiringNamedStructuralSourceArg(std::initializer_list<WiringNamedPortRef> refs) : fields(refs) {}
        explicit WiringNamedStructuralSourceArg(std::vector<WiringNamedPortRef> refs) : fields(std::move(refs)) {}
    };

    struct WiringServiceImplementationEndpoint
    {
        std::string   endpoint{};
        ResolutionMap resolution{};
    };

    struct WiringServiceClientRecord
    {
        std::string base_path{};
        std::string endpoint_path{};
        std::string kind{};
        std::string interface_name{};
        std::string specialization{};
        bool        receive{true};
    };

    namespace wiring_path_detail
    {
        template <typename>
        inline constexpr bool always_false_v = false;

        struct TypedPathValue
        {
            std::string   value{};
            ResolutionMap resolution{};
            bool          has_typed_suffix{false};
        };

    }   // reopened below; see BoundaryPath

    /**
     * The wiring path of a service or adaptor boundary: a path string, the
     * resolution its scalar qualifiers imply, and whether it carries a typed
     * suffix.
     *
     * ``service::ServicePath`` and ``adaptor::AdaptorPath`` were separate but
     * field-identical structs; they are now aliases of this one type
     * (RFC 0011 step 8), which is what lets a single implementation span both
     * families.
     */
    using BoundaryPath = wiring_path_detail::TypedPathValue;

    /**
     * How one interface participates in a MULTI-interface registration group.
     *
     * Specialized for service interfaces in ``service_wiring.h`` and for
     * adaptor interfaces in ``adaptor_wiring.h``. Those two headers do not
     * include each other, so the customization point lives here: with both
     * visible, one implementation may span an adaptor and a service in a
     * single atomic registration (RFC 0011 step 7). This is the same extension
     * idiom as ``wire_customization``.
     */
    namespace boundary_detail
    {
        template <typename Interface, typename = void>
        struct group_member;   // primary intentionally undefined

    }


    namespace wiring_path_detail
    {

        inline void append_escaped_path_component(std::string &out, std::string_view value)
        {
            constexpr char hex[] = "0123456789ABCDEF";
            for (unsigned char c : value)
            {
                const bool plain = (c >= 'a' && c <= 'z') ||
                                   (c >= 'A' && c <= 'Z') ||
                                   (c >= '0' && c <= '9') ||
                                   c == '_' || c == '-' || c == '.';
                if (plain)
                {
                    out.push_back(static_cast<char>(c));
                }
                else
                {
                    out.push_back('%');
                    out.push_back(hex[c >> 4U]);
                    out.push_back(hex[c & 0x0FU]);
                }
            }
        }

        [[nodiscard]] inline std::string_view metadata_name(const ValueTypeMetaData *meta)
        {
            if (meta == nullptr) { return "<unresolved>"; }
            return meta->name().empty() ? std::string_view{"<unnamed>"} : meta->name();
        }

        [[nodiscard]] inline std::string_view metadata_name(const TSValueTypeMetaData *meta)
        {
            if (meta == nullptr) { return "<unresolved>"; }
            return meta->name().empty() ? std::string_view{"<unnamed>"} : meta->name();
        }

        template <typename T>
        void append_scalar_path_value(std::string &out, const T &value)
        {
            using V = std::remove_cvref_t<T>;
            if constexpr (std::same_as<V, Str>)
            {
                append_escaped_path_component(out, value);
            }
            else if constexpr (std::same_as<V, std::string_view>)
            {
                append_escaped_path_component(out, value);
            }
            else if constexpr (std::same_as<V, const char *>)
            {
                append_escaped_path_component(out, value != nullptr ? std::string_view{value} : std::string_view{});
            }
            else if constexpr (std::same_as<V, char *>)
            {
                append_escaped_path_component(out, value != nullptr ? std::string_view{value} : std::string_view{});
            }
            else if constexpr (std::same_as<V, Bool>)
            {
                out.append(value ? "true" : "false");
            }
            else if constexpr (std::integral<V>)
            {
                out.append(std::to_string(value));
            }
            else if constexpr (std::floating_point<V>)
            {
                std::ostringstream stream;
                stream << value;
                append_escaped_path_component(out, stream.str());
            }
            else if constexpr (std::same_as<V, const TSValueTypeMetaData *>)
            {
                append_escaped_path_component(out, metadata_name(value));
            }
            else if constexpr (std::same_as<V, const ValueTypeMetaData *>)
            {
                append_escaped_path_component(out, metadata_name(value));
            }
            else
            {
                static_assert(always_false_v<V>,
                              "service/adaptor path values must be primitive scalars or named type metadata");
            }
        }

        template <typename Arg>
        void append_scalar_path_segment(std::string &out, std::size_t index, const Arg &argument)
        {
            using A = std::remove_cvref_t<Arg>;
            if constexpr (call_args_detail::is_named_arg_v<A>)
            {
                if constexpr (call_args_detail::is_static_named_arg_v<A>)
                {
                    append_escaped_path_component(out, A::field_name.sv());
                }
                else
                {
                    append_escaped_path_component(out, argument.name);
                }
                out.push_back('=');
                append_scalar_path_value(out, argument.value);
            }
            else
            {
                out.push_back('$');
                out.append(std::to_string(index));
                out.push_back('=');
                append_scalar_path_value(out, argument);
            }
        }

        template <typename Arg>
        void bind_path_resolution(ResolutionMap &resolution, const Arg &argument)
        {
            using A = std::remove_cvref_t<Arg>;
            if constexpr (call_args_detail::is_named_arg_v<A>)
            {
                const std::string_view name = argument.name;
                using V = std::remove_cvref_t<decltype(argument.value)>;
                if constexpr (std::same_as<V, const TSValueTypeMetaData *>)
                {
                    resolution.bind_ts(name, argument.value);
                }
                else if constexpr (std::same_as<V, const ValueTypeMetaData *>)
                {
                    resolution.bind_scalar(name, argument.value);
                }
            }
        }

        template <typename... Args>
        [[nodiscard]] TypedPathValue typed_path_value(std::string_view base, const Args &...args)
        {
            TypedPathValue typed;
            typed.value = std::string{base};
            if constexpr (sizeof...(Args) > 0)
            {
                typed.has_typed_suffix = true;
                typed.value.push_back('[');
                std::size_t index = 0;
                (
                    [&] {
                        if (index != 0) { typed.value.push_back(','); }
                        append_scalar_path_segment(typed.value, index, args);
                        bind_path_resolution(typed.resolution, args);
                        ++index;
                    }(),
                    ...);
                typed.value.push_back(']');
            }
            return typed;
        }

        [[nodiscard]] inline bool has_resolution(const ResolutionMap &resolution)
        {
            return !resolution.ts_vars.empty() || !resolution.scalar_vars.empty() || !resolution.size_vars.empty();
        }

        inline void merge_resolution(ResolutionMap &target, const ResolutionMap &source)
        {
            for (const auto &[name, meta] : source.ts_vars) { target.bind_ts(name, meta); }
            for (const auto &[name, meta] : source.scalar_vars) { target.bind_scalar(name, meta); }
            for (const auto &[name, size] : source.size_vars) { target.bind_size(name, size); }
        }

        [[nodiscard]] inline std::vector<std::pair<std::string, std::string>> missing_resolution_segments(
            const ResolutionMap &target,
            const ResolutionMap &already_in_path)
        {
            std::vector<std::pair<std::string, std::string>> segments;
            for (const auto &[name, meta] : target.ts_vars)
            {
                if (already_in_path.find_ts(name) != nullptr) { continue; }
                std::string value;
                append_escaped_path_component(value, metadata_name(meta));
                segments.emplace_back(name, std::move(value));
            }
            for (const auto &[name, meta] : target.scalar_vars)
            {
                if (already_in_path.find_scalar(name) != nullptr) { continue; }
                std::string value;
                append_escaped_path_component(value, metadata_name(meta));
                segments.emplace_back(name, std::move(value));
            }
            for (const auto &[name, size] : target.size_vars)
            {
                if (already_in_path.find_size(name).has_value()) { continue; }
                segments.emplace_back(name, std::to_string(size));
            }
            std::sort(segments.begin(), segments.end(),
                      [](const auto &lhs, const auto &rhs) { return lhs.first < rhs.first; });
            return segments;
        }

        inline void append_resolution_segments(std::string &value,
                                               bool &has_typed_suffix,
                                               const std::vector<std::pair<std::string, std::string>> &segments)
        {
            if (segments.empty()) { return; }

            if (has_typed_suffix && !value.empty() && value.back() == ']')
            {
                value.pop_back();
                value.push_back(',');
            }
            else
            {
                value.push_back('[');
                has_typed_suffix = true;
            }

            for (std::size_t index = 0; index < segments.size(); ++index)
            {
                if (index != 0) { value.push_back(','); }
                append_escaped_path_component(value, segments[index].first);
                value.push_back('=');
                value.append(segments[index].second);
            }
            value.push_back(']');
        }

        template <typename Path>
        [[nodiscard]] Path with_resolution(Path path, const ResolutionMap &inferred)
        {
            if (!has_resolution(inferred)) { return path; }

            ResolutionMap merged = path.resolution;
            merge_resolution(merged, inferred);
            auto segments = missing_resolution_segments(merged, path.resolution);
            append_resolution_segments(path.value, path.has_typed_suffix, segments);
            path.resolution = std::move(merged);
            return path;
        }

        /**
         * Helpers shared by the service and adaptor boundary families
         * (RFC 0011 step 8). These were byte-for-byte copies in both
         * ``service_wiring.h`` and ``adaptor_wiring.h``, differing only in an
         * error string; the family name is now a parameter.
         */

        /** Wiring intern key for a boundary path. Boundary source nodes do not
         *  carry a path scalar at runtime - this exists only so two nodes at
         *  the same path dedup. */
        [[nodiscard]] inline Value boundary_path_key(const std::string &full_path)
        {
            return Value{Str{full_path}};
        }

    }  // namespace wiring_path_detail

    /**
     * The interned wiring identity. It pairs a node's ``NodeBuilder`` (which
     * carries any per-instance scalar configuration) with its time-series input
     * edges; identity is the node definition plus the input edges **and** the
     * scalar values. Runtime edges are derived from ``inputs`` at build time.
     */
    struct WiringInstance
    {
        std::type_index                     definition{typeid(void)};
        NodeBuilder                         builder;
        std::vector<WiringInputRef>          inputs;
        std::vector<const WiringInstance *> rank_dependencies;
    };

    struct CompiledSubGraph;   // defined in subgraph_wiring.h

    /**
     * The node's resolved schema identity — the (registry-interned, hence
     * stable) schema pointers that enter the interning key. Normally derived
     * from a builder's ``NodeTypeMetaData``; supplied explicitly for the
     * deferred-builder ``add_node`` overload.
     */
    struct WiringNodeSchema
    {
        const TSValueTypeMetaData *input{nullptr};
        const TSValueTypeMetaData *output{nullptr};
        const TSValueTypeMetaData *error_output{nullptr};
        const TSValueTypeMetaData *recordable_state{nullptr};
        const ValueTypeMetaData   *scalar{nullptr};
        const ValueTypeMetaData   *state{nullptr};

        bool operator==(const WiringNodeSchema &) const noexcept = default;
    };

    enum class WiringKind : std::uint8_t
    {
        TopLevel,
        SubGraph,
    };

    struct WiringObserverRegistry;

    class HGRAPH_EXPORT WiringObservationScope
    {
      public:
        WiringObservationScope() noexcept = default;
        WiringObservationScope(Wiring &wiring, WiringScopeEvent event);
        WiringObservationScope(const WiringObservationScope &) = delete;
        WiringObservationScope &operator=(const WiringObservationScope &) = delete;
        WiringObservationScope(WiringObservationScope &&other) noexcept;
        WiringObservationScope &operator=(WiringObservationScope &&other) noexcept;
        ~WiringObservationScope() noexcept;

        void complete();
        void fail(std::string_view error);

      private:
        void cancel_noexcept() noexcept;

        Wiring         *wiring_{nullptr};
        WiringScopeEvent event_{};
        bool            active_{false};
    };

    /**
     * Shared runtime wiring core: accumulates interned ``WiringInstance``s and, on
     * ``finish``, topologically sorts + ranks them into a ``GraphBuilder``. (The
     * Python wiring bridge will drive this same core.)
     */
    class HGRAPH_EXPORT Wiring
    {
      public:
        explicit Wiring(WiringKind kind = WiringKind::TopLevel);
        ~Wiring();
        Wiring(const Wiring &)            = delete;
        Wiring &operator=(const Wiring &) = delete;
        Wiring(Wiring &&) noexcept;
        Wiring &operator=(Wiring &&) noexcept;

        /** Process-unique identity for associating wiring-lifetime adapter
            state across owned and borrowed language wrappers. */
        [[nodiscard]] std::uint64_t identity() const noexcept;

        /**
         * Retain extension-owned state until this Wiring is destroyed.
         *
         * Language bridges use this for wiring-time state recorded through a
         * borrowed wrapper: the wrapper may be ephemeral, while the state is
         * still needed by a later build_services()/finish() call on the
         * underlying Wiring. The runtime does not inspect the retained value.
         */
        void retain_extension_state(std::shared_ptr<void> state);

        /**
         * Return wiring-lifetime state for ``key``, creating it once on first
         * use. Native wiring extensions use this to share planning state
         * without process globals; the state is owned by this Wiring.
         */
        [[nodiscard]] std::shared_ptr<void> acquire_extension_state(
            std::type_index key,
            std::function<std::shared_ptr<void>()> create);

        /** Register an idempotent extension finalizer run after lazy service
         *  materialization and before rank dependencies are applied. */
        void register_pre_rank_finalizer(std::function<void(Wiring &)> finalizer);

        /** Whether this wiring is a root graph or an isolated child graph. */
        [[nodiscard]] WiringKind kind() const noexcept;

        /** User-facing label copied to the produced graph. */
        Wiring &label(std::string label);
        [[nodiscard]] std::string_view label() const noexcept;

        /**
         * Intern a node with its input edges + scalar configuration and return its
         * output port. ``def`` is the node *definition's* stable identity
         * (``typeid(T)`` for a C++ static node) — two calls with the same ``def``,
         * equal inputs **and** equal ``scalars`` dedup to one instance. ``builder``
         * is the build artifact stored for ``finish`` (the ``scalars`` are recorded
         * on it). Pass an empty ``Value`` for a node with no scalar inputs.
         */
        /**
         * Diagnostic label hint (issue #247): the next ``add_node`` whose
         * builder schema or default label names ``expected_operator`` labels
         * its node, then the hint clears. Lets erased wire
         * paths attach user-facing identity (a python function name) without
         * threading a label through every candidate wire signature; the
         * operator-name guard keeps auxiliary nodes (const lifts) unlabeled.
         */
        void set_pending_node_label(std::string expected_operator, std::string label);
        /** Consume a pending label using an explicit operator alias.
         *
         * Erased language bridges call operators by their public name while
         * the selected native node schema names the concrete implementation.
         * This hook labels only that bridge-directed call; ordinary native
         * operator wiring keeps the implementation label used by observers.
         */
        void apply_pending_node_label(std::string_view operator_alias,
                                      NodeBuilder &builder);
        void clear_pending_node_label() noexcept;

        WiringPortRef add_node(std::type_index def, NodeBuilder builder, std::span<const WiringInputRef> inputs,
                               Value scalars);

        /**
         * Convenience overload for ordinary positional node inputs. Each source
         * port is bound to the input path matching its argument index.
         */
        WiringPortRef add_node(std::type_index def, NodeBuilder builder, std::span<const WiringPortRef> inputs,
                               Value scalars);

        /**
         * Add a value-producing node without interning. Use this for nodes whose
         * identity is their allocation site rather than their structural inputs,
         * such as feedback sources.
         */
        WiringPortRef add_unique_node(std::type_index def, NodeBuilder builder,
                                      std::span<const WiringInputRef> inputs,
                                      Value scalars);

        /** Convenience overload for ordinary positional unique-node inputs. */
        WiringPortRef add_unique_node(std::type_index def, NodeBuilder builder,
                                      std::span<const WiringPortRef> inputs,
                                      Value scalars);

        /**
         * Set a graph trait — parent-chained key-value metadata carried onto
         * the produced ``GraphBuilder`` (see ``GraphView::trait_or`` and the
         * record/replay design record, P5). Typical use:
         * ``w.set_trait(std::string{record_replay::RECORDABLE_ID_TRAIT}, ...)``.
         */
        void set_trait(std::string_view name, const ValueView &value);
        void set_trait(std::string_view name, Value &&value);


        /**
         * Add a layout-only dependency. ``node`` is ranked after ``depends_on``,
         * but no runtime edge is emitted.
         */
        void add_rank_dependency(const WiringInstance *node, const WiringInstance *depends_on);

        /**
         * Declare a same-cycle boundary pair (shared-output relays): the
         * ``source`` is rank-constrained after the ``capture``, and ``finish``
         * VALIDATES the final order, so the runtime schedules the source for
         * the current evaluation time with no hot-path checks. Deferred
         * request stubs do not use this helper. Ranked reply-less
         * request/reply clients use the service-rank contract instead because
         * their source may be external to a nested graph. Same-time work is
         * ordered as the first sending client, then the source, then later
         * sending clients.
         */
        void add_same_cycle_pair(const WiringInstance *capture, const WiringInstance *source);

        /**
         * Register an implementation-owned service/adaptor identity. This mirrors
         * Python's wiring-context duplicate checks: clients may refer to a path
         * many times, but only one implementation may own a concrete interface
         * identity in a wiring graph.
         */
        void register_built_service_path(std::string path, std::string_view kind);

        void register_service_client_path(std::string path, std::string_view kind,
                                          std::string_view interface_name = {},
                                          std::string_view specialization = {});
        void register_service_rank_anchor(std::string path, const WiringInstance *node);
        void register_service_client_rank(std::string path, std::string_view kind,
                                          const WiringInstance *node, bool receive);

        /** Record a service/adaptor implementation candidate without composing
            it. The materializer is invoked once, at build_services(), when at
            least one of its concrete interface paths has been requested. */
        void register_service_implementation_candidate(
            std::vector<std::string> paths,
            std::string description,
            std::function<void(Wiring &)> materialize);

        /** Record the interface-default implementation used as a fallback for
            any concrete user path of the same service/adaptor identity. */
        void register_default_service_implementation_candidate(
            std::string path_prefix,
            std::string path_suffix,
            std::string description,
            std::function<void(Wiring &, std::string_view)> materialize);

        /** Record an atomic multi-interface default implementation. Demand
            through any selector materializes the whole group at that user
            path; an overlapping exact implementation is ambiguous. */
        void register_default_service_implementation_candidate(
            std::vector<std::pair<std::string, std::string>> path_selectors,
            std::string description,
            std::function<void(Wiring &, std::string_view)> materialize);

        /** Record an implementation resolver which inspects current client
            demand itself. Catch-all resolvers compose once during the
            fixed-point build phase, never at registration time. */
        void register_catch_all_service_implementation_candidate(
            std::string description,
            std::function<void(Wiring &)> materialize);

        /** Materialize requested implementation candidates to a fixed point.
            Safe to call explicitly; finish() calls it again for later demand. */
        void build_services();
        [[nodiscard]] std::vector<std::pair<std::string, std::string>>
        service_client_paths() const;
        [[nodiscard]] std::vector<WiringServiceClientRecord>
        service_client_records() const;
        [[nodiscard]] std::vector<std::pair<std::string, std::string>>
        built_service_paths() const;
        [[nodiscard]] std::string_view service_materialization_path() const noexcept;

        /**
         * Stable state for the active implementation scope. Service/adaptor
         * client registration flips the value to true. Consumers may retain
         * the handle until pre-rank finalization, after the scope has closed.
         */
        [[nodiscard]] std::shared_ptr<const bool>
        service_implementation_boundary_dependency() const;

        /**
         * RAII wrapper for implementation-owned service/adaptor stub scopes.
         *
         * Construction pushes the expected stub set; ``complete`` validates and
         * pops it. Destruction cancels an unfinished scope, so exceptions while
         * wiring an implementation cannot leak a stale active scope into the
         * enclosing graph wiring.
         */
        class HGRAPH_EXPORT ServiceImplementationScope
        {
          public:
            ServiceImplementationScope() noexcept = default;
            ServiceImplementationScope(Wiring &wiring,
                                       std::string description,
                                       std::vector<WiringServiceImplementationEndpoint> required_endpoints,
                                       bool require_all = true);
            ServiceImplementationScope(Wiring &wiring,
                                       std::string description,
                                       std::vector<std::string> required_endpoints);
            ServiceImplementationScope(const ServiceImplementationScope &) = delete;
            ServiceImplementationScope &operator=(const ServiceImplementationScope &) = delete;
            ServiceImplementationScope(ServiceImplementationScope &&other) noexcept;
            ServiceImplementationScope &operator=(ServiceImplementationScope &&other) noexcept;
            ~ServiceImplementationScope() noexcept;

            void complete();

          private:
            void cancel_if_active() noexcept;

            Wiring *wiring_{nullptr};
            bool    active_{false};
        };

        [[nodiscard]] ServiceImplementationScope service_implementation_scope(
            std::string description,
            std::vector<WiringServiceImplementationEndpoint> required_endpoints,
            bool require_all = true);
        [[nodiscard]] ServiceImplementationScope service_implementation_scope(
            std::string description,
            std::vector<std::string> required_endpoints);

        void begin_service_implementation(std::string description, std::vector<std::string> required_endpoints);
        void begin_service_implementation(std::string description,
                                          std::vector<WiringServiceImplementationEndpoint> required_endpoints,
                                          bool require_all = true);
        [[nodiscard]] std::vector<std::string> service_implementation_used_endpoints() const;
        void register_service_implementation_stub(std::string endpoint, std::string_view kind);
        [[nodiscard]] ResolutionMap service_implementation_stub_resolution(const std::string &endpoint) const;
        void end_service_implementation();
        void cancel_service_implementation() noexcept;

        /**
         * Deferred-builder overload: intern by ``(def, schema, inputs, scalars)``
         * and call ``make_builder`` only when no interned instance exists. Use
         * when constructing the builder has a side effect or real cost that must
         * not happen for a deduped instance — e.g. a nested-graph node
         * registering its program-lifetime child-graph context.
         */
        WiringPortRef add_node(std::type_index def, const WiringNodeSchema &schema,
                               std::span<const WiringInputRef> inputs, Value scalars,
                               std::function<NodeBuilder()> make_builder);

        /** Convenience form of the deferred-builder overload for ordinary inputs. */
        WiringPortRef add_node(std::type_index def, const WiringNodeSchema &schema,
                               std::span<const WiringPortRef> inputs, Value scalars,
                               std::function<NodeBuilder()> make_builder);

        /**
         * Activate error capture on an already-added node: re-bind its builder
         * with an error output (``error_output_schema`` = ``error_schema``,
         * ``captures_errors`` = true) so its evaluation runs under a try/catch
         * and the error output is allocated. Returns the error-output schema.
         * Used by ``exception_time_series`` (the node is the port's producer).
         */
        const TSValueTypeMetaData *activate_error_capture(const WiringInstance *node,
                                                          const TSValueTypeMetaData *error_schema,
                                                          ErrorCaptureOptions options = {});

        /**
         * A view over the wiring-time ``GlobalState``. A ``compose`` body can seed
         * the store here; ``finish`` carries the populated state onto the produced
         * ``GraphBuilder`` (and thence onto each graph it builds).
         */
        [[nodiscard]] GlobalStateView global_state() noexcept;
        /** State visible to operator resolution; sub-graphs read the active root seed. */
        [[nodiscard]] GlobalStateView operator_state() noexcept;

        /** Add a borrowed wiring observer. It must outlive this wiring and its children. */
        void add_wiring_observer(WiringObserver *observer);
        [[nodiscard]] bool has_wiring_observers() const noexcept;
        /** Fresh child wiring carrying the same observer registry and current path. */
        [[nodiscard]] Wiring child_wiring() const;
        [[nodiscard]] std::vector<std::string> current_wiring_path() const;
        void notify_overload_resolution(const WiringResolutionEvent &event) const;

        [[nodiscard]] WiringObservationScope observation_scope(WiringScopeEvent event)
        {
            return WiringObservationScope{*this, std::move(event)};
        }

        template <typename Fn>
        decltype(auto) observe(WiringScopeEvent event, Fn &&fn)
        {
            if (!has_wiring_observers())
            {
                return std::invoke(std::forward<Fn>(fn));
            }

            WiringObservationScope scope{*this, std::move(event)};
            return annotate_on_exception<std::exception>(
                [&]() -> decltype(auto) {
                if constexpr (std::is_void_v<std::invoke_result_t<Fn>>)
                {
                    std::invoke(std::forward<Fn>(fn));
                    scope.complete();
                    return;
                }
                else
                {
                    decltype(auto) result = std::invoke(std::forward<Fn>(fn));
                    scope.complete();
                    return result;
                }
                },
                [&](const std::exception &error) {
                    static_cast<void>(fallback_on_exception(false, [&] {
                        scope.fail(error.what());
                        return true;
                    }));
                });
        }

        /** Topologically sort + rank the wired nodes into a rank-ordered GraphBuilder. */
        [[nodiscard]] GraphBuilder finish() &&;

        /**
         * Build a runnable graph from the wiring **as it currently stands**,
         * leaving the wiring open for further wiring (interactive/notebook
         * sessions — design record ``developer_guide/notebook.rst``). Same
         * validation as ``finish()``; the wiring ``GlobalState`` is COPIED
         * onto the builder rather than moved. Repeated snapshots are safe:
         * rank-dependency application de-duplicates.
         */
        [[nodiscard]] GraphBuilder snapshot() &;

        /**
         * Compile this wiring as a **sub-graph**: rank the nodes into a child
         * ``GraphBuilder`` and convert every boundary-sourced input into a
         * nested-graph input binding instead of an edge. ``output`` is the
         * sub-graph's returned output port, ``input_schemas`` the declared
         * boundary arg schemas in arg
         * order. Used by ``compile_subgraph<G>`` (see ``subgraph_wiring.h``).
         */
        [[nodiscard]] CompiledSubGraph finish_subgraph(
            std::optional<WiringPortRef> output,
            std::vector<const TSValueTypeMetaData *> input_schemas) &&;

        /** Register an enclosing-wiring source as an implicit child input. */
        [[nodiscard]] WiringPortRef capture_outer_source(WiringPortRef source);

        /** Claim a component's fully-qualified recordable id for this wiring;
            a second claim of the same id throws (one component instance per
            id per graph build - python parity). */
        void claim_component_id(std::string_view fq_recordable_id);

      private:
        friend class WiringObservationScope;

        Wiring(WiringKind kind,
               std::shared_ptr<WiringObserverRegistry> observers,
               std::vector<std::string> path);
        [[nodiscard]] WiringScopeEvent begin_observation(WiringScopeEvent event);
        void end_observation(const WiringScopeEvent &event, std::string_view error);
        void apply_service_rank_dependencies();
        void finalize_extensions();
        /** Shared body of finish()/snapshot(): validate + rank + build; the
            wiring GlobalState is moved when consuming, copied otherwise. */
        [[nodiscard]] GraphBuilder finish_top_level(bool consume_state);
        void validate_same_cycle_pairs(
            const std::unordered_map<const WiringInstance *, std::size_t> &index_of) const;

        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

    /** Typed wiring handle: an output port carrying its static schema. */
    template <typename Schema>
    class Port
    {
      public:
        using schema = Schema;

        Port() noexcept = default;
        Port(const WiringInstance *node, std::vector<std::size_t> path)
            : ref_{WiringPortRef::peered_source(node, std::move(path), schema_descriptor<Schema>::ts_meta())}
        {
        }
        Port(Wiring &wiring, const WiringInstance *node, std::vector<std::size_t> path)
            : wiring_(&wiring),
              ref_{WiringPortRef::peered_source(node, std::move(path), schema_descriptor<Schema>::ts_meta())}
        {
        }
        Port(Wiring *wiring, const WiringInstance *node, std::vector<std::size_t> path)
            : wiring_(wiring),
              ref_{WiringPortRef::peered_source(node, std::move(path), schema_descriptor<Schema>::ts_meta())}
        {
        }
        explicit Port(WiringPortRef ref) noexcept
            : ref_(std::move(ref))
        {
            stamp_schema();
        }
        Port(Wiring &wiring, WiringPortRef ref) noexcept
            : wiring_(&wiring),
              ref_(std::move(ref))
        {
            stamp_schema();
        }
        Port(Wiring *wiring, WiringPortRef ref) noexcept
            : wiring_(wiring),
              ref_(std::move(ref))
        {
            stamp_schema();
        }

        [[nodiscard]] const WiringInstance           *node() const noexcept { return ref_.peered_node_or_null(); }
        [[nodiscard]] const std::vector<std::size_t> &path() const noexcept { return ref_.peered_path_or_empty(); }
        [[nodiscard]] GraphEdgeSourceKind             output_kind() const noexcept
        {
            return ref_.peered_output_kind_or_default();
        }
        [[nodiscard]] Wiring                         *wiring() const noexcept { return wiring_; }
        [[nodiscard]] Wiring                         &checked_wiring() const
        {
            if (wiring_ == nullptr) { throw std::logic_error("Port does not carry a wiring context"); }
            return *wiring_;
        }
        template <typename OutSchema>
        [[nodiscard]] Port<OutSchema> as() const;

        /** Erase to the runtime port form (the runtime schema comes from ``Schema``). */
        [[nodiscard]] WiringPortRef erased() const { return ref_; }
        [[nodiscard]] operator WiringPortRef() const { return erased(); }

      private:
        // A concrete ``Schema`` stamps its interned runtime schema onto the ref; a
        // non-concrete (type-variable) schema keeps the ref's runtime-resolved
        // schema — the form an operator graph overload's generic ``Port``
        // parameter receives (e.g. ``Port<TSL<TsVar<"V">>>``).
        void stamp_schema() noexcept
        {
            if (const auto *meta = schema_descriptor<Schema>::ts_meta(); meta != nullptr) { ref_.schema = meta; }
        }

        Wiring        *wiring_{nullptr};
        WiringPortRef ref_{};
    };

    /**
     * **Erased** output port: a generic node whose output type is only known at
     * wiring time (resolved from inferred type variables, not supplied explicitly)
     * returns this form, carrying the resolved runtime schema instead of a static
     * one. Downstream ``wire<>`` accepts it and matches/unifies against the runtime
     * schema. (A generic source wired with an explicit output schema returns the
     * ordinary typed ``Port<S>`` instead.)
     */
    template <>
    class Port<void>
    {
      public:
        using schema = void;

        Port() noexcept = default;
        Port(const WiringInstance *node, std::vector<std::size_t> path, const TSValueTypeMetaData *schema)
            : ref_{WiringPortRef::peered_source(node, std::move(path), schema)}
        {
        }
        Port(Wiring &wiring, const WiringInstance *node, std::vector<std::size_t> path, const TSValueTypeMetaData *schema)
            : wiring_(&wiring),
              ref_{WiringPortRef::peered_source(node, std::move(path), schema)}
        {
        }
        Port(Wiring *wiring, const WiringInstance *node, std::vector<std::size_t> path, const TSValueTypeMetaData *schema)
            : wiring_(wiring),
              ref_{WiringPortRef::peered_source(node, std::move(path), schema)}
        {
        }
        explicit Port(WiringPortRef ref) noexcept
            : ref_(std::move(ref))
        {
        }
        Port(Wiring &wiring, WiringPortRef ref) noexcept
            : wiring_(&wiring),
              ref_(std::move(ref))
        {
        }
        Port(Wiring *wiring, WiringPortRef ref) noexcept
            : wiring_(wiring),
              ref_(std::move(ref))
        {
        }

        [[nodiscard]] const WiringInstance           *node() const noexcept { return ref_.peered_node_or_null(); }
        [[nodiscard]] const std::vector<std::size_t> &path() const noexcept { return ref_.peered_path_or_empty(); }
        [[nodiscard]] GraphEdgeSourceKind             output_kind() const noexcept
        {
            return ref_.peered_output_kind_or_default();
        }
        [[nodiscard]] const TSValueTypeMetaData      *runtime_schema() const noexcept { return ref_.schema; }
        [[nodiscard]] Wiring                         *wiring() const noexcept { return wiring_; }
        [[nodiscard]] Wiring                         &checked_wiring() const
        {
            if (wiring_ == nullptr) { throw std::logic_error("Port does not carry a wiring context"); }
            return *wiring_;
        }
        template <typename OutSchema>
        [[nodiscard]] Port<OutSchema> as() const;

        [[nodiscard]] WiringPortRef erased() const { return ref_; }
        [[nodiscard]] operator WiringPortRef() const { return erased(); }

      private:
        Wiring        *wiring_{nullptr};
        WiringPortRef ref_{};
    };

    /** Type-erased core of ``delayed_binding``; Python uses this same handle. */
    class HGRAPH_EXPORT ErasedDelayedBindingWiringPort
    {
      public:
        ErasedDelayedBindingWiringPort() noexcept = default;
        ErasedDelayedBindingWiringPort(Wiring &wiring, const TSValueTypeMetaData *schema);

        [[nodiscard]] WiringPortRef port() const;
        ErasedDelayedBindingWiringPort &bind(WiringPortRef source);
        [[nodiscard]] bool bound() const;
        [[nodiscard]] const TSValueTypeMetaData *schema() const;
        [[nodiscard]] Wiring *wiring() const;

      private:
        [[nodiscard]] WiringDelayedBindingControl &control() const;

        std::shared_ptr<WiringDelayedBindingControl> control_{};
    };

    /**
     * A typed wiring placeholder whose producer may be supplied after its
     * consumers have been wired. It changes construction order only: the
     * resolved edge remains a normal same-cycle dependency and cycles fail
     * during graph ranking. Use ``feedback`` when a cycle and one-cycle delay
     * are intentional.
     */
    template <typename Schema>
    class DelayedBindingWiringPort
    {
      public:
        using schema = Schema;

        DelayedBindingWiringPort() noexcept = default;
        explicit DelayedBindingWiringPort(ErasedDelayedBindingWiringPort erased)
            : erased_(std::move(erased))
        {
        }

        [[nodiscard]] Port<Schema> operator()() const
        {
            return Port<Schema>{erased_.wiring(), erased_.port()};
        }

        template <typename SourceSchema>
        DelayedBindingWiringPort &operator()(const Port<SourceSchema> &source)
        {
            erased_.bind(source.erased());
            return *this;
        }

        template <typename SourceSchema>
        DelayedBindingWiringPort &bind(const Port<SourceSchema> &source)
        {
            return (*this)(source);
        }

        [[nodiscard]] bool bound() const { return erased_.bound(); }

      private:
        ErasedDelayedBindingWiringPort erased_{};
    };

    template <typename Schema>
    [[nodiscard]] DelayedBindingWiringPort<Schema> delayed_binding(Wiring &wiring)
    {
        const auto *schema = schema_descriptor<Schema>::ts_meta();
        if (schema == nullptr)
        {
            throw std::invalid_argument("delayed_binding<Schema> requires a concrete time-series schema");
        }
        return DelayedBindingWiringPort<Schema>{ErasedDelayedBindingWiringPort{wiring, schema}};
    }

    /**
     * A **named** port parameter — gives a ``compose`` time-series parameter a
     * name so keyword arguments can target it (the port analogue of
     * ``Scalar<"name", T>``; node inputs are named via ``In<"name", …>``).
     * Behaves exactly like ``Port<S>`` and is accepted anywhere a port
     * parameter is (operator graph overloads, sub-graphs, ``WiredFn``
     * functions — where the name also resolves the function's ``**kwargs``).
     */
    /**
     * Mark a port usage PASSIVE (Python's ``passive(ts)``): the receiving
     * node's matching input is removed from its active list, so ticks on it
     * no longer schedule the node (values still read normally). Returns a
     * tagged COPY — passivity applies only where the returned port is used.
     */
    template <typename S>
    [[nodiscard]] Port<S> passive(Port<S> port)
    {
        return Port<S>{port.checked_wiring(), port.erased().with_arg_tag(WiringPortRef::ArgTag::Passive)};
    }

    template <fixed_string Name, typename S>
    struct NamedPort : Port<S>
    {
        static constexpr auto field_name = Name;

        using Port<S>::Port;
        NamedPort(Port<S> base) : Port<S>(std::move(base)) {}
    };

    namespace graph_wiring_detail
    {
        [[nodiscard]] inline std::logic_error special_output_error(std::string_view function_name,
                                                                   std::string_view detail)
        {
            std::string message{function_name};
            message += ": ";
            message += detail;
            return std::logic_error(message);
        }

        [[nodiscard]] inline const TSValueTypeMetaData *special_output_schema(
            const WiringPortRef &source,
            GraphEdgeSourceKind  output_kind,
            std::string_view     function_name)
        {
            if (!source.is_peered_source())
            {
                throw special_output_error(function_name, "requires a peered node output port");
            }
            if (!source.peered_path().empty())
            {
                throw special_output_error(function_name, "is only available from the node's root output port");
            }
            if (source.peered_output_kind() != GraphEdgeSourceKind::Output)
            {
                throw special_output_error(function_name, "requires the node's ordinary output port");
            }

            const WiringInstance       *node = source.peered_node();
            const NodeTypeMetaData     *meta = node->builder.type().schema();
            const TSValueTypeMetaData  *schema = nullptr;
            switch (output_kind)
            {
                case GraphEdgeSourceKind::ErrorOutput:
                    schema = meta != nullptr ? meta->error_output_schema : nullptr;
                    if (schema == nullptr)
                    {
                        throw special_output_error(function_name, "source node has no error output");
                    }
                    return schema;
                case GraphEdgeSourceKind::RecordableState:
                    schema = meta != nullptr ? meta->recordable_state_schema : nullptr;
                    if (schema == nullptr)
                    {
                        throw special_output_error(function_name, "source node has no recordable state output");
                    }
                    return schema;
                case GraphEdgeSourceKind::Output:
                    break;
            }
            throw special_output_error(function_name, "unsupported special output endpoint");
        }

        [[nodiscard]] inline WiringPortRef special_output_source(const WiringPortRef &source,
                                                                 GraphEdgeSourceKind  output_kind,
                                                                 std::string_view     function_name)
        {
            const TSValueTypeMetaData *schema = special_output_schema(source, output_kind, function_name);
            return WiringPortRef::peered_source(source.peered_node(), {}, schema, output_kind);
        }
    }  // namespace graph_wiring_detail

    /** C++-only access to a node's hidden recordable-state output for system wiring. */
    template <typename Schema>
    [[nodiscard]] Port<void> recordable_state(const Port<Schema> &port)
    {
        return Port<void>{port.wiring(), graph_wiring_detail::special_output_source(
                                             port.erased(), GraphEdgeSourceKind::RecordableState, "recordable_state")};
    }

    /** C++-only access to a node's hidden error output for system wiring. */
    template <typename Schema>
    [[nodiscard]] Port<void> error_output(const Port<Schema> &port)
    {
        return Port<void>{port.wiring(), graph_wiring_detail::special_output_source(
                                             port.erased(), GraphEdgeSourceKind::ErrorOutput, "error_output")};
    }

    /** Result of type-erased operator wiring before the public ``wire<>`` return is shaped by the operator marker. */
    struct OperatorWireResult
    {
        bool       has_output{false};
        Port<void> output{};
    };

    /** Base of every ``Operator<>`` marker; ``wire<>`` routes a type deriving it to operator dispatch. */
    struct operator_tag
    {
    };

    namespace operator_dispatch_detail
    {
        // The operator arm of ``wire<>`` — defined in ``operator_dispatch.h`` (a
        // translation unit that wires operators must include it). Forward-declared
        // here so the ``wire<>`` body parses; only instantiated for an ``Operator``.
        template <typename X, typename OutSchema, typename... Args>
        OperatorWireResult wire_operator_result(Wiring &w, const Args &...args);
    }  // namespace operator_dispatch_detail

    template <typename G>
    struct StaticGraphSignature;   // defined below; forward-declared for use in wire<G>

    namespace graph_wiring_detail
    {
        // A graph definition is a struct with a static ``compose(Wiring &, ...)``; a
        // node definition has a static ``eval(...)`` instead.
        template <typename X>
        concept is_graph_def = requires { &X::compose; };

        template <typename TImplementation>
        [[nodiscard]] NodeBuilder build_node_builder()
        {
            NodeBuilder nb;
            nb.implementation<TImplementation>();
            return nb;
        }

        // Recognise the typed port handle and recover an ``In<Name, S>``'s schema S.
        template <typename T> struct is_port : std::false_type {};
        template <typename S> struct is_port<Port<S>> : std::true_type {};
        template <fixed_string N, typename S> struct is_port<NamedPort<N, S>> : std::true_type {};

        template <typename T> struct is_named_port : std::false_type {};
        template <fixed_string N, typename S> struct is_named_port<NamedPort<N, S>> : std::true_type {};
        template <typename T> struct named_port_schema;
        template <fixed_string N, typename S> struct named_port_schema<NamedPort<N, S>> { using type = S; };

        template <typename T> struct is_structural_source_arg : std::false_type {};
        template <> struct is_structural_source_arg<WiringStructuralSourceArg> : std::true_type {};
        template <> struct is_structural_source_arg<WiringNamedStructuralSourceArg> : std::true_type {};

        // The erased port (``Port<void>``): carries only a runtime schema.
        template <typename T> struct is_erased_port : std::false_type {};
        template <> struct is_erased_port<Port<void>> : std::true_type {};

        template <typename T> struct in_param_schema;
        template <fixed_string N, typename S, auto... P> struct in_param_schema<In<N, S, P...>> { using type = S; };

        // The schema type a Scalar<Name, V> wire-param carries (``V`` — a concrete
        // scalar type, or a ``ScalarVar`` for a generic node).
        template <typename T> struct scalar_param_schema;
        template <fixed_string N, typename V> struct scalar_param_schema<Scalar<N, V>> { using type = V; };

        // The value type of a wiring scalar argument (a plain value, or a forwarded
        // ``Scalar<>`` selector whose value is unpacked).
        template <typename A> struct arg_value_type { using type = A; };
        template <fixed_string N, typename V> struct arg_value_type<Scalar<N, V>> { using type = V; };

        template <typename T> struct is_scalar_var : std::false_type {};
        template <fixed_string N, typename... C> struct is_scalar_var<ScalarVar<N, C...>> : std::true_type {};

        template <typename T>
        concept has_resolve_default_types = requires(ResolutionMap &resolution) {
            T::resolve_default_types(resolution);
        };

        template <typename T>
        struct dereferenced_static_schema
        {
            using type = T;
        };
        template <typename T>
        using dereferenced_static_schema_t = typename dereferenced_static_schema<T>::type;

        template <typename TSchema>
        struct dereferenced_static_schema<REF<TSchema>>
        {
            using type = dereferenced_static_schema_t<TSchema>;
        };
        template <typename TKey, typename TValueSchema>
        struct dereferenced_static_schema<TSD<TKey, TValueSchema>>
        {
            using type = TSD<TKey, dereferenced_static_schema_t<TValueSchema>>;
        };
        template <typename TElementSchema, auto FixedSize>
        struct dereferenced_static_schema<TSL<TElementSchema, FixedSize>>
        {
            using type = TSL<dereferenced_static_schema_t<TElementSchema>, FixedSize>;
        };
        template <typename TElementSchema>
        struct dereferenced_static_schema<Args<TElementSchema>>
        {
            using type = Args<dereferenced_static_schema_t<TElementSchema>>;
        };
        template <fixed_string Name, typename TSchema>
        struct dereferenced_field
        {
            using type = Field<Name, dereferenced_static_schema_t<TSchema>>;
        };
        template <typename TField>
        struct dereferenced_static_field;
        template <fixed_string Name, typename TSchema>
        struct dereferenced_static_field<Field<Name, TSchema>> : dereferenced_field<Name, TSchema>
        {
        };
        template <typename... TFields>
        struct dereferenced_static_schema<UnNamedTSB<TFields...>>
        {
            using type = UnNamedTSB<typename dereferenced_static_field<TFields>::type...>;
        };
        template <typename... TFields>
        struct dereferenced_static_schema<Kwargs<TFields...>>
        {
            using type = Kwargs<typename dereferenced_static_field<TFields>::type...>;
        };
        template <fixed_string Name, typename... TFields>
        struct dereferenced_static_schema<TSB<Name, TFields...>>
        {
            using type = TSB<Name, typename dereferenced_static_field<TFields>::type...>;
        };

        template <auto Lhs, auto Rhs>
        [[nodiscard]] consteval bool static_size_equivalent()
        {
            using lhs = static_schema_detail::size_parameter_descriptor<Lhs>;
            using rhs = static_schema_detail::size_parameter_descriptor<Rhs>;
            if constexpr (lhs::is_concrete() && rhs::is_concrete())
            {
                return lhs::concrete_size() == rhs::concrete_size();
            }
            else if constexpr (!lhs::is_concrete() && !rhs::is_concrete())
            {
                return lhs::name() == rhs::name();
            }
            else { return false; }
        }

        template <typename Lhs, typename Rhs>
        struct static_schema_equivalent : std::bool_constant<std::is_same_v<Lhs, Rhs>>
        {
        };

        template <typename LhsElement, auto LhsSize, typename RhsElement, auto RhsSize>
        struct static_schema_equivalent<TSL<LhsElement, LhsSize>, TSL<RhsElement, RhsSize>>
            : std::bool_constant<static_schema_equivalent<LhsElement, RhsElement>::value &&
                                 static_size_equivalent<LhsSize, RhsSize>()>
        {
        };

        template <typename LhsKey, typename LhsValue, typename RhsKey, typename RhsValue>
        struct static_schema_equivalent<TSD<LhsKey, LhsValue>, TSD<RhsKey, RhsValue>>
            : std::bool_constant<std::is_same_v<LhsKey, RhsKey> &&
                                 static_schema_equivalent<LhsValue, RhsValue>::value>
        {
        };

        template <typename InputSchema, typename OutputSchema>
        inline constexpr bool statically_accepts_output_v =
            std::is_same_v<InputSchema, SIGNAL> ||
            static_schema_equivalent<dereferenced_static_schema_t<InputSchema>,
                                     dereferenced_static_schema_t<OutputSchema>>::value;

        [[nodiscard]] inline bool input_accepts_output_schema(const TSValueTypeMetaData *input_schema,
                                                              const TSValueTypeMetaData *output_schema)
        {
            if (input_schema == nullptr || output_schema == nullptr) { return false; }
            if (input_schema->kind == TSTypeKind::SIGNAL) { return true; }

            auto &registry = TypeRegistry::instance();
            const auto *input = registry.dereference(input_schema);
            const auto *output = registry.dereference(output_schema);
            if (time_series_schema_equivalent(input, output)) { return true; }
            return input != nullptr && output != nullptr && input->kind == TSTypeKind::TS &&
                   output->kind == TSTypeKind::TS && input->value_schema != nullptr &&
                   output->value_schema != nullptr && input->value_schema->is_named_bundle() &&
                   output->value_schema->is_named_bundle() &&
                   registry.bundle_is_a(output->value_schema, input->value_schema);
        }

        template <typename OutSchema>
        void validate_port_cast_schema(const TSValueTypeMetaData *source_schema)
        {
            static_assert(!std::is_void_v<OutSchema>, "Port::as<Schema>() requires a concrete output schema");
            const auto *target_schema = schema_descriptor<OutSchema>::ts_meta();
            if (!input_accepts_output_schema(target_schema, source_schema))
            {
                throw std::logic_error("Port::as<Schema>: runtime port schema does not match the requested schema");
            }
        }

        [[nodiscard]] inline const TSValueTypeMetaData *structural_target_schema_for_input(
            const TSValueTypeMetaData *input_schema)
        {
            if (input_schema == nullptr)
            {
                throw std::logic_error("wire<T>: structural initializer requires an input schema");
            }
            return input_schema->kind == TSTypeKind::REF ? input_schema->referenced_ts() : input_schema;
        }

        [[nodiscard]] inline WiringPortRef structural_source_for_input_schema(
            const TSValueTypeMetaData *input_schema, const WiringStructuralSourceArg &arg)
        {
            const TSValueTypeMetaData *source_schema = structural_target_schema_for_input(input_schema);
            if (source_schema == nullptr)
            {
                throw std::logic_error("wire<T>: structural initializer target schema is unresolved");
            }

            switch (source_schema->kind)
            {
                case TSTypeKind::TSL:
                {
                    if (source_schema->fixed_size() == 0)
                    {
                        throw std::logic_error("wire<T>: structural initializer requires a fixed-size TSL input");
                    }
                    if (arg.children.size() != source_schema->fixed_size())
                    {
                        throw std::logic_error(
                            "wire<T>: structural initializer child count does not match the TSL input schema");
                    }
                    const auto *element_schema = source_schema->element_ts();
                    for (const WiringPortRef &child : arg.children)
                    {
                        if (!input_accepts_output_schema(element_schema, child.schema))
                        {
                            throw std::logic_error(
                                "wire<T>: structural initializer child schema does not match the TSL element schema");
                        }
                    }
                    break;
                }

                case TSTypeKind::TSB:
                {
                    if (arg.children.size() != source_schema->field_count())
                    {
                        throw std::logic_error(
                            "wire<T>: structural initializer child count does not match the TSB input schema");
                    }
                    for (std::size_t index = 0; index < arg.children.size(); ++index)
                    {
                        const auto *field_schema = source_schema->fields()[index].type;
                        if (!input_accepts_output_schema(field_schema, arg.children[index].schema))
                        {
                            throw std::logic_error(
                                "wire<T>: structural initializer child schema does not match the TSB field schema");
                        }
                    }
                    break;
                }

                default:
                    throw std::logic_error("wire<T>: structural initializer requires a TSL or TSB input schema");
            }

            return WiringPortRef::structural_source(source_schema, arg.children);
        }

        [[nodiscard]] inline std::size_t tsb_field_index_for_name(const TSValueTypeMetaData &schema,
                                                                  std::string_view           name)
        {
            for (std::size_t index = 0; index < schema.field_count(); ++index)
            {
                const auto &field      = schema.fields()[index];
                const auto  field_name = field.name != nullptr ? std::string_view{field.name} : std::string_view{};
                if (field_name == name) { return index; }
            }
            throw std::logic_error("wire<T>: named structural initializer field does not exist on the TSB schema");
        }

        [[nodiscard]] inline WiringPortRef structural_source_for_input_schema(
            const TSValueTypeMetaData *input_schema, const WiringNamedStructuralSourceArg &arg)
        {
            const TSValueTypeMetaData *source_schema = structural_target_schema_for_input(input_schema);
            if (source_schema == nullptr)
            {
                throw std::logic_error("wire<T>: named structural initializer target schema is unresolved");
            }
            if (source_schema->kind != TSTypeKind::TSB)
            {
                throw std::logic_error("wire<T>: named structural initializer requires a TSB input schema");
            }

            std::vector<WiringPortRef> children(source_schema->field_count());
            std::vector<bool>          seen(source_schema->field_count(), false);
            for (const WiringNamedPortRef &field_ref : arg.fields)
            {
                const std::size_t index = tsb_field_index_for_name(*source_schema, field_ref.name);
                if (seen[index])
                {
                    throw std::logic_error("wire<T>: named structural initializer contains a duplicate TSB field");
                }

                const auto *field_schema = source_schema->fields()[index].type;
                if (!input_accepts_output_schema(field_schema, field_ref.source.schema))
                {
                    throw std::logic_error(
                        "wire<T>: named structural initializer child schema does not match the TSB field schema");
                }

                children[index] = field_ref.source;
                seen[index]     = true;
            }

            for (std::size_t index = 0; index < children.size(); ++index)
            {
                if (!seen[index]) { children[index] = WiringPortRef::null_source(source_schema->fields()[index].type); }
            }
            return WiringPortRef::structural_source(source_schema, std::move(children));
        }

        template <typename Pattern>
        struct structural_arg_schema_infer
        {
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringStructuralSourceArg &) noexcept
            {
                return nullptr;
            }
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringNamedStructuralSourceArg &) noexcept
            {
                return nullptr;
            }
        };

        template <typename ElementSchema, auto FixedSize>
        struct structural_arg_schema_infer<TSL<ElementSchema, FixedSize>>
        {
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringStructuralSourceArg &arg)
            {
                using size = static_schema_detail::size_parameter_descriptor<FixedSize>;
                if constexpr (size::is_concrete())
                {
                    if (size::concrete_size() != 0 && arg.children.size() != size::concrete_size()) { return nullptr; }
                }
                if (arg.children.empty() || arg.children.front().schema == nullptr) { return nullptr; }

                const TSValueTypeMetaData *element = arg.children.front().schema;
                for (const WiringPortRef &child : arg.children)
                {
                    if (!time_series_schema_equivalent(element, child.schema)) { return nullptr; }
                }
                const std::size_t fixed_size =
                    size::is_concrete() && size::concrete_size() != 0 ? size::concrete_size() : arg.children.size();
                return TypeRegistry::instance().tsl(element, fixed_size);
            }
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringNamedStructuralSourceArg &) noexcept
            {
                return nullptr;
            }
        };

        template <typename ElementSchema>
        struct structural_arg_schema_infer<Args<ElementSchema>>
            : structural_arg_schema_infer<TSL<ElementSchema, SIZE<"args_len">>>
        {
        };

        namespace structural_arg_detail
        {
            template <typename Field>
            [[nodiscard]] std::pair<std::string, const TSValueTypeMetaData *>
            inferred_tsb_field(const WiringStructuralSourceArg &arg, std::size_t index)
            {
                if (index >= arg.children.size() || arg.children[index].schema == nullptr)
                {
                    return {ts_field_descriptor<Field>::field_name(), nullptr};
                }
                return {ts_field_descriptor<Field>::field_name(), arg.children[index].schema};
            }

            template <typename... Fields, std::size_t... I>
            [[nodiscard]] std::vector<std::pair<std::string, const TSValueTypeMetaData *>>
            inferred_tsb_fields(const WiringStructuralSourceArg &arg, std::index_sequence<I...>)
            {
                return {inferred_tsb_field<Fields>(arg, I)...};
            }

            [[nodiscard]] inline std::vector<std::pair<std::string, const TSValueTypeMetaData *>>
            inferred_named_tsb_fields(const WiringNamedStructuralSourceArg &arg)
            {
                std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
                fields.reserve(arg.fields.size());
                for (const WiringNamedPortRef &field : arg.fields)
                {
                    if (field.source.schema != nullptr) { fields.emplace_back(field.name, field.source.schema); }
                }
                return fields;
            }
        }  // namespace structural_arg_detail

        template <typename... Fields>
        struct structural_arg_schema_infer<UnNamedTSB<Fields...>>
        {
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringStructuralSourceArg &arg)
            {
                if (arg.children.size() != sizeof...(Fields)) { return nullptr; }
                auto fields = structural_arg_detail::inferred_tsb_fields<Fields...>(
                    arg, std::index_sequence_for<Fields...>{});
                for (const auto &field : fields)
                {
                    if (field.second == nullptr) { return nullptr; }
                }
                return TypeRegistry::instance().un_named_tsb(fields);
            }
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringNamedStructuralSourceArg &arg)
            {
                auto fields = structural_arg_detail::inferred_named_tsb_fields(arg);
                if (fields.empty()) { return nullptr; }
                return TypeRegistry::instance().un_named_tsb(fields);
            }
        };

        template <fixed_string Name, typename... Fields>
        struct structural_arg_schema_infer<TSB<Name, Fields...>>
        {
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringStructuralSourceArg &arg)
            {
                if (arg.children.size() != sizeof...(Fields)) { return nullptr; }
                auto fields = structural_arg_detail::inferred_tsb_fields<Fields...>(
                    arg, std::index_sequence_for<Fields...>{});
                for (const auto &field : fields)
                {
                    if (field.second == nullptr) { return nullptr; }
                }
                return TypeRegistry::instance().tsb(Name.sv(), fields);
            }
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringNamedStructuralSourceArg &arg)
            {
                auto fields = structural_arg_detail::inferred_named_tsb_fields(arg);
                if (fields.empty()) { return nullptr; }
                return TypeRegistry::instance().tsb(Name.sv(), fields);
            }
        };

        template <fixed_string VarName, typename... TConstraints>
        struct structural_arg_schema_infer<UnNamedTSB<TsVar<VarName, TConstraints...>>>
        {
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringStructuralSourceArg &) noexcept
            {
                return nullptr;
            }

            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringNamedStructuralSourceArg &arg)
            {
                auto fields = structural_arg_detail::inferred_named_tsb_fields(arg);
                if (fields.empty()) { return nullptr; }
                return TypeRegistry::instance().un_named_tsb(fields);
            }
        };

        template <fixed_string Name, fixed_string VarName, typename... TConstraints>
        struct structural_arg_schema_infer<TSB<Name, TsVar<VarName, TConstraints...>>>
        {
            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringStructuralSourceArg &) noexcept
            {
                return nullptr;
            }

            [[nodiscard]] static const TSValueTypeMetaData *infer(const WiringNamedStructuralSourceArg &arg)
            {
                auto fields = structural_arg_detail::inferred_named_tsb_fields(arg);
                if (fields.empty()) { return nullptr; }
                return TypeRegistry::instance().tsb(Name.sv(), fields);
            }
        };

        template <typename... Fields>
        struct structural_arg_schema_infer<Kwargs<Fields...>> : structural_arg_schema_infer<UnNamedTSB<Fields...>>
        {
        };

        template <>
        struct structural_arg_schema_infer<Kwargs<>> : structural_arg_schema_infer<UnNamedTSB<TsVar<"kwargs">>>
        {
        };

        template <typename Schema>
        struct structural_arg_schema_infer<REF<Schema>> : structural_arg_schema_infer<Schema>
        {
        };

        [[nodiscard]] HGRAPH_EXPORT WiringPortRef adapt_source_for_input(
            Wiring &w, const TSValueTypeMetaData *input_schema, WiringPortRef source);

        // ---- context scopes (see *Contexts* in services.rst) ----
        // The wiring-time context stack lives on the OperatorRegistry singleton
        // (mesh-scope precedent); these free functions keep operator_dispatch.h
        // out of this header. `resolve` throws a precise wiring error when the
        // context is missing. A publication from an enclosing Wiring becomes
        // an implicit captured boundary input on the child.
        [[nodiscard]] HGRAPH_EXPORT WiringPortRef resolve_context_source(Wiring &w,
                                                                         std::string_view name);
        [[nodiscard]] HGRAPH_EXPORT bool has_context_source(const Wiring &w,
                                                            std::string_view name) noexcept;
        HGRAPH_EXPORT void push_context_source(const Wiring &w, std::string_view name,
                                               WiringPortRef port);
        HGRAPH_EXPORT void pop_context_source() noexcept;

        [[nodiscard]] inline TSEndpointSchema endpoint_for_source(const TSValueTypeMetaData *schema,
                                                                  const WiringPortRef       &source)
        {
            if (schema == nullptr) { throw std::logic_error("wire<T>: input endpoint source has no schema"); }
            if (!source.is_structural_source()) { return TSEndpointSchema::peered(schema); }

            switch (schema->kind)
            {
                case TSTypeKind::TSB:
                {
                    const auto &source_children = source.structural_children();
                    if (source_children.size() != schema->field_count())
                    {
                        throw std::logic_error(
                            "wire<T>: structural TSB source child count does not match the input schema");
                    }
                    std::vector<TSEndpointSchema> children;
                    children.reserve(schema->field_count());
                    for (std::size_t index = 0; index < schema->field_count(); ++index)
                    {
                        const auto *field_schema = schema->fields()[index].type;
                        children.push_back(endpoint_for_source(field_schema, source_children[index]));
                    }
                    return TSEndpointSchema::non_peered(schema, std::move(children));
                }

                case TSTypeKind::TSL:
                {
                    if (schema->fixed_size() == 0)
                    {
                        throw std::logic_error("wire<T>: structural TSL input endpoint requires a fixed-size TSL");
                    }
                    const auto &source_children = source.structural_children();
                    if (source_children.size() != schema->fixed_size())
                    {
                        throw std::logic_error(
                            "wire<T>: structural TSL source child count does not match the input schema");
                    }
                    std::vector<TSEndpointSchema> children;
                    children.reserve(schema->fixed_size());
                    for (std::size_t index = 0; index < schema->fixed_size(); ++index)
                    {
                        children.push_back(endpoint_for_source(schema->element_ts(), source_children[index]));
                    }
                    return TSEndpointSchema::non_peered(schema, std::move(children));
                }

                default:
                    throw std::logic_error("wire<T>: structural source requires a fixed structural input schema");
            }
        }

        [[nodiscard]] inline TSEndpointSchema input_endpoint_for_sources(const TSValueTypeMetaData       *input_schema,
                                                                         std::span<const WiringPortRef>  sources)
        {
            if (input_schema == nullptr)
            {
                if (!sources.empty()) { throw std::logic_error("wire<T>: sources supplied for a node with no inputs"); }
                return TSEndpointSchema{};
            }
            if (input_schema->kind != TSTypeKind::TSB)
            {
                throw std::logic_error("wire<T>: node input schema must be a TSB");
            }
            if (input_schema->field_count() != sources.size())
            {
                throw std::logic_error("wire<T>: source count does not match the node input schema");
            }

            std::vector<TSEndpointSchema> children;
            children.reserve(sources.size());
            for (std::size_t index = 0; index < sources.size(); ++index)
            {
                children.push_back(endpoint_for_source(input_schema->fields()[index].type, sources[index]));
            }
            return TSEndpointSchema::non_peered(input_schema, std::move(children));
        }

        // Drop the leading ``Wiring &`` from a ``compose`` parameter tuple.
        template <typename Tuple> struct drop_first;
        template <typename A0, typename... As> struct drop_first<std::tuple<A0, As...>> { using type = std::tuple<As...>; };

        // Coerce a wiring-time scalar argument to its underlying value of type V.
        // The argument may be a plain value (used directly) or a ``Scalar<Name, T>``
        // selector (its value is unpacked) — so a scalar received as a node/graph
        // parameter can be forwarded straight on, without an explicit ``.value()``.
        // The names need not match; only the value type must be convertible.
        template <typename V, typename Arg>
        [[nodiscard]] V coerce_scalar_value(Arg &&arg)
        {
            using A = std::remove_cvref_t<Arg>;
            if constexpr (static_node_detail::is_scalar_selector<A>::value)
            {
                static_assert(std::is_convertible_v<typename A::value_type, V>,
                              "wire/build_graph: the Scalar<> argument's value type does not match the target "
                              "Scalar<> type");
                return static_cast<V>(arg.value());
            }
            else
            {
                static_assert(std::is_convertible_v<A, V>,
                              "wire/build_graph: scalar argument is not convertible to the target Scalar<> type");
                return static_cast<V>(std::forward<Arg>(arg));
            }
        }

        template <typename Arg>
        [[nodiscard]] const ValueTypeMetaData *scalar_argument_meta(const Arg &arg)
        {
            using A = std::remove_cvref_t<Arg>;
            if constexpr (static_node_detail::is_scalar_selector<A>::value)
            {
                using V = typename arg_value_type<A>::type;
                if constexpr (std::is_same_v<V, Value>)
                {
                    return arg.value().schema();
                }
                else
                {
                    return scalar_descriptor<V>::value_meta();
                }
            }
            else if constexpr (std::is_same_v<A, Value>)
            {
                return arg.schema();
            }
            else
            {
                return scalar_descriptor<A>::value_meta();
            }
        }

        // Build a graph ``compose`` ``Scalar<Name, T>`` parameter from a wiring
        // argument (a plain value or a ``Scalar<>`` selector to unpack).
        template <typename ScalarParam, typename Arg>
        [[nodiscard]] ScalarParam make_scalar_param(Arg &&arg)
        {
            return ScalarParam{coerce_scalar_value<typename ScalarParam::value_type>(std::forward<Arg>(arg))};
        }

        template <typename ParamsTuple, std::size_t... I>
        [[nodiscard]] consteval bool all_scalar_params(std::index_sequence<I...>)
        {
            return (true && ... &&
                    static_node_detail::is_scalar_selector<std::tuple_element_t<I, ParamsTuple>>::value);
        }

        template <std::size_t ParamIndex, typename ParamsTuple, typename ArgsTuple, typename DefaultsTuple>
        [[nodiscard]] auto make_bound_scalar_param(const ArgsTuple &args, const DefaultsTuple &defaults)
            -> std::tuple_element_t<ParamIndex, ParamsTuple>
        {
            using Param = std::tuple_element_t<ParamIndex, ParamsTuple>;
            constexpr std::size_t arg_index =
                call_args_detail::bound_arg_index<ParamIndex, ParamsTuple, ArgsTuple>();
            constexpr std::size_t default_index =
                call_args_detail::default_arg_index<ParamIndex, ParamsTuple, DefaultsTuple>();
            if constexpr (arg_index == call_args_detail::npos)
            {
                if constexpr (default_index == call_args_detail::npos)
                {
                    throw std::invalid_argument("build_graph<G>: missing scalar argument '" +
                                                std::string{Param::field_name.sv()} + "'");
                }
                else
                {
                    return make_scalar_param<Param>(call_args_detail::payload_at<default_index>(defaults));
                }
            }
            else
            {
                return make_scalar_param<Param>(call_args_detail::payload_at<arg_index>(args));
            }
        }

        // Build the owned ``Value`` for one scalar field of a *generic* node's
        // configuration bundle. For a concrete ``Scalar<Name, V>`` the field type is
        // ``V``; for a var ``Scalar<Name, ScalarVar<...>>`` it is the supplied
        // argument's own value type (which also pins the scalar variable).
        template <typename P, typename Arg>
        [[nodiscard]] Value make_scalar_field(Arg &&arg)
        {
            using ST = typename scalar_param_schema<P>::type;
            if constexpr (is_scalar_var<ST>::value)
            {
                using VT = typename arg_value_type<std::remove_cvref_t<Arg>>::type;
                if constexpr (std::is_same_v<VT, Value>)
                {
                    return coerce_scalar_value<VT>(std::forward<Arg>(arg));
                }
                else
                {
                    return Value{coerce_scalar_value<VT>(std::forward<Arg>(arg))};
                }
            }
            else
            {
                return Value{coerce_scalar_value<ST>(std::forward<Arg>(arg))};
            }
        }

        // Transform one ``wire<G>`` argument into its ``compose`` parameter ``P``:
        // pass a ``Port`` straight through (schema-checked), or wrap a plain value
        // into the ``Scalar<>`` parameter — the sub-graph mirror of how ``wire<T>``
        // handles a node's In ports and Scalar arguments.
        template <typename P, typename Arg>
        [[nodiscard]] auto make_compose_arg(Wiring &w, Arg &&arg)
        {
            if constexpr (is_port<P>::value)
            {
                using A = std::remove_cvref_t<Arg>;
                static_assert(is_port<A>::value || is_structural_source_arg<A>::value,
                              "wire<G>: a time-series input expects a Port argument or structural initializer");
                if constexpr (is_structural_source_arg<A>::value)
                {
                    static_assert(!is_erased_port<P>::value,
                                  "wire<G>: structural initializer requires a typed sub-graph Port parameter");
                    const auto *expected = schema_descriptor<typename P::schema>::ts_meta();
                    WiringPortRef ref = structural_source_for_input_schema(expected, arg);
                    return P{w, adapt_source_for_input(w, expected, std::move(ref))};
                }
                else if constexpr (is_erased_port<P>::value)
                {
                    return P{w, arg.erased()};
                }
                else if constexpr (is_erased_port<A>::value)
                {
                    const auto *expected = schema_descriptor<typename P::schema>::ts_meta();
                    if (!input_accepts_output_schema(expected, arg.erased().schema))
                    {
                        throw std::logic_error(
                            "wire<G>: erased input port schema does not match the sub-graph's time-series input");
                    }
                    return P{w, arg.erased()};
                }
                else
                {
                    static_assert(statically_accepts_output_v<typename P::schema, typename A::schema>,
                                  "wire<G>: input port schema does not match the sub-graph's time-series input");
                    const auto *expected = schema_descriptor<typename P::schema>::ts_meta();
                    return P{w, adapt_source_for_input(w, expected, arg.erased())};
                }
            }
            else
            {
                return make_scalar_param<P>(std::forward<Arg>(arg));
            }
        }

        template <std::size_t ParamIndex, typename ParamsTuple, typename ArgsTuple, typename DefaultsTuple>
        [[nodiscard]] auto make_bound_compose_arg(Wiring &w, const ArgsTuple &args, const DefaultsTuple &defaults)
            -> std::tuple_element_t<ParamIndex, ParamsTuple>
        {
            using P = std::tuple_element_t<ParamIndex, ParamsTuple>;
            constexpr std::size_t arg_index =
                call_args_detail::bound_arg_index<ParamIndex, ParamsTuple, ArgsTuple>();
            constexpr std::size_t default_index =
                call_args_detail::default_arg_index<ParamIndex, ParamsTuple, DefaultsTuple>();
            if constexpr (arg_index == call_args_detail::npos)
            {
                if constexpr (default_index == call_args_detail::npos)
                {
                    throw std::invalid_argument("wire<G>: missing argument " +
                                                call_args_detail::missing_parameter_name(
                                                    call_args_detail::parameter_name<P>(), ParamIndex));
                }
                else
                {
                    return make_compose_arg<P>(w, call_args_detail::payload_at<default_index>(defaults));
                }
            }
            else
            {
                return make_compose_arg<P>(w, call_args_detail::payload_at<arg_index>(args));
            }
        }

        enum class node_collection_pack_kind : std::uint8_t
        {
            none,
            tsl,
            tsb,
        };

        template <typename TSchema>
        struct node_collection_pack_kind_of
            : std::integral_constant<node_collection_pack_kind, node_collection_pack_kind::none>
        {
        };
        template <typename TElementSchema>
        struct node_collection_pack_kind_of<Args<TElementSchema>>
            : std::integral_constant<node_collection_pack_kind, node_collection_pack_kind::tsl>
        {
        };
        template <typename... TFields>
        struct node_collection_pack_kind_of<Kwargs<TFields...>>
            : std::integral_constant<node_collection_pack_kind, node_collection_pack_kind::tsb>
        {
        };
        template <typename TSchema>
        struct node_collection_pack_kind_of<REF<TSchema>> : node_collection_pack_kind_of<TSchema>
        {
        };

        template <typename P>
        [[nodiscard]] consteval node_collection_pack_kind input_pack_kind()
        {
            if constexpr (static_node_detail::is_input_selector<P>::value)
            {
                return node_collection_pack_kind_of<typename in_param_schema<P>::type>::value;
            }
            else
            {
                return node_collection_pack_kind::none;
            }
        }

        template <typename ParamsTuple, std::size_t... I>
        [[nodiscard]] consteval std::size_t single_tail_collection_input_index(std::index_sequence<I...>)
        {
            std::size_t found      = call_args_detail::npos;
            std::size_t found_count = 0;
            bool        input_after = false;

            (
                [&] {
                    using P = std::tuple_element_t<I, ParamsTuple>;
                    if constexpr (static_node_detail::is_input_selector<P>::value)
                    {
                        [[maybe_unused]] constexpr node_collection_pack_kind kind = input_pack_kind<P>();
                        if constexpr (kind != node_collection_pack_kind::none)
                        {
                            if (found_count == 0) { found = I; }
                            ++found_count;
                        }
                        else if (found_count != 0)
                        {
                            input_after = true;
                        }
                    }
                }(),
                ...);

            return found_count == 1 && !input_after ? found : call_args_detail::npos;
        }

        template <typename ParamsTuple>
        [[nodiscard]] consteval std::size_t single_tail_collection_input_index()
        {
            return single_tail_collection_input_index<ParamsTuple>(
                std::make_index_sequence<std::tuple_size_v<ParamsTuple>>{});
        }

        template <typename T>
        struct port_static_schema
        {
            using type = void;
        };
        template <typename S>
        struct port_static_schema<Port<S>>
        {
            using type = S;
        };
        template <fixed_string N, typename S>
        struct port_static_schema<NamedPort<N, S>>
        {
            using type = S;
        };

        template <typename A>
        [[nodiscard]] consteval node_collection_pack_kind port_pack_kind()
        {
            using Schema = typename port_static_schema<std::remove_cvref_t<A>>::type;
            if constexpr (std::is_void_v<Schema>) { return node_collection_pack_kind::none; }
            else { return node_collection_pack_kind_of<Schema>::value; }
        }

        template <std::size_t I, typename ArgsTuple, std::size_t... J>
        [[nodiscard]] consteval std::size_t positional_ordinal_impl(std::index_sequence<J...>)
        {
            std::size_t ordinal = 0;
            ((ordinal += call_args_detail::is_named_arg_v<std::tuple_element_t<J, ArgsTuple>> ? 0U : 1U), ...);
            return ordinal;
        }

        template <std::size_t I, typename ArgsTuple>
        [[nodiscard]] consteval std::size_t positional_ordinal()
        {
            return positional_ordinal_impl<I, ArgsTuple>(std::make_index_sequence<I>{});
        }

        template <typename ParamsTuple, typename Arg, std::size_t... I>
        [[nodiscard]] consteval bool static_named_arg_matches_any_parameter(std::index_sequence<I...>)
        {
            using A = std::remove_cvref_t<Arg>;
            if constexpr (!call_args_detail::is_static_named_arg_v<A>) { return false; }
            else
            {
                return (false || ... || call_args_detail::static_named_arg_matches_parameter<I, ParamsTuple, A>());
            }
        }

        template <typename ParamsTuple, typename Arg>
        [[nodiscard]] consteval bool static_named_arg_matches_any_parameter()
        {
            return static_named_arg_matches_any_parameter<ParamsTuple, Arg>(
                std::make_index_sequence<std::tuple_size_v<ParamsTuple>>{});
        }

        template <node_collection_pack_kind InputKind, typename A>
        [[nodiscard]] consteval bool sole_port_candidate_is_direct_collection_input()
        {
            if constexpr (!is_port<std::remove_cvref_t<A>>::value) { return false; }
            else if constexpr (is_erased_port<std::remove_cvref_t<A>>::value)
            {
                return true;
            }
            else
            {
                constexpr node_collection_pack_kind arg_kind = port_pack_kind<A>();
                return arg_kind == InputKind;
            }
        }

        template <typename ParamsTuple, std::size_t PackIndex, typename ArgsTuple, std::size_t... I>
        [[nodiscard]] consteval std::size_t positional_port_pack_candidate_count(std::index_sequence<I...>)
        {
            std::size_t count = 0;
            (
                [&] {
                    using A0 = std::remove_cvref_t<std::tuple_element_t<I, ArgsTuple>>;
                    if constexpr (!call_args_detail::is_named_arg_v<A0>)
                    {
                        [[maybe_unused]] constexpr std::size_t ordinal = positional_ordinal<I, ArgsTuple>();
                        if constexpr (ordinal >= PackIndex)
                        {
                            using A = call_args_detail::payload_t<A0>;
                            if constexpr (is_port<A>::value) { ++count; }
                        }
                    }
                }(),
                ...);
            return count;
        }

        template <typename ParamsTuple, std::size_t PackIndex, typename ArgsTuple, std::size_t... I>
        [[nodiscard]] consteval bool sole_positional_port_candidate_is_direct(std::index_sequence<I...>)
        {
            constexpr auto input_kind = input_pack_kind<std::tuple_element_t<PackIndex, ParamsTuple>>();
            bool           direct     = false;
            (
                [&] {
                    using A0 = std::remove_cvref_t<std::tuple_element_t<I, ArgsTuple>>;
                    if constexpr (!call_args_detail::is_named_arg_v<A0>)
                    {
                        constexpr std::size_t ordinal = positional_ordinal<I, ArgsTuple>();
                        if constexpr (ordinal >= PackIndex)
                        {
                            using A = call_args_detail::payload_t<A0>;
                            if constexpr (is_port<A>::value)
                            {
                                if constexpr (ordinal == PackIndex &&
                                              sole_port_candidate_is_direct_collection_input<input_kind, A>())
                                {
                                    direct = true;
                                }
                            }
                        }
                    }
                }(),
                ...);
            return direct;
        }

        template <typename ParamsTuple, std::size_t PackIndex, typename ArgsTuple, std::size_t... I>
        [[nodiscard]] consteval bool has_named_collection_pack_fields(std::index_sequence<I...>)
        {
            constexpr auto input_kind = input_pack_kind<std::tuple_element_t<PackIndex, ParamsTuple>>();
            if constexpr (input_kind != node_collection_pack_kind::tsb)
            {
                return false;
            }
            else
            {
                return (false || ... ||
                        []<std::size_t ArgIndex>() consteval {
                            using A0 = std::remove_cvref_t<std::tuple_element_t<ArgIndex, ArgsTuple>>;
                            if constexpr (!call_args_detail::is_static_named_arg_v<A0>) { return false; }
                            else if constexpr (static_named_arg_matches_any_parameter<ParamsTuple, A0>()) { return false; }
                            else
                            {
                                using A = call_args_detail::payload_t<A0>;
                                return is_port<A>::value;
                            }
                        }.template operator()<I>());
            }
        }

        template <typename ParamsTuple, std::size_t PackIndex, typename ArgsTuple>
        [[nodiscard]] consteval bool node_collection_pack_needed()
        {
            constexpr std::size_t arg_count = std::tuple_size_v<ArgsTuple>;
            if constexpr (PackIndex == call_args_detail::npos) { return false; }
            else if constexpr (has_named_collection_pack_fields<ParamsTuple, PackIndex, ArgsTuple>(
                                   std::make_index_sequence<arg_count>{}))
            {
                return true;
            }
            else
            {
                constexpr std::size_t port_candidates =
                    positional_port_pack_candidate_count<ParamsTuple, PackIndex, ArgsTuple>(
                        std::make_index_sequence<arg_count>{});
                if constexpr (port_candidates == 0) { return false; }
                else if constexpr (port_candidates == 1)
                {
                    return !sole_positional_port_candidate_is_direct<ParamsTuple, PackIndex, ArgsTuple>(
                        std::make_index_sequence<arg_count>{});
                }
                else
                {
                    return true;
                }
            }
        }

        [[nodiscard]] inline bool field_name_seen(std::span<const WiringNamedPortRef> fields, std::string_view name)
        {
            for (const WiringNamedPortRef &field : fields)
            {
                if (field.name == name) { return true; }
            }
            return false;
        }

        inline void append_unique_named_pack_field(std::vector<WiringNamedPortRef> &fields,
                                                   std::string_view                 name,
                                                   WiringPortRef                    source)
        {
            if (field_name_seen(std::span<const WiringNamedPortRef>{fields.data(), fields.size()}, name))
            {
                throw std::invalid_argument("wire<T>: packed TSB arguments contain a duplicate field '" +
                                            std::string{name} + "'");
            }
            fields.emplace_back(name, std::move(source));
        }

        template <typename ParamsTuple, std::size_t PackIndex, typename ArgsTuple, std::size_t... I>
        void validate_node_collection_pack_args(const ArgsTuple &args, std::index_sequence<I...>)
        {
            constexpr auto input_kind = input_pack_kind<std::tuple_element_t<PackIndex, ParamsTuple>>();
            bool           seen_named = false;
            bool           saw_packed = false;

            (
                [&] {
                    using A0 = std::remove_cvref_t<std::tuple_element_t<I, ArgsTuple>>;
                    [[maybe_unused]] const auto &argument = std::get<I>(args);
                    if constexpr (call_args_detail::is_named_arg_v<A0>)
                    {
                        seen_named = true;
                        if constexpr (!call_args_detail::is_static_named_arg_v<A0>)
                        {
                            throw std::invalid_argument(
                                "wire<T>: collection argument packing requires arg<\"name\">(...) keyword wrappers");
                        }
                        else if constexpr (!static_named_arg_matches_any_parameter<ParamsTuple, A0>())
                        {
                            if constexpr (input_kind != node_collection_pack_kind::tsb)
                            {
                                throw std::invalid_argument("wire<T>: unexpected keyword argument '" +
                                                            std::string{argument.name} + "'");
                            }
                            else
                            {
                                using A = call_args_detail::payload_t<A0>;
                                if constexpr (!is_port<A>::value)
                                {
                                    throw std::invalid_argument(
                                        "wire<T>: packed TSB keyword arguments must be time-series ports");
                                }
                                saw_packed = true;
                            }
                        }
                    }
                    else
                    {
                        if (seen_named)
                        {
                            throw std::invalid_argument("wire<T>: positional argument follows a named argument");
                        }

                        [[maybe_unused]] constexpr std::size_t ordinal =
                            positional_ordinal<I, std::remove_reference_t<ArgsTuple>>();
                        if constexpr (ordinal >= PackIndex)
                        {
                            using A = call_args_detail::payload_t<A0>;
                            if constexpr (is_port<A>::value)
                            {
                                saw_packed = true;
                            }
                            else if constexpr (is_structural_source_arg<A>::value)
                            {
                                if (saw_packed)
                                {
                                    throw std::invalid_argument(
                                        "wire<T>: cannot combine an explicit collection input with packed arguments");
                                }
                            }
                            else
                            {
                                throw std::invalid_argument(
                                    "wire<T>: positional scalar arguments after packed collection inputs must be named");
                            }
                        }
                    }
                }(),
                ...);
        }

        template <typename ParamsTuple, std::size_t PackIndex, typename ArgsTuple>
        void validate_node_collection_pack_args(const ArgsTuple &args)
        {
            validate_node_collection_pack_args<ParamsTuple, PackIndex>(
                args, std::make_index_sequence<std::tuple_size_v<std::remove_reference_t<ArgsTuple>>>{});
        }

        template <typename ParamsTuple, std::size_t PackIndex, std::size_t I, typename ArgsTuple>
        void append_tsl_pack_child(std::vector<WiringPortRef> &children, const ArgsTuple &args)
        {
            using A0 = std::remove_cvref_t<std::tuple_element_t<I, std::remove_reference_t<ArgsTuple>>>;
            if constexpr (!call_args_detail::is_named_arg_v<A0>)
            {
                constexpr std::size_t ordinal = positional_ordinal<I, std::remove_reference_t<ArgsTuple>>();
                if constexpr (ordinal >= PackIndex)
                {
                    using A = call_args_detail::payload_t<A0>;
                    if constexpr (is_port<A>::value)
                    {
                        children.push_back(call_args_detail::payload_at<I>(args).erased());
                    }
                }
            }
        }

        template <typename ParamsTuple, std::size_t PackIndex, typename ArgsTuple, std::size_t... I>
        [[nodiscard]] WiringStructuralSourceArg make_tsl_node_collection_arg(const ArgsTuple &args,
                                                                             std::index_sequence<I...>)
        {
            std::vector<WiringPortRef> children;
            children.reserve(sizeof...(I));
            (append_tsl_pack_child<ParamsTuple, PackIndex, I>(children, args), ...);
            return WiringStructuralSourceArg{std::move(children)};
        }

        template <typename ParamsTuple, std::size_t PackIndex, typename ArgsTuple>
        [[nodiscard]] WiringStructuralSourceArg make_tsl_node_collection_arg(const ArgsTuple &args)
        {
            return make_tsl_node_collection_arg<ParamsTuple, PackIndex>(
                args, std::make_index_sequence<std::tuple_size_v<std::remove_reference_t<ArgsTuple>>>{});
        }

        template <typename ParamsTuple, std::size_t PackIndex, std::size_t I, typename ArgsTuple>
        void append_tsb_pack_field(std::vector<WiringNamedPortRef> &fields,
                                   std::size_t                    &positional_field_count,
                                   const ArgsTuple                 &args)
        {
            using A0 = std::remove_cvref_t<std::tuple_element_t<I, std::remove_reference_t<ArgsTuple>>>;
            if constexpr (call_args_detail::is_named_arg_v<A0>)
            {
                if constexpr (call_args_detail::is_static_named_arg_v<A0> &&
                              !static_named_arg_matches_any_parameter<ParamsTuple, A0>())
                {
                    using A = call_args_detail::payload_t<A0>;
                    if constexpr (is_port<A>::value)
                    {
                        const auto &argument = std::get<I>(args);
                        append_unique_named_pack_field(fields, argument.name, argument.value.erased());
                    }
                }
            }
            else
            {
                constexpr std::size_t ordinal = positional_ordinal<I, std::remove_reference_t<ArgsTuple>>();
                if constexpr (ordinal >= PackIndex)
                {
                    using A = call_args_detail::payload_t<A0>;
                    if constexpr (is_port<A>::value)
                    {
                        ++positional_field_count;
                        append_unique_named_pack_field(fields, "_" + std::to_string(positional_field_count),
                                                       call_args_detail::payload_at<I>(args).erased());
                    }
                }
            }
        }

        template <typename ParamsTuple, std::size_t PackIndex, typename ArgsTuple, std::size_t... I>
        [[nodiscard]] WiringNamedStructuralSourceArg make_tsb_node_collection_arg(const ArgsTuple &args,
                                                                                  std::index_sequence<I...>)
        {
            std::vector<WiringNamedPortRef> fields;
            fields.reserve(sizeof...(I));
            std::size_t positional_field_count = 0;
            (append_tsb_pack_field<ParamsTuple, PackIndex, I>(fields, positional_field_count, args), ...);
            return WiringNamedStructuralSourceArg{std::move(fields)};
        }

        template <typename ParamsTuple, std::size_t PackIndex, typename ArgsTuple>
        [[nodiscard]] WiringNamedStructuralSourceArg make_tsb_node_collection_arg(const ArgsTuple &args)
        {
            return make_tsb_node_collection_arg<ParamsTuple, PackIndex>(
                args, std::make_index_sequence<std::tuple_size_v<std::remove_reference_t<ArgsTuple>>>{});
        }

        template <typename ParamsTuple, std::size_t PackIndex, std::size_t I, typename ArgsTuple>
        [[nodiscard]] consteval bool node_collection_arg_passes_through()
        {
            using A0 = std::remove_cvref_t<std::tuple_element_t<I, ArgsTuple>>;
            if constexpr (call_args_detail::is_named_arg_v<A0>)
            {
                if constexpr (call_args_detail::is_static_named_arg_v<A0> &&
                              !static_named_arg_matches_any_parameter<ParamsTuple, A0>())
                {
                    constexpr auto input_kind = input_pack_kind<std::tuple_element_t<PackIndex, ParamsTuple>>();
                    using A = call_args_detail::payload_t<A0>;
                    if constexpr (input_kind == node_collection_pack_kind::tsb && is_port<A>::value)
                    {
                        return false;
                    }
                }
                return true;
            }
            else
            {
                constexpr std::size_t ordinal = positional_ordinal<I, ArgsTuple>();
                if constexpr (ordinal < PackIndex) { return true; }
                else
                {
                    using A = call_args_detail::payload_t<A0>;
                    return is_structural_source_arg<A>::value;
                }
            }
        }

        template <typename ParamsTuple, std::size_t PackIndex, std::size_t I, typename ArgsTuple>
        [[nodiscard]] auto node_collection_passthrough_piece(const ArgsTuple &args)
        {
            if constexpr (node_collection_arg_passes_through<ParamsTuple, PackIndex, I,
                                                             std::remove_reference_t<ArgsTuple>>())
            {
                using A0 = std::remove_cvref_t<std::tuple_element_t<I, std::remove_reference_t<ArgsTuple>>>;
                return std::tuple<A0>{std::get<I>(args)};
            }
            else
            {
                return std::tuple<>{};
            }
        }

        template <typename ParamsTuple, std::size_t PackIndex, typename ArgsTuple, std::size_t... I>
        [[nodiscard]] auto node_collection_passthrough_args(const ArgsTuple &args, std::index_sequence<I...>)
        {
            return std::tuple_cat(node_collection_passthrough_piece<ParamsTuple, PackIndex, I>(args)...);
        }

        template <typename ParamsTuple, std::size_t PackIndex, typename ArgsTuple>
        [[nodiscard]] auto node_collection_passthrough_args(const ArgsTuple &args)
        {
            return node_collection_passthrough_args<ParamsTuple, PackIndex>(
                args, std::make_index_sequence<std::tuple_size_v<std::remove_reference_t<ArgsTuple>>>{});
        }
    }  // namespace graph_wiring_detail

    template <typename Schema>
    template <typename OutSchema>
    [[nodiscard]] Port<OutSchema> Port<Schema>::as() const
    {
        graph_wiring_detail::validate_port_cast_schema<OutSchema>(ref_.schema);
        return Port<OutSchema>{wiring_, ref_};
    }

    template <typename OutSchema>
    [[nodiscard]] Port<OutSchema> Port<void>::as() const
    {
        graph_wiring_detail::validate_port_cast_schema<OutSchema>(ref_.schema);
        return Port<OutSchema>{wiring_, ref_};
    }

    namespace graph_wiring_detail
    {
        template <typename X, typename OutSchema, typename... Args>
        struct wire_customization
        {
            static constexpr bool enabled = false;
        };

        template <typename X, typename OutSchema = void, typename... Args>
        auto wire_static_node_normal(Wiring &w, const Args &...args)
        {
            using signature   = StaticNodeSignature<X>;
            using wire_params = typename signature::wire_param_types;
            static_assert(sizeof...(Args) <= std::tuple_size_v<wire_params>,
                          "wire<T>: too many arguments for the node's In + Scalar parameters");

            auto arg_tuple    = std::forward_as_tuple(args...);
            auto default_args = call_args_detail::default_args_for<X>();
            call_args_detail::validate_call_args<wire_params>("wire<T>", arg_tuple, default_args);

            if constexpr (signature::is_generic())
            {
                // ---------- generic node: resolve type variables at wiring time ----------
                ResolutionMap map;

                // (a) explicit output schema, for a source-side variable (e.g. replay).
                if constexpr (!std::is_void_v<OutSchema>)
                {
                    ts_unifier<typename signature::output_schema_type>::unify(ts_type<OutSchema>(), map);
                }

                // (b) bind from connected input ports + infer scalar variables from values.
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    (
                        [&] {
                            using P = std::tuple_element_t<I, wire_params>;
                            [[maybe_unused]] constexpr std::size_t arg_index =
                                call_args_detail::bound_arg_index<I, wire_params, decltype(arg_tuple)>();
                            [[maybe_unused]] constexpr std::size_t default_index =
                                call_args_detail::default_arg_index<I, wire_params, decltype(default_args)>();
                            if constexpr (static_node_detail::is_input_selector<P>::value &&
                                          call_args_detail::auto_context_param_v<P> &&
                                          arg_index == call_args_detail::npos)
                            {
                                // Context-bound input with no keyword override:
                                // unify from the published port's schema.
                                ts_unifier<typename graph_wiring_detail::in_param_schema<P>::type>::unify(
                                    graph_wiring_detail::resolve_context_source(w, P::field_name.sv()).schema,
                                    map);
                            }
                            else if constexpr (arg_index != call_args_detail::npos ||
                                               default_index != call_args_detail::npos)
                            {
                                if constexpr (static_node_detail::is_input_selector<P>::value)
                                {
                                    if constexpr (arg_index != call_args_detail::npos)
                                    {
                                        using A0 = std::remove_cvref_t<std::tuple_element_t<
                                            arg_index, std::remove_reference_t<decltype(arg_tuple)>>>;
                                        using A = call_args_detail::payload_t<A0>;
                                        static_assert(graph_wiring_detail::is_port<A>::value ||
                                                          graph_wiring_detail::is_structural_source_arg<A>::value,
                                                      "wire<T>: a time-series input expects a Port argument or structural initializer");
                                        if constexpr (graph_wiring_detail::is_structural_source_arg<A>::value)
                                        {
                                            using Expected = typename graph_wiring_detail::in_param_schema<P>::type;
                                            const auto *inferred =
                                                graph_wiring_detail::structural_arg_schema_infer<Expected>::infer(
                                                    call_args_detail::payload_at<arg_index>(arg_tuple));
                                            if (inferred != nullptr) { ts_unifier<Expected>::unify(inferred, map); }
                                        }
                                        else
                                        {
                                            ts_unifier<typename graph_wiring_detail::in_param_schema<P>::type>::unify(
                                                call_args_detail::payload_at<arg_index>(arg_tuple).erased().schema, map);
                                        }
                                    }
                                }
                                else if constexpr (static_node_detail::is_scalar_selector<P>::value)
                                {
                                    using ST = typename graph_wiring_detail::scalar_param_schema<P>::type;
                                    if constexpr (arg_index != call_args_detail::npos)
                                    {
                                        scalar_unifier<ST>::unify(graph_wiring_detail::scalar_argument_meta(
                                            call_args_detail::payload_at<arg_index>(arg_tuple)), map);
                                    }
                                    else
                                    {
                                        scalar_unifier<ST>::unify(graph_wiring_detail::scalar_argument_meta(
                                            call_args_detail::payload_at<default_index>(default_args)), map);
                                    }
                                }
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<std::tuple_size_v<wire_params>>{});

                if constexpr (graph_wiring_detail::has_resolve_default_types<X>)
                {
                    X::resolve_default_types(map);
                }

                // Input ports (the In positions): validate against the resolved input schema.
                std::vector<WiringPortRef> inputs;
                inputs.reserve(signature::input_count());
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    (
                        [&] {
                            using P = std::tuple_element_t<I, wire_params>;
                            if constexpr (static_node_detail::is_input_selector<P>::value)
                            {
                                constexpr std::size_t arg_index =
                                    call_args_detail::bound_arg_index<I, wire_params, decltype(arg_tuple)>();
                                if constexpr (arg_index != call_args_detail::npos)
                                {
                                    using A0 = std::remove_cvref_t<std::tuple_element_t<
                                        arg_index, std::remove_reference_t<decltype(arg_tuple)>>>;
                                    using A = call_args_detail::payload_t<A0>;
                                    const auto *expected =
                                        ts_resolver<typename graph_wiring_detail::in_param_schema<P>::type>::resolve(map);
                                    if constexpr (graph_wiring_detail::is_structural_source_arg<A>::value)
                                    {
                                        WiringPortRef ref = graph_wiring_detail::structural_source_for_input_schema(
                                            expected, call_args_detail::payload_at<arg_index>(arg_tuple));
                                        inputs.push_back(
                                            graph_wiring_detail::adapt_source_for_input(w, expected, std::move(ref)));
                                    }
                                    else
                                    {
                                        WiringPortRef ref = call_args_detail::payload_at<arg_index>(arg_tuple).erased();
                                        if (!graph_wiring_detail::input_accepts_output_schema(expected, ref.schema))
                                        {
                                            throw std::logic_error(
                                                "wire<T>: input port schema does not match the node's time-series input");
                                        }
                                        inputs.push_back(
                                            graph_wiring_detail::adapt_source_for_input(w, expected, std::move(ref)));
                                    }
                                }
                                else if constexpr (call_args_detail::auto_context_param_v<P>)
                                {
                                    // Context-bound input: source from the nearest
                                    // enclosing context::scope with this name.
                                    const auto *expected =
                                        ts_resolver<typename graph_wiring_detail::in_param_schema<P>::type>::resolve(map);
                                    WiringPortRef ref =
                                        graph_wiring_detail::resolve_context_source(w, P::field_name.sv());
                                    if (!graph_wiring_detail::input_accepts_output_schema(expected, ref.schema))
                                    {
                                        throw std::logic_error(
                                            "wire<T>: context '" + std::string{P::field_name.sv()} +
                                            "' schema does not match the node's Context<> input");
                                    }
                                    inputs.push_back(
                                        graph_wiring_detail::adapt_source_for_input(w, expected, std::move(ref)));
                                }
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<std::tuple_size_v<wire_params>>{});

                // Resolved scalar configuration: assembled from owned field Values so
                // a var field's (now-resolved) type is honoured.
                Value scalars;
                if constexpr (signature::scalar_count() > 0)
                {
                    const auto binding = value_type_for_wiring(signature::scalar_schema(map));
                    BundleBuilder bundle{binding};
                    [&]<std::size_t... I>(std::index_sequence<I...>) {
                        (
                            [&] {
                                using P = std::tuple_element_t<I, wire_params>;
                                if constexpr (static_node_detail::is_scalar_selector<P>::value)
                                {
                                    [[maybe_unused]] constexpr std::size_t arg_index =
                                        call_args_detail::bound_arg_index<I, wire_params, decltype(arg_tuple)>();
                                    [[maybe_unused]] constexpr std::size_t default_index =
                                        call_args_detail::default_arg_index<I, wire_params, decltype(default_args)>();
                                    if constexpr (arg_index != call_args_detail::npos)
                                    {
                                        Value field = graph_wiring_detail::make_scalar_field<P>(
                                            call_args_detail::payload_at<arg_index>(arg_tuple));
                                        bundle.set(P::field_name.sv(), std::move(field));
                                    }
                                    else if constexpr (default_index != call_args_detail::npos)
                                    {
                                        Value field = graph_wiring_detail::make_scalar_field<P>(
                                            call_args_detail::payload_at<default_index>(default_args));
                                        bundle.set(P::field_name.sv(), std::move(field));
                                    }
                                }
                            }(),
                            ...);
                    }(std::make_index_sequence<std::tuple_size_v<wire_params>>{});
                    scalars = bundle.build();
                }

                NodeBuilder nb;
                nb.implementation<X>(map);
                nb.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                    nb.type().schema() != nullptr ? nb.type().schema()->input_schema : nullptr,
                    std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                WiringPortRef out =
                    w.add_node(std::type_index(typeid(X)), std::move(nb), inputs, std::move(scalars));

                if constexpr (signature::has_output())
                {
                    if constexpr (!std::is_void_v<OutSchema>)
                    {
                        return Port<OutSchema>{w, std::move(out)};         // typed: explicit output schema
                    }
                    else
                    {
                        return Port<void>{w, std::move(out)};              // erased: runtime-resolved
                    }
                }
            }
            else
            {
                // ---------- concrete node ----------
                static_assert(std::is_void_v<OutSchema>,
                              "wire<T, OutSchema>: an explicit output schema applies only to generic nodes");

                // Time-series input ports: a typed port is schema-checked at compile
                // time; an erased port is matched against the node's input at runtime.
                std::vector<WiringPortRef> inputs;
                inputs.reserve(signature::input_count());
                [&]<std::size_t... I>(std::index_sequence<I...>) {
                    (
                        [&] {
                            using P = std::tuple_element_t<I, wire_params>;
                            if constexpr (static_node_detail::is_input_selector<P>::value)
                            {
                                [[maybe_unused]] constexpr std::size_t arg_index =
                                    call_args_detail::bound_arg_index<I, wire_params, decltype(arg_tuple)>();
                                if constexpr (arg_index != call_args_detail::npos)
                                {
                                    using A0 = std::remove_cvref_t<std::tuple_element_t<
                                        arg_index, std::remove_reference_t<decltype(arg_tuple)>>>;
                                    using A = call_args_detail::payload_t<A0>;
                                    static_assert(graph_wiring_detail::is_port<A>::value ||
                                                      graph_wiring_detail::is_structural_source_arg<A>::value,
                                                  "wire<T>: a time-series input expects a Port argument or structural initializer");
                                    const auto *expected = schema_descriptor<
                                        typename graph_wiring_detail::in_param_schema<P>::type>::ts_meta();
                                    if constexpr (graph_wiring_detail::is_structural_source_arg<A>::value)
                                    {
                                        WiringPortRef ref = graph_wiring_detail::structural_source_for_input_schema(
                                            expected, call_args_detail::payload_at<arg_index>(arg_tuple));
                                        inputs.push_back(
                                            graph_wiring_detail::adapt_source_for_input(w, expected, std::move(ref)));
                                    }
                                    else if constexpr (graph_wiring_detail::is_erased_port<A>::value)
                                    {
                                        WiringPortRef ref = call_args_detail::payload_at<arg_index>(arg_tuple).erased();
                                        if (!graph_wiring_detail::input_accepts_output_schema(expected, ref.schema))
                                        {
                                            throw std::logic_error(
                                                "wire<T>: erased port schema does not match the node's time-series input");
                                        }
                                        inputs.push_back(
                                            graph_wiring_detail::adapt_source_for_input(w, expected, std::move(ref)));
                                    }
                                    else
                                    {
                                        static_assert(graph_wiring_detail::statically_accepts_output_v<
                                                          typename graph_wiring_detail::in_param_schema<P>::type,
                                                          typename A::schema>,
                                                      "wire<T>: input port schema does not match the node's time-series input");
                                        inputs.push_back(graph_wiring_detail::adapt_source_for_input(
                                            w, expected, call_args_detail::payload_at<arg_index>(arg_tuple).erased()));
                                    }
                                }
                                else if constexpr (call_args_detail::auto_context_param_v<P>)
                                {
                                    // Context-bound input: source from the nearest
                                    // enclosing context::scope with this name.
                                    const auto *expected = schema_descriptor<
                                        typename graph_wiring_detail::in_param_schema<P>::type>::ts_meta();
                                    WiringPortRef ref =
                                        graph_wiring_detail::resolve_context_source(w, P::field_name.sv());
                                    if (!graph_wiring_detail::input_accepts_output_schema(expected, ref.schema))
                                    {
                                        throw std::logic_error(
                                            "wire<T>: context '" + std::string{P::field_name.sv()} +
                                            "' schema does not match the node's Context<> input");
                                    }
                                    inputs.push_back(
                                        graph_wiring_detail::adapt_source_for_input(w, expected, std::move(ref)));
                                }
                            }
                        }(),
                        ...);
                }(std::make_index_sequence<std::tuple_size_v<wire_params>>{});

                // Compound scalar configuration (the Scalar positions), if any.
                Value scalars;
                if constexpr (signature::scalar_count() > 0)
                {
                    const auto binding = value_type_for_wiring(signature::scalar_schema());
                    scalars             = Value{binding};
                    auto mutation       = scalars.as_bundle().begin_mutation();
                    [&]<std::size_t... I>(std::index_sequence<I...>) {
                        (
                            [&] {
                                using P = std::tuple_element_t<I, wire_params>;
                                if constexpr (static_node_detail::is_scalar_selector<P>::value)
                                {
                                    constexpr std::size_t arg_index =
                                        call_args_detail::bound_arg_index<I, wire_params, decltype(arg_tuple)>();
                                    [[maybe_unused]] constexpr std::size_t default_index =
                                        call_args_detail::default_arg_index<I, wire_params, decltype(default_args)>();
                                    if constexpr (arg_index != call_args_detail::npos)
                                    {
                                        using V = typename P::value_type;
                                        mutation[P::field_name.sv()].template checked_mutable_as<V>() =
                                            graph_wiring_detail::coerce_scalar_value<V>(
                                                call_args_detail::payload_at<arg_index>(arg_tuple));
                                    }
                                    else if constexpr (default_index != call_args_detail::npos)
                                    {
                                        using V = typename P::value_type;
                                        mutation[P::field_name.sv()].template checked_mutable_as<V>() =
                                            graph_wiring_detail::coerce_scalar_value<V>(
                                                call_args_detail::payload_at<default_index>(default_args));
                                    }
                                }
                            }(),
                            ...);
                    }(std::make_index_sequence<std::tuple_size_v<wire_params>>{});
                }

                NodeBuilder builder = graph_wiring_detail::build_node_builder<X>();
                builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                    builder.type().schema() != nullptr ? builder.type().schema()->input_schema : nullptr,
                    std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                WiringPortRef out = w.add_node(std::type_index(typeid(X)), std::move(builder), inputs, std::move(scalars));

                if constexpr (signature::has_output())
                {
                    return Port<typename signature::output_schema_type>{w, std::move(out)};
                }
            }
        }

        template <typename X, typename OutSchema, typename ParamsTuple, std::size_t PackIndex, typename... Args>
        auto wire_static_node_collection_pack(Wiring &w, const Args &...args)
        {
            using PackParam = std::tuple_element_t<PackIndex, ParamsTuple>;
            constexpr node_collection_pack_kind kind = input_pack_kind<PackParam>();

            auto arg_tuple = std::forward_as_tuple(args...);
            validate_node_collection_pack_args<ParamsTuple, PackIndex>(arg_tuple);
            auto passthrough = node_collection_passthrough_args<ParamsTuple, PackIndex>(arg_tuple);

            if constexpr (kind == node_collection_pack_kind::tsl)
            {
                auto packed = arg<PackParam::field_name>(
                    make_tsl_node_collection_arg<ParamsTuple, PackIndex>(arg_tuple));
                return std::apply(
                    [&](const auto &...kept) {
                        return wire_static_node_normal<X, OutSchema>(w, kept..., packed);
                    },
                    passthrough);
            }
            else
            {
                auto packed = arg<PackParam::field_name>(
                    make_tsb_node_collection_arg<ParamsTuple, PackIndex>(arg_tuple));
                return std::apply(
                    [&](const auto &...kept) {
                        return wire_static_node_normal<X, OutSchema>(w, kept..., packed);
                    },
                    passthrough);
            }
        }
    }  // namespace graph_wiring_detail

    /**
     * Wire ``X`` into ``w``.
     *
     * - If ``X`` is a **node** (has ``eval``): add it with the given wiring
     *   arguments — a ``Port`` for each ``In`` and a scalar value for each
     *   ``Scalar``, **in eval-parameter order** — and return a typed ``Port`` to
     *   its output (or ``void`` for a sink). The arguments are checked against the
     *   node's parameters at compile time.
     * - If ``X`` is a **sub-graph** (has ``compose``): inline its body into ``w``
     *   (graphs flatten — no runtime node is produced) and return its output port.
     *   The arguments follow the same rule as for a node — a ``Port`` for each
     *   ``Port`` parameter and a scalar value for each ``Scalar`` parameter, **in
     *   compose-parameter order** — and are checked at compile time.
     */
    template <typename X, typename OutSchema = void, typename... Args>
    auto wire(Wiring &w, const Args &...args)
    {
        if constexpr (graph_wiring_detail::wire_customization<X, OutSchema, Args...>::enabled)
        {
            return graph_wiring_detail::wire_customization<X, OutSchema, Args...>::wire(w, args...);
        }
        else if constexpr (graph_wiring_detail::is_graph_def<X>)
        {
            // sub-graph: inline its body (flatten), forwarding ports through and
            // wrapping scalar literals into the compose Scalar<> parameters.
            using sig = StaticGraphSignature<X>;
            static_assert(sizeof...(Args) <= sig::param_count(),
                          "wire<G>: too many arguments for the sub-graph's Port + Scalar parameters");
            auto arg_tuple    = std::forward_as_tuple(args...);
            auto default_args = call_args_detail::default_args_for<X>();
            call_args_detail::validate_call_args<typename sig::param_types>("wire<G>", arg_tuple, default_args);
            auto compose = [&]() -> decltype(auto) {
                return [&]<std::size_t... I>(std::index_sequence<I...>) -> decltype(auto) {
                    return X::compose(
                        w,
                        graph_wiring_detail::make_bound_compose_arg<
                            I, typename sig::param_types>(w, arg_tuple,
                                                          default_args)...);
                }(std::make_index_sequence<sig::param_count()>{});
            };
            if (!w.has_wiring_observers()) { return compose(); }

            const std::string label = static_node_detail::diagnostic_name<X>();
            return w.observe(
                WiringScopeEvent{
                    .kind = WiringScopeKind::NestedGraph,
                    .label = label,
                    .signature = label,
                },
                compose);
        }
        else if constexpr (std::is_base_of_v<operator_tag, X>)
        {
            // operator: erase the arguments and dispatch to the registry, which picks
            // the most specific registered overload and wires it (see *Operators*).
            OperatorWireResult result = operator_dispatch_detail::wire_operator_result<X, OutSchema>(w, args...);
            if constexpr (X::has_output)
            {
                if (!result.has_output)
                {
                    throw std::logic_error("wire<Operator>: selected overload has no output");
                }
                if constexpr (!std::is_void_v<OutSchema>)
                {
                    const auto *expected = ts_type<OutSchema>();
                    if (!graph_wiring_detail::input_accepts_output_schema(expected, result.output.erased().schema))
                    {
                        throw std::logic_error("wire<Operator, OutSchema>: selected overload output schema does not match");
                    }
                    return Port<OutSchema>{w, result.output.erased()};
                }
                else
                {
                    return result.output;
                }
            }
            else
            {
                static_assert(std::is_void_v<OutSchema>,
                              "wire<Operator, OutSchema>: an explicit output schema requires an output operator");
                if (result.has_output)
                {
                    throw std::logic_error("wire<Operator>: selected overload unexpectedly produced an output");
                }
                return;
            }
        }
        else
        {
            using wire_params = typename StaticNodeSignature<X>::wire_param_types;
            constexpr std::size_t pack_index =
                graph_wiring_detail::single_tail_collection_input_index<wire_params>();
            using args_tuple = std::tuple<std::remove_cvref_t<Args>...>;
            if constexpr (graph_wiring_detail::node_collection_pack_needed<wire_params, pack_index, args_tuple>())
            {
                return graph_wiring_detail::wire_static_node_collection_pack<X, OutSchema, wire_params, pack_index>(
                    w, args...);
            }
            else
            {
                return graph_wiring_detail::wire_static_node_normal<X, OutSchema>(w, args...);
            }
        }
    }

    /**
     * Brace-initializer entry points for ``wire<X>(w, {a, b}, …)``.
     *
     * A braced-init-list cannot be deduced through a forwarding ``Args...`` pack,
     * so each leading argument position that may take a ``{…}`` structural input
     * needs its own overload (unnamed ``{a, b}`` → TSL/TSB by position; named
     * ``{{"f", a}}`` → TSB by field name). Only the first three positions are
     * covered today; a ``{…}`` in a later position must be wrapped explicitly as a
     * ``WiringStructuralSourceArg`` / ``WiringNamedStructuralSourceArg``.
     */
    template <typename X, typename OutSchema = void, typename... Rest>
    auto wire(Wiring &w, std::initializer_list<WiringPortRef> first, const Rest &...rest)
    {
        return wire<X, OutSchema>(w, WiringStructuralSourceArg{first}, rest...);
    }

    template <typename X, typename OutSchema = void, typename... Rest>
    auto wire(Wiring &w, std::initializer_list<WiringNamedPortRef> first, const Rest &...rest)
    {
        return wire<X, OutSchema>(w, WiringNamedStructuralSourceArg{first}, rest...);
    }

    template <typename X, typename OutSchema = void, typename A0, typename... Rest>
    auto wire(Wiring &w, const A0 &a0, std::initializer_list<WiringPortRef> second, const Rest &...rest)
    {
        return wire<X, OutSchema>(w, a0, WiringStructuralSourceArg{second}, rest...);
    }

    template <typename X, typename OutSchema = void, typename A0, typename... Rest>
    auto wire(Wiring &w, const A0 &a0, std::initializer_list<WiringNamedPortRef> second, const Rest &...rest)
    {
        return wire<X, OutSchema>(w, a0, WiringNamedStructuralSourceArg{second}, rest...);
    }

    template <typename X, typename OutSchema = void, typename A0, typename A1, typename... Rest>
    auto wire(Wiring &w, const A0 &a0, const A1 &a1, std::initializer_list<WiringPortRef> third,
              const Rest &...rest)
    {
        return wire<X, OutSchema>(w, a0, a1, WiringStructuralSourceArg{third}, rest...);
    }

    template <typename X, typename OutSchema = void, typename A0, typename A1, typename... Rest>
    auto wire(Wiring &w, const A0 &a0, const A1 &a1, std::initializer_list<WiringNamedPortRef> third,
              const Rest &...rest)
    {
        return wire<X, OutSchema>(w, a0, a1, WiringNamedStructuralSourceArg{third}, rest...);
    }

    /**
     * Compile-time reflection of a graph's ``compose`` signature — the graph-level
     * mirror of ``StaticNodeSignature``. It reflects ``&G::compose`` **skipping the
     * leading ``Wiring&``**: ``Port`` parameters are the graph's time-series inputs,
     * ``Scalar`` parameters are its scalar inputs, and the return type is its
     * time-series output(s).
     */
    template <typename G>
    struct StaticGraphSignature
    {
      private:
        using compose_args = typename static_node_detail::fn_traits<decltype(&G::compose)>::args_tuple;
        using params       = typename graph_wiring_detail::drop_first<compose_args>::type;
        using indices      = std::make_index_sequence<std::tuple_size_v<params>>;

        template <std::size_t... I>
        static constexpr std::size_t count_ports(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (graph_wiring_detail::is_port<
                         static_node_detail::selector_of<std::tuple_element_t<I, params>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

        template <std::size_t... I>
        static constexpr std::size_t count_scalars(std::index_sequence<I...>)
        {
            return (std::size_t{0} + ... +
                    (static_node_detail::is_scalar_selector<
                         static_node_detail::selector_of<std::tuple_element_t<I, params>>>::value
                         ? std::size_t{1}
                         : std::size_t{0}));
        }

      public:
        /** Tuple of the ``compose`` parameter selector types (``Port`` / ``Scalar``), in order. */
        using param_types = params;
        /** The graph's time-series output type (the ``compose`` return type), or ``void``. */
        using output_type = typename static_node_detail::fn_traits<decltype(&G::compose)>::return_type;

        [[nodiscard]] static constexpr std::size_t param_count() { return std::tuple_size_v<params>; }
        [[nodiscard]] static constexpr std::size_t input_count() { return count_ports(indices{}); }
        [[nodiscard]] static constexpr std::size_t scalar_count() { return count_scalars(indices{}); }
    };

    /**
     * Build a top-level graph ``G`` — its ``static compose(Wiring &, …)`` runs at
     * wiring time. A top-level graph has **no time-series inputs or outputs**, but
     * **may take ``Scalar`` parameters**: pass scalar values positionally or by
     * name with ``arg<"name">(value)``. Values are wrapped into the graph's
     * ``Scalar<>`` parameters and forwarded to ``compose``.
     */
    template <typename G, typename... Args>
    [[nodiscard]] GraphBuilder build_graph_with_observers(
        std::span<WiringObserver *const> observers, Args &&...args)
    {
        using sig    = StaticGraphSignature<G>;
        using params = typename sig::param_types;
        static_assert(sig::input_count() == 0,
                      "build_graph<G>: a top-level graph has no time-series inputs (only Scalar parameters)");
        static_assert(sig::param_count() == sig::scalar_count(),
                      "build_graph<G>: every compose parameter after Wiring& must be Scalar<>");
        static_assert(sizeof...(Args) <= sig::scalar_count(),
                      "build_graph<G>: too many arguments for the graph's Scalar parameters");
        static_assert(graph_wiring_detail::all_scalar_params<params>(std::make_index_sequence<sig::param_count()>{}),
                      "build_graph<G>: every compose parameter after Wiring& must be Scalar<>");

        Wiring w;
        for (WiringObserver *observer : observers) { w.add_wiring_observer(observer); }
        auto   arg_tuple    = std::forward_as_tuple(std::forward<Args>(args)...);
        auto   default_args = call_args_detail::default_args_for<G>();
        call_args_detail::validate_call_args<params>("build_graph<G>", arg_tuple, default_args, "scalar parameter");
        auto build = [&] {
            [&]<std::size_t... I>(std::index_sequence<I...>) {
                G::compose(
                    w,
                    graph_wiring_detail::make_bound_scalar_param<I, params>(
                        arg_tuple, default_args)...);
            }(std::make_index_sequence<sig::param_count()>{});
            return std::move(w).finish();
        };
        GraphBuilder graph_builder = [&] {
            if (!w.has_wiring_observers()) { return build(); }
            const std::string label = static_node_detail::diagnostic_name<G>();
            return w.observe(
                WiringScopeEvent{
                    .kind = WiringScopeKind::Graph,
                    .label = label,
                    .signature = label,
                },
                build);
        }();
        if constexpr (static_node_detail::has_name<G>) {
            graph_builder.label(std::string{static_node_detail::name_view<G>()});
        }
        return graph_builder;
    }

    template <typename G, typename... Args>
    [[nodiscard]] GraphBuilder build_graph(Args &&...args)
    {
        return build_graph_with_observers<G>(
            std::span<WiringObserver *const>{}, std::forward<Args>(args)...);
    }

    namespace boundary_detail
    {
        /**
         * The shared-output relay both boundary families are built from
         * (RFC 0011 step 9).
         *
         * ``service::detail::reference_shared_output_source`` and
         * ``adaptor::detail::output_source`` were the same function under two
         * names, as were their capture halves - identical wiring schema,
         * identical ``rank_dependency = false`` on the capture's second input,
         * identical ``add_same_cycle_pair`` contract - differing only in a path
         * string, a marker typeid and an error message. Both now call these,
         * so there is ONE implementation behind the two spellings.
         *
         * The erased runtime already shared its equivalents; this closes the
         * same gap on the template surface.
         */
        [[nodiscard]] inline WiringPortRef shared_output_relay_source(
            Wiring &w, std::type_index marker, const TSValueTypeMetaData *target_meta,
            std::string full_path)
        {
            const auto *ref_meta = TypeRegistry::instance().ref(target_meta);
            WiringNodeSchema schema;
            schema.output = ref_meta;
            schema.state  = ref_meta->value_schema;
            Value path_key{Str{full_path}};
            return w.add_node(
                marker, schema, std::span<const WiringPortRef>{}, std::move(path_key),
                [path = std::move(full_path), target_meta]() {
                    return make_shared_output_source_node(path, *target_meta);
                });
        }

        [[nodiscard]] inline const WiringInstance *shared_output_relay_capture(
            Wiring &w, std::type_index marker, const TSValueTypeMetaData *output_meta,
            const std::string &full_path, WiringPortRef output, WiringPortRef shared_output)
        {
            std::array<WiringPortRef, 2> sources{output, shared_output};
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                // Sanctioned backward link: the capture reads the source it
                // feeds, purely to locate it. Not a rank dependency.
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            NodeBuilder builder = make_shared_output_capture_node(full_path, *output_meta);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema,
                std::span<const WiringPortRef>{sources.data(), sources.size()}));
            WiringPortRef capture = w.add_node(
                marker, std::move(builder),
                std::span<const WiringInputRef>{inputs.data(), inputs.size()}, Value{});
            // Shared-output relays are RANK-CORRECT and same-cycle: the rank
            // dependency places the paired source after this capture, so the
            // capture schedules the source for the CURRENT evaluation time.
            w.add_same_cycle_pair(capture.peered_node(), shared_output.peered_node());
            return capture.peered_node();
        }
    }

}  // namespace hgraph

#endif  // HGRAPH_CPP_ROOT_GRAPH_WIRING_H
