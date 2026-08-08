#ifndef HGRAPH_TYPES_WIRING_OBSERVER_H
#define HGRAPH_TYPES_WIRING_OBSERVER_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/type_record.h>

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph
{
    struct ValueTypeMetaData;
    struct TSValueTypeMetaData;

    /** Stable handle to one interned value or time-series schema. */
    struct HGRAPH_EXPORT WiringTypeHandle
    {
        WiringTypeHandle() noexcept = default;
        explicit WiringTypeHandle(const ValueTypeMetaData *type) noexcept;
        explicit WiringTypeHandle(const TSValueTypeMetaData *type) noexcept;

        [[nodiscard]] TypeFamily family() const noexcept;
        [[nodiscard]] std::string_view name() const noexcept;
        [[nodiscard]] const ValueTypeMetaData *value_type() const noexcept;
        [[nodiscard]] const TSValueTypeMetaData *time_series_type() const noexcept;
        [[nodiscard]] explicit operator bool() const noexcept
        {
            return schema != nullptr;
        }

        const SchemaHeader *schema{nullptr};
        const void         *metadata{nullptr};

        [[nodiscard]] constexpr bool operator==(const WiringTypeHandle &) const noexcept = default;
    };

    enum class WiringScopeKind
    {
        Graph,
        NestedGraph,
        Node,
    };

    enum class WiringCandidateSource
    {
        Cpp,
        Python,
    };

    /** Owned wiring-time identity. It remains valid after wiring has finished. */
    struct WiringScopeEvent
    {
        WiringScopeKind         kind{WiringScopeKind::Node};
        std::vector<std::string> path{};
        std::string              label{};
        std::string              signature{};
        std::vector<WiringTypeHandle> input_types{};
        std::vector<std::string> input_schemas{};
        WiringTypeHandle output_type{};
        std::string              output_schema{};
    };

    /** One considered overload, including the effective rank after adaptation. */
    struct WiringCandidateDiagnostic
    {
        std::string           label{};
        int                   rank{0};
        WiringCandidateSource source{WiringCandidateSource::Cpp};
        std::string           rejection_reason{};
    };

    /** Owned result of one native operator-registry resolution pass. */
    struct WiringResolutionEvent
    {
        std::vector<std::string>                 path{};
        std::string                              operator_name{};
        std::vector<WiringTypeHandle>             argument_types{};
        std::vector<std::string>                 argument_schemas{};
        std::optional<WiringCandidateDiagnostic> selected{};
        std::vector<WiringCandidateDiagnostic>   rejected{};
        std::vector<WiringCandidateDiagnostic>   ambiguous{};
        std::string                              error{};
    };

    /**
     * Wiring-time observer. Unlike lifecycle observers, every callback receives
     * an owned diagnostic record rather than a borrowed runtime view.
     */
    class HGRAPH_EXPORT WiringObserver
    {
      public:
        virtual ~WiringObserver() = default;

        virtual void on_enter_graph_wiring(const WiringScopeEvent &) {}
        virtual void on_exit_graph_wiring(const WiringScopeEvent &, std::string_view) {}
        virtual void on_enter_nested_graph_wiring(const WiringScopeEvent &) {}
        virtual void on_exit_nested_graph_wiring(const WiringScopeEvent &, std::string_view) {}
        virtual void on_enter_node_wiring(const WiringScopeEvent &) {}
        virtual void on_exit_node_wiring(const WiringScopeEvent &, std::string_view) {}
        virtual void on_overload_resolution(const WiringResolutionEvent &) {}
    };

    /** Native formatter for the wiring-observer protocol. */
    class HGRAPH_EXPORT WiringTracer final : public WiringObserver
    {
      public:
        explicit WiringTracer(std::string filter = {}, bool graph = true,
                              bool node = true);

        void on_enter_graph_wiring(const WiringScopeEvent &event) override;
        void on_exit_graph_wiring(const WiringScopeEvent &event,
                                  std::string_view error) override;
        void on_enter_nested_graph_wiring(const WiringScopeEvent &event) override;
        void on_exit_nested_graph_wiring(const WiringScopeEvent &event,
                                         std::string_view error) override;
        void on_enter_node_wiring(const WiringScopeEvent &event) override;
        void on_exit_node_wiring(const WiringScopeEvent &event,
                                 std::string_view error) override;
        void on_overload_resolution(const WiringResolutionEvent &event) override;

        [[nodiscard]] std::span<const std::string> lines() const noexcept
        {
            return lines_;
        }
        void clear() noexcept { lines_.clear(); }

      private:
        [[nodiscard]] bool accepts(std::span<const std::string> path) const;
        void append_scope(std::string_view action, const WiringScopeEvent &event,
                          std::string_view error = {});

        std::string              filter_{};
        bool                     graph_{true};
        bool                     node_{true};
        std::vector<std::string> lines_{};
    };
}

#endif  // HGRAPH_TYPES_WIRING_OBSERVER_H
