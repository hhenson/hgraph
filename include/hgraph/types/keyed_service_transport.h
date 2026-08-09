#ifndef HGRAPH_TYPES_KEYED_SERVICE_TRANSPORT_H
#define HGRAPH_TYPES_KEYED_SERVICE_TRANSPORT_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/graph_wiring.h>

#include <cstdint>
#include <string>
#include <typeindex>

namespace hgraph
{
    /**
     * Immutable wiring-time transport selected for one concrete keyed service
     * implementation (RFC 0014). No policy dispatch occurs at runtime.
     */
    enum class KeyedServiceTransportPlan : std::uint8_t
    {
        Direct,          ///< ranked request relay and direct response relay
        RequestDeferred, ///< next-cycle request relay and direct response relay
        FullFeedback,    ///< next-cycle request relay and feedback response relay
    };

    namespace keyed_service_transport
    {
        /** Defer a request/reply client capture until its implementation plan is known. */
        HGRAPH_EXPORT void defer_request_reply_client(
            Wiring &w, std::string base_path, std::string request_path,
            std::string response_path, const TSValueTypeMetaData &request_schema,
            WiringPortRef request, WiringPortRef request_source,
            WiringPortRef request_id, WiringPortRef response_source,
            std::type_index capture_role);

        /**
         * Wire the subscription response gate immediately and defer only the
         * key capture until its implementation plan is known. The returned
         * value is therefore a concrete port that can cross nested wiring and
         * context boundaries before final planning.
         */
        [[nodiscard]] HGRAPH_EXPORT WiringPortRef defer_subscription_client(
            Wiring &w, std::string base_path, std::string request_path,
            std::string response_path, const ValueTypeMetaData &key_schema,
            const TSValueTypeMetaData &response_schema, WiringPortRef key,
            WiringPortRef request_source, WiringPortRef response,
            WiringPortRef response_source, std::type_index capture_role,
            std::type_index gate_role);

        /** Record the implementation-owned request source used for causality analysis. */
        HGRAPH_EXPORT void register_implementation_input(
            Wiring &w, std::string base_path, const WiringInstance *request_source);

        /**
         * Defer publication of one implementation output. The active
         * implementation scope is retained so later service/adaptor calls in
         * the same graph conservatively select full feedback.
         */
        HGRAPH_EXPORT void defer_request_reply_implementation_output(
            Wiring &w, std::string base_path, std::string response_path,
            const TSValueTypeMetaData &response_dict_schema,
            WiringPortRef output, WiringPortRef response_source,
            std::type_index capture_role);

        /**
         * Defer publication of a subscription implementation dictionary.
         * Subscription coupling is broken by deferring the key relay; the
         * shared keyed response and per-client selection remain direct so a
         * short-lived response cannot be hidden by a second boundary.
         */
        HGRAPH_EXPORT void defer_subscription_implementation_output(
            Wiring &w, std::string base_path, std::string response_path,
            const TSValueTypeMetaData &response_dict_schema,
            WiringPortRef output, WiringPortRef response_source,
            std::type_index capture_role);
    }  // namespace keyed_service_transport
}  // namespace hgraph

#endif
