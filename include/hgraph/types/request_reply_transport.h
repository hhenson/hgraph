#ifndef HGRAPH_TYPES_REQUEST_REPLY_TRANSPORT_H
#define HGRAPH_TYPES_REQUEST_REPLY_TRANSPORT_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/graph_wiring.h>

#include <cstdint>
#include <string>
#include <typeindex>

namespace hgraph
{
    /**
     * Immutable wiring-time transport selected for one concrete request/reply
     * implementation (RFC 0014). No policy dispatch occurs at runtime.
     */
    enum class RequestReplyTransportPlan : std::uint8_t
    {
        Direct,          ///< ranked request relay and direct response relay
        RequestDeferred, ///< next-cycle request relay and direct response relay
        FullFeedback,    ///< next-cycle request relay and feedback response relay
    };

    namespace request_reply_transport
    {
        /** Defer a reply-full client capture until its implementation plan is known. */
        HGRAPH_EXPORT void defer_client(Wiring &w, std::string base_path, std::string request_path,
                                        std::string response_path, const TSValueTypeMetaData &request_schema,
                                        WiringPortRef request, WiringPortRef request_source, WiringPortRef request_id,
                                        WiringPortRef response_source, std::type_index capture_role);

        /** Record the implementation-owned request source used for causality analysis. */
        HGRAPH_EXPORT void register_implementation_input(Wiring &w, std::string base_path,
                                                         const WiringInstance *request_source);

        /**
         * Defer publication of one implementation output. The active
         * implementation scope is retained so later service/adaptor calls in
         * the same graph conservatively select full feedback.
         */
        HGRAPH_EXPORT void defer_implementation_output(Wiring &w, std::string base_path, std::string response_path,
                                                       const TSValueTypeMetaData &response_dict_schema,
                                                       WiringPortRef output, WiringPortRef response_source,
                                                       std::type_index capture_role);
    }  // namespace request_reply_transport
}  // namespace hgraph

#endif
