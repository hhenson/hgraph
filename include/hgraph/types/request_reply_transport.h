#ifndef HGRAPH_TYPES_REQUEST_REPLY_TRANSPORT_H
#define HGRAPH_TYPES_REQUEST_REPLY_TRANSPORT_H

#include <hgraph/types/keyed_service_transport.h>

#include <utility>

namespace hgraph
{
    /** Compatibility name retained for the original RFC 0014 contract. */
    using RequestReplyTransportPlan = KeyedServiceTransportPlan;

    namespace request_reply_transport
    {
        /** Defer a reply-full client capture until its implementation plan is known. */
        inline void defer_client(Wiring &w, std::string base_path, std::string request_path,
                                 std::string response_path, const TSValueTypeMetaData &request_schema,
                                 WiringPortRef request, WiringPortRef request_source, WiringPortRef request_id,
                                 WiringPortRef response_source, std::type_index capture_role)
        {
            keyed_service_transport::defer_request_reply_client(
                w, std::move(base_path), std::move(request_path), std::move(response_path), request_schema,
                std::move(request), std::move(request_source), std::move(request_id),
                std::move(response_source), capture_role);
        }

        /** Record the implementation-owned request source used for causality analysis. */
        inline void register_implementation_input(Wiring &w, std::string base_path,
                                                  const WiringInstance *request_source)
        {
            keyed_service_transport::register_implementation_input(
                w, std::move(base_path), request_source);
        }

        /**
         * Defer publication of one implementation output. The active
         * implementation scope is retained so later service/adaptor calls in
         * the same graph conservatively select full feedback.
         */
        inline void defer_implementation_output(Wiring &w, std::string base_path, std::string response_path,
                                                const TSValueTypeMetaData &response_dict_schema,
                                                WiringPortRef output, WiringPortRef response_source,
                                                std::type_index capture_role)
        {
            keyed_service_transport::defer_request_reply_implementation_output(
                w, std::move(base_path), std::move(response_path), response_dict_schema,
                std::move(output), std::move(response_source), capture_role);
        }
    }  // namespace request_reply_transport
}  // namespace hgraph

#endif
