#ifndef HGRAPH_WEB_TESTING_FAKE_TRANSPORT_H
#define HGRAPH_WEB_TESTING_FAKE_TRANSPORT_H

#include <hgraph/web/export.h>
#include <hgraph/web/service.h>
#include <hgraph/web/value_builders.h>

#include <chrono>
#include <memory>
#include <vector>

namespace hgraph::web::testing
{
    namespace detail
    {
        struct FakeServerAccess;
        struct FakeClientAccess;
    }  // namespace detail

    struct FakeResponse
    {
        Int   client_id{};
        Int   request_id{};
        Value response{};
    };

    struct FakeWsSend
    {
        Int   client_id{};
        Int   connection_id{};
        Value frame{};
    };

    /** The socketless server transport: the same standard channel push
     * sources, graph projections, graph sinks, and service composition as the
     * real transport with only the external task swapped (RFC 0024/0027).
     * Production configuration never accepts a transport object; this seam is
     * confined to testing. */
    class HGRAPH_WEB_EXPORT FakeWebServer
    {
      public:
        FakeWebServer();
        ~FakeWebServer();

        FakeWebServer(const FakeWebServer &)            = delete;
        FakeWebServer &operator=(const FakeWebServer &) = delete;

        [[nodiscard]] bool wait_until_attached(std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool wait_until_detached(std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool wait_for_http_routes(std::size_t count, std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool wait_for_ws_routes(std::size_t count, std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool wait_for_responses(std::size_t count, std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool wait_for_ws_sends(std::size_t count, std::chrono::milliseconds timeout) const;

        [[nodiscard]] std::size_t              attach_count() const;
        [[nodiscard]] std::vector<Value>       http_routes() const;
        [[nodiscard]] std::vector<Value>       removed_http_routes() const;
        [[nodiscard]] std::vector<Value>       ws_routes() const;
        [[nodiscard]] std::vector<Value>       removed_ws_routes() const;
        [[nodiscard]] std::vector<FakeResponse> responses() const;
        [[nodiscard]] std::vector<FakeWsSend>  ws_sends() const;

        void emit_request(Value route, Value request);
        void emit_route_state(Value route, WebRouteState state);
        void emit_ws_event(Value route, Value event);
        void emit_ws_frame(Value route, Value inbound_frame);
        void emit_event(Value event, bool stop_graph = false);
        void emit_stats(Value stats);

      private:
        friend struct detail::FakeServerAccess;
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

    using FakeWebServerPtr = std::shared_ptr<FakeWebServer>;

    HGRAPH_WEB_EXPORT void register_fake_server(Wiring &w, service::ServicePath path, Value server_config,
                                                FakeWebServerPtr server);

    struct FakeHttpCall
    {
        Int   client_id{};
        Value request{};
        Value options{};
    };

    struct FakeClientWsSend
    {
        Int   client_id{};
        Value key{};
        Value frame{};
    };

    /** The socketless client transport counterpart of FakeWebServer. */
    class HGRAPH_WEB_EXPORT FakeWebClient
    {
      public:
        FakeWebClient();
        ~FakeWebClient();

        FakeWebClient(const FakeWebClient &)            = delete;
        FakeWebClient &operator=(const FakeWebClient &) = delete;

        [[nodiscard]] bool wait_until_attached(std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool wait_until_detached(std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool wait_for_calls(std::size_t count, std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool wait_for_ws_keys(std::size_t count, std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool wait_for_ws_sends(std::size_t count, std::chrono::milliseconds timeout) const;

        [[nodiscard]] std::size_t                   attach_count() const;
        [[nodiscard]] std::vector<FakeHttpCall>     calls() const;
        [[nodiscard]] std::vector<Value>            ws_keys() const;
        [[nodiscard]] std::vector<Value>            removed_ws_keys() const;
        [[nodiscard]] std::vector<FakeClientWsSend> ws_sends() const;

        void respond(Int client_id, Value response);
        void fail(Int client_id, Value transport_error);
        void emit_ws_event(Value key, Value event);
        void emit_ws_frame(Value key, Value frame);
        void emit_event(Value event, bool stop_graph = false);
        void emit_stats(Value stats);

      private:
        friend struct detail::FakeClientAccess;
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

    using FakeWebClientPtr = std::shared_ptr<FakeWebClient>;

    HGRAPH_WEB_EXPORT void register_fake_client(Wiring &w, service::ServicePath path, Value client_config,
                                                FakeWebClientPtr client);
}  // namespace hgraph::web::testing

#endif  // HGRAPH_WEB_TESTING_FAKE_TRANSPORT_H
