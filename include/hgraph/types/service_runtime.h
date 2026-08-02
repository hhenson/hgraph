#ifndef HGRAPH_TYPES_SERVICE_RUNTIME_H
#define HGRAPH_TYPES_SERVICE_RUNTIME_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/wired_fn.h>

#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph
{
    class Wiring;
    struct WiringPortRef;

    /**
     * Runtime service identity (design record: services.rst *Runtime service
     * identity*; rulings 2026-07-05). A ``RuntimeServiceDescriptor`` is the
     * erased form of a service interface: C++ descriptor types synthesise one
     * at first use, the Python bridge builds one from a stub's annotations.
     * Descriptors intern by name, optional generic specialization, flavour,
     * and erased schemas (immortal registry). Re-registration with the same
     * identity returns the interned record. Same-named Python interfaces with
     * different schemas remain distinct records while retaining the same
     * runtime path grammar.
     *
     * Node identity rides the name-qualified full path (the path scalar) plus
     * the per-ROLE markers — the same key the C++ templates use — so a Python
     * implementation can serve a C++ interface stub on the same path (the Q1
     * direction; the reverse is not supported).
     */
    enum class ServiceFlavour : std::uint8_t
    {
        Reference,
        Subscription,
        RequestReply,
        Adaptor,
        ServiceAdaptor,
    };

    struct RuntimeServiceDescriptor
    {
        std::string                name;
        /** Registry discriminator for a concrete specialization of a generic
            interface (for example ``NUMBER=int``). It is not part of the
            service name or runtime path; callers put the same typed suffix on
            the user path, matching ``service::path(..., arg<...>(...))``. */
        std::string                specialization;
        ServiceFlavour             flavour{ServiceFlavour::Reference};
        const TSValueTypeMetaData *output_schema{nullptr};     ///< reference
        const ValueTypeMetaData   *key_type{nullptr};          ///< subscription
        const TSValueTypeMetaData *value_schema{nullptr};      ///< subscription
        const TSValueTypeMetaData *request_schema{nullptr};    ///< request/reply
        const TSValueTypeMetaData *response_schema{nullptr};   ///< request/reply
        const TSValueTypeMetaData *input_schema{nullptr};      ///< adaptor (optional)
        // (adaptor output reuses output_schema; either may be null for
        //  sink-/source-only adaptors)
        std::string                default_path;
    };

    /** Intern by name, optional specialization, flavour, and erased schemas.
        The returned reference is stable for the process lifetime. */
    [[nodiscard]] HGRAPH_EXPORT const RuntimeServiceDescriptor &intern_service_descriptor(
        RuntimeServiceDescriptor descriptor);

    /** The unique unspecialized descriptor for ``name``, or null when absent
        or ambiguous. */
    [[nodiscard]] HGRAPH_EXPORT const RuntimeServiceDescriptor *find_service_descriptor(std::string_view name);

    /** Every interned descriptor name (sorted) — discovery for the bridge. */
    [[nodiscard]] HGRAPH_EXPORT std::vector<std::string> service_descriptor_names();

    // ------------------------------------------------------------------
    // The erased flavour flows (the runtime counterparts of the template
    // client/register functions; identical path grammar, role markers and
    // node makers). ``path`` is the USER path ("" = the descriptor's
    // default). Implementations arrive as WiredFn graph callables; note the
    // WiredFn contract is ts-only, so path injection into implementations is
    // not available through this surface.
    // ------------------------------------------------------------------

    [[nodiscard]] HGRAPH_EXPORT WiringPortRef reference_service_client(Wiring &w,
                                                                       const RuntimeServiceDescriptor &descriptor,
                                                                       std::string_view path);
    HGRAPH_EXPORT void register_reference_service_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                       std::string_view path, const WiredFn &impl,
                                                       std::span<const WiringPortRef> implementation_inputs = {},
                                                       bool default_fallback = false);

    [[nodiscard]] HGRAPH_EXPORT WiringPortRef subscription_service_subscribe(
        Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path, const WiringPortRef &key);
    HGRAPH_EXPORT void register_subscription_service_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                          std::string_view path, const WiredFn &impl,
                                                          std::span<const WiringPortRef> implementation_inputs = {},
                                                          bool default_fallback = false);

    [[nodiscard]] HGRAPH_EXPORT WiringPortRef request_reply_service_call(
        Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path, const WiringPortRef &request);
    HGRAPH_EXPORT void register_request_reply_service_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                           std::string_view path, const WiredFn &impl,
                                                           std::span<const WiringPortRef> implementation_inputs = {},
                                                           bool default_fallback = false);

    /**
     * Multi-interface implementations (the ``register_services`` +
     * ``impl_input``/``impl_output`` shape, erased): ONE graph implements
     * several interfaces by fetching each interface's input inside its body
     * and publishing each output explicitly.
     */
    [[nodiscard]] HGRAPH_EXPORT WiringPortRef service_impl_input(Wiring &w,
                                                                 const RuntimeServiceDescriptor &descriptor,
                                                                 std::string_view path);
    HGRAPH_EXPORT void service_impl_output(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                           std::string_view path, const WiringPortRef &out);
    HGRAPH_EXPORT void register_multi_service_impl(Wiring &w,
                                                   std::span<const RuntimeServiceDescriptor *const> descriptors,
                                                   std::string_view path, const WiredFn &impl,
                                                   std::span<const WiringPortRef> implementation_inputs = {},
                                                   bool default_fallback = false);

    /** Adaptor client: publishes ``in`` (when the interface has an input)
        and returns the adaptor output ref (empty for sink-only adaptors). */
    [[nodiscard]] HGRAPH_EXPORT WiringPortRef adaptor_client(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                             std::string_view path, const WiringPortRef *in);
    /** Impl-side: the client input (called inside a registered impl). */
    [[nodiscard]] HGRAPH_EXPORT WiringPortRef adaptor_from_graph(Wiring &w,
                                                                 const RuntimeServiceDescriptor &descriptor,
                                                                 std::string_view path);
    /** Impl-side: publish the adaptor output back to clients. */
    HGRAPH_EXPORT void adaptor_to_graph(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                        std::string_view path, const WiringPortRef &out);
    enum class AdaptorImplMode : std::uint8_t
    {
        Automatic,
        Manual,
    };
    HGRAPH_EXPORT void register_adaptor_impl(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                             std::string_view path, const WiredFn &impl,
                                             AdaptorImplMode mode = AdaptorImplMode::Manual,
                                             std::span<const WiringPortRef> implementation_inputs = {},
                                             bool default_fallback = false);
    HGRAPH_EXPORT void register_unbound_adaptor_impl(
        Wiring &w, const WiredFn &impl,
        std::span<const WiringPortRef> implementation_inputs = {});

    /** Per-client keyed adaptor exchange (the erased counterpart of
        ``service_adaptor::adaptor`` / ``wire<Interface>``). Returns an empty
        port for a sink-only descriptor with no output schema. */
    [[nodiscard]] HGRAPH_EXPORT WiringPortRef service_adaptor_client(
        Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path,
        const WiringPortRef &in);
    /** Client-side request publication under an explicitly supplied ID. */
    HGRAPH_EXPORT void service_adaptor_client_from_graph(
        Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path,
        const WiringPortRef &in, const WiringPortRef &request_id);
    /** Client-side reply selection for an explicitly supplied ID. */
    [[nodiscard]] HGRAPH_EXPORT WiringPortRef service_adaptor_client_to_graph(
        Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path,
        const WiringPortRef &request_id);
    /** Implementation-side keyed request dictionary. */
    [[nodiscard]] HGRAPH_EXPORT WiringPortRef service_adaptor_from_graph(
        Wiring &w, const RuntimeServiceDescriptor &descriptor, std::string_view path);
    /** Implementation-side keyed reply publication. */
    HGRAPH_EXPORT void service_adaptor_to_graph(Wiring &w, const RuntimeServiceDescriptor &descriptor,
                                                std::string_view path, const WiringPortRef &out);
    /** Auto-wire one implementation taking ``TSD<Int, input>`` and returning
        ``TSD<Int, output>``. */
    HGRAPH_EXPORT void register_service_adaptor_impl(Wiring &w,
                                                     const RuntimeServiceDescriptor &descriptor,
                                                     std::string_view path,
                                                     const WiredFn &impl,
                                                     bool default_fallback = false);
}  // namespace hgraph

#endif  // HGRAPH_TYPES_SERVICE_RUNTIME_H
