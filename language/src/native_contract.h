#pragma once

#include <string>

namespace hgl
{
    enum class NativeImplementationKind { Declaration, Value, Graph, Node, InlineCpp };

    constexpr const char *native_implementation_name(NativeImplementationKind kind) {
        switch (kind) {
            case NativeImplementationKind::Declaration: return "declaration";
            case NativeImplementationKind::Value: return "value";
            case NativeImplementationKind::Graph: return "graph";
            case NativeImplementationKind::Node: return "node";
            case NativeImplementationKind::InlineCpp: return "inline_cpp";
        }
        return "declaration";
    }

    // Legacy inline bodies and descriptors predate the temporal/value split.
    enum class NativeExecutionRole { LegacyValue, Value, Temporal };

    /// Shared checking data; neither native classes nor provider code are read.
    struct NativeValueCapabilities
    {
        bool owning_copy{false};
        bool text{false};
        bool equality{false};
        bool hash{false};
        bool order{false};
        bool serialization{false};

        friend bool operator==(const NativeValueCapabilities &, const NativeValueCapabilities &) = default;
    };

    /// A declaration maps to a provider's existing canonical scalar identity.
    struct NativeTypeContract
    {
        std::string             module_identity{};
        std::string             identity{};
        std::string             name{};
        std::string             canonical_identity{};
        std::string             cpp_type{};
        std::string             public_header{};
        NativeValueCapabilities capabilities{};
        bool                    atomic_value{false};
        bool                    exported{false};
        std::string             descriptor_fingerprint{};

        friend bool operator==(const NativeTypeContract &, const NativeTypeContract &) = default;
    };
}  // namespace hgl
