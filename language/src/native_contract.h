#pragma once

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
}  // namespace hgl
