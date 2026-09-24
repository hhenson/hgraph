#pragma once

namespace hgl
{
    // Legacy inline bodies and descriptors predate the temporal/value split.
    enum class NativeExecutionRole { LegacyValue, Value, Temporal };
}  // namespace hgl
