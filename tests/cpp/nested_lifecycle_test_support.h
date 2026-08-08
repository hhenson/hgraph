#ifndef HGRAPH_TESTS_CPP_NESTED_LIFECYCLE_TEST_SUPPORT_H
#define HGRAPH_TESTS_CPP_NESTED_LIFECYCLE_TEST_SUPPORT_H

#include <hgraph/types/static_node.h>

#include <cstddef>
#include <functional>
#include <string_view>

namespace hgraph::testing
{
    struct NestedLifecycleSnapshot
    {
        std::size_t constructed{};
        std::size_t live{};
        std::size_t started{};
        std::size_t stopped{};
        std::size_t destroyed{};

        [[nodiscard]] bool operator==(const NestedLifecycleSnapshot &) const noexcept = default;
    };

    struct NestedLifecycleCounters
    {
        static inline std::size_t constructed{};
        static inline std::size_t live{};
        static inline std::size_t started{};
        static inline std::size_t stopped{};
        static inline std::size_t destroyed{};

        static void reset() noexcept
        {
            constructed = 0;
            live        = 0;
            started     = 0;
            stopped     = 0;
            destroyed   = 0;
        }

        [[nodiscard]] static NestedLifecycleSnapshot snapshot() noexcept
        {
            return {
                .constructed = constructed,
                .live        = live,
                .started     = started,
                .stopped     = stopped,
                .destroyed   = destroyed,
            };
        }
    };

    struct NestedLifecycleState
    {
        NestedLifecycleState() noexcept
        {
            ++NestedLifecycleCounters::constructed;
            ++NestedLifecycleCounters::live;
        }

        NestedLifecycleState(const NestedLifecycleState &other) noexcept : value(other.value)
        {
            ++NestedLifecycleCounters::constructed;
            ++NestedLifecycleCounters::live;
        }

        NestedLifecycleState(NestedLifecycleState &&other) noexcept : value(other.value)
        {
            ++NestedLifecycleCounters::constructed;
            ++NestedLifecycleCounters::live;
        }

        NestedLifecycleState &operator=(const NestedLifecycleState &) noexcept = default;
        NestedLifecycleState &operator=(NestedLifecycleState &&) noexcept      = default;

        ~NestedLifecycleState()
        {
            --NestedLifecycleCounters::live;
            ++NestedLifecycleCounters::destroyed;
        }

        int value{};

        [[nodiscard]] bool operator==(const NestedLifecycleState &) const noexcept = default;
    };

    struct NestedLifecycleNode
    {
        static constexpr auto name = "nested_lifecycle_node";

        static void start(State<NestedLifecycleState>) { ++NestedLifecycleCounters::started; }

        static void stop(State<NestedLifecycleState>) { ++NestedLifecycleCounters::stopped; }

        static void eval(In<"ts", TS<Int>> ts, State<NestedLifecycleState>, Out<TS<Int>> out)
        {
            out.set(ts.value());
        }
    };
}  // namespace hgraph::testing

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<hgraph::testing::NestedLifecycleState>
    {
        static constexpr std::string_view value{"nested_lifecycle_state"};
    };
}  // namespace hgraph::static_schema_detail

template <>
struct std::hash<hgraph::testing::NestedLifecycleState>
{
    [[nodiscard]] std::size_t operator()(const hgraph::testing::NestedLifecycleState &state) const noexcept
    {
        return std::hash<int>{}(state.value);
    }
};

#endif  // HGRAPH_TESTS_CPP_NESTED_LIFECYCLE_TEST_SUPPORT_H
