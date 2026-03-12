#include <hgraph/types/time_series/link_observer_registry.h>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>

namespace hgraph {

TEST_CASE("TSLinkObserverRegistry stores known keys in dedicated slots") {
    TSLinkObserverRegistry registry;
    std::shared_ptr<void> removed_state = std::make_shared<int>(1);
    std::shared_ptr<void> visible_state = std::make_shared<int>(2);
    std::shared_ptr<void> ref_state = std::make_shared<int>(3);

    registry.set_feature_state(std::string{TSLinkObserverRegistry::kTsdRemovedChildSnapshotsKey}, removed_state);
    registry.set_feature_state(std::string{TSLinkObserverRegistry::kTsdVisibleKeyHistoryKey}, visible_state);
    registry.set_feature_state(std::string{TSLinkObserverRegistry::kRefUnboundItemChangesKey}, ref_state);

    REQUIRE(registry.feature_state(TSLinkObserverRegistry::kTsdRemovedChildSnapshotsKey) == removed_state);
    REQUIRE(registry.feature_state(TSLinkObserverRegistry::kTsdVisibleKeyHistoryKey) == visible_state);
    REQUIRE(registry.feature_state(TSLinkObserverRegistry::kRefUnboundItemChangesKey) == ref_state);

    registry.clear_feature_state(TSLinkObserverRegistry::kTsdVisibleKeyHistoryKey);
    REQUIRE_FALSE(registry.feature_state(TSLinkObserverRegistry::kTsdVisibleKeyHistoryKey));
    REQUIRE(registry.feature_state(TSLinkObserverRegistry::kTsdRemovedChildSnapshotsKey) == removed_state);
    REQUIRE(registry.feature_state(TSLinkObserverRegistry::kRefUnboundItemChangesKey) == ref_state);
}

TEST_CASE("TSLinkObserverRegistry keeps custom feature keys in lazy fallback storage") {
    TSLinkObserverRegistry registry;
    constexpr std::string_view kCustomKey = "custom_runtime_state";
    std::shared_ptr<void> custom_state = std::make_shared<std::string>("state");

    REQUIRE_FALSE(registry.feature_state(kCustomKey));

    registry.set_feature_state(std::string{kCustomKey}, custom_state);
    REQUIRE(registry.feature_state(kCustomKey) == custom_state);

    registry.clear_feature_state(kCustomKey);
    REQUIRE_FALSE(registry.feature_state(kCustomKey));
}

TEST_CASE("TSLinkObserverRegistry clear resets known and fallback states") {
    TSLinkObserverRegistry registry;
    constexpr std::string_view kCustomKey = "custom_runtime_state";

    registry.set_feature_state(
        std::string{TSLinkObserverRegistry::kTsdRemovedChildSnapshotsKey},
        std::make_shared<int>(10));
    registry.set_feature_state(std::string{kCustomKey}, std::make_shared<int>(20));

    registry.clear();

    REQUIRE_FALSE(registry.feature_state(TSLinkObserverRegistry::kTsdRemovedChildSnapshotsKey));
    REQUIRE_FALSE(registry.feature_state(kCustomKey));
}

}  // namespace hgraph
