#include <hgraph/types/registry_reset.h>
#include <hgraph/persistence/store_location.h>

#include <catch2/catch_session.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>

namespace
{
    class RegistryResetListener final : public Catch::EventListenerBase
    {
      public:
        using Catch::EventListenerBase::EventListenerBase;

        void testCaseStarting(const Catch::TestCaseInfo &) override
        {
            hgraph::reset_all_registries();
        }

        void testCaseEnded(const Catch::TestCaseStats &) override
        {
            hgraph::reset_all_registries();
        }

        void testRunEnded(const Catch::TestRunStats &) override
        {
            hgraph::persistence::store::finalize_s3();
        }
    };
}  // namespace

CATCH_REGISTER_LISTENER(RegistryResetListener)
