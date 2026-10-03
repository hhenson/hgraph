#include <standard.h>

#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>

namespace
{
    class StandardLibraryFixture final : public Catch::EventListenerBase
    {
      public:
        using Catch::EventListenerBase::EventListenerBase;

        void testRunStarting(const Catch::TestRunInfo &) override {
            provider_ = hgraph_::std_::register_operators();
        }

        void testRunEnded(const Catch::TestRunStats &) override {
            hgraph::OperatorRegistry::instance().remove_provider(provider_);
        }

      private:
        hgraph::OperatorProviderHandle provider_{};
    };
}

CATCH_REGISTER_LISTENER(StandardLibraryFixture)
