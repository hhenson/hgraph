// Catch2 event listener that resets every process-wide registry/factory
// before each test case. ``TypeRegistry::reset()`` re-seeds the standard
// scalar/TS vocabulary itself, so each case starts with the default types
// (int / float / str / bool / date / datetime / timedelta / …) registered.
// Tests that mutate the registries (intern atomic scalars, register named
// bundles, etc.) would otherwise leak state into later cases — most visibly
// through the named-bundle / named-tsb uniqueness checks, which see prior
// registrations as conflicts.
//
// The ordered teardown itself is library-owned: ``reset_all_registries()``
// (``hgraph/types/registry_reset.h``) encodes the load-bearing
// borrowers-before-lenders sequence in ONE place. Do not re-implement the
// ordering here — new registries are added there.
//
// Linking this translation unit into the test executable activates the
// listener via Catch2's CATCH_REGISTER_LISTENER macro; no test code needs
// to call anything explicitly.

#include <catch2/catch_session.hpp>
#include <catch2/reporters/catch_reporter_event_listener.hpp>
#include <catch2/reporters/catch_reporter_registrars.hpp>

#include <hgraph/types/registry_reset.h>

namespace
{
    class RegistryResetListener final : public Catch::EventListenerBase
    {
      public:
        using Catch::EventListenerBase::EventListenerBase;

        void testCaseStarting(const Catch::TestCaseInfo &) override { hgraph::reset_all_registries(); }

        void testCaseEnded(const Catch::TestCaseStats &) override { hgraph::reset_all_registries(); }
    };
}  // namespace

CATCH_REGISTER_LISTENER(RegistryResetListener)
