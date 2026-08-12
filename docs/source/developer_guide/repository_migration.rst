Repository migration for hgraph 0.8
===================================

Version 0.8.0 moves the C++-first implementation into the canonical
``hhenson/hgraph`` repository and ``hgraph`` distribution. The Python-first
implementation remains maintained on ``release/0.5``.

History and tree boundary
-------------------------

The replacement is a two-parent merge rather than an orphan branch or force
push. Its first parent is hgraph ``main`` at ``b56a22f9`` and its imported
parent is hg_cpp ``main`` at ``571f3403``. The resulting tree comes from the
imported C++-first implementation, followed by the repository and package
identity changes documented here. This keeps both histories reachable while
still making the replacement visible as an ordinary pull request.

The old implementation is not duplicated on the new ``main`` tree. Its latest
maintenance head at migration time is ``release/0.5`` commit ``27819fa6``
(``v_0.5.41``). No reference checkout is embedded in ``main``; compatibility
campaigns install the pinned ``hgraph==0.5.41`` release in an isolated
environment, while maintenance continues directly on ``release/0.5``.

The hgraph MIT ``LICENSE`` is retained. Active project, package, release, and
issue-publishing links target ``hhenson/hgraph``. Links in parity corpus and
decision records that identify historical hg_cpp issues remain unchanged
because they are provenance, not active routing.

Release identity
----------------

The root Python distribution is ``hgraph`` and the native CMake/Conan project
version is 0.8.0. ``pyproject.toml`` deliberately remains at the untagged
``0.0.0`` sentinel; release artifacts are restamped from ``v_<version>`` tags,
and release validation rejects a core version below 0.8.0.

The separately published Kafka and analytics extensions require
``hgraph>=0.8.0``. The
differential parity controller installs Python-first ``hgraph==0.5.41`` in its
reference environment and installs the locally built 0.8 candidate wheel in a
separate environment. The explicit pin prevents a later hgraph release from
turning the campaign into a comparison of the candidate with itself.

External release configuration
------------------------------

The migrated pipeline retains the established ``release-wheels.yml`` filename,
repository ``hhenson/hgraph``, and GitHub environment ``release`` so the existing
PyPI trusted publisher remains valid for ``hgraph`` and ``hgraph-kafka``. The
new ``hgraph-analytics`` PyPI project must authorize the same workflow and
``release`` environment before its first tagged publish. The first
release-candidate run should still verify the complete publish route.
Repository branch protection and documentation hosting should be reviewed
against the migrated jobs.

Acceptance
----------

The migration is accepted only after the fresh native and Python 3.14 gates in
``AGENTS.md`` pass from the imported tree, packaging checks build ``hgraph``
artifacts, the installed CMake consumer passes, Sphinx builds with warnings as
errors, and the pull request shows the intended deletion/import boundary. CI
provides Linux and Windows evidence after the branch is pushed; it does not
replace the local gates.
