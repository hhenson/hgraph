Quick Start
===========

The C++-first implementation executes real graphs today: you author nodes and
graphs in C++, test them with the ``eval_node`` harness, and run them through
the graph executor in simulation or real-time mode. This page takes you from a
clean checkout to a first tested node; the deeper guides are
:doc:`authoring_nodes_cpp`, :doc:`authoring_graphs_cpp` and
:doc:`testing_graphs_cpp`.

Build the C++ Project
---------------------

From the repository root:

.. code-block:: bash

   cmake -S . -B build
   cmake --build build -j
   ctest --test-dir build --output-on-failure

This builds ``hgraph_core`` (linked as ``hgraph::core``) and the Catch2 test
suite (``hgraph_unit_tests``). No Python is required — Python bindings are
opt-in (see below).

A First Node, Tested
--------------------

A compute node is a struct with a ``name`` and a static ``eval``; the
``eval_node`` harness runs it inside a real graph (a replay source feeding the
node, a record sink capturing its output) under the executor. Inputs are given
one value per engine cycle, with ``none`` meaning "no tick this cycle":

.. code-block:: cpp

   #include <hgraph/lib/testing/check_output.h>
   #include <hgraph/lib/testing/eval_node.h>
   #include <hgraph/types/metadata/type_registry.h>
   #include <hgraph/types/static_node.h>

   #include <catch2/catch_test_macros.hpp>

   using namespace hgraph;
   using namespace hgraph::testing;  // eval_node, none, CHECK_OUTPUT

   struct AddOne
   {
       static constexpr auto name = "add_one";
       static void           eval(In<"in", TS<Int>> in, Out<TS<Int>> out) { out.set(in.value() + 1); }
   };

   TEST_CASE("add_one maps each input tick")
   {
       (void)TypeRegistry::instance().register_scalar<Int>("int");

       // A skipped input cycle (none) stays skipped in the output.
       CHECK_OUTPUT(eval_node<AddOne>({1, none, 3}), {2, none, 4});
   }

Add the file to ``tests/cpp/CMakeLists.txt`` and it runs as part of
``hgraph_unit_tests``. From here:

- **authoring nodes** — state, scalars, schedulers, lifecycle hooks, all the
  time-series kinds: :doc:`authoring_nodes_cpp`;
- **wiring graphs** — ``compose``/``wire``, the standard operator library,
  ``map_`` / ``switch_`` / ``reduce`` and friends: :doc:`authoring_graphs_cpp`;
- **testing** — the full ``eval_node`` harness, collection deltas, replay and
  record: :doc:`testing_graphs_cpp`.

Install Locally
---------------

.. code-block:: bash

   cmake --install build --prefix /tmp/hgraph-install

This installs the public headers, the ``hgraph_core`` library, and the CMake
package files needed by downstream C++ projects.

Python Integration
------------------

Python bindings and Python user-node support are opt-in. Normal CMake configure
and build should remain usable without Python or nanobind.

The relevant options are:

.. code-block:: bash

   -DHGRAPH_BUILD_PYTHON_BINDINGS=ON
   -DHGRAPH_ENABLE_PYTHON_USER_NODES=ON

The installed ``hgraph`` wheel exposes the Python authoring surface and native
``_hgraph`` bridge described in :doc:`python_compatibility`.
