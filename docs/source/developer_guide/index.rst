Developer Guide
===============

The developer guide is the authoritative design record for the C++ implementation: runtime behaviour, ownership, type and schema representation, the operator library, and the Python compatibility boundary. A page here and the code it describes change together — a divergence between the two is a bug, not a documentation backlog item.

These pages describe *how the runtime is built*. For how to write programs with it, see the :doc:`../user_guide/index`; for proposed changes to public types, runtime, or the extension surface, see the :doc:`../rfc/index`.

.. toctree::
   :maxdepth: 2

   build_system
   repository_migration
   extension_policy
   debugging
   documentation_conventions
   roadmap
   replacement_gap_plan
   release_readiness
   architecture
   binding_vocabulary
   data_structures
   wiring
   graph_wiring
   nested_graphs
   error_handling
   mesh
   services
   operators
   parity_matrix
   parity_testing
   tornado_parity
   perspective_parity
   record_replay_table
   python_integration
   python_bridge
   type_reflection
   notebook
   testing
   memory_utilisation
