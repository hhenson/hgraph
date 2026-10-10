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
   real_time_adaptors
   operators
   hgl_temporal_publications
   hgl_bytes_values
   hgl_any_values
   hgl_native_atomic_values
   hgl_enum_publications
   hgl_scalar_collection_keys
   hgl_atomic_set_map_publications
   hgl_growing_list_publications
   hgl_rolling_publications
   hgl_optional_atomic_publications
   hgl_recursive_atomic_publications
   hgl_abstract_atomic_publications
   hgl_composite_collection_keys
   hgl_negative_tests
   hgl_contextual_bindings
   writing_nodes
   parity_matrix
   parity_testing
   surface_triage
   tornado_parity
   perspective_parity
   record_replay_table
   component_recovery_plan
   python_integration
   python_bridge
   type_reflection
   notebook
   testing
   memory_utilisation
