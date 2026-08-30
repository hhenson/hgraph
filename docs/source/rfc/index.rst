hgraph RFCs
===========

The RFC catalogue records proposed and accepted changes to hgraph's public
types, runtime, operator model, extension surface, and cross-language contract.
RFCs live with the core code they govern. Domain-specific proposals remain in
their downstream repository unless and until they require a generally useful
hgraph change.

The lifecycle, required sections, numbering, and proposal/implementation
workflow are defined by :doc:`rfc_0000`.

Research notes
--------------

Research notes capture design evidence and open questions before a numbered
RFC fixes a public contract. They are informative rather than normative.

.. toctree::
   :maxdepth: 1

   research_layered_network_services

RFC catalogue
-------------

.. toctree::
   :maxdepth: 2

   rfc_0000
   rfc_0001_typed_frame_metadata
   rfc_0002_temporal_types
   rfc_0003_extension_scalar_registration
   rfc_0004_python_owned_structured_scalars
   rfc_0005_hgraph_1_0_api
   rfc_0006_tsw_reset_and_clear
   rfc_0007_scheduled_duration_tsw_eviction
   rfc_0008_prepared_node_inputs
   rfc_0009_time_series_endpoint_visitors
   rfc_0010_value_view_visitors
   rfc_0011_source_only_adaptor_collapse
   rfc_0012_replyless_request_reply_relay
   rfc_0013_pooled_polymorphic_compound_scalars
   rfc_0014_request_reply_transport_planning
   rfc_0015_kafka_extension_api
   rfc_0016_object_store_frame_persistence
   rfc_0017_binary_value_codec
   rfc_0018_analytics_relative_change
   rfc_0019_native_table_recording
   rfc_0020_analytics_statistics
   rfc_0021_recording_versions
   rfc_0022_serializable_graph_manifest
   rfc_0023_graph_checkpoint_recovery
   rfc_0024_web_extension_api
   rfc_0025_hgraph_persistence
   rfc_0026_versioned_dataflow_fabric
   rfc_0027_bounded_push_source_queues
   rfc_0028_shared_value_representation
   rfc_0029_value_pool_ownership_and_binding
   rfc_0030_typed_value_persistence
