Connect services and adaptors
=============================

Use a service when graph code needs a typed logical interface. Use an adaptor
when that interface crosses an external-system boundary.

Service workflow
----------------

#. Declare a ``reference_service``, ``subscription_service`` or
   ``request_reply_service``.
#. Implement it with ``service_impl``.
#. Register the implementation once while wiring the application.
#. Call the declared service stub from graph code.

This keeps clients independent of transport and allows implementations to be
replaced at wiring time. ``get_service_inputs`` and ``set_service_output`` are
for implementation plumbing, not ordinary clients.

Adaptor workflow
----------------

#. Install the extra required by the adaptor family.
#. Import from its documented ``hgraph.adaptors.<name>`` package.
#. Register server-side or shared implementations once in the application.
#. Keep transport configuration scalar so it is fixed while wiring.

Core-only helpers such as ``json``, ``dataclass``, ``executor`` and
``run_graph_on_thread`` do not require a packaging extra. SQL, Delta,
data-frame, Tornado and Perspective integrations do. See
:doc:`../../adaptors/index` and :doc:`../../../reference/modules` for the
module and installation matrix.

Extension boundary
------------------

Kafka ships as ``hgraph-kafka`` because it contains its own native extension.
Third-party extensions should depend on the installed hgraph SDK and expose a
first-class C++ wiring path, with Python adapting values and callables to that
path where appropriate.
