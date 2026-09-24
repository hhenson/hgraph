Open parity questions
=====================

Questions raised while triaging parity issues that neither the runtime
specification nor an earlier ruling decides. Each names the issue, the
observations, the options and a recommendation; the owner's answer moves it
into :doc:`parity_matrix` (accepted) or into a fix.

``compare``'s ``recordable_id`` (issue #818 item 5.4)
-----------------------------------------------------

Released hgraph 0.5.41 declares ``compare(lhs, rhs)`` and reads the
``recordable_id`` trait of the enclosing component; outside a component it
fails at stop with ``ValueError: Trait recordable_id not found``. This runtime
has two ``compare`` backends, which already answer differently:

.. list-table::
   :header-rows: 1

   * - Call, outside any component
     - Released hgraph 0.5.41
     - Here, core in-memory backend
     - Here, persistence frame backend
   * - ``compare(a, b)``
     - wires; fails at stop (no trait)
     - ``WiringError``: ``recordable_id`` is required
     - wires (``recordable_id`` defaults to empty); fails at start (no id,
       no trait)
   * - ``compare(a, b, recordable_id="x")``
     - ``WiringError``: no such parameter
     - wires and compares under ``"x"``
     - wires and compares under ``"x"``

Inside a component, released hgraph and the frame backend both take the
component's id. The decision recorded on #818, "every spelling resolves the
same way in both runtimes", cannot be met without deleting the documented
``recordable_id=`` spelling.

Options:

A. **Default ``recordable_id`` to empty in the core backend too**
   (recommended), so ``compare(a, b)`` wires on every backend, as released
   hgraph does, and takes the component's id. The explicit keyword stays as
   a documented superset. The default alone is not enough outside a
   component: the core recorder then runs a "throw-only" comparison with no
   id and no summary, where released hgraph fails. So A also needs a rule for
   "no id and no trait". The frame backend's rule is to fail at start, one
   phase before released hgraph's failure at stop. The acceptance criterion
   on #818 would become "every released spelling works".
B. **Remove the parameter** and read only the trait, exactly as released
   hgraph. Breaks the documented ``recordable_id=`` spelling on both backends.
C. **Keep the current signatures** and accept the difference.
