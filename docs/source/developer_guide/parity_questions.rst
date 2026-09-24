Open parity questions
=====================

Questions raised while triaging parity issues that neither the runtime
specification nor an earlier ruling decides. Each names the issue, the
observations, the options and a recommendation; the owner's answer moves it
into :doc:`parity_matrix` (accepted) or into a fix.

``compare``'s ``recordable_id`` (issue #818 item 5.4)
-----------------------------------------------------

Released hgraph 0.5.41 declares ``compare(lhs, rhs)`` and reads the
``recordable_id`` trait of the enclosing component at run time. This runtime
declares ``compare(lhs, rhs, recordable_id)`` and requires the id at wiring.
The accepted call shapes are disjoint:

.. list-table::
   :header-rows: 1

   * - Call, outside any component
     - Released hgraph 0.5.41
     - This runtime
   * - ``compare(a, b)``
     - wires; fails at stop with ``ValueError: Trait recordable_id not found``
     - ``WiringError``: no overload with two arguments
   * - ``compare(a, b, recordable_id="x")``
     - ``WiringError``: no such parameter
     - wires and compares under ``"x"``

The decision recorded on #818 was "every spelling resolves the same way in
both runtimes", which cannot be met without deleting the documented
``recordable_id=`` spelling here.

Options:

A. **Default ``recordable_id`` to empty, resolved from the component trait
   when empty** (recommended). ``compare(a, b)`` wires, as released hgraph
   does, and inside a component compares under the component's id. Outside
   one it fails for the missing id, as released hgraph does, but at wiring
   rather than at stop. The explicit keyword stays as a documented superset.
   The acceptance criterion on #818 becomes "every released spelling works".
B. **Remove the parameter** and read only the trait, exactly as released
   hgraph. Breaks the documented ``recordable_id=`` spelling.
C. **Keep the current signature** and accept the difference.
