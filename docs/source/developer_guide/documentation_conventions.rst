Documentation Conventions
=========================

Developer documentation may use Mermaid diagrams for lightweight architecture sketches and LaTeX-style math for runtime invariants.

Mermaid
-------

.. mermaid::

   flowchart LR
      Wiring[Wiring] --> Runtime[Runtime Graph]
      Runtime --> Scheduler[Scheduler]
      Scheduler --> Nodes[Node Evaluation]

Math
----

Use Sphinx math directives for equations and MyST dollar math in Markdown pages.

.. math::

   rank(parent) < rank(child)

This invariant expresses the expected topological ordering for forward graph evaluation.
