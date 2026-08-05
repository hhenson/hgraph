"""Focused contracts for the public API surface classifier."""

from tools.parity.surface import classify_findings


def test_surface_rule_with_exact_signatures_does_not_mask_later_drift():
    reference = [
        {"name": "value", "kind": "POSITIONAL_OR_KEYWORD", "default": None},
    ]
    candidate = [
        {"name": "value", "kind": "POSITIONAL_OR_KEYWORD", "default": "None"},
    ]
    rule = {
        "module": "hgraph.example",
        "name": "example",
        "kind": "signature-mismatch",
        "reference": reference,
        "candidate": candidate,
        "reason": "the candidate deliberately widens the argument",
    }
    finding = {
        "module": "hgraph.example",
        "name": "example",
        "kind": "signature-mismatch",
        "reference": reference,
        "candidate": candidate,
    }

    actionable, accepted = classify_findings([finding], [rule])
    assert not actionable
    assert accepted[0]["accepted_reason"] == rule["reason"]

    changed = {
        **finding,
        "candidate": [
            {"name": "renamed", "kind": "POSITIONAL_OR_KEYWORD", "default": "None"},
        ],
    }
    actionable, accepted = classify_findings([changed], [rule])
    assert actionable == [changed]
    assert not accepted
