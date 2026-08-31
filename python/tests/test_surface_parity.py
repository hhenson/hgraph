"""Focused contracts for the public API surface classifier."""

import sys
from types import ModuleType

from tools.parity.surface import (
    _describe_module,
    classify_findings,
    compare_surfaces,
)


def test_surface_probe_records_a_failing_lazy_export(monkeypatch):
    module = ModuleType("surface_probe_lazy_export")
    module.__all__ = ["available", "optional"]
    module.available = lambda: None

    def resolve(name):
        if name == "optional":
            raise ModuleNotFoundError("optional distribution is not installed")
        raise AttributeError(name)

    module.__getattr__ = resolve
    monkeypatch.setitem(sys.modules, module.__name__, module)

    surface = _describe_module(module.__name__)["surface"]

    assert surface["available"]["kind"] == "callable"
    assert surface["optional"] == {
        "kind": "attribute-error",
        "error": "ModuleNotFoundError: optional distribution is not installed",
    }


def test_surface_probe_compares_lazy_export_failure_details():
    def module(error):
        return {
            "hgraph": {
                "surface": {
                    "optional": {
                        "kind": "attribute-error",
                        "error": error,
                    }
                },
                "has_all": True,
            }
        }

    missing = module("ModuleNotFoundError: optional package is not installed")
    broken = module("RuntimeError: lazy resolver failed")

    assert compare_surfaces(missing, missing)["findings"] == []
    assert compare_surfaces(missing, broken)["findings"] == [
        {
            "module": "hgraph",
            "name": "optional",
            "kind": "attribute-error-mismatch",
            "reference_error": "ModuleNotFoundError: optional package is not installed",
            "candidate_error": "RuntimeError: lazy resolver failed",
        }
    ]


def test_surface_probe_reports_json_public_signature_drift():
    def module(to_json, from_json):
        return {
            "hgraph": {
                "surface": {
                    "to_json": {"kind": "callable", "signature": to_json},
                    "from_json": {"kind": "callable", "signature": from_json},
                },
                "has_all": True,
            }
        }

    ts = {"name": "ts", "kind": "POSITIONAL_OR_KEYWORD", "default": None}
    delta = {
        "name": "delta",
        "kind": "POSITIONAL_OR_KEYWORD",
        "default": "False",
    }
    internal_type = {
        "name": "_tp",
        "kind": "POSITIONAL_OR_KEYWORD",
        "default": "AUTO_RESOLVE",
    }
    reference = module([ts, delta], [ts])
    candidate = module([ts, internal_type, delta], [ts, internal_type, delta])

    findings = compare_surfaces(reference, candidate)["findings"]

    assert [(finding["name"], finding["kind"]) for finding in findings] == [
        ("from_json", "signature-mismatch"),
        ("to_json", "signature-mismatch"),
    ]


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
