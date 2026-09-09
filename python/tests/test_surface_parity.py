"""Focused contracts for the public API surface classifier."""

import sys
from types import ModuleType

from tools.parity.surface import (
    _describe_callable,
    _describe_module,
    classify_findings,
    compare_surfaces,
)


class _Native:
    """A stand-in for a nanobind method: no introspectable signature, but a
    declaration on the first line of ``__doc__`` (surface_triage.rst)."""

    def __init__(self, doc):
        self.__doc__ = doc

    def __call__(self, *args, **kwargs):  # pragma: no cover - never invoked
        raise AssertionError

    @property
    def __signature__(self):
        raise ValueError("no signature found for builtin")


def test_surface_probe_reads_a_native_callable_declared_signature():
    described = _describe_callable(
        _Native("debug(self, msg: object, *args, **kwargs) -> None")
    )

    assert described["signature"] == [
        {"name": "self", "kind": "POSITIONAL_OR_KEYWORD", "default": None},
        {"name": "msg", "kind": "POSITIONAL_OR_KEYWORD", "default": None},
        {"name": "args", "kind": "VAR_POSITIONAL", "default": None},
        {"name": "kwargs", "kind": "VAR_KEYWORD", "default": None},
    ]


def test_surface_probe_records_native_defaults_and_keyword_only_parameters():
    described = _describe_callable(
        _Native("get_trait_or(self, name: str, /, *, default: object = None) -> object")
    )

    assert described["signature"] == [
        {"name": "self", "kind": "POSITIONAL_ONLY", "default": None},
        {"name": "name", "kind": "POSITIONAL_ONLY", "default": None},
        {"name": "default", "kind": "KEYWORD_ONLY", "default": "None"},
    ]


def test_surface_probe_leaves_an_overloaded_native_signature_unavailable():
    overloaded = _Native(
        "schedule(self, when: datetime.datetime, tag: str | None = None) -> None\n"
        "schedule(self, when: datetime.timedelta, tag: str | None = None) -> None\n"
        "\n"
        "Schedule the node."
    )

    assert _describe_callable(overloaded) == {"signature": None}
    assert _describe_callable(_Native("A prose docstring, not a declaration.")) == {
        "signature": None
    }


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
