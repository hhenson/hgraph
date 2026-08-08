"""Repository-level guards for compatibility evidence hidden by pytest markers."""

import ast
from pathlib import Path


TEST_ROOT = Path(__file__).parent


def _test_modules():
    yield from sorted(TEST_ROOT.rglob("test_*.py"))


def _marker_name(decorator: ast.expr) -> str | None:
    value = decorator.func if isinstance(decorator, ast.Call) else decorator
    parts = []
    while isinstance(value, ast.Attribute):
        parts.append(value.attr)
        value = value.value
    if isinstance(value, ast.Name):
        parts.append(value.id)
    return ".".join(reversed(parts))


def _skip_reason(decorator: ast.expr) -> str | None:
    if not isinstance(decorator, ast.Call):
        return None
    for keyword in decorator.keywords:
        if keyword.arg == "reason" and isinstance(keyword.value, ast.Constant):
            return keyword.value.value
    if decorator.args and isinstance(decorator.args[0], ast.Constant):
        return decorator.args[0].value
    return None


def test_release_suite_has_no_hidden_wip_or_xfail_tests():
    hidden = []
    for path in _test_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                for decorator in node.decorator_list:
                    marker = _marker_name(decorator)
                    if marker in {"pytest.mark.wip", "pytest.mark.xfail"}:
                        hidden.append(f"{path.relative_to(TEST_ROOT)}:{node.lineno}: {marker}")
            elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                value = node.value
                if value is not None and _marker_name(value) in {
                    "pytest.mark.wip",
                    "pytest.mark.xfail",
                }:
                    hidden.append(
                        f"{path.relative_to(TEST_ROOT)}:{node.lineno}: {_marker_name(value)}"
                    )
    assert hidden == [], "release suite contains hidden tests:\n" + "\n".join(hidden)


def test_static_skip_markers_are_classified():
    unclassified = []
    for path in _test_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _marker_name(node.func) == "pytest.skip":
                reason = (
                    node.args[0].value
                    if node.args and isinstance(node.args[0], ast.Constant)
                    else None
                )
                if not isinstance(reason, str) or not reason.startswith(("deviation:", "gap:")):
                    unclassified.append(
                        f"{path.relative_to(TEST_ROOT)}:{node.lineno}: {reason!r}"
                    )
                continue
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            for decorator in node.decorator_list:
                if _marker_name(decorator) != "pytest.mark.skip":
                    continue
                reason = _skip_reason(decorator)
                if not isinstance(reason, str) or not reason.startswith(("deviation:", "gap:")):
                    unclassified.append(
                        f"{path.relative_to(TEST_ROOT)}:{node.lineno}: {reason!r}"
                    )
    assert unclassified == [], "unclassified compatibility skips:\n" + "\n".join(unclassified)
