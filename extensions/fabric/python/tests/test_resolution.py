from __future__ import annotations

from datetime import datetime, timedelta
import hgraph_fabric as hgf


BASE = datetime(2027, 1, 15)


def revision(
    data_id: str,
    revision_id: int,
    output_version: int,
    dependencies: tuple[tuple[str, int], ...] = (),
    self_predecessor: int | None = None,
) -> hgf.DataRevision:
    return hgf.DataRevision(
        format_version=1,
        data_id=data_id,
        revision=revision_id,
        output_version=output_version,
        dependencies=tuple(hgf.DataDependency(*item) for item in dependencies),
        self_predecessor=self_predecessor,
        as_of=BASE + timedelta(microseconds=revision_id),
    )


def bootstrap() -> list[hgf.DataRevision]:
    values: list[hgf.DataRevision] = []
    for revision_id in range(1, 4):
        values.append(revision("D1", revision_id, revision_id))
        values.append(
            revision(
                "D2",
                revision_id,
                revision_id,
                (("D1", revision_id),),
            )
        )
    values.extend(
        [
            revision("D3", 1, 1, (("D2", 1),)),
            revision("D3", 2, 2, (("D2", 2),)),
        ]
    )
    return values


def cut(result) -> dict[str, tuple[int, int]]:
    return {data_id: (revision_id, version) for data_id, revision_id, version in result["cut"]}


def test_python_uses_native_resolver_for_bootstrap_and_held_parent_release():
    initial = hgf._resolve_fixture(tuple(bootstrap()), ("D1", "D3"))
    assert initial["status"] is hgf.ResolutionStatus.READY
    assert cut(initial) == {"D1": (2, 2), "D2": (2, 2), "D3": (2, 2)}
    assert initial["changed_roots"] == [("D1", 2, 2), ("D3", 2, 2)]

    released_history = bootstrap()
    released_history.append(revision("D3", 3, 2, (("D2", 3),)))
    released = hgf._resolve_fixture(
        tuple(released_history),
        ("D1", "D3"),
        (("D1", 2), ("D3", 2)),
    )
    assert released["status"] is hgf.ResolutionStatus.READY
    assert cut(released)["D3"] == (3, 2)
    assert released["changed_roots"] == [("D1", 3, 3)]


def test_python_native_resolver_rolls_same_output_ancestry_newest_first():
    count = 40
    history: list[hgf.DataRevision] = []
    for revision_id in range(1, count + 1):
        history.append(revision("P", revision_id, revision_id))
        history.append(
            revision(
                "A",
                revision_id,
                count + 1,
                (("P", revision_id),),
            )
        )
    history.append(revision("D", 1, 1, (("A", count + 1),)))

    result = hgf._resolve_fixture(tuple(history), ("D", "P"))
    assert result["status"] is hgf.ResolutionStatus.READY
    assert cut(result)["A"] == (count, count + 1)
    assert result["metrics"]["output_index_hits"] >= count


def test_python_native_resolver_reports_ambiguous_cyclic_and_self_audit():
    ambiguous = hgf._resolve_fixture(
        (
            revision("X", 1, 1),
            revision("X", 2, 2),
            revision("A", 1, 1, (("X", 1),)),
            revision("A", 2, 2, (("X", 2),)),
            revision("B", 1, 1, (("X", 2),)),
            revision("B", 2, 2, (("X", 1),)),
        ),
        ("A", "B"),
    )
    assert ambiguous["status"] is hgf.ResolutionStatus.AMBIGUOUS

    cyclic = hgf._resolve_fixture(
        (
            revision("A", 1, 1, (("B", 1),)),
            revision("B", 1, 1, (("A", 1),)),
        ),
        ("A",),
    )
    assert cyclic["status"] is hgf.ResolutionStatus.CYCLIC

    self_audit = hgf._resolve_fixture(
        (revision("A", 1, 1), revision("A", 2, 2, self_predecessor=1)),
        ("A",),
    )
    assert self_audit["status"] is hgf.ResolutionStatus.READY
    assert cut(self_audit)["A"] == (2, 2)
