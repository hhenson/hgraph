import csv
import io
import tarfile
import zipfile
from pathlib import Path

from tools.restamp_distribution import restamp_sdist, restamp_wheel


def test_restamp_wheel_updates_metadata_directory_and_record(tmp_path: Path):
    old_info = "hgraph_kafka-0.1.0.dist-info"
    wheel = tmp_path / "hgraph_kafka-0.1.0-cp312-abi3-any.whl"
    entries = {
        "hgraph_kafka/__init__.py": b"",
        f"{old_info}/METADATA": b"Name: hgraph-kafka\nVersion: 0.1.0\n",
        f"{old_info}/WHEEL": b"Wheel-Version: 1.0\n",
    }
    record_name = f"{old_info}/RECORD"
    record = io.StringIO(newline="")
    csv.writer(record, lineterminator="\n").writerows(
        [[name, "", ""] for name in (*entries, record_name)]
    )
    entries[record_name] = record.getvalue().encode()
    with zipfile.ZipFile(wheel, "w") as archive:
        for name, data in entries.items():
            archive.writestr(name, data)

    stamped = restamp_wheel(wheel, "0.2.0")

    assert stamped.name == "hgraph_kafka-0.2.0-cp312-abi3-any.whl"
    assert not wheel.exists()
    with zipfile.ZipFile(stamped) as archive:
        names = set(archive.namelist())
        new_info = "hgraph_kafka-0.2.0.dist-info"
        assert f"{new_info}/METADATA" in names
        assert old_info not in "\n".join(names)
        assert b"Version: 0.2.0" in archive.read(f"{new_info}/METADATA")
        rows = list(
            csv.reader(io.StringIO(archive.read(f"{new_info}/RECORD").decode()))
        )
        assert rows[-1] == [f"{new_info}/RECORD", "", ""]
        assert all(row[1].startswith("sha256=") for row in rows[:-1])


def test_restamp_sdist_updates_root_metadata_and_project(tmp_path: Path):
    old_root = "hgraph_kafka-0.1.0"
    sdist = tmp_path / f"{old_root}.tar.gz"
    entries = {
        f"{old_root}/PKG-INFO": b"Name: hgraph-kafka\nVersion: 0.1.0\n",
        f"{old_root}/pyproject.toml": (
            b'[project]\nname = "hgraph-kafka"\nversion = "0.1.0"\n'
        ),
    }
    with tarfile.open(sdist, "w:gz") as archive:
        for name, data in entries.items():
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))

    stamped = restamp_sdist(sdist, "0.2.0")

    assert stamped.name == "hgraph_kafka-0.2.0.tar.gz"
    assert not sdist.exists()
    with tarfile.open(stamped) as archive:
        new_root = "hgraph_kafka-0.2.0"
        assert set(archive.getnames()) == {
            f"{new_root}/PKG-INFO",
            f"{new_root}/pyproject.toml",
        }
        metadata = archive.extractfile(f"{new_root}/PKG-INFO")
        project = archive.extractfile(f"{new_root}/pyproject.toml")
        assert metadata is not None and b"Version: 0.2.0" in metadata.read()
        assert project is not None and b'version = "0.2.0"' in project.read()
