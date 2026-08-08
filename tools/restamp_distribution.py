"""Restamp already-tested wheels and sdists to a release tag version."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import re
import tarfile
import zipfile
from pathlib import Path


def _record_hash(data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest())
    return "sha256=" + digest.decode().rstrip("=")


def restamp_wheel(path: Path, version: str) -> Path:
    name, old_version, _ = path.name.split("-", 2)
    if old_version == version:
        print(f"{path.name}: already at {version}")
        return path

    old_info = f"{name}-{old_version}.dist-info"
    new_info = f"{name}-{version}.dist-info"
    entries: dict[str, bytes] = {}
    with zipfile.ZipFile(path) as archive:
        for item in archive.infolist():
            entries[item.filename] = archive.read(item.filename)

    renamed: dict[str, bytes] = {}
    for item_name, data in entries.items():
        new_name = item_name.replace(old_info + "/", new_info + "/")
        if new_name == f"{new_info}/METADATA":
            metadata = data.decode()
            metadata, count = re.subn(
                rf"(?m)^Version: {re.escape(old_version)}$",
                f"Version: {version}",
                metadata,
                count=1,
            )
            if count != 1:
                raise ValueError(f"{path}: wheel METADATA Version line not found")
            data = metadata.encode()
        renamed[new_name] = data

    record_name = f"{new_info}/RECORD"
    rows: list[list[str]] = []
    for row in csv.reader(io.StringIO(renamed[record_name].decode())):
        if not row:
            continue
        entry = row[0].replace(old_info + "/", new_info + "/")
        if entry == record_name:
            rows.append([entry, "", ""])
        else:
            data = renamed[entry]
            rows.append([entry, _record_hash(data), str(len(data))])
    buffer = io.StringIO(newline="")
    csv.writer(buffer, lineterminator="\n").writerows(rows)
    renamed[record_name] = buffer.getvalue().encode()

    new_path = path.with_name(path.name.replace(f"-{old_version}-", f"-{version}-", 1))
    with zipfile.ZipFile(new_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for item_name, data in renamed.items():
            archive.writestr(item_name, data)
    if new_path != path:
        path.unlink()
    print(f"{path.name} -> {new_path.name}")
    return new_path


def restamp_sdist(path: Path, version: str) -> Path:
    name, old_version = path.name.removesuffix(".tar.gz").rsplit("-", 1)
    if old_version == version:
        print(f"{path.name}: already at {version}")
        return path

    old_root = f"{name}-{old_version}"
    new_root = f"{name}-{version}"
    members: list[tuple[tarfile.TarInfo, bytes | None]] = []
    with tarfile.open(path, "r:gz") as archive:
        for member in archive.getmembers():
            source = archive.extractfile(member) if member.isfile() else None
            data = source.read() if source is not None else None
            member.name = member.name.replace(old_root, new_root, 1)
            member.pax_headers.pop("path", None)
            member.pax_headers.pop("linkpath", None)
            if member.name == f"{new_root}/PKG-INFO":
                metadata = data.decode()
                metadata, count = re.subn(
                    rf"(?m)^Version: {re.escape(old_version)}$",
                    f"Version: {version}",
                    metadata,
                    count=1,
                )
                if count != 1:
                    raise ValueError(f"{path}: sdist PKG-INFO Version line not found")
                data = metadata.encode()
            elif member.name == f"{new_root}/pyproject.toml":
                project = data.decode()
                project, count = re.subn(
                    r'(?m)^version = "[^"]+"$',
                    f'version = "{version}"',
                    project,
                    count=1,
                )
                if count != 1:
                    raise ValueError(f"{path}: sdist pyproject version not found")
                data = project.encode()
            if data is not None:
                member.size = len(data)
            members.append((member, data))

    new_path = path.with_name(f"{new_root}.tar.gz")
    with tarfile.open(new_path, "w:gz") as archive:
        for member, data in members:
            archive.addfile(member, io.BytesIO(data) if data is not None else None)
    if new_path != path:
        path.unlink()
    print(f"{path.name} -> {new_path.name}")
    return new_path


def restamp_directory(directory: Path, version: str) -> None:
    distributions = sorted(directory.glob("*.whl")) + sorted(directory.glob("*.tar.gz"))
    if not distributions:
        raise FileNotFoundError(f"no wheel or sdist found in {directory}")
    for distribution in distributions:
        if distribution.suffix == ".whl":
            restamp_wheel(distribution, version)
        else:
            restamp_sdist(distribution, version)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("version")
    args = parser.parse_args()
    restamp_directory(args.directory, args.version)


if __name__ == "__main__":
    main()
