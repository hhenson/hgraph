"""Restamp already-tested wheels and sdists to a release tag version."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import re
import stat
import tarfile
import zipfile
from pathlib import Path, PurePosixPath


def _record_hash(data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest())
    return "sha256=" + digest.decode().rstrip("=")


def _safe_archive_name(name: str, *, required_root: str | None = None) -> str:
    normalized = name.rstrip("/")
    path = PurePosixPath(normalized)
    if (
        not normalized
        or "\\" in name
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in normalized.split("/"))
        or (required_root is not None and path.parts[0] != required_root)
    ):
        raise ValueError(f"unsafe archive member path: {name!r}")
    return normalized + ("/" if name.endswith("/") else "")


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
            item_name = _safe_archive_name(item.filename)
            file_type = (item.external_attr >> 16) & 0o170000
            if file_type == stat.S_IFLNK:
                raise ValueError(f"symbolic links are not allowed in wheels: {item_name!r}")
            if item_name in entries:
                raise ValueError(f"duplicate wheel member: {item_name!r}")
            entries[item_name] = archive.read(item)

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
    seen_names: set[str] = set()
    with tarfile.open(path, "r:gz") as archive:
        for member in archive.getmembers():
            member.name = _safe_archive_name(member.name, required_root=old_root)
            if member.name in seen_names:
                raise ValueError(f"duplicate sdist member: {member.name!r}")
            seen_names.add(member.name)
            if member.issym() or member.islnk():
                raise ValueError(
                    f"symbolic and hard links are not allowed in sdists: {member.name!r}"
                )
            source = archive.extractfile(member) if member.isfile() else None
            data = source.read() if source is not None else None
            member_path = PurePosixPath(member.name)
            member.name = str(PurePosixPath(new_root, *member_path.parts[1:]))
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
