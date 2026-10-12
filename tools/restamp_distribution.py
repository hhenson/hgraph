"""Restamp already-tested wheels and sdists to a release tag version."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import posixpath
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
        # Normalisation must be the identity: a name that normpath would
        # rewrite still carries a traversal or a redundant segment.
        or posixpath.normpath(normalized) != normalized
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
    archive_mtime = int(path.stat().st_mtime)
    root_prefix = new_root + "/"
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
            # Relocate under the new root and check the normalised name
            # itself, the exact string the new entry is given, with one
            # prefix test; the root directory entry is written under its own
            # constant name and never consults the source name.
            relocated = posixpath.normpath(posixpath.join(new_root, *member_path.parts[1:]))
            if relocated != new_root and not relocated.startswith(root_prefix):
                raise ValueError(f"sdist member escapes its root: {member.name!r}")
            # A fresh entry carries the vetted name and the entry kind, with
            # the conventional reproducible header otherwise (0644/0755, owner
            # 0, the archive file's own timestamp); nothing from the source
            # entry's header reaches the new archive. An sdist holds files and
            # directories only.
            if not (member.isfile() or member.isdir()):
                raise ValueError(f"unsupported sdist member type: {member.name!r}")
            is_directory = member.isdir()
            if relocated == new_root:
                entry = tarfile.TarInfo(new_root)
            else:
                if not relocated.startswith(root_prefix):
                    raise ValueError(f"sdist member escapes its root: {relocated!r}")
                entry = tarfile.TarInfo(relocated)
            entry.type = tarfile.DIRTYPE if is_directory else tarfile.REGTYPE
            entry.mode = 0o755 if is_directory else 0o644
            entry.mtime = archive_mtime
            entry.uid = 0
            entry.gid = 0
            if entry.name == f"{new_root}/PKG-INFO":
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
            elif entry.name == f"{new_root}/pyproject.toml":
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
                entry.size = len(data)
            members.append((entry, data))

    new_path = path.with_name(f"{new_root}.tar.gz")
    with tarfile.open(new_path, "w:gz") as archive:
        for entry, data in members:
            if data is None:
                archive.addfile(entry)
            else:
                archive.addfile(entry, io.BytesIO(data))
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
