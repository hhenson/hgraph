"""Fingerprint an interpreter's installed hgraph sources and distribution artifacts."""
import hashlib
import importlib.metadata
import json
from pathlib import Path


def source_hashes(root):
    return {path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(root.rglob('*'))
            if path.is_file() and '__pycache__' not in path.parts
            and path.suffix not in ('.pyc', '.pyo')}


def reference_identity(package, distribution):
    sources = source_hashes(Path(package.__file__).resolve().parent)
    artifacts = {}
    for entry in sorted(distribution.files or [], key=str):
        if '__pycache__' in entry.parts or entry.suffix in ('.pyc', '.pyo'):
            continue
        path = Path(distribution.locate_file(entry))
        if not path.is_file():
            raise ValueError('Missing package artifact: ' + str(entry))
        artifacts[entry.as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    if not sources or not artifacts:
        raise ValueError('Package sources and distribution artifacts are required')
    content = {'version': distribution.version, 'sources_sha256': sources, 'artifacts_sha256': artifacts}
    return {**content, 'identity_sha256': hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()}


def main():
    import hgraph
    print(json.dumps(reference_identity(hgraph, importlib.metadata.distribution('hgraph'))))


if __name__ == '__main__':
    main()
