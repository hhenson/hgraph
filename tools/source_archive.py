"""Export a release source archive including pinned, materialized shared inputs."""
import argparse
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--prefix', default='hgraph')
args = parser.parse_args()
if not args.prefix or '/' in args.prefix or '\\' in args.prefix or args.prefix in ('.', '..'):
    parser.error('prefix must be one directory name')
subprocess.run([sys.executable, str(ROOT / 'tools/shared_artifacts.py'), '--check'], check=True)
subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--'], cwd=ROOT, check=True)
archive = subprocess.check_output(['git', 'archive', '--format=tar', '--prefix=' + args.prefix + '/', 'HEAD'], cwd=ROOT)
manifest = json.loads((ROOT / 'shared-artifacts.json').read_text())
args.output.parent.mkdir(parents=True, exist_ok=True)
with tarfile.open(fileobj=io.BytesIO(archive)) as source, tarfile.open(args.output, 'w:gz') as target:
    for member in source:
        target.addfile(member, source.extractfile(member) if member.isfile() else None)
    for name in sorted(manifest['files']):
        data = (ROOT / name).read_bytes()
        member = tarfile.TarInfo(args.prefix + '/' + name)
        member.mode = 0o644
        member.size = len(data)
        target.addfile(member, io.BytesIO(data))
print(args.output)
