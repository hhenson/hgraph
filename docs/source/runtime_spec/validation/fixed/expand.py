"""Expand independently written snapshots; never import measured observations."""
import json
from pathlib import Path
ROOT = Path(__file__).parent


def assertions():
    rows = []

    def walk(case, value, path):
        if isinstance(value, dict) and path.rsplit('/', 1)[-1] not in ('value', 'delta'):
            if value and path.rsplit('/', 1)[-1] in ('children', 'retired'):
                rows.append(dict(case=case, path=path, projection='keys', expected=sorted(value)))
            if not value:
                rows.append(dict(case=case, path=path, expected=value))
            for key, child in value.items():
                walk(case, child, path + '/' + str(key))
        else:
            rows.append(dict(case=case, path=path, expected=value))

    for case, data in json.loads((ROOT / 'reasoned.json').read_text())['cases'].items():
        for tick, value in enumerate(data['expected']):
            walk(case, value, f'/ticks/{tick}')
        rows.append(dict(case=case, path='/ticks', projection='length', expected=len(data['expected'])))
        if 'events' in data:
            walk(case, data['events'], '/events')
    return rows


if __name__ == '__main__':
    print('[\n' + ',\n'.join('  ' + json.dumps(a) for a in assertions()) + '\n]')
