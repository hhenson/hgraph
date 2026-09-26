"""Compare recorded observations with independently stated JSON assertions."""
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).parent


def at(value, pointer):
    for part in pointer.strip('/').split('/'):
        value = value[int(part)] if isinstance(value, list) else value[part]
    return value


def classify(assertion, observed):
    sides = {}
    for side in ('python', 'cpp'):
        result = observed[side]
        assert result['stable'] and len(result['replay_digests']) == 3 and len(set(result['replay_digests'])) == 1
        if result['status'] != 'ok':
            sides[side] = {'unavailable': result['status']}
        else:
            try:
                sides[side] = at(result['observation'], assertion['path'])
            except (KeyError, IndexError):
                sides[side] = {'unavailable': 'missing observation'}
    matches = [side for side in sides if sides[side] == assertion['expected']]
    status = ('both-agree' if len(matches) == 2 else
              'accepted-with-variation' if matches else
              'recheck-reasoning' if sides['python'] == sides['cpp'] else
              'needs-decision')
    return {**assertion, **sides, 'status': status}


def main():
    assertions = json.loads((ROOT / 'assertions.json').read_text())
    observed = json.loads((ROOT / 'observed.json').read_text())['cases']
    results = [classify(a, observed[a['case']]) for a in assertions]
    decisions = json.loads((ROOT / 'decisions.json').read_text())
    for result in results:
        for decision in decisions:
            if (result['case'], result['path']) == (decision['case'], decision['path']):
                assert result['expected'] == decision['expected']
                result['status'] = decision['status']
                result['rationale'] = decision['rationale']
    print(json.dumps(dict(Counter(r['status'] for r in results)), sort_keys=True))
    for result in results:
        if result['status'] in ('needs-decision', 'recheck-reasoning'):
            print(json.dumps(result, sort_keys=True))
    (ROOT / 'assessment.json').write_text(
        '[\n' + ',\n'.join('  ' + json.dumps(r) for r in results) + '\n]\n')
    if any(r['status'] in ('needs-decision', 'recheck-reasoning') for r in results):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
