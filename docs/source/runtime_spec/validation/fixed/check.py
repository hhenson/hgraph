"""Compare recorded observations with independently stated JSON assertions."""
import json
import hashlib
from collections import Counter
from pathlib import Path
from expand import assertions as expand_assertions

ROOT = Path(__file__).parent


def at(value, pointer):
    for part in pointer.strip('/').split('/'):
        value = value[int(part)] if isinstance(value, list) else value[part]
    return value


def equal(a, b):
    return json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)


def classify(assertion, observed):
    sides = {}
    for side in ('python', 'cpp'):
        result = observed[side]
        assert result['stable'] and len(result['replay_digests']) == 3 and len(set(result['replay_digests'])) == 1
        if result.get('observation') is None:
            sides[side] = {'unavailable': result['status']}
        else:
            try:
                value = at(result['observation'], assertion['path'])
                if assertion.get('projection') == 'keys':
                    value = sorted(value.keys())
                elif assertion.get('projection') == 'length':
                    value = len(value)
                sides[side] = value
            except (KeyError, IndexError, TypeError, AttributeError):
                sides[side] = {'unavailable': 'missing observation'}
    matches = [side for side in sides if equal(sides[side], assertion['expected'])]
    status = ('both-agree' if len(matches) == 2 else
              'accepted-with-variation' if matches else
              'unvalidated' if any(isinstance(v, dict) and 'unavailable' in v for v in sides.values()) else
              'recheck-reasoning' if equal(sides['python'], sides['cpp']) else
              'needs-decision')
    return {**assertion, **sides, 'status': status}


def main():
    assertions = expand_assertions()
    evidence = json.loads((ROOT / 'observed.json').read_text())
    observed = evidence['cases']
    provenance = evidence['provenance']
    for field, filename in (('adapter_sha256', 'adapter.patch'), ('reasoning_sha256', 'reasoned.json')):
        assert provenance[field] == hashlib.sha256((ROOT / filename).read_bytes()).hexdigest(), filename + ' changed after recording'
    for case, sides in observed.items():
        recipe = json.loads((ROOT / 'recipes' / (case + '.json')).read_text())
        recipe_digest = hashlib.sha256(json.dumps(recipe, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
        for side in sides.values():
            assert side['recipe_fingerprint'] == recipe_digest, case + ' recipe changed after recording'
            content = {k: side[k] for k in ('status', 'observation', 'exception')}
            recorded_digest = hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()
            assert all(recorded_digest == digest for digest in side['replay_digests']), case + ' observation fingerprint differs'
    corrections = json.loads((ROOT / 'reasoning_corrections.json').read_text())
    for assertion in assertions:
        for correction in corrections:
            if assertion['case'] in correction['cases'] and assertion['path'] == correction['path']:
                assert assertion['expected'] == correction['initial']
                assertion['initial'] = assertion['expected']
                assertion['expected'] = correction['expected']
                assertion['correction'] = correction['reason']
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
        if result['status'] in ('needs-decision', 'recheck-reasoning', 'unvalidated'):
            print(json.dumps(result, sort_keys=True))
    summary = {'counts': dict(Counter(r['status'] for r in results)),
               'cases': {case: dict(Counter(r['status'] for r in results if r['case'] == case))
                         for case in sorted(observed)}}
    prefix = json.dumps(summary, indent=2)[:-2]
    differences = [r for r in results if r['status'] != 'both-agree']
    (ROOT / 'assessment.json').write_text(
        prefix + ',\n  "differences": [\n' +
        ',\n'.join('    ' + json.dumps(r) for r in differences) + '\n  ]\n}\n')
    if any(r['status'] in ('needs-decision', 'recheck-reasoning', 'unvalidated') for r in results):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
