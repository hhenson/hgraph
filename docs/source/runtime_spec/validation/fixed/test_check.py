"""Acceptance must distinguish agreement, missing evidence and instability."""
import unittest
import check


def side(value, status='ok'):
    return {'status': status, 'observation': {'ticks': [value]},
            'stable': True, 'replay_digests': ['same'] * 3}


class AcceptanceTests(unittest.TestCase):
    def classify(self, expected, python, cpp):
        return check.classify({'case': 'test', 'path': '/ticks/0', 'expected': expected},
                              {'python': python, 'cpp': cpp})['status']

    def test_three_way_agreement_and_variation(self):
        self.assertEqual(self.classify(7, side(7), side(7)), 'both-agree')
        self.assertEqual(self.classify(7, side(7), side(8)), 'accepted-with-variation')

    def test_reasoning_must_be_rechecked(self):
        self.assertEqual(self.classify(7, side(8), side(8)), 'recheck-reasoning')
        self.assertEqual(self.classify(7, side(8), side(9)), 'needs-decision')

    def test_failure_keeps_prefix_but_not_missing_suffix(self):
        partial = side(7, 'error')
        self.assertEqual(self.classify(7, partial, side(7)), 'both-agree')
        partial['observation']['ticks'] = []
        self.assertEqual(self.classify(7, partial, side(8)), 'unvalidated')

    def test_missing_evidence_is_not_a_negative_result(self):
        missing = side(None, 'error')
        missing['observation'] = None
        self.assertEqual(self.classify(None, missing, missing), 'unvalidated')

    def test_missing_side_cannot_accept_a_matching_side(self):
        for observation in (None, {'ticks': []}):
            missing = side(None, 'error')
            missing['observation'] = observation
            self.assertEqual(self.classify(7, missing, side(7)), 'unvalidated')
            self.assertEqual(self.classify(7, side(7), missing), 'unvalidated')

    def test_observed_null_and_unavailable_named_field_are_values(self):
        for value in (None, {'unavailable': 'a real field'}):
            self.assertEqual(self.classify(value, side(value), side(value)), 'both-agree')

    def test_unstable_replays_cannot_be_accepted(self):
        unstable = side(7)
        unstable['replay_digests'][-1] = 'different'
        with self.assertRaises(AssertionError):
            self.classify(7, unstable, side(7))

    def test_ruling_changes_expectation_but_preserves_measured_disagreement(self):
        assertions = [{'case': 'test', 'path': '/ticks/0', 'expected': 7}]
        decisions = [{'id': 'ruling', 'rationale': 'Explicit user choice', 'assertions': [
            {'case': 'test', 'path': '/ticks/0', 'previous_expected': 7, 'expected': 9}]}]
        check.apply_decisions(assertions, decisions)
        result = check.classify(assertions[0], {'python': side(7), 'cpp': side(8)})
        self.assertEqual(result['status'], 'accepted-by-decision')
        self.assertEqual(result['comparison'], 'needs-decision')
        self.assertEqual(result['previous_expected'], 7)
        self.assertEqual(result['expected'], 9)
        self.assertEqual((result['python'], result['cpp']), (7, 8))
        agreed = check.classify(assertions[0], {'python': side(7), 'cpp': side(7)})
        self.assertEqual(agreed['status'], 'accepted-by-decision')
        self.assertEqual(agreed['comparison'], 'recheck-reasoning')

    def test_ruling_does_not_fill_missing_evidence(self):
        assertion = {'case': 'test', 'path': '/ticks/0', 'expected': 7, 'decision': 'ruling'}
        missing = side(None, 'error')
        missing['observation'] = None
        result = check.classify(assertion, {'python': missing, 'cpp': side(7)})
        self.assertEqual(result['status'], 'unvalidated')

    def test_stale_duplicate_and_unknown_rulings_are_rejected(self):
        assertion = {'case': 'test', 'path': '/ticks/0', 'expected': 7}
        change = {'case': 'test', 'path': '/ticks/0', 'previous_expected': 7, 'expected': 9}
        decision = {'id': 'ruling', 'rationale': 'Explicit user choice', 'assertions': [change]}
        with self.assertRaises(AssertionError):
            check.apply_decisions([{**assertion, 'expected': True}], [decision])
        with self.assertRaises(AssertionError):
            check.apply_decisions([assertion.copy()], [{**decision, 'assertions': [change, change]}])
        with self.assertRaises(KeyError):
            check.apply_decisions([], [decision])

    def test_scalar_types_are_preserved(self):
        self.assertEqual(self.classify(False, side(0), side(0)), 'recheck-reasoning')
        self.assertEqual(self.classify(0, side(False), side(0)), 'accepted-with-variation')


if __name__ == '__main__':
    unittest.main()
