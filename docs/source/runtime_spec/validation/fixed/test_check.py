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

    def test_unstable_replays_cannot_be_accepted(self):
        unstable = side(7)
        unstable['replay_digests'][-1] = 'different'
        with self.assertRaises(AssertionError):
            self.classify(7, unstable, side(7))

    def test_scalar_types_are_preserved(self):
        self.assertEqual(self.classify(False, side(0), side(0)), 'recheck-reasoning')
        self.assertEqual(self.classify(0, side(False), side(0)), 'accepted-with-variation')


if __name__ == '__main__':
    unittest.main()
