"""Synthetic regressions for observed clusters and separate entry evidence."""
from collections import Counter
import copy
import hashlib
from pathlib import Path
import tempfile
import unittest

import numpy as np

from clusters import ClusterStore, RegionMatcher, classification, detect, encode, entry_summary


def stable_groups(xy, ids, codes, **kwargs):
    groups, owners = detect(xy, ids, codes, **kwargs)
    return [
        (ids[g['seed']], tuple(ids[i] for i in g['members']), g['anchor_only'])
        for g in groups
    ], {identity: int(owners[i]) for i, identity in enumerate(ids)}


def candidate(node, one, three, distance, pending=False):
    return {'node': node, 'label': node + ' 후보', 'one': one, 'three': three,
            'distance_m': distance, 'pending': pending}


class DetectionTest(unittest.TestCase):
    def test_seed_radius_is_inclusive_but_nearby_outside_facility_is_unassigned(self):
        xy = [[0, 0]] * 8 + [[750, 0], [-750.01, 0]]
        ids = ['a' + str(i) for i in range(8)] + ['boundary', 'outside']
        groups, owners = detect(xy, ids, ['31'] * len(ids))
        self.assertEqual(len(groups), 1)
        self.assertIn(8, groups[0]['members'])
        self.assertNotIn(9, groups[0]['members'])
        self.assertEqual(owners[9], -1)

    def test_a_continuous_medical_street_does_not_chain_into_one_city_cluster(self):
        xy = np.array([[100 * i, 0] for i in range(46)], dtype=float)
        ids = [f'f{i:03}' for i in range(len(xy))]
        groups, owners = detect(xy, ids, ['31'] * len(ids))
        self.assertGreater(len(groups), 1)
        # Single-linkage would connect every adjacent pair, spanning 4.5 km.
        self.assertLessEqual(max(np.diff(xy[:, 0])), 750)
        assigned = []
        for gi, group in enumerate(groups):
            members = group['members']
            distances = np.linalg.norm(xy[members] - xy[group['seed']], axis=1)
            self.assertTrue(np.all(distances <= 750))
            self.assertLessEqual(float(np.ptp(xy[members, 0])), 1500)
            self.assertTrue(all(0 <= owners[i] <= gi for i in members))
            assigned.extend(members)
        self.assertEqual(set(assigned), set(np.flatnonzero(owners >= 0)))
        self.assertGreater(len(assigned), len(set(assigned)))
        for i in set(assigned):
            self.assertEqual(owners[i], next(j for j, group in enumerate(groups) if i in group['members']))
        centers = xy[[group['seed'] for group in groups]]
        for i in range(len(centers)):
            self.assertTrue(np.all(np.linalg.norm(centers[i + 1:] - centers[i], axis=1) > 750))

    def test_input_permutation_cannot_change_seeds_memberships_or_anchor_status(self):
        xy = np.array([[100 * i, 0] for i in range(37)] + [[10000, 0], [10010, 0]])
        ids = [f'f{i:03}' for i in range(len(xy))]
        codes = ['31'] * 37 + ['11', '51']
        expected = stable_groups(xy, ids, codes)
        for seed in range(8):
            order = np.random.default_rng(seed).permutation(len(ids))
            result = stable_groups(xy[order], [ids[i] for i in order], [codes[i] for i in order])
            self.assertEqual(result, expected)

    def test_minimum_eight_is_distinct_from_a_single_general_hospital_anchor(self):
        groups, owners = detect([[0, 0]] * 7, [str(i) for i in range(7)], ['31'] * 7)
        self.assertEqual(groups, [])
        self.assertTrue(np.all(owners == -1))
        groups, _ = detect([[0, 0]] * 8, [str(i) for i in range(8)], ['31'] * 8)
        self.assertEqual(len(groups), 1)
        self.assertFalse(groups[0]['anchor_only'])
        for code in ('01', '11'):
            groups, owners = detect([[0, 0]], ['hospital'], [code])
            self.assertEqual(groups[0]['members'], [0])
            self.assertTrue(groups[0]['anchor_only'])
            self.assertEqual(owners.tolist(), [0])
        for code in ('21', '41', '92'):
            self.assertEqual(detect([[0, 0]], ['hospital'], [code])[0], [])

    def test_small_anchor_includes_overlapping_facility_but_keeps_first_primary_owner(self):
        xy = [[-500, 0]] * 8 + [[0, 0]] * 8 + [[500, 0], [1200, 0], [5000, 0]]
        ids = [f'a{i:02}' for i in range(17)] + ['general', 'unassigned']
        groups, owners = detect(xy, ids, ['31'] * 17 + ['11', '51'])
        self.assertEqual(len(groups), 2)
        # Both circles report the edge clinic, while coverage counts it once.
        self.assertIn(16, groups[0]['members'])
        self.assertEqual(groups[1]['members'], [16, 17])
        self.assertTrue(groups[1]['anchor_only'])
        self.assertEqual(owners[16], 0)
        self.assertEqual(owners[17], 1)
        self.assertEqual(owners[-1], -1)
        self.assertEqual(len({i for g in groups for i in g['members']}), 18)

    def test_overlapping_circles_count_all_facilities_and_residuals_cannot_shrink_scale(self):
        # First center reports 78 institutions. The second has 31 within its
        # full circle, even though only its seed lacks a primary owner.
        xy = [[-500, 0]] * 40 + [[0, 0]] * 8 + [[500, 0]] * 30 + [[1200, 0]]
        ids = [f'f{i:03}' for i in range(len(xy))]
        for last_code in ('31', '11'):
            codes = ['31'] * 78 + [last_code]
            groups, owners = detect(xy, ids, codes)
            self.assertEqual([len(g['members']) for g in groups], [78, 31])
            self.assertFalse(groups[1]['anchor_only'])
            self.assertEqual(int(np.sum(owners == 1)), 1)
            self.assertEqual(classification(Counter(codes[i] for i in groups[1]['members']), False)[1],
                             '중규모 집적(30–99)')
            overlap = set(groups[0]['members']) & set(groups[1]['members'])
            self.assertEqual(len(overlap), 30)
            self.assertTrue(all(owners[i] == 0 for i in overlap))
            self.assertEqual(int(np.sum(owners >= 0)), 79)
            self.assertEqual(len(set(i for group in groups for i in group['members'])), 79)
            for group in groups:
                distances = np.linalg.norm(np.array(xy) - np.array(xy[group['seed']]), axis=1)
                self.assertEqual(set(group['members']), set(np.flatnonzero(distances <= 750)))

    def test_empty_input_and_corrupt_facilities(self):
        groups, owners = detect([], [], [])
        self.assertEqual(groups, [])
        self.assertEqual(len(owners), 0)
        for xy, ids in [([[0, 0], [1, 1]], ['duplicate', 'duplicate']),
                        ([[0, 0]], ['a', 'b']), ([[np.nan, 0]], ['a']),
                        ([[0, np.inf]], ['a'])]:
            with self.assertRaisesRegex(ValueError, 'invalid_facility_coordinates_or_ids'):
                detect(xy, ids, ['31'] * len(ids))


class ClassificationTest(unittest.TestCase):
    def test_medical_and_korean_hospitals_have_separate_classification(self):
        self.assertEqual(classification(Counter({'21': 7, '51': 1}), False)[0], '의과 비중 높은 집적')
        self.assertEqual(classification(Counter({'92': 4, '93': 1, '31': 3}), False)[0], '한의 비중 높은 집적')
        self.assertEqual(classification(Counter({'41': 2, '51': 2, '31': 4}), False)[0], '치과 비중 높은 집적')
        self.assertEqual(classification(Counter({'31': 4, '51': 2, '93': 2}), False)[0], '의과·치과·한의 혼합')

    def test_anchor_and_large_hospital_precede_composition(self):
        self.assertEqual(classification(Counter({'11': 1}), True),
                         ('독립 대형병원 거점', '집적 기준 미달 병원 거점'))
        self.assertEqual(classification(Counter({'01': 1, '93': 20}), False)[0], '대형병원 동반 집적')

    def test_medical_share_and_scale_boundaries_are_explicit(self):
        self.assertEqual(classification(Counter({'31': 13, '51': 7}), False)[0], '의과 비중 높은 집적')
        self.assertEqual(classification(Counter({'31': 12, '51': 8}), False)[0], '의과·치과·한의 혼합')
        for count, scale in [(8, '소규모 집적(8–29)'), (29, '소규모 집적(8–29)'),
                             (30, '중규모 집적(30–99)'), (99, '중규모 집적(30–99)'),
                             (100, '대규모 집적(100기관 이상)')]:
            self.assertEqual(classification(Counter({'31': count}), False)[1], scale)


class RegionMatcherTest(unittest.TestCase):
    def setUp(self):
        pairs = [('서울특별시', '중구'), ('부산광역시', '중구'), ('경기도', '수원시'),
                 ('경기도', '수원시 팔달구'), ('강원도', '춘천시'),
                 ('전라북도', '전주시 완산구'), ('세종특별자치시', '세종시')]
        self.matcher = RegionMatcher({p + '|' + d: {'province': p, 'district': d} for p, d in pairs})

    def test_same_district_name_needs_exact_parent_province(self):
        self.assertEqual(self.matcher.match('서울특별시 중구 세종대로 1'), '서울특별시|중구')
        self.assertEqual(self.matcher.match('부산광역시 중구 중앙대로 1'), '부산광역시|중구')
        for address in ['', '중구 중앙대로 1', '서울 중구 세종대로 1', '서울특별시 중구로 1', '대구광역시 중구']:
            self.assertIsNone(self.matcher.match(address), address)

    def test_longest_administrative_prefix_and_documented_aliases(self):
        self.assertEqual(self.matcher.match('경기도 수원시 팔달구 인계동 1'), '경기도|수원시 팔달구')
        self.assertEqual(self.matcher.match('강원특별자치도 춘천시 춘천로 1'), '강원도|춘천시')
        self.assertEqual(self.matcher.match('전북특별자치도 전주시 완산구'), '전라북도|전주시 완산구')
        self.assertEqual(self.matcher.match('세종특별자치시 한누리대로 1'), '세종특별자치시|세종시')

    def test_ambiguous_longest_prefix_is_not_arbitrarily_assigned(self):
        matcher = RegionMatcher({'first': {'province': '서울특별시', 'district': '중구'},
                                 'second': {'province': '서울특별시', 'district': '중구'}})
        self.assertIsNone(matcher.match('서울특별시 중구 세종대로 1'))


class EntrySummaryTest(unittest.TestCase):
    def test_pending_extremes_excluded_and_nearest_valid_is_representative(self):
        rows = [candidate('pending', 10000, 30000, 0, True),
                candidate('near-low', 10, 30, 100), candidate('far-high', 90, 270, 400)]
        original = copy.deepcopy(rows)
        summary = entry_summary(rows)
        self.assertEqual(summary['status'], 'ok')
        self.assertEqual((summary['one_median'], summary['one_min'], summary['one_max']), (50, 10, 90))
        self.assertEqual(summary['three_median'], 150)
        self.assertEqual(summary['representative_node'], 'near-low')
        self.assertEqual(summary['representative_label'], 'near-low 후보')
        self.assertEqual(rows, original)

    def test_no_candidate_and_all_pending_remain_distinct_missing_states(self):
        for rows, status in [([], 'no_candidate'), ([candidate('p', 100, 300, 10, True)], 'pending')]:
            summary = entry_summary(rows)
            self.assertEqual(summary['status'], status)
            self.assertTrue(all(value is None for key, value in summary.items() if key != 'status'))

    def test_zero_is_observed_value_and_distance_ties_use_stable_node(self):
        rows = [candidate('z', 20, 60, 10), candidate('a', 0, 0, 10)]
        self.assertEqual(entry_summary(rows)['one_min'], 0)
        self.assertEqual(entry_summary(rows)['representative_node'], 'a')
        self.assertEqual(entry_summary(rows), entry_summary(list(reversed(rows))))


class ClusterStoreTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        row = {'id': 'cluster-a', 'label': '합성 집적', 'entry': entry_summary([candidate('n1', 10, 30, 0)])}
        self.data = {'summary': {'clusters': 1}, 'definitions': {'baseline_unchanged': True},
                     'rows': [row], 'details': {'cluster-a': {'facilities': [{'id': 'f1'}],
                     'candidates': [candidate('n1', 10, 30, 0)], 'geometry': {'type': 'Polygon'}}},
                     'node_map': {'n1': 'cluster-a'},
                     'pins': {'baseline_release_sha256': 'baseline', 'baseline_source_pins': {'source': 'pin'}}}
        raw = encode(self.data)
        self.release = hashlib.sha256(raw).hexdigest()[:20]
        directory = self.root / self.release
        directory.mkdir()
        self.data_path = directory / 'data.json'
        self.data_path.write_bytes(raw)
        self.pointer = {'release': self.release, 'sha256': hashlib.sha256(raw).hexdigest()}
        (self.root / 'latest.json').write_bytes(encode(self.pointer))

    def test_index_detail_and_candidate_use_same_verified_release(self):
        store = ClusterStore(self.root)
        index = store.index()
        detail = store.detail('cluster-a')
        linked = store.candidate('n1')
        self.assertEqual(index['release'], self.release)
        self.assertEqual(index['rows'][0], detail['cluster'])
        self.assertEqual(linked['cluster'], detail['cluster'])
        self.assertEqual(linked['candidate']['node'], 'n1')
        self.assertEqual(linked['status'], 'ok')
        self.assertEqual(store.candidate('no-match')['status'], 'unlinked')
        with self.assertRaises(KeyError):
            store.detail('missing-cluster')

    def test_payload_tampering_is_rejected_before_reading_data(self):
        self.data_path.write_bytes(self.data_path.read_bytes() + b' ')
        with self.assertRaisesRegex(ValueError, 'cluster_hash_mismatch'):
            ClusterStore(self.root)

    def test_invalid_release_cannot_traverse_to_another_path(self):
        for release in ['../other-release', 'f' * 19, 'F' * 20, 'g' * 20]:
            (self.root / 'latest.json').write_bytes(encode(dict(self.pointer, release=release)))
            with self.assertRaisesRegex(ValueError, 'invalid_cluster_release'):
                ClusterStore(self.root)


if __name__ == '__main__':
    unittest.main()
