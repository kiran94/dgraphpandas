import unittest
from parameterized import parameterized

import pandas as pd

from dgraphpandas.writers.upserts import generate_upserts


class UpsertTests(unittest.TestCase):

    @parameterized.expand([
        (None, pd.DataFrame()),
        (pd.DataFrame(), None),
    ])
    def test_generate_upserts_null_parameters(self, intrinsic, edges):
        '''
        Ensures when null parameters are provided, then
        an error is raised
        '''
        with self.assertRaises(ValueError):
            generate_upserts(intrinsic, edges)

    @parameterized.expand([
        ###
        (
            'subject_no_exist',
            pd.DataFrame(data={
                        'predicate': ['age', 'hair'],
                        'object': [23, 'black'],
                        'type': ['<xs:int>', '<xs:string>']
            }),
        ),
        ###
        (
            'predicate_no_exist',
            pd.DataFrame(data={
                        'subject': ['customer_1', 'customer_2'],
                        'object': [23, 'black'],
                        'type': ['<xs:int>', '<xs:string>']
            }),
        ),
        ###
        (
            'object_no_exist',
            pd.DataFrame(data={
                        'subject': ['customer_1', 'customer_2'],
                        'predicate': ['age', 'hair'],
                        'type': ['<xs:int>', '<xs:string>']
            }),
        ),
        ###
        (
            'type_no_exist',
            pd.DataFrame(data={
                        'subject': ['customer_1', 'customer_2'],
                        'predicate': ['age', 'hair'],
                        'object': [23, 'black'],
            }),
        )
    ])
    def test_generate_upsert_intrinsic_has_bad_columns(self, name, intrinsic):
        '''
        Ensures when the intrinsic frame does not have the correct
        columns, then an exception is raised
        '''
        edges = pd.DataFrame(columns=['subject', 'predicate', 'object', 'type'])
        with self.assertRaises(ValueError):
            generate_upserts(intrinsic, edges)

    @parameterized.expand([
        ###
        (
            'subject_no_exist',
            pd.DataFrame(data={
                        'predicate': ['location', 'knows'],
                        'object': ['loc34', 'customer_2'],
            }),
        ),
        ###
        (
            'predicate_no_exist',
            pd.DataFrame(data={
                        'subject': ['customer_1', 'customer_2'],
                        'object': ['loc34', 'customer_2'],
            }),
        ),
        ###
        (
            'object_no_exist',
            pd.DataFrame(data={
                        'subject': ['customer_1', 'customer_2'],
                        'predicate': ['location', 'knows'],
            }),
        )
    ])
    def test_generate_upsert_edges_has_bad_columns(self, name, edges):
        '''
        Ensures when the edges frame does not have the correct
        columns, then an exception is raised
        '''
        intrinsic = pd.DataFrame(columns=['subject', 'predicate', 'object', 'type'])
        with self.assertRaises(ValueError):
            generate_upserts(intrinsic, edges)

    @parameterized.expand([
        ###
        (
            'single_record',
            pd.DataFrame(data={
                        'subject': ['customer_1'],
                        'predicate': ['age'],
                        'object': [23],
                        'type': ['<xs:int>']
            }),
            ['<customer_1> <age> "23"^^<xs:int> .']
        ),
        ###
        (
            'multiple_records',
            pd.DataFrame(data={
                        'subject': ['customer_1', 'customer_2', 'customer_1', 'customer_3'],
                        'predicate': ['age', 'hair', 'dob', 'weight'],
                        'object': [23, 'black', '20210302T00:00:00', 1.32],
                        'type': ['<xs:int>', '<xs:string>', '<xs:dateTime>', '<xs:float>']
            }),
            [
                '<customer_1> <age> "23"^^<xs:int> .',
                '<customer_2> <hair> "black"^^<xs:string> .',
                '<customer_1> <dob> "20210302T00:00:00"^^<xs:dateTime> .',
                '<customer_3> <weight> "1.32"^^<xs:float> .'
            ]
        )
    ])
    def test_generate_upsert_only_intrinsic(self, name, intrinsic, expected_output):
        '''
        Ensures when only intrinsic data is passed, we generate upserts only for intrinsic.
        '''
        edges = pd.DataFrame(columns=['subject', 'predicate', 'object', 'type'])

        dql_intrinsic, dql_edges = generate_upserts(intrinsic, edges)

        self.assertEqual(len(dql_edges), 0)
        self.assertEqual(dql_intrinsic, expected_output)

    @parameterized.expand([
        ###
        (
            'single_record',
            pd.DataFrame(data={
                        'subject': ['customer_1'],
                        'predicate': ['location'],
                        'object': ['loc_32'],
                        'type': [None]
            }),
            ['<customer_1> <location> <loc_32> .']
        ),
        ###
        (
            'multiple_records',
            pd.DataFrame(data={
                        'subject': ['customer_1', 'customer_3', 'customer_2'],
                        'predicate': ['location', 'group', 'location'],
                        'object': ['loc_32', 'group_89', 'loc_90'],
                        'type': [None]*3
            }),
            [
                '<customer_1> <location> <loc_32> .',
                '<customer_3> <group> <group_89> .',
                '<customer_2> <location> <loc_90> .'
            ]
        )
    ])
    def test_generate_upsert_only_edges(self, name, edges, expected_output):
        '''
        Ensures when there are only edges, the expected output is generated
        '''
        intrinsic = pd.DataFrame(columns=['subject', 'predicate', 'object', 'type'])

        dql_intrinsic, dql_edges = generate_upserts(intrinsic, edges)

        self.assertEqual(len(dql_intrinsic), 0)
        self.assertEqual(dql_edges, expected_output)

    @parameterized.expand([
        ###
        (
            'single_intrinsic_and_edge',
            pd.DataFrame(data={
                        'subject': ['customer_1'],
                        'predicate': ['age'],
                        'object': [23],
                        'type': ['<xs:int>']
            }),
            pd.DataFrame(data={
                        'subject': ['customer_1'],
                        'predicate': ['location'],
                        'object': ['loc_32'],
                        'type': [None]
            }),
            [
                '<customer_1> <age> "23"^^<xs:int> .',
                '<customer_1> <location> <loc_32> .'
            ]
        ),
        ###
        (
            'multiple_records',
            pd.DataFrame(data={
                        'subject': ['customer_1', 'customer_2', 'customer_1', 'customer_3'],
                        'predicate': ['age', 'hair', 'dob', 'weight'],
                        'object': [23, 'black', '20210302T00:00:00', 1.32],
                        'type': ['<xs:int>', '<xs:string>', '<xs:dateTime>', '<xs:float>']
            }),
            pd.DataFrame(data={
                        'subject': ['customer_1', 'customer_3', 'customer_2'],
                        'predicate': ['location', 'group', 'location'],
                        'object': ['loc_32', 'group_89', 'loc_90'],
                        'type': [None]*3
            }),
            [
                '<customer_1> <age> "23"^^<xs:int> .',
                '<customer_2> <hair> "black"^^<xs:string> .',
                '<customer_1> <dob> "20210302T00:00:00"^^<xs:dateTime> .',
                '<customer_3> <weight> "1.32"^^<xs:float> .',
                '<customer_1> <location> <loc_32> .',
                '<customer_3> <group> <group_89> .',
                '<customer_2> <location> <loc_90> .'
            ]
        )
    ])
    def test_generate_upsert_both_intrinsic_edges(self, name, intrinsic, edges, expected_output):
        '''
        Ensures when there are both intrinsic and edges, the expected output is generated
        '''
        dql_intrinsic, dql_edges = generate_upserts(intrinsic, edges)
        all_dql = dql_intrinsic + dql_edges
        self.assertEqual(all_dql, expected_output)
