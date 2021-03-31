import unittest
import pandas as pd

from pandas.testing import assert_frame_equal
from parameterized import parameterized

from dgraphpandas.strategies.vertical_helpers import (_expand_csv_edges, _join_key_fields)


class VerticalHelpers(unittest.TestCase):

    @parameterized.expand([(None, ['csv_edge'])])
    def test_expand_csv_edges_null_parameters_exception(self, frame, csv_edges):
        '''
        Ensures when any of the parameters are invalid,
        an exception is raised
        '''
        with self.assertRaises(ValueError):
            _expand_csv_edges(frame, csv_edges)

    @parameterized.expand([
        (None,),
        ([],)
    ])
    def test_expand_csv_edges_no_csv_edges_same_frame_returned(self, csv_edges):
        '''
        Ensures when there are no csv_edges passed, then the same frame is returned.
        '''
        frame = pd.DataFrame(data={
            'subject': ['subject1', 'subject2', 'subject3'],
            'predicate': ['predicate1', 'predicate2', 'predicate3'],
            'object': ['object1', 'object2', 'object3']
        })

        result_frame = _expand_csv_edges(frame, csv_edges)
        assert_frame_equal(frame, result_frame)

    def test_expand_csv_edges_csv_edges_provided_but_no_eligible_values(self):
        '''
        Ensures when csv_edges are passed but the dataframe itself has no
        eligible values (which are in csv) then the same frame is returned
        '''
        csv_edges = ['predicate1']
        frame = pd.DataFrame(data={
            'subject': ['subject1', 'subject2', 'subject3'],
            'predicate': ['predicate1', 'predicate2', 'predicate3'],
            'object': ['object1', 'object2', 'object3']
        })

        result_frame = _expand_csv_edges(frame, csv_edges)

        expected = frame.sort_index().sort_values(by='subject')
        output = result_frame.sort_index().sort_values(by='subject')
        assert_frame_equal(expected, output)

    def test_expand_csv_edges_csv_edge_not_found_frame_unchanged(self):
        '''
        Ensures when the given csv edge does not actually exist
        on the frame, then the frame is unchanged
        '''
        csv_edges = ['predicate_no_exist']
        frame = pd.DataFrame(data={
            'subject': ['subject1', 'subject2', 'subject3'],
            'predicate': ['predicate1', 'predicate2', 'predicate3'],
            'object': ['object1', 'object2', 'object3']
        })

        result_frame = _expand_csv_edges(frame, csv_edges)

        expected = frame.sort_index().sort_values(by='subject')
        output = result_frame.sort_index().sort_values(by='subject')
        assert_frame_equal(expected, output)

    def test_expand_csv_edges_field_found(self):
        '''
        Ensures when the csv edge is found on the frame,
        the records are split out, new records are generated
        and the subject, predicate are copied out.

        Also ensures the default seperator is csv
        '''
        csv_edges = ['predicate2']
        frame = pd.DataFrame(data={
            'subject': ['subject1', 'subject2', 'subject3'],
            'predicate': ['predicate1', 'predicate2', 'predicate3'],
            'object': ['object1', 'object2a,object2b,object2c', 'object3']
        })

        expected = pd.DataFrame(data={
            'subject': ['subject1', 'subject3', 'subject2', 'subject2', 'subject2'],
            'predicate': ['predicate1', 'predicate3', 'predicate2', 'predicate2', 'predicate2'],
            'object': ['object1', 'object3', 'object2a', 'object2b', 'object2c']
        })

        result_frame = _expand_csv_edges(frame, csv_edges)

        actual = expected.reset_index(drop=True).sort_index()
        output = result_frame.reset_index(drop=True).sort_index()
        assert_frame_equal(actual, output)


    @parameterized.expand([('|',), ('#',)])
    def test_expand_csv_edges_alternative_seperator(self, seperator: str):
        '''
        Ensures when alternative separators are passed, they are broken
        up correctly.
        '''
        csv_edges = ['predicate2']
        frame = pd.DataFrame(data={
            'subject': ['subject1', 'subject2', 'subject3'],
            'predicate': ['predicate1', 'predicate2', 'predicate3'],
            'object': ['object1', seperator.join(['object2a', 'object2b', 'object2c']), 'object3']
        })

        expected = pd.DataFrame(data={
            'subject': ['subject1', 'subject3', 'subject2', 'subject2', 'subject2'],
            'predicate': ['predicate1', 'predicate3', 'predicate2', 'predicate2', 'predicate2'],
            'object': ['object1', 'object3', 'object2a', 'object2b', 'object2c']
        })

        result_frame = _expand_csv_edges(frame, csv_edges, seperator=seperator)

        actual = expected.reset_index(drop=True).sort_index()
        output = result_frame.reset_index(drop=True).sort_index()
        assert_frame_equal(actual, output)

    @parameterized.expand([
        (None, ['key'], 'key_seperator', 'type'),
        (pd.DataFrame(), None, 'key_seperator', 'type'),
        (pd.DataFrame(), ['key'], None, 'type'),
        (pd.DataFrame(), ['key'], 'key_seperator', None),
    ])
    def test_join_key_fields_null_parameters_exception(self, frame, key, key_seperator, type):
        '''
        Ensures when any of the input parameters are null, then
        an error is raised.
        '''
        with self.assertRaises(ValueError):
            _join_key_fields(frame, key, key_seperator, type)

    def test_join_key_fields_one_key_subject_created(self):
        '''
        Ensures when there is only one key, the
        subject is constructed and prefixed with type
        '''
        key = ['customer_id']
        key_seperator = '_'
        type = 'customer'
        frame = pd.DataFrame(data={
            'customer_id': [1, 2, 3],
            'predicate': ['age', 'age', 'age'],
            'object': [23, 45, 12],
        })

        expected = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_2', 'customer_3'],
            'predicate': ['age', 'age', 'age'],
            'object': [23, 45, 12],
        })

        result_frame = _join_key_fields(frame, key, key_seperator, type)

        actual = expected.reset_index(drop=True)[['subject', 'predicate', 'object']]
        output = result_frame.reset_index(drop=True)[['subject', 'predicate', 'object']]
        assert_frame_equal(actual, output)

    def test_join_key_fields_multiple_keys_subject_created(self):
        '''
        Ensures when there are multiple keys, then they are
        concatted together and the type is suffixed.
        '''
        key = ['customer_id', 'order_id']
        key_seperator = '_'
        type = 'order'
        frame = pd.DataFrame(data={
            'customer_id': [1, 2, 3],
            'order_id': [10, 22, 36],
            'predicate': ['value', 'value', 'value'],
            'object': [45.4, 76.9, 12.3],
        })

        expected = pd.DataFrame(data={
            'subject': ['order_1_10', 'order_2_22', 'order_3_36'],
            'predicate': ['value', 'value', 'value'],
            'object': [45.4, 76.9, 12.3],
        })

        result_frame = _join_key_fields(frame, key, key_seperator, type)

        actual = expected.reset_index(drop=True)[['subject', 'predicate', 'object']]
        output = result_frame.reset_index(drop=True)[['subject', 'predicate', 'object']]
        assert_frame_equal(actual, output)
