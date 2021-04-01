from datetime import datetime
import unittest
from unittest.case import expectedFailure
from unittest.mock import patch, Mock
import numpy as np

import pandas as pd
from pandas.testing import assert_frame_equal
from parameterized import parameterized

from dgraphpandas.strategies.vertical_helpers import (
    _compile_illegal_characters_regex, _expand_csv_edges, _join_key_fields, _add_dgraph_type_records,
    _break_up_intrinsic_and_edges, _apply_rdf_types, _format_date_fields, _remove_illegal_rdf_characters, _remove_na_objects)
from dgraphpandas.types import default_rdf_type


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

        result_frame = _expand_csv_edges(frame.copy(), csv_edges)
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

        result_frame = _expand_csv_edges(frame.copy(), csv_edges)

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

        result_frame = _expand_csv_edges(frame.copy(), csv_edges)

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

        result_frame = _expand_csv_edges(frame.copy(), csv_edges)

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

        result_frame = _expand_csv_edges(frame.copy(), csv_edges, seperator=seperator)

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

        result_frame = _join_key_fields(frame.copy(), key, key_seperator, type)

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

        result_frame = _join_key_fields(frame.copy(), key, key_seperator, type)

        actual = expected.reset_index(drop=True)[['subject', 'predicate', 'object']]
        output = result_frame.reset_index(drop=True)[['subject', 'predicate', 'object']]
        assert_frame_equal(actual, output)

    def test_add_dgraph_type_records_enabled_records_added(self):
        '''
        Ensures when add_dgraph_type_records is passed, then dgraph.type
        records are generated and appended to the DataFrame.
        '''
        type = 'customer'
        frame = pd.DataFrame(data={
            'subject': [1, 2, 3],
            'predicate': ['age', 'age', 'age'],
            'object': [23, 45, 12],
        })

        result_frame = _add_dgraph_type_records(frame.copy(), add_dgraph_type_records=True, type=type)

        expected = pd.DataFrame(data={
            'subject': [1, 2, 3, 1, 2, 3],
            'predicate': ['age']*3 + ['dgraph.type']*3,
            'object': [23, 45, 12] + [type]*3
        })

        actual = expected.reset_index(drop=True)[['subject', 'predicate', 'object']]
        output = result_frame.reset_index(drop=True)[['subject', 'predicate', 'object']]

        self.assertEqual(6, result_frame.shape[0])
        without_dgraph_type = result_frame[~result_frame['predicate'].isin(['dgraph.type'])]
        with_dgraph_type = result_frame[result_frame['predicate'].isin(['dgraph.type'])]
        self.assertEqual(3, without_dgraph_type.shape[0])
        self.assertEqual(3, with_dgraph_type.shape[0])
        self.assertEqual(with_dgraph_type['predicate'].values.tolist(), ['dgraph.type']*3)
        self.assertEqual(without_dgraph_type['predicate'].values.tolist(), ['age']*3)

        assert_frame_equal(actual, output)

    def test_add_dgraph_type_records_disabled_records_added(self):
        '''
        Ensures when add_dgraph_type_records is disabled, then
        the frame remains unchanged
        '''
        type = 'customer'
        frame = pd.DataFrame(data={
            'subject': [1, 2, 3],
            'predicate': ['age', 'age', 'age'],
            'object': [23, 45, 12],
        })

        result_frame = _add_dgraph_type_records(frame.copy(), add_dgraph_type_records=False, type=type)

        actual = frame.reset_index(drop=True)[['subject', 'predicate', 'object']]
        output = result_frame.reset_index(drop=True)[['subject', 'predicate', 'object']]
        assert_frame_equal(actual, output)

    def test_break_up_intrinsic_and_edges_null_parameter_exception(self):
        '''
        Ensures when parameters are null, then an exception is raised.
        '''
        with self.assertRaises(ValueError):
            _break_up_intrinsic_and_edges(None, ['edges'])

    @parameterized.expand([
        (None,),
        ([],)
    ])
    def test_break_up_intrinsic_and_edges_no_edges_only_intrinsic_and_default_edges(self, edges):
        '''
        Ensures when no edges have been defined, then only intrinsic has data
        and edges is the default.
        '''
        frame = pd.DataFrame(data={
            'subject': [1, 2, 3],
            'predicate': ['age', 'age', 'age'],
            'object': [23, 45, 12],
        })

        intrinsic, edges = _break_up_intrinsic_and_edges(frame.copy(), edges)

        actual = frame.reset_index(drop=True)[['subject', 'predicate', 'object']]
        output = intrinsic.reset_index(drop=True)[['subject', 'predicate', 'object']]
        assert_frame_equal(actual, output)

        self.assertTrue(edges.empty)
        self.assertEqual(edges.columns.tolist(), ['subject', 'predicate', 'object', 'type'])

    def test_break_up_intrinsic_and_edges_edges_exist_default_id_stripped(self):
        '''
        Ensures when edges exist they are split into edges frame
        and by default _id is stripped
        '''

        frame = pd.DataFrame(data={
            'subject': ['1', '2', '3', '34'],
            'predicate': ['age', 'location_id', 'age', 'class_id'],
            'object': ['23', 'london', '12', 'first'],
        })

        expected_intrinsic = pd.DataFrame(data={
            'subject': ['1', '3'],
            'predicate': ['age', 'age'],
            'object': ['23', '12'],
        })

        expected_edges = pd.DataFrame(data={
            'subject': ['2', '34'],
            'predicate': ['location', 'class'],
            'object': ['london', 'first'],
        })

        intrinsic, edges = _break_up_intrinsic_and_edges(frame.copy(), ['location_id', 'class_id'])

        expected_intrinsic = expected_intrinsic.reset_index(drop=True)[['subject', 'predicate', 'object']]
        actual_intrinsic = intrinsic.reset_index(drop=True)[['subject', 'predicate', 'object']]
        assert_frame_equal(expected_intrinsic, actual_intrinsic)

        expected_edges = expected_edges.reset_index(drop=True)[['subject', 'predicate', 'object']]
        actual_intrinsic = edges.reset_index(drop=True)[['subject', 'predicate', 'object']]
        assert_frame_equal(expected_edges, actual_intrinsic)

    @parameterized.expand([
        (True,),
        (False,)
    ])
    def test_break_up_intrinsic_and_edges_edges_exist_id_stripped_passed(self, strip_id_from_edge_names: bool):
        '''
        Ensures when the strip_id_from_edge_names parameter is passed
        the edge names are stripped accordingly.
        '''
        frame = pd.DataFrame(data={
            'subject': ['1', '2', '3', '34'],
            'predicate': ['age', 'location_id', 'age', 'class_id'],
            'object': ['23', 'london', '12', 'first'],
        })

        expected_intrinsic = pd.DataFrame(data={
            'subject': ['1', '3'],
            'predicate': ['age', 'age'],
            'object': ['23', '12'],
        })

        expected_edges = pd.DataFrame(data={
            'subject': ['2', '34'],
            'predicate': (['location', 'class'] if strip_id_from_edge_names else ['location_id', 'class_id']),
            'object': ['london', 'first'],
        })

        intrinsic, edges = _break_up_intrinsic_and_edges(frame.copy(), ['location_id', 'class_id'], strip_id_from_edge_names)

        expected_intrinsic = expected_intrinsic.reset_index(drop=True)[['subject', 'predicate', 'object']]
        actual_intrinsic = intrinsic.reset_index(drop=True)[['subject', 'predicate', 'object']]
        assert_frame_equal(expected_intrinsic, actual_intrinsic)

        expected_edges = expected_edges.reset_index(drop=True)[['subject', 'predicate', 'object']]
        actual_edges = edges.reset_index(drop=True)[['subject', 'predicate', 'object']]
        assert_frame_equal(expected_edges, actual_edges)

    @parameterized.expand([
        (None, {'column1': '<xs:int>'}),
        (pd.DataFrame(), None)
    ])
    def test_apply_rdf_types_nullparameters_exception(self, frame, types):
        '''
        Ensures when either of the parameters is null, then a value exception is raised
        '''
        with self.assertRaises(ValueError):
            _apply_rdf_types(frame, types)

    def test_apply_rdf_types_some_columns_to_map(self):
        '''
        Ensures when some of the columns have matching RDF types
        they are mapped and the unmapped are given the default rdf type
        '''
        frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_1', 'customer_1'],
            'predicate':  ['hair_colour', 'dob', 'weight'],
            'object':  ['black', '1921-05-03T00:00:00', '50']
        })

        types = {
            'dob': 'datetime64',
            'weight': 'int32'
        }

        expected_frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_1', 'customer_1'],
            'predicate':  ['hair_colour', 'dob', 'weight'],
            'object':  ['black', '1921-05-03T00:00:00', '50'],
            'type':  [default_rdf_type, '<xs:dateTime>', '<xs:int>']
        })

        result_frame = _apply_rdf_types(frame.copy(), types)
        assert_frame_equal(result_frame, expected_frame)

    def test_apply_rdf_types_all_columns_to_map(self):
        '''
        Ensures when all columns have a mapping, they are mapped to their
        corresponding rdf type
        '''
        frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_1', 'customer_1'],
            'predicate':  ['hair_colour', 'dob', 'weight'],
            'object':  ['black', '1921-05-03T00:00:00', '50']
        })

        types = {
            'hair_colour': 'object',
            'dob': 'datetime64',
            'weight': 'int32'
        }

        expected_frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_1', 'customer_1'],
            'predicate':  ['hair_colour', 'dob', 'weight'],
            'object':  ['black', '1921-05-03T00:00:00', '50'],
            'type':  ['<xs:string>', '<xs:dateTime>', '<xs:int>']
        })

        result_frame = _apply_rdf_types(frame.copy(), types)
        assert_frame_equal(result_frame, expected_frame)

    def test_apply_rdf_types_no_columns_to_map(self):
        '''
        Ensures when there are no mappings defined, then all columns
        are given the default rdf type
        '''
        frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_1', 'customer_1'],
            'predicate':  ['hair_colour', 'dob', 'weight'],
            'object':  ['black', '1921-05-03T00:00:00', '50']
        })

        types = {}

        expected_frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_1', 'customer_1'],
            'predicate':  ['hair_colour', 'dob', 'weight'],
            'object':  ['black', '1921-05-03T00:00:00', '50'],
            'type':  [default_rdf_type]*3
        })

        result_frame = _apply_rdf_types(frame.copy(), types)
        assert_frame_equal(result_frame, expected_frame)

    def test_format_date_fields_null_frame_error(self):
        '''
        Ensures when parameters are null, then an error is raised
        '''
        with self.assertRaises(ValueError):
            _format_date_fields(None)

    def test_format_date_fields_no_date_fields(self):
        '''
        Ensures when there are no date fields,
        then the frame is unchanged
        '''
        frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_1', 'customer_1'],
            'predicate':  ['hair_colour', 'height', 'weight'],
            'object':  ['black', '172', '50'],
            'type':  ['<xs:string>', '<xs:int>', '<xs:int>']
        })

        result_frame = _format_date_fields(frame.copy())
        assert_frame_equal(frame, result_frame)

    def test_format_date_fields_date_fields_exist(self):
        '''
        Ensures when there are fields marked with datetime type
        then they are converted into ISO format.
        '''
        frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_1', 'customer_1'],
            'predicate':  ['hair_colour', 'dob', 'weight'],
            'object':  ['black', datetime(2021, 1, 4), '50'],
            'type':  ['<xs:string>', '<xs:dateTime>', '<xs:int>']
        })

        expected = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_1', 'customer_1'],
            'predicate':  ['hair_colour', 'dob', 'weight'],
            'object':  ['black', '2021-01-04T00:00:00', '50'],
            'type':  ['<xs:string>', '<xs:dateTime>', '<xs:int>']
        })

        result_frame = _format_date_fields(frame.copy())

        result_frame = result_frame.sort_values(by='predicate').reset_index(drop=True)
        expected = expected.sort_values(by='predicate').reset_index(drop=True)
        assert_frame_equal(result_frame, expected)

    def test_format_date_fields_date_fields_exist_but_not_datetime(self):
        '''
        Ensures when a date field is provided but it's not
        an actual datetime object, an error is raised
        '''
        frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_1', 'customer_1'],
            'predicate':  ['hair_colour', 'dob', 'weight'],
            'object':  ['black', '2021 Jan 21', '50'],
            'type':  ['<xs:string>', '<xs:dateTime>', '<xs:int>']
        })

        with self.assertRaises(AttributeError):
            _format_date_fields(frame)

    def test_compile_illegal_characters_regex_nonecharacters(self):
        '''
        Ensure when none characters are passed, then none
        is returned
        '''
        self.assertIsNone(_compile_illegal_characters_regex(None))

    @parameterized.expand([
        (['@'], '@'),
        (['$', '@'], '$|@'),
        (['\\^', '\\#', '\\~', '\\+'], '\^|\#|\~|\+'),
    ])
    def test_compile_illegal_characters_regex_characterspassed(self, characters, expected_regex):
        '''
        Ensures when the given regex is provided
        then the predefined regex pattern is constructed.
        '''
        regex = _compile_illegal_characters_regex(characters)
        self.assertEqual(expected_regex, regex.pattern)

    @parameterized.expand([
        (None, 'subject'),
        (pd.DataFrame(), None),
        (pd.DataFrame(), ''),
    ])
    def test_remove_illegal_rdf_characters_null_parameters(self, frame, field):
        '''
        Ensures when parameters are null, then an error is raised
        '''
        with self.assertRaises(ValueError):
            _remove_illegal_rdf_characters(frame, ['$', '@'], field)

    def test_remove_illegal_rdf_characters_illegalcharacters_found(self):
        '''
        Ensures when there are illegal characters found
        in the field, then they are removed
        '''
        frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_2'],
            'predicate': ['description', 'description'],
            'object': ['hello @world#', 'this is a fine string']
        })
        illegal_characters = ['@', '#']
        field = 'object'

        expected_frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_2'],
            'predicate': ['description', 'description'],
            'object': ['hello world', 'this is a fine string']
        })

        result_frame = _remove_illegal_rdf_characters(frame.copy(), illegal_characters, field)
        assert_frame_equal(expected_frame, result_frame)

    def test_remove_illegal_rdf_characters_illegalcharacters_notfound(self):
        '''
        Ensures when no illegal characters are found, then
        the frame is unchanged.
        '''
        frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_2'],
            'predicate': ['description', 'description'],
            'object': ['hello world', 'this is a fine string']
        })
        illegal_characters = ['@', '#']
        field = 'object'

        result_frame = _remove_illegal_rdf_characters(frame.copy(), illegal_characters, field)
        assert_frame_equal(frame, result_frame)

    def test_remove_illegal_rdf_characters_only_one_field(self):
        '''
        Ensures when there are illegal characters in both subject and object
        but we are only working on one of them right now then the target
        field is cleaned but the other is untouched.
        '''
        frame = pd.DataFrame(data={
            'subject': ['customer_1@@', 'custo#mer_2'],
            'predicate': ['description', 'description'],
            'object': ['hello @world#', 'this is a fine string']
        })
        illegal_characters = ['@', '#']
        field = 'subject'

        expected_frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_2'],
            'predicate': ['description', 'description'],
            'object': ['hello @world#', 'this is a fine string']
        })

        result_frame = _remove_illegal_rdf_characters(frame.copy(), illegal_characters, field)
        assert_frame_equal(expected_frame, result_frame)

    def test_remove_na_objects_null_parameters(self):
        '''
        Ensures when parameters are null, then an error is raised
        '''
        with self.assertRaises(ValueError):
            _remove_na_objects(None)

    @parameterized.expand([
        (np.nan,),
        (None,),
        (pd.NA,),
        ('NA',)
    ])
    def test_remove_na_objects_false(self, na_value):
        '''
        Ensures when there are NA values but the drop_na flag
        is set to false, then the frame is unchanged
        '''
        frame = pd.DataFrame(data={
            'subject': ['subject1', 'subject2', 'subject3'],
            'predicate': ['predicate1', 'predicate2', 'predicate3'],
            'object': ['object1', na_value, 'object3']
        })

        result_frame = _remove_na_objects(frame.copy(), drop_na=False)
        assert_frame_equal(frame, result_frame)

    @parameterized.expand([
            (np.nan,),
            (None,),
            (pd.NA,)
    ])
    def test_remove_na_objects_true(self, na_value):
        '''
        Ensures when drop_na is set to true then
        those records are removed from the frame.
        '''
        frame = pd.DataFrame(data={
            'subject': ['subject1', 'subject2', 'subject3'],
            'predicate': ['predicate1', 'predicate2', 'predicate3'],
            'object': ['object1', na_value, 'object3']
        })

        expected_frame = pd.DataFrame(data={
            'subject': ['subject1', 'subject3'],
            'predicate': ['predicate1', 'predicate3'],
            'object': ['object1', 'object3']
        })

        result_frame = _remove_na_objects(frame.copy(), drop_na=True)

        result_frame = result_frame.reset_index(drop=True).sort_index()
        expected_frame = expected_frame.reset_index(drop=True).sort_index()
        assert_frame_equal(expected_frame, result_frame)
