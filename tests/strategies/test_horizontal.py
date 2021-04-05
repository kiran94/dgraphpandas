import unittest
from unittest.mock import patch, Mock

import pandas as pd
from pandas.testing import assert_frame_equal
from parameterized import parameterized

from dgraphpandas.strategies.horizontal import horizontal_transform


class HorizontalTests(unittest.TestCase):

    @parameterized.expand([
        (None, {'config': {}}, 'config_key'),
        (pd.DataFrame(), None, 'config_key'),
        (pd.DataFrame(), '', 'config_key'),
        (pd.DataFrame(), {'config': {}}, None),
        (pd.DataFrame(), {'config': {}}, ''),
    ])
    def test_horizontal_transform_null_parameters(self, frame, config, config_file_key):
        '''
        Ensures when parameters are null, then an
        error is raised
        '''
        with self.assertRaises(ValueError):
            horizontal_transform(frame, config, config_file_key)

    def test_horizontal_config_key_does_not_exist(self):
        '''
        Ensures when the config key does not exist
        within the config then an error is raised
        '''
        frame = pd.DataFrame()
        config_key = 'my_key'
        config = {
            'files': {
                'some_other_key': {}
            }
        }

        with self.assertRaises(KeyError):
            horizontal_transform(frame, config, config_key)

    @parameterized.expand([
        ('',),
        (None,),
    ])
    def test_horizontal_subject_fields_not_provided(self, subject_fields):
        '''
        Ensures when subject fields is not provided
        then an error is raised
        '''
        frame = pd.DataFrame()
        config_key = 'my_key'
        config = {
            'files': {
                'my_key': {
                    'subject_fields': subject_fields
                }
            }
        }

        with self.assertRaises(ValueError):
            horizontal_transform(frame, config, config_key)

    def test_horizontal_could_not_convert_type(self):
        '''
        Ensures when a type could not be applied to a column,
        then an error is raised
        '''
        frame = pd.DataFrame(data={
            'customer_id': [1, 2, 3],
            'age': [23, 'not number', 56]
        })
        config = {
            'files': {
                'customer': {
                    'subject_fields': ['customer_id'],
                    'type_overrides': {
                        'customer_id': 'int32',
                        'age': 'int32'
                    }
                }
            }
        }
        config_file_key = 'customer'
        with self.assertRaises(SystemExit):
            horizontal_transform(frame, config, config_file_key)

    @parameterized.expand([
        ###
        (
            'single_predicate',
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'age': [23, 67, 56]
            }),
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'type_overrides': {
                            'customer_id': 'int32',
                            'age': 'int32'
                        }
                    }
                }
            },
            'customer',
            pd.DataFrame(data={
                'customer_id': pd.Series([1, 2, 3], dtype='int32'),
                'predicate': pd.Series(['age']*3, dtype='O'),
                'object': pd.Series([23, 67, 56], dtype='int32')
            })
        ),
        ###
        (
            'multiple_predicates',
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'age': [23, 67, 56],
                'weight': [189, 167, 190]
            }),
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'type_overrides': {
                            'customer_id': 'int32',
                            'age': 'int32',
                            'weight': 'int32'
                        }
                    }
                }
            },
            'customer',
            pd.DataFrame(data={
                'customer_id': pd.Series([1, 2, 3, 1, 2, 3], dtype='int32'),
                'predicate': pd.Series(['age']*3 + ['weight']*3, dtype='O'),
                'object': pd.Series([23, 67, 56, 189, 167, 190], dtype='int32')
            })
        ),
        ###
        (
            'multiple_subject_fields',
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'order_id': [405, 210, 321],
                'value': [200, 321, 67],
            }),
            {
                'files': {
                    'order': {
                        'subject_fields': ['customer_id', 'order_id'],
                        'type_overrides': {
                            'customer_id': 'int32',
                            'order_id': 'int32',
                            'value': 'int32'
                        }
                    }
                }
            },
            'order',
            pd.DataFrame(data={
                'customer_id': pd.Series([1, 2, 3], dtype='int32'),
                'order_id': pd.Series([405, 210, 321], dtype='int32'),
                'predicate': pd.Series(['value']*3, dtype='O'),
                'object': pd.Series([200, 321, 67], dtype='int32')
            })
        )
    ])
    @patch('dgraphpandas.strategies.horizontal.vertical_transform')
    def test_horizontal_melted_passed(self, name, frame, config, config_file_key, expected_melted, transform_mock: Mock):
        '''
        Ensures that the passed horizontal frame is melted and
        passed into the vertical_transform.
        Also ensures the same config and key are passed through
        '''
        intrinsic_mock = Mock(spec=pd.DataFrame)
        edges_mock = Mock(spec=pd.DataFrame)
        transform_mock.return_value = (intrinsic_mock, edges_mock)

        intrinsic, edges = horizontal_transform(frame, config, config_file_key)

        transform_mock.assert_called_once()
        args, kwargs = transform_mock.call_args_list[0]
        invoked_frame, invoked_config, invoked_key = args

        assert_frame_equal(invoked_frame, expected_melted)
        self.assertEqual(invoked_config, config)
        self.assertEqual(invoked_key, config_file_key)
        self.assertEqual(kwargs, {})
        self.assertEqual(intrinsic_mock, intrinsic)
        self.assertEqual(edges_mock, edges)

    def test_horizontal_frame_only_has_subject_and_no_data_fields(self):
        '''
        Ensures when the horizontal frame only has subject fields
        and no actual data fields then an error is raised
        '''
        frame = pd.DataFrame(data={
            'customer_id': [1, 2, 3],
            'order_id': [405, 210, 321]
        })

        config = {
            'files': {
                'order': {
                    'subject_fields': ['customer_id', 'order_id'],
                    'type_overrides': {
                        'customer_id': 'int32',
                        'order_id': 'int32',
                    }
                }
            }
        }
        config_key = 'order'

        with self.assertRaises(ValueError):
            horizontal_transform(frame, config, config_key)
