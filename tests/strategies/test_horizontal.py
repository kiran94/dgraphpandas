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

    @patch('dgraphpandas.strategies.horizontal.vertical_transform')
    @patch('dgraphpandas.strategies.horizontal.pd.read_csv', spec=pd.read_csv)
    def test_horizontal_melted_file_path_passed(self, mock_pandas: Mock, mock_transform: Mock):
        '''
        Ensures when a file path(str) it passed into the transform, then the file
        is read using read_csv before going into logic.
        '''
        file = 'test.csv'
        frame = pd.DataFrame(data={
            'customer_id': [1, 2, 3],
            'age': [23, 67, 56]
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
        expected_melted = pd.DataFrame(data={
            'customer_id': pd.Series([1, 2, 3], dtype='int32'),
            'predicate': pd.Series(['age']*3, dtype='O'),
            'object': pd.Series([23, 67, 56], dtype='int32')

        })

        mock_pandas.return_value = frame

        horizontal_transform(file, config, config_file_key)

        args, kwargs = mock_pandas.call_args_list[0]
        self.assertEqual(file, args[0])
        self.assertEqual({}, kwargs)

        args, kwargs = mock_transform.call_args_list[0]
        assert_frame_equal(expected_melted, args[0])
        self.assertEqual(config, args[1])
        self.assertEqual(config_file_key, args[2])

    @patch('dgraphpandas.strategies.horizontal.vertical_transform')
    @patch('dgraphpandas.strategies.horizontal.pd.read_csv', spec=pd.read_csv)
    def test_horizontal_melted_file_path_custom_csv_passed(self, mock_pandas: Mock, mock_transform: Mock):
        '''
        Ensures when a read_csv_options option is defined inside file configuration
        it is applied to the pd.read_csv call.
        '''
        file = 'test.csv'
        read_csv_options = {'sep': ';'}
        frame = pd.DataFrame(data={
            'customer_id': [1, 2, 3],
            'age': [23, 67, 56]
        })
        config = {
            'files': {
                'customer': {
                    'subject_fields': ['customer_id'],
                    'type_overrides': {
                        'customer_id': 'int32',
                        'age': 'int32'
                    },
                    'read_csv_options': read_csv_options
                }
            }
        }
        config_file_key = 'customer'
        expected_melted = pd.DataFrame(data={
            'customer_id': pd.Series([1, 2, 3], dtype='int32'),
            'predicate': pd.Series(['age']*3, dtype='O'),
            'object': pd.Series([23, 67, 56], dtype='int32')
        })

        mock_pandas.return_value = frame

        horizontal_transform(file, config, config_file_key)

        args, kwargs = mock_pandas.call_args_list[0]
        self.assertEqual(file, args[0])
        self.assertEqual(read_csv_options, kwargs)

        args, kwargs = mock_transform.call_args_list[0]
        assert_frame_equal(expected_melted, args[0])
        self.assertEqual(config, args[1])
        self.assertEqual(config_file_key, args[2])

    @parameterized.expand([
        ###
        (
            'year_wrong_order',
            {'dob': {'format': "%Y-%m-%d"}},
            pd.DataFrame(data={
                'customer_id': [1, 2],
                'dob': ['03-02-2021', '01-03-1945'],
                'weight': [50, 32]
            })
        ),
        ###
        (
            'alphanumerical_string',
            {'dob': {'format': "%Y-%m-%d"}},
            pd.DataFrame(data={
                'customer_id': [1, 2],
                'dob': ['not a date', '01-03-1945'],
                'weight': [50, 32]
            })
        ),
        ###
        (
            'missing_dashes',
            {'dob': {'format': "%Y-%m%d"}},
            pd.DataFrame(data={
                'customer_id': [1, 2],
                'dob': ['2021-03-02', '19450301'],
                'weight': [50, 32]
            })
        ),
        ###
        (
            'missing_dots',
            {'dob': {'format': "%Y.%m.%d"}},
            pd.DataFrame(data={
                'customer_id': [1, 2],
                'dob': ['2021-03-02', '1945.03&01'],
                'weight': [50, 32]
            })
        ),
        ###
        (
            'malformed_month_string',
            {'dob': {'format': "%d-%b-%Y"}},
            pd.DataFrame(data={
                'customer_id': [1, 2],
                'dob': ['02-FebFake-2021', '01-Mar-1945'],
                'weight': [50, 32]
            })
        )
    ])
    @patch('dgraphpandas.strategies.horizontal.vertical_transform')
    def test_horizontal_transform_incorrect_date_format(self, name, date_format, frame, transform_mock: Mock):
        '''
        Ensures when the date format provided does not match the value within the frame,
        then an error is raised.
        '''
        config_file_key = 'customer'
        config = {
            'files': {
                config_file_key: {
                    'subject_fields': ['customer_id'],
                    'date_fields': date_format
                }
            }
        }

        with self.assertRaisesRegex(ValueError, "time data (.*) (doesn't|does not) match format(.*)"):
            horizontal_transform(frame, config, config_file_key)
        transform_mock.assert_not_called()

    @parameterized.expand([
        ###
        (
            'uncoverted_month_day',
            {'dob': {'format': "%Y"}},
            pd.DataFrame(data={
                'customer_id': [1, 2],
                'dob': ['2021-03-02', '1945-03-01'],
                'weight': [50, 32]
            })
        ),
        ###
        (
            'uncoverted_month_year',
            {'dob': {'format': "%m-%d"}},
            pd.DataFrame(data={
                'customer_id': [1, 2],
                'dob': ['03-02-2021', '03-01-2021'],
                'weight': [50, 32]
            })
        )
    ])
    @patch('dgraphpandas.strategies.horizontal.vertical_transform')
    def test_horizontal_transform_unconverted_date_parts(self, name, date_format, frame, transform_mock: Mock):
        '''
        Ensures when the date partially matches and there are some converted
        parts, an error is raised
        '''
        config_file_key = 'customer'
        config = {
            'files': {
                config_file_key: {
                    'subject_fields': ['customer_id'],
                    'date_fields': date_format
                }
            }
        }

        with self.assertRaisesRegex(ValueError, "unconverted data remains: (.*)"):
            horizontal_transform(frame, config, config_file_key)
        transform_mock.assert_not_called()

    @parameterized.expand([
        ###
        (
            'dash_format',
            {'dob': {'format': "%Y-%m-%d"}},
            pd.DataFrame(data={
                'customer_id': [1, 2],
                'dob': ['2021-03-02', '1945-03-01'],
                'weight': [50, 32]
            }),
            pd.DataFrame(data={
                'customer_id': [1, 2, 1, 2],
                'predicate': ['dob', 'dob', 'weight', 'weight'],
                'object':[pd.to_datetime('2021-03-02 00:00:00'), pd.to_datetime('1945-03-01 00:00:00'), 50, 32]
            })
        ),
        ###
        (
            'dot_format',
            {'dob': {'format': "%Y.%m.%d"}},
            pd.DataFrame(data={
                'customer_id': [1, 2],
                'dob': ['1999.05.09', '1789.02.12'],
                'weight': [50, 32]
            }),
            pd.DataFrame(data={
                'customer_id': [1, 2, 1, 2],
                'predicate': ['dob', 'dob', 'weight', 'weight'],
                'object': [pd.to_datetime('1999-05-09 00:00:00'), pd.to_datetime('1789-02-12 00:00:00'), 50, 32]
            })
        ),
        ###
        (
            'multiple_date_fields',
            {'updated_at': {'format': '%Y.%m.%d'}, 'dob': {'format': "%Y.%m.%d"}},
            pd.DataFrame(data={
                'customer_id': [1, 2],
                'dob': ['1999.05.09', '1789.02.12'],
                'updated_at': ['2021.03.02', '2021.03.04'],
                'weight': [50, 32]
            }),
            pd.DataFrame(data={
                'customer_id': [1, 2, 1, 2, 1, 2],
                'predicate': ['dob', 'dob', 'updated_at', 'updated_at', 'weight', 'weight'],
                'object': [
                    pd.to_datetime('1999-05-09 00:00:00'),
                    pd.to_datetime('1789-02-12 00:00:00'),
                    pd.to_datetime('2021-03-02 00:00:00'),
                    pd.to_datetime('2021-03-04 00:00:00'),
                    50,
                    32]
            })
        ),
        ###
        (
            'multiple_date_fields_different_formats',
            {'updated_at': {'format': '%Y$%m$%d'}, 'dob': {'format': "%Y.%m.%d"}},
            pd.DataFrame(data={
                'customer_id': [1, 2],
                'dob': ['1999.05.09', '1789.02.12'],
                'updated_at': ['2021$03$02', '2021$03$04'],
                'weight': [50, 32]
            }),
            pd.DataFrame(data={
                'customer_id': [1, 2, 1, 2, 1, 2],
                'predicate': ['dob', 'dob', 'updated_at', 'updated_at', 'weight', 'weight'],
                'object': [
                    pd.to_datetime('1999-05-09 00:00:00'),
                    pd.to_datetime('1789-02-12 00:00:00'),
                    pd.to_datetime('2021-03-02 00:00:00'),
                    pd.to_datetime('2021-03-04 00:00:00'),
                    50,
                    32]
            })
        )
    ])
    @patch('dgraphpandas.strategies.horizontal.vertical_transform')
    def test_horizontal_transform_correct_date_format(self, name, date_format, frame, expected_melted, transform_mock: Mock):
        '''
        Ensures when the date_format provided is in the correct format,
        no error is raised
        '''
        config_file_key = 'customer'
        config = {
            'files': {
                config_file_key: {
                    'subject_fields': ['customer_id'],
                    'date_fields': date_format
                }
            }
        }

        horizontal_transform(frame, config, config_file_key)

        transform_mock.assert_called_once()
        args, kwargs = transform_mock.call_args_list[0]

        passed_frame, passed_config, passed_config_key = args

        assert_frame_equal(passed_frame, expected_melted)
        self.assertEqual(passed_config, config)
        self.assertEqual(passed_config_key, config_file_key)
        self.assertEqual(kwargs, {})
