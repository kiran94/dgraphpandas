from collections import namedtuple
import unittest
from numpy.core.arrayprint import dtype_is_implied
from parameterized import parameterized

import pandas as pd
from pandas.testing import assert_frame_equal

from dgraphpandas.strategies.vertical import vertical_transform


class Vertical(unittest.TestCase):

    @parameterized.expand([
        (None, {'config', 'fake'}, 'config'),
        (pd.DataFrame(), None, 'config'),
        (pd.DataFrame(), {'config', 'fake'}, None),
        (pd.DataFrame(), {'config', 'fake'}, None),
        (pd.DataFrame(), {}, 'config'),
        (pd.DataFrame(), None, '')
    ])
    def test_vertical_transform_null_parameter(self, frame, config, config_file_key):
        '''
        Ensures when parameters are null, an error is raised.
        '''
        with self.assertRaises(ValueError):
            vertical_transform(frame, config, config_file_key)

    def test_vertical_transform_config_file_key_not_in_config(self):
        '''
        Ensures when the passed config key is not within the config
        then an error is raised.
        '''
        frame = pd.DataFrame()
        config_file_key = 'not_my_file'
        config = {
            'files': {'my_file': {}}
        }
        with self.assertRaises(KeyError):
            vertical_transform(frame, config, config_file_key)

    def test_vertical_transform_subject_fields_not_defined(self):
        '''
        Ensures when subject_fields has not been defined,
        then an error is raised.
        '''
        frame = pd.DataFrame()
        config_file_key = 'customer'
        config = {
            'files': {
                config_file_key: {
                    'type_overrides': {}
                }
            }
        }

        with self.assertRaises(ValueError):
            vertical_transform(frame, config, config_file_key)

    def test_vertical_predicate_column_not_defined(self):
        '''
        Ensures when the predicate column is not defined
        then we throw an error.
        '''
        frame = pd.DataFrame(data={
            'customer_id': [1, 2, 3],
            'not_a_predicate': ['age', 'weight', 'orders'],
            'object': [23, 90, 10]
        })

        config_file_key = 'customer'
        config = {
            'files': {
                'customer': {
                    'subject_fields': ['customer_id'],
                }
            }
        }

        with self.assertRaises(KeyError):
            vertical_transform(frame, config, config_file_key)

    def test_vertical_object_column_not_defined(self):
        '''
        Ensures when the object column is not defined
        then we throw an error.
        '''
        frame = pd.DataFrame(data={
            'customer_id': [1, 2, 3],
            'predicate': ['age', 'weight', 'orders'],
            'not_a_object': [23, 90, 10]
        })

        config_file_key = 'customer'
        config = {
            'files': {
                'customer': {
                    'subject_fields': ['customer_id'],
                }
            }
        }

        with self.assertRaises(KeyError):
            vertical_transform(frame, config, config_file_key)

    def test_vertical_transform_intrinsic_with_default_values(self):
        '''
        Ensures when a DataFrame is passed with the default options
        then an intrinsic dataframe is created
        and an empty edges frame is created.
        '''
        frame = pd.DataFrame(data={
            'customer_id': [1, 2, 3],
            'predicate': ['age', 'weight', 'orders'],
            'object': [23, 90, 10]
        })

        config_file_key = 'customer'
        config = {
            'files': {
                'customer': {
                    'subject_fields': ['customer_id'],
                    'dgraph_type': "customer"
                }
            }
        }

        expected_frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_2', 'customer_3', 'customer_1', 'customer_2', 'customer_3'],
            'predicate': ['age', 'weight', 'orders', 'dgraph.type', 'dgraph.type', 'dgraph.type'],
            'object': [23, 90, 10, 'customer', 'customer', 'customer'],
            'type': ['<xs:string>']*6
        })

        intrinsic, edges = vertical_transform(frame, config, config_file_key)

        intrinsic = intrinsic.reset_index(drop=True)
        expected_frame = expected_frame.reset_index(drop=True)

        assert_frame_equal(expected_frame, intrinsic)
        self.assertTrue(edges.empty)

    def test_vertical_transform_with_type_overrides(self):
        '''
        Ensures when type overrides have been provided, they are
        applied
        '''
        frame = pd.DataFrame(data={
            'customer_id': [1, 2, 3],
            'predicate': ['age', 'weight', 'orders'],
            'object': [23, 90, 10]
        })

        config_file_key = 'customer'
        config = {
            'files': {
                'customer': {
                    'subject_fields': ['customer_id'],
                    'dgraph_type': "customer",
                    'type_overrides': {
                        'age': 'Int32',
                        'weight': 'float32',
                        'orders': 'Int32',
                    }
                }
            }
        }

        expected_frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_2', 'customer_3', 'customer_1', 'customer_2', 'customer_3'],
            'predicate': ['age', 'weight', 'orders', 'dgraph.type', 'dgraph.type', 'dgraph.type'],
            'object': [23, 90, 10, 'customer', 'customer', 'customer'],
            'type': ['<xs:int>', '<xs:float>', '<xs:int>'] + ['<xs:string>']*3
        })

        intrinsic, edges = vertical_transform(frame, config, config_file_key)

        intrinsic = intrinsic.reset_index(drop=True)
        expected_frame = expected_frame.reset_index(drop=True)

        assert_frame_equal(expected_frame, intrinsic)
        self.assertTrue(edges.empty)

    @parameterized.expand([
        ###
        (
            'with_default_values',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'dgraph_type': "customer",
                        'type_overrides': {
                            'age': 'Int32',
                            'weight': 'float32',
                            'orders': 'Int32',
                        }
                    }
                }
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, 10]
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2', 'customer_3', 'customer_1', 'customer_2', 'customer_3'],
                'predicate': ['age', 'weight', 'orders', 'dgraph.type', 'dgraph.type', 'dgraph.type'],
                'object': [23, 90, 10, 'customer', 'customer', 'customer'],
                'type': ['<xs:int>', '<xs:float>', '<xs:int>'] + ['<xs:string>']*3
            }),
            pd.DataFrame(columns=['subject', 'predicate', 'object', 'type']),
            {}
        ),
        ###
        (
            'no_dgraph_type_defaults_to_config_key',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'type_overrides': {
                            'age': 'Int32',
                            'weight': 'float32',
                            'orders': 'Int32',
                        }
                    }
                }
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, 10]
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2', 'customer_3', 'customer_1', 'customer_2', 'customer_3'],
                'predicate': ['age', 'weight', 'orders', 'dgraph.type', 'dgraph.type', 'dgraph.type'],
                'object': [23, 90, 10, 'customer', 'customer', 'customer'],
                'type': ['<xs:int>', '<xs:float>', '<xs:int>'] + ['<xs:string>']*3
            }),
            pd.DataFrame(columns=['subject', 'predicate', 'object', 'type']),
            {}
        ),
        ###
        (
            'rename_fields',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'pre_rename': {
                            'orders': 'no_of_orders',
                            'age': 'birth'
                        }
                    }
                }
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, 10]
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2', 'customer_3', 'customer_1', 'customer_2', 'customer_3'],
                'predicate': ['birth', 'weight', 'no_of_orders', 'dgraph.type', 'dgraph.type', 'dgraph.type'],
                'object': [23, 90, 10, 'customer', 'customer', 'customer'],
                'type': ['<xs:string>']*6
            }),
            pd.DataFrame(columns=['subject', 'predicate', 'object', 'type']),
            {}
        ),
        ###
        (
            'rename_fields_kwargs',
            'customer',
            {
                'files': {
                    'customer': {'subject_fields': ['customer_id'] }
                }
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, 10]
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2', 'customer_3', 'customer_1', 'customer_2', 'customer_3'],
                'predicate': ['birth', 'weight', 'no_of_orders', 'dgraph.type', 'dgraph.type', 'dgraph.type'],
                'object': [23, 90, 10, 'customer', 'customer', 'customer'],
                'type': ['<xs:string>']*6
            }),
            pd.DataFrame(columns=['subject', 'predicate', 'object', 'type']),
            {
                'pre_rename': {
                    'orders': 'no_of_orders',
                    'age': 'birth'
                }
            }
        ),
        ###
        (
            'ignore_fields',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'ignore_fields': ['age']
                    }
                }
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, 10]
            }),
            pd.DataFrame(data={
                'subject': ['customer_2', 'customer_3', 'customer_2', 'customer_3'],
                'predicate': ['weight', 'orders', 'dgraph.type', 'dgraph.type'],
                'object': [90, 10, 'customer', 'customer'],
                'type': ['<xs:string>']*4
            }),
            pd.DataFrame(columns=['subject', 'predicate', 'object', 'type']),
            {}
        ),
        ###
        (
            'ignore_fields_kwargs',
            'customer',
            {
                'files': {
                    'customer': {'subject_fields': ['customer_id']}
                }
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, 10]
            }),
            pd.DataFrame(data={
                'subject': ['customer_2', 'customer_3', 'customer_2', 'customer_3'],
                'predicate': ['weight', 'orders', 'dgraph.type', 'dgraph.type'],
                'object': [90, 10, 'customer', 'customer'],
                'type': ['<xs:string>']*4
            }),
            pd.DataFrame(columns=['subject', 'predicate', 'object', 'type']),
            {'ignore_fields': ['age']}
        ),
        ###
        (
            'with_csv_edges',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'csv_edges': ['orders']
                    },
                },
                'add_dgraph_type_records': False
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, '1,2,3']
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2', 'customer_3', 'customer_3', 'customer_3'],
                'predicate': ['age', 'weight', 'orders', 'orders', 'orders'],
                'object': [23, 90, '1', '2', '3'],
                'type': ['<xs:string>']*5
            }),
            pd.DataFrame(columns=['subject', 'predicate', 'object', 'type']),
            {}
        ),
        ###
        (
            'with_csv_edges_kwargs',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                    },
                },
                'add_dgraph_type_records': False
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, '1,2,3']
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2', 'customer_3', 'customer_3', 'customer_3'],
                'predicate': ['age', 'weight', 'orders', 'orders', 'orders'],
                'object': [23, 90, '1', '2', '3'],
                'type': ['<xs:string>']*5
            }),
            pd.DataFrame(columns=['subject', 'predicate', 'object', 'type']),
            {'csv_edges': ['orders']}
        )

    ])
    def test_vertical_transform(
            self,
            name,
            config_file_key,
            config,
            input_frame,
            expected_intrinsic,
            expected_edges,
            kwargs):
        '''

        '''
        intrinsic, edges = vertical_transform(input_frame, config, config_file_key, **kwargs)

        intrinsic = intrinsic.reset_index(drop=True)
        expected_intrinsic = expected_intrinsic.reset_index(drop=True)

        edges = edges.reset_index(drop=True)
        expected_edges = expected_edges.reset_index(drop=True)

        try:
            assert_frame_equal(expected_intrinsic, intrinsic)
            assert_frame_equal(expected_edges, edges)
        except AssertionError:
            print('Input:')
            print(input_frame)
            print('Expected Intrinsic:')
            print(expected_intrinsic)
            print('Intrinsic:')
            print(intrinsic)
            raise
