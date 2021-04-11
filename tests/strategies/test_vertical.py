import unittest
from unittest.mock import patch, Mock
from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal
from parameterized import parameterized

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
                        'age': 'int32',
                        'weight': 'float32',
                        'orders': 'int32',
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

    @patch('dgraphpandas.strategies.vertical.pd.read_csv')
    def test_vertical_transform_csv_file(self, mock_pandas: Mock):
        '''
        Ensures when a file path is passed, then the file is read
        from pandas with the given options
        '''
        file = 'test.csv'
        read_csv_options = {'sep': ';'}

        frame = pd.DataFrame(data={
            'customer_id': [1, 2, 3],
            'predicate': ['age', 'weight', 'orders'],
            'object': [23, 90, 10]
        })
        mock_pandas.return_value = frame

        config_file_key = 'customer'
        config = {
            'files': {
                'customer': {
                    'subject_fields': ['customer_id'],
                    'dgraph_type': "customer",
                    'type_overrides': {
                        'age': 'int32',
                        'weight': 'float32',
                        'orders': 'int32',
                    },
                    'read_csv_options': read_csv_options
                }
            }
        }

        expected_frame = pd.DataFrame(data={
            'subject': ['customer_1', 'customer_2', 'customer_3', 'customer_1', 'customer_2', 'customer_3'],
            'predicate': ['age', 'weight', 'orders', 'dgraph.type', 'dgraph.type', 'dgraph.type'],
            'object': [23, 90, 10, 'customer', 'customer', 'customer'],
            'type': ['<xs:int>', '<xs:float>', '<xs:int>'] + ['<xs:string>']*3
        })

        intrinsic, edges = vertical_transform(file, config, config_file_key)

        args, kwargs = mock_pandas.call_args_list[0]
        self.assertEqual(file, args[0])
        self.assertEqual(read_csv_options, kwargs)

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
                            'age': 'int32',
                            'weight': 'float32',
                            'orders': 'int32',
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
                            'age': 'int32',
                            'weight': 'float32',
                            'orders': 'int32',
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
            'custom_predicate',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'type_overrides': {
                            'age': 'int32',
                            'weight': 'float32',
                            'orders': 'int32',
                        },
                        'predicate_field': 'my_field'
                    }
                }
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'my_field': ['age', 'weight', 'orders'],
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
            'custom_object',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'type_overrides': {
                            'age': 'int32',
                            'weight': 'float32',
                            'orders': 'int32',
                        },
                        'object_field': 'my_object_field'
                    }
                }
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'predicate': ['age', 'weight', 'orders'],
                'my_object_field': [23, 90, 10]
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
            'custom_predicate_and_object',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'type_overrides': {
                            'age': 'int32',
                            'weight': 'float32',
                            'orders': 'int32',
                        },
                        'object_field': 'my_object_field',
                        'predicate_field': 'my_field'
                    }
                }
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'my_field': ['age', 'weight', 'orders'],
                'my_object_field': [23, 90, 10]
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
                    'customer': {'subject_fields': ['customer_id']}
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
        ),
        ###
        (
            'with_csv_edges_custom_sep',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'csv_edges': ['orders'],
                        'csv_edges_seperator': '@'
                    },
                },
                'add_dgraph_type_records': False
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, '1@2@3']
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
            'with_composite_key',
            'order',
            {
                'files': {
                    'order': {
                        'subject_fields': ['customer_id', 'order_id']
                    }
                },
                'add_dgraph_type_records': False
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'order_id': [908, 456, 287],
                'predicate': ['total_value', 'quantity', 'item'],
                'object': [2031, 5, 'ipads']
            }),
            pd.DataFrame(data={
                'subject': ['order_1_908', 'order_2_456', 'order_3_287'],
                'predicate': ['total_value', 'quantity', 'item'],
                'object': [2031, 5, 'ipads'],
                'type': ['<xs:string>']*3
            }),
            pd.DataFrame(columns=['subject', 'predicate', 'object', 'type']),
            {}
        ),
        ###
        (
            'with_edges',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'edge_fields': ['location_id']
                    },
                },
                'add_dgraph_type_records': False
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3, 1, 2],
                'predicate': ['age', 'weight', 'orders', 'location_id', 'location_id'],
                'object': [23, 90, '1', 'loc45', 'loc64']
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2', 'customer_3'],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, '1'],
                'type': ['<xs:string>']*3
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2'],
                'predicate': ['location', 'location'],
                'object': ['location_loc45', 'location_loc64'],
                'type': [None]*2
            }),
            {}
        ),
        ###
        (
            'strip_id_from_edge_names',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'edge_fields': ['location_id']
                    },
                },
                'strip_id_from_edge_names': False,
                'add_dgraph_type_records': False,
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3, 1, 2],
                'predicate': ['age', 'weight', 'orders', 'location_id', 'location_id'],
                'object': [23, 90, '1', 'loc45', 'loc64']
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2', 'customer_3'],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, '1'],
                'type': ['<xs:string>']*3
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2'],
                'predicate': ['location_id', 'location_id'],
                'object': ['location_id_loc45', 'location_id_loc64'],
                'type': [None]*2
            }),
            {}
        ),
        ###
        (
            'strip_id_from_edge_names_kwargs',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'edge_fields': ['location_id']
                    },
                },
                'strip_id_from_edge_names': False
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3, 1, 2],
                'predicate': ['age', 'weight', 'orders', 'location_id', 'location_id'],
                'object': [23, 90, '1', 'loc45', 'loc64']
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2', 'customer_3'],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, '1'],
                'type': ['<xs:string>']*3
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2'],
                'predicate': ['location_id', 'location_id'],
                'object': ['location_id_loc45', 'location_id_loc64'],
                'type': [None]*2
            }),
            {'add_dgraph_type_records': False}
        ),
        ###
        (
            'edge_id_convention',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'edge_id_convention': True
                    },
                }
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3, 1, 2],
                'predicate': ['age', 'weight', 'orders', 'location_id', 'location_id'],
                'object': [23, 90, '1', 'loc45', 'loc64']
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2', 'customer_3'],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, '1'],
                'type': ['<xs:string>']*3
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2'],
                'predicate': ['location', 'location'],
                'object': ['location_loc45', 'location_loc64'],
                'type': [None]*2
            }),
            {'add_dgraph_type_records': False}
        ),
        ###
        (
            'key_seperator override',
            'order',
            {
                'files': {
                    'order': {
                        'subject_fields': ['customer_id', 'order_id']
                    }
                },
                'add_dgraph_type_records': False,
                'key_separator': "@"
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3],
                'order_id': [908, 456, 287],
                'predicate': ['total_value', 'quantity', 'item'],
                'object': [2031, 5, 'ipads']
            }),
            pd.DataFrame(data={
                'subject': ['order@1@908', 'order@2@456', 'order@3@287'],
                'predicate': ['total_value', 'quantity', 'item'],
                'object': [2031, 5, 'ipads'],
                'type': ['<xs:string>']*3
            }),
            pd.DataFrame(columns=['subject', 'predicate', 'object', 'type']),
            {}
        ),
        ###
        (
            'with datetime fields',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'edge_fields': ['location_id'],
                        'type_overrides': {
                            'dob': 'datetime64'
                        }
                    },
                },
                'add_dgraph_type_records': False,
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3, 1, 2],
                'predicate': ['dob', 'weight', 'orders', 'location_id', 'location_id'],
                'object': [datetime(2021, 4, 5), 90, '1', 'loc45', 'loc64']
            }),
            pd.DataFrame(data={
                'subject': ['customer_2', 'customer_3', 'customer_1'],
                'predicate': ['weight', 'orders', 'dob'],
                'object': [90, '1', '2021-04-05T00:00:00'],
                'type': ['<xs:string>']*2 + ['<xs:dateTime>']
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2'],
                'predicate': ['location', 'location'],
                'object': ['location_loc45', 'location_loc64'],
                'type': [None]*2
            }),
            {}
        ),
        ###
        (
            'with datetime_formats',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'edge_fields': ['location_id'],
                        'date_fields': {'dob': {'format': '%Y %b %d'}}
                    },
                },
                'add_dgraph_type_records': False,
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3, 1, 2],
                'predicate': ['dob', 'weight', 'orders', 'location_id', 'location_id'],
                'object': ['2021 Mar 13', 90, '1', 'loc45', 'loc64']
            }),
            pd.DataFrame(data={
                'subject': ['customer_2', 'customer_3', 'customer_1'],
                'predicate': ['weight', 'orders', 'dob'],
                'object': [90, '1', '2021-03-13T00:00:00'],
                'type': ['<xs:string>']*2 + ['<xs:dateTime>']
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2'],
                'predicate': ['location', 'location'],
                'object': ['location_loc45', 'location_loc64'],
                'type': [None]*2
            }),
            {}
        ),
        ###
        (
            'illegal_characters',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'edge_fields': ['location_id']
                    },
                },
                'illegal_characters': ['%'],
                'illegal_characters_intrinsic_object': ['\"'],
                'add_dgraph_type_records': False,
            },
            pd.DataFrame(data={
                'customer_id': ['%1%', 2, 3, 1, 2],
                'predicate': ['age', 'weight', 'orders', 'location_id', 'location_id'],
                'object': [23, '"90"', '1', 'loc45', 'loc64']
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2', 'customer_3'],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, '90', '1'],
                'type': ['<xs:string>']*3
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2'],
                'predicate': ['location', 'location'],
                'object': ['location_loc45', 'location_loc64'],
                'type': [None]*2
            }),
            {}
        ),
        ###
        (
            'null_objects',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'edge_fields': ['location_id']
                    },
                },
                'add_dgraph_type_records': False,
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3, 1, 2],
                'predicate': ['age', 'weight', 'orders', 'location_id', 'location_id'],
                'object': [23, None, '1', 'loc45', None]
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_3'],
                'predicate': ['age', 'orders'],
                'object': [23, '1'],
                'type': ['<xs:string>']*2
            }),
            pd.DataFrame(data={
                'subject': ['customer_1'],
                'predicate': ['location'],
                'object': ['location_loc45'],
                'type': [None]
            }),
            {}
        ),
        ###
        (
            'override_edge_name',
            'customer',
            {
                'files': {
                    'customer': {
                        'subject_fields': ['customer_id'],
                        'edge_fields': ['location_id'],
                        'override_edge_name': {
                            'location': {
                                'predicate': 'habitat',
                                'target_node_type': 'hab'
                            }
                        }
                    },
                },
                'add_dgraph_type_records': False
            },
            pd.DataFrame(data={
                'customer_id': [1, 2, 3, 1, 2],
                'predicate': ['age', 'weight', 'orders', 'location_id', 'location_id'],
                'object': [23, 90, '1', 'loc45', 'loc64']
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2', 'customer_3'],
                'predicate': ['age', 'weight', 'orders'],
                'object': [23, 90, '1'],
                'type': ['<xs:string>']*3
            }),
            pd.DataFrame(data={
                'subject': ['customer_1', 'customer_2'],
                'predicate': ['habitat', 'habitat'],
                'object': ['hab_loc45', 'hab_loc64'],
                'type': [None]*2
            }),
            {'add_dgraph_type_records': False}
        ),

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
        Ensures that Vertical Transforms are working.
        Each of the parameter sets being passed here represents:
            For a given input frame and configuration,
            does the output match the given intrinsic and edges

        Note that the core flow of running the logic is the same but
        all the different combination of inputs is coming from the parameters.

        add_dgraph_type_records is added in some of these tests only to reduce
        the verbosity of our DataFrame declarations.
        '''
        intrinsic, edges = vertical_transform(input_frame, config, config_file_key, **kwargs)

        self.assertIsNotNone(intrinsic)
        self.assertIsNotNone(edges)

        intrinsic = intrinsic.reset_index(drop=True)
        expected_intrinsic = expected_intrinsic.reset_index(drop=True)

        edges = edges.reset_index(drop=True)
        expected_edges = expected_edges.reset_index(drop=True)

        try:
            assert_frame_equal(expected_intrinsic, intrinsic)
            assert_frame_equal(expected_edges, edges)
        except AssertionError:  # pragma: no cover
            print('Failed', name)  # pragma: no cover
            print('Input:')  # pragma: no cover
            print(input_frame)  # pragma: no cover
            print('Expected Intrinsic:')  # pragma: no cover
            print(expected_intrinsic)  # pragma: no cover
            print('Intrinsic:')  # pragma: no cover
            print(intrinsic)  # pragma: no cover
            print('#####')  # pragma: no cover
            print('Expected Edges:')  # pragma: no cover
            print(expected_edges)  # pragma: no cover
            print('Edges:')  # pragma: no cover
            print(edges)  # pragma: no cover
            raise  # pragma: no cover
