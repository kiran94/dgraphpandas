import unittest
from unittest.mock import MagicMock, Mock, patch, call
from parameterized import parameterized
import pandas as pd
from pandas.testing import assert_frame_equal
from dgraphpandas.rdf import _resolve_transform, to_rdf
from dgraphpandas.strategies.horizontal import horizontal_transform
from dgraphpandas.strategies.vertical import vertical_transform


class RdfTests(unittest.TestCase):

    def test_resolve_transform_null_parameter(self):
        '''
        Ensures when the config is null
        an error is raised
        '''
        config = None
        with self.assertRaises(ValueError):
            _resolve_transform(config)

    def test_resolve_transform_horizontal(self):
        '''
        Ensures when transform is set to horizontal,
        then a horizontal is returned
        '''
        config = {'transform': 'horizontal'}
        transform_func = _resolve_transform(config)
        self.assertEqual(transform_func, horizontal_transform)

    def test_resolve_transform_vertical(self):
        '''
        Ensures when transform is set to vertical,
        then a vertical is returned
        '''
        config = {'transform': 'vertical'}
        transform_func = _resolve_transform(config)
        self.assertEqual(transform_func, vertical_transform)

    def test_resolve_transform_unknown(self):
        '''
        Ensures when the transform is unknown,
        then the default horizontal is used
        '''
        config = {'transform': 'something'}
        transform_func = _resolve_transform(config)
        self.assertEqual(transform_func, horizontal_transform)

    def test_resolve_transform_none_provided(self):
        '''
        Ensures when no transform is provided,
        then the default horizontal is used
        '''
        config = {}
        transform_func = _resolve_transform(config)
        self.assertEqual(transform_func, horizontal_transform)

    @parameterized.expand([
        (None, {'transform': 'horizontal'}, 'key'),
        (pd.DataFrame(), None, 'key'),
        (pd.DataFrame(), {'transform': 'horizontal'}, None)
    ])
    def test_to_rdf_null_parameters(self, frame, config, key):
        '''
        Ensures when parameters are null then an exception
        is thrown
        '''
        with self.assertRaises(ValueError):
            to_rdf(frame, config, key)

    @parameterized.expand([
        ('with_export_csv', {'export_csv': True}),
        ('with_export_rdf', {'export_rdf': True}),
    ])
    @patch('dgraphpandas.rdf.generate_upserts')
    @patch('dgraphpandas.rdf._get_config')
    @patch('dgraphpandas.rdf._resolve_transform')
    @patch('dgraphpandas.rdf.pd.read_csv')
    @patch('dgraphpandas.rdf.gzip.open')
    @patch('dgraphpandas.rdf.os.makedirs')
    def test_to_rdf_from_memory_frame_non_output_dir(
            self,
            name,
            options,
            mock_os_makedirs: Mock,
            mock_gzip: Mock,
            mock_pandas_read_csv_mock: Mock,
            mock_transform_func: Mock,
            mock_config: Mock,
            mock_upsert: Mock):
        '''
        Ensures when a memory frame is passed, it is passed through
        transformations and nothing is written to disk
        when output_dir is none even if the export params are set.
        '''
        frame = pd.DataFrame(data={
            'subject': [1, 2, 3],
            'predicate': ['age']*3,
            'object': [23, 43, 12]
        })
        config = {
            'transform': 'horizontal',
            'files': {'my_file': {
                'subject_fields': ['id']
            }}
        }
        config_key = 'my_file'

        mock_config.return_value = config
        mock_transform = MagicMock(return_value=(1, 2))
        mock_transform_func.return_value = mock_transform
        mock_upsert.return_value = (['intrinsic'], ['edges'])

        result = to_rdf(frame, config, config_key, output_dir=None, **(options))
        self.assertEqual(result, (['intrinsic'], ['edges']))

        args, kwargs = mock_config.call_args_list[0]
        self.assertEqual(args, (config,))
        self.assertEqual(kwargs, {})

        args, kwargs = mock_transform.call_args_list[0]
        passed_frame = args[0]
        passed_config = args[1]
        passed_key = args[2]

        assert_frame_equal(frame, passed_frame)
        self.assertEqual(config, passed_config)
        self.assertEqual(config_key, passed_key)
        self.assertEqual(kwargs, options)

        mock_pandas_read_csv_mock.assert_not_called()
        mock_gzip.assert_not_called()
        mock_os_makedirs.assert_not_called()

    @parameterized.expand([
        ###
        (
            'with_export_csv',
            [
                pd.DataFrame(data={
                    'subject': [1, 2, 3],
                    'predicate': ['age']*3,
                    'object': [23, 43, 12]
                })
            ],
            {
                'transform': 'horizontal',
                'files': {
                    'my_file': {
                        'subject_fields': ['id']
                    }
                }
            },
            'my_file',
            {'export_csv': True}
        ),
        ###
        (
            'with_export_rdf',
            [
                pd.DataFrame(data={
                    'subject': [1, 2, 3],
                    'predicate': ['age']*3,
                    'object': [23, 43, 12]
                })
            ],
            {
                'transform': 'horizontal',
                'files': {
                    'my_file': {
                        'subject_fields': ['id']
                    }
                }
            },
            'my_file',
            {'export_rdf': True}
        ),
        ###
        (
            'with_both_csv_and_rdf',
            [
                pd.DataFrame(data={
                    'subject': [1, 2, 3],
                    'predicate': ['age']*3,
                    'object': [23, 43, 12]
                })
            ],
            {
                'transform': 'horizontal',
                'files': {
                    'my_file': {
                        'subject_fields': ['id']
                    }
                }
            },
            'my_file',
            {'export_rdf': True, 'export_csv': True}
        ),
        ###
        (
            'with_console',
            [
                pd.DataFrame(data={
                    'subject': [1, 2, 3],
                    'predicate': ['age']*3,
                    'object': [23, 43, 12]
                })
            ],
            {
                'transform': 'horizontal',
                'files': {
                    'my_file': {
                        'subject_fields': ['id']
                    }
                }
            },
            'my_file',
            {'console': True}
        ),
        ###
        (
            'multiple_chunks',
            [
                pd.DataFrame(data={
                    'subject': [4, 5, 6],
                    'predicate': ['age']*3,
                    'object': [23, 43, 12]
                }),

                pd.DataFrame(data={
                    'subject': [1, 2, 3],
                    'predicate': ['age']*3,
                    'object': [23, 43, 12]
                })
            ],
            {
                'transform': 'horizontal',
                'files': {
                    'my_file': {
                        'subject_fields': ['id']
                    }
                }
            },
            'my_file',
            {'export_rdf': True, 'export_csv': True}
        ),
    ])
    @patch('dgraphpandas.rdf.generate_upserts')
    @patch('dgraphpandas.rdf._get_config')
    @patch('dgraphpandas.rdf._resolve_transform')
    @patch('dgraphpandas.rdf.pd.read_csv')
    @patch('dgraphpandas.rdf.gzip.open')
    @patch('dgraphpandas.rdf.os.makedirs')
    @patch('builtins.print')
    def test_to_rdf_from_file(
            self,
            name,
            frames,
            config,
            config_key,
            options,
            mock_print: Mock,
            mock_os_makedirs: Mock,
            mock_gzip: Mock,
            mock_pandas_read_csv_mock: Mock,
            mock_transform_func: Mock,
            mock_config: Mock,
            mock_upsert: Mock):
        '''
        Ensures when a file is provided, then transform is called
        '''
        mock_pandas_read_csv_mock.return_value = frames
        mock_config.return_value = config
        mock_intrinsic = MagicMock(spec=pd.DataFrame)
        mock_edges = MagicMock(spec=pd.DataFrame)
        mock_transform = MagicMock(return_value=(mock_intrinsic, mock_edges))
        mock_transform_func.return_value = mock_transform
        mock_upsert.return_value = (['intrinsic'], ['edges'])

        result = to_rdf('my_file.csv', config, config_key, output_dir='data/', **(options))
        self.assertEqual(result, [(['intrinsic'], ['edges'])]*len(frames))

        args, kwargs = mock_pandas_read_csv_mock.call_args_list[0]
        self.assertEqual(('my_file.csv',), args)
        self.assertEqual({'chunksize': 10_000_000}, kwargs)
        self.assertEqual(call('data/', exist_ok=True), mock_os_makedirs.call_args_list[0])

        if 'export_csv' in options:
            self.assertEqual(len(frames), len(mock_intrinsic.method_calls))
            self.assertEqual(len(frames), len(mock_edges.method_calls))
            self.assertEqual(mock_intrinsic.method_calls[0], call.to_csv('data/my_file_intrinsic.csv', encoding='utf-8', index=False))
            self.assertEqual(mock_edges.method_calls[0], call.to_csv('data/my_file_edges.csv', encoding='utf-8', index=False))
        else:
            self.assertEqual(0, len(mock_intrinsic.method_calls))
            self.assertEqual(0, len(mock_edges.method_calls))

        if 'export_rdf' in options:
            self.assertEqual(len(frames)*2, len(mock_gzip.call_args_list))
            self.assertEqual(call('data/my_file_intrinsic.gz', compresslevel=9, mode='wb'), mock_gzip.call_args_list[0])
            self.assertEqual(call('data/my_file_edges.gz', compresslevel=9, mode='wb'), mock_gzip.call_args_list[1])
        else:
            self.assertEqual(0, len(mock_gzip.call_args_list))

        if 'console' in options:
            mock_print.assert_called()

        mock_config.assert_called()
        mock_transform.assert_called()
        mock_transform_func.assert_called()
        mock_upsert.assert_called()
