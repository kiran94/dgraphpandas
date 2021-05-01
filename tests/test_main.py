from unittest.mock import patch, Mock
from typing import List

import pytest
from parameterized import parameterized

from dgraphpandas import __version__
from dgraphpandas.__main__ import main


@patch('dgraphpandas.__main__.sys')
def test_no_arguments(argv_mock: Mock, capsys):
    '''
    Ensures when no arguments are passed, then usage is outputted
    '''
    with pytest.raises(SystemExit, match='2'):
        argv_mock.argv = ['script']
        main()

    assert 'usage:' in capsys.readouterr().err


@patch('dgraphpandas.__main__.sys')
def test_missing_required_arguments(argv_mock: Mock, capsys):
    '''
    Ensures when the required arguments are missing then usage is outputted
    '''
    with pytest.raises(SystemExit, match='2'):
        argv_mock.argv = [
            'script',
            '-x', 'upsert',
            '-f', 'my_file.csv',
            # '-c', 'my_config.json' # <- missing config
        ]
        main()

    assert 'usage:' in capsys.readouterr().err


@patch('dgraphpandas.__main__.logging')
@patch('dgraphpandas.__main__.to_rdf')
@patch('dgraphpandas.__main__.sys')
def test_bad_operation(
        argv_mock: Mock,
        to_rdf_mock: Mock,
        logger_mock: Mock,
        capsys):
    '''
    Ensures when the operation is unknown, then usage is raised
    '''
    argv_mock.argv = [
        'script',
        '-x', 'unknown',
        '-f', 'my_file.csv',
        '-c', 'my_config.json'
    ]

    with pytest.raises(SystemExit, match='2'):
        main()

    assert "invalid choice: 'unknown'" in capsys.readouterr().err


@patch('dgraphpandas.__main__.logging')
@patch('dgraphpandas.__main__.to_rdf')
@patch('dgraphpandas.__main__.sys')
def test_upsert_missing_config_file_key(
        argv_mock: Mock,
        to_rdf_mock: Mock,
        logger_mock: Mock,
        capsys):
    '''
    Ensures when config_file_key is not provided then an error is raised
    '''
    argv_mock.argv = [
        'script',
        '-x', 'upserts',
        '-f', 'my_file.csv',
        '-c', 'my_config.json'
    ]

    with pytest.raises(ValueError, match='config_file_key must be provided in upsert mode'):
        main()


@patch('dgraphpandas.__main__.logging')
@patch('dgraphpandas.__main__.to_rdf')
@patch('dgraphpandas.__main__.sys')
def test_upsert_missing_file(
        argv_mock: Mock,
        to_rdf_mock: Mock,
        logger_mock: Mock,
        capsys):
    '''
    Ensures when file is not provided then an error is raised
    '''
    argv_mock.argv = [
        'script',
        '-x', 'upserts',
        '-c', 'config.json',
        '-ck', 'my_key'
    ]

    with pytest.raises(ValueError, match='file must be provided in upsert mode'):
        main()


@patch('dgraphpandas.__main__.logging')
@patch('dgraphpandas.__main__.to_rdf')
@patch('dgraphpandas.__main__.sys')
def test_upsert(
        argv_mock: Mock,
        to_rdf_mock: Mock,
        logger_mock: Mock,
        capsys):
    '''
    Ensures when upsert is called with required parameters
    then to_rdf is called
    '''
    argv_mock.argv = [
        'script',
        '-x', 'upserts',
        '-c', 'config.json',
        '-ck', 'my_key',
        '-f', 'my_file'
    ]

    main()

    assert to_rdf_mock.called
    args, kwargs = to_rdf_mock.call_args_list[0]

    assert args == ('my_file', 'config.json', 'my_key', '.')
    assert kwargs == {
        'export_rdf': True,
        'add_dgraph_type_records': True,
        'drop_na_intrinsic_objects': True,
        'drop_na_edge_objects': True,
        'illegal_characters': ['%', '\\.', '\\s', '"', '\\n', '\\r\\n'],
        'illegal_characters_intrinsic_object': ['"', '\\n', '\\r\\n'],
        'chunk_size': 10000000
    }


@patch('dgraphpandas.__main__.logging')
@patch('dgraphpandas.__main__.generate_schema')
@patch('dgraphpandas.__main__.create_schema')
@patch('dgraphpandas.__main__.sys')
def test_schema(
        argv_mock: Mock,
        create_schema_mock: Mock,
        generate_schema_mock: Mock,
        logger_mock: Mock,
        capsys):
    '''
    Ensure when schema operation is called with the valid args
    then generate_schema is called
    '''
    argv_mock.argv = [
        'script',
        '-x', 'schema',
        '-c', 'config.json',
    ]

    create_schema_mock.return_value = 'fake_schema'

    main()

    assert create_schema_mock.called
    assert generate_schema_mock.called

    args, kwargs = create_schema_mock.call_args_list[0]

    assert args == ('config.json',)
    assert kwargs == {
        'add_dgraph_type_records': True,
        'drop_na_intrinsic_objects': True,
        'drop_na_edge_objects': True,
        'illegal_characters': ['%', '\\.', '\\s', '"', '\\n', '\\r\\n'],
        'illegal_characters_intrinsic_object': ['"', '\\n', '\\r\\n'],
        'chunk_size': 10000000,
        'ensure_xid_predicate': True
    }

    args, kwargs = generate_schema_mock.call_args_list[0]

    assert args == ('fake_schema',)
    assert kwargs == {
        'add_dgraph_type_records': True,
        'drop_na_intrinsic_objects': True,
        'drop_na_edge_objects': True,
        'illegal_characters': ['%', '\\.', '\\s', '"', '\\n', '\\r\\n'],
        'illegal_characters_intrinsic_object': ['"', '\\n', '\\r\\n'],
        'chunk_size': 10000000,
        'export_schema': True
    }


@parameterized.expand([
    ([], 'INFO'),
    (['--verbosity', 'DEBUG'], 'DEBUG'),
    (['--verbosity', 'INFO'], 'INFO'),
    (['--verbosity', 'WARNING'], 'WARNING'),
    (['--verbosity', 'ERROR'], 'ERROR'),
    (['--verbosity', 'NOTSET'], 'NOTSET'),
])
@patch('dgraphpandas.__main__.logging')
@patch('dgraphpandas.__main__.generate_schema')
@patch('dgraphpandas.__main__.create_schema')
@patch('dgraphpandas.__main__.sys')
def test_logging_verbosity_set(
        logging_options: List[str],
        expected_set_level: str,
        argv_mock: Mock,
        create_schema_mock: Mock,
        generate_schema_mock: Mock,
        logger_mock: Mock):
    '''
    Ensures when logging is set via cli then its passed
    to the logger
    '''
    argv_mock.argv = [
        'script',
        '-x', 'schema',
        '-c', 'config.json'
    ]
    argv_mock.argv.extend(logging_options)

    main()

    args, kwargs = logger_mock.basicConfig.call_args_list[0]

    assert not args
    assert kwargs == {'level': expected_set_level}


@patch('dgraphpandas.__main__.sys')
def test_version(
        argv_mock: Mock,
        capsys):
    '''
    Ensures when the version is requested then it is printed
    to stdout
    '''
    argv_mock.argv = ['script', '--version']

    with pytest.raises(SystemExit, match='0'):
        main()

    assert __version__ in capsys.readouterr().out


@patch('dgraphpandas.__main__.logging')
@patch('dgraphpandas.__main__.generate_types')
@patch('dgraphpandas.__main__.create_schema')
@patch('dgraphpandas.__main__.sys')
def test_types(
        argv_mock: Mock,
        create_schema_mock: Mock,
        generate_types_mock: Mock,
        logger_mock: Mock,
        capsys):
    '''
    Ensures when types is called, the underlying services
    are called
    '''
    argv_mock.argv = [
        'script',
        '-x', 'types',
        '-c', 'config.json',
    ]

    create_schema_mock.return_value = 'fake_schema'

    main()

    assert create_schema_mock.called
    assert generate_types_mock.called

    args, kwargs = create_schema_mock.call_args_list[0]

    assert args == ('config.json',)
    assert kwargs == {
        'add_dgraph_type_records': True,
        'drop_na_intrinsic_objects': True,
        'drop_na_edge_objects': True,
        'illegal_characters': ['%', '\\.', '\\s', '"', '\\n', '\\r\\n'],
        'illegal_characters_intrinsic_object': ['"', '\\n', '\\r\\n'],
        'chunk_size': 10000000,
        'ensure_xid_predicate': True
    }

    args, kwargs = generate_types_mock.call_args_list[0]

    assert args == ('fake_schema',)
    assert kwargs == {
        'add_dgraph_type_records': True,
        'drop_na_intrinsic_objects': True,
        'drop_na_edge_objects': True,
        'illegal_characters': ['%', '\\.', '\\s', '"', '\\n', '\\r\\n'],
        'illegal_characters_intrinsic_object': ['"', '\\n', '\\r\\n'],
        'chunk_size': 10000000,
        'export_schema': True
    }
