from unittest.mock import patch, Mock, mock_open

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from parameterized import parameterized

from dgraphpandas.strategies.schema import create_schema


def test_create_schema_none_source_config():
    '''
    Ensures when the source config is None, then an error is raised
    '''
    with pytest.raises(ValueError):
        create_schema(None)


@patch('dgraphpandas.strategies.schema.json.load')
@patch('builtins.open', callable=mock_open)
def test_create_schema_no_files(mock_file: Mock, mock_json_loads: Mock):
    '''
    Ensures when no files is defined in config then an error is raised
    '''
    mock_json_loads.return_value = {"some_other_entry": {}}
    with pytest.raises(KeyError, match='files'):
        create_schema('my_config.json')

    mock_file.assert_called()
    mock_json_loads.assert_called()

    args, kwargs = mock_file.call_args_list[0]
    assert args == ('my_config.json', 'r')
    assert kwargs == {}


@patch('dgraphpandas.strategies.schema.json.load')
@patch('builtins.open', callable=mock_open)
def test_create_schema_file_path_config(mock_file: Mock, mock_json_loads: Mock):
    '''
    Ensures when a file path is passed which has files attribute, is it loaded
    but nothing is returned
    '''
    mock_json_loads.return_value = {"files": {}}
    result = create_schema('my_config.json')

    assert result is None

    mock_file.assert_called()
    mock_json_loads.assert_called()

    args, kwargs = mock_file.call_args_list[0]
    assert args == ('my_config.json', 'r')
    assert kwargs == {}


@parameterized.expand([
    ###
    (
        'single_file',
        {
            'files': {
                "animal": {
                    'edge_fields': ['habitat']
                }
            }
        },
        'animal does not have subject_fields.'
    ),
    ###
    (
        'multiple_files_after_first_has_no_subject_fields',
        {
            'files': {
                "animal": {
                    'subject_fields': ['animal_id'],
                    'edge_fields': ['habitat']
                },
                "habitat": {
                    'edge_fields': ['animal_id']
                }
            }
        },
        'habitat does not have subject_fields.'
    ),
    ###
    (
        'multiple_files_first_has_no_subject_fields',
        {
            'files': {
                "habitat": {
                    'edge_fields': ['animal_id']
                },
                "animal": {
                    'subject_fields': ['animal_id'],
                    'edge_fields': ['habitat']
                }
            }
        },
        'habitat does not have subject_fields.'
    ),
])
def test_create_schema_missing_subject_fields(name, config, expected_error_message):
    '''
    Ensures when the schema is missing subject fields, then an error is raised
    '''
    with pytest.raises(ValueError, match=expected_error_message):
        create_schema(config)


@parameterized.expand([
    ###
    (
        'only_subject_fields',
        {
            'files': {
                "animal": {
                    'subject_fields': ['animal_id'],
                }
            }
        },
        pd.DataFrame(data={
            'column': ['animal'],
            'type': ['string'],
            'table': 'animal',
            'options': None
        })
    ),
    ###
    (
        'with_type_overrides',
        {
            'files': {
                "animal": {
                    'subject_fields': ['animal_id'],
                    'type_overrides': {
                        'legs': 'int',
                        'weight': 'float'
                    }
                }
            }
        },
        pd.DataFrame(data={
            'column': ['animal', 'legs', 'weight'],
            'type': ['string', 'int', 'float'],
            'table': ['animal']*3,
            'options': [None]*3
        })
    ),
    ###
    (
        'with_edge_fields',
        {
            'files': {
                "animal": {
                    'subject_fields': ['animal_id'],
                    'type_overrides': {
                        'legs': 'int',
                        'weight': 'float'
                    },
                    'edge_fields': ['habitat_id']
                }
            }
        },
        pd.DataFrame(data={
            'column': ['animal', 'legs', 'weight', 'habitat'],
            'type': ['string', 'int', 'float', 'uid'],
            'table': ['animal']*4,
            'options': [None]*4
        })
    ),
    ###
    (
        'with_csv_edges',
        {
            'files': {
                "animal": {
                    'subject_fields': ['animal_id'],
                    'type_overrides': {
                        'legs': 'int',
                        'weight': 'float'
                    },
                    'edge_fields': ['habitat_id'],
                    'csv_edges': ['group_id']
                }
            }
        },
        pd.DataFrame(data=[
            ('animal', 'string', 'animal', None),
            ('legs', 'int', 'animal', None),
            ('weight', 'float', 'animal', None),
            ('habitat', 'uid', 'animal', None),
            ('group', 'uid', 'animal', None),
        ], columns=['column', 'type', 'table', 'options'])
    ),
    ###
    (
        'with_ignore_fields',
        {
            'files': {
                "animal": {
                    'subject_fields': ['animal_id'],
                    'type_overrides': {
                        'legs': 'int',
                        'weight': 'float'
                    },
                    'edge_fields': ['habitat_id', 'group_id'],
                    'ignore_fields': ['group_id']
                }
            }
        },
        pd.DataFrame(data=[
            ('animal', 'string', 'animal', None),
            ('legs', 'int', 'animal', None),
            ('weight', 'float', 'animal', None),
            ('habitat', 'uid', 'animal', None),
        ], columns=['column', 'type', 'table', 'options'])
    ),
    ###
    (
        'with_override_edge_fields',
        {
            'files': {
                "animal": {
                    'subject_fields': ['animal_id'],
                    'type_overrides': {
                        'legs': 'int',
                        'weight': 'float'
                    },
                    'edge_fields': ['habitat_id'],
                    'override_edge_name': {
                        'habitat_id': {
                            'predicate': 'habo'
                        }
                    }
                }
            }
        },
        pd.DataFrame(data=[
            ('animal', 'string', 'animal', None),
            ('legs', 'int', 'animal', None),
            ('weight', 'float', 'animal', None),
            ('habo', 'uid', 'animal', None),
            ('habitat', 'uid', 'animal', None),
        ], columns=['column', 'type', 'table', 'options'])
    ),
    ###
    (
        'without_strip_id_from_edge_names',
        {
            'strip_id_from_edge_names': False,
            'files': {
                "animal": {
                    'subject_fields': ['animal_id'],
                    'type_overrides': {
                        'legs': 'int',
                        'weight': 'float'
                    },
                    'edge_fields': ['habitat_id']
                }
            }
        },
        pd.DataFrame(data=[
            ('animal_id', 'string', 'animal', None),
            ('legs', 'int', 'animal', None),
            ('weight', 'float', 'animal', None),
            ('habitat_id', 'uid', 'animal', None),
        ], columns=['column', 'type', 'table', 'options'])
    ),
    ###
    (
        'with_prerename',
        {
            'files': {
                "animal": {
                    'subject_fields': ['animal_id'],
                    'type_overrides': {
                        'legs': 'int',
                        'weight': 'float'
                    },
                    'edge_fields': ['habitat_id'],
                    'pre_rename': {
                        'legs': 'number_of_legs',
                        'habitat': 'habo'
                    }
                }
            }
        },
        pd.DataFrame(data=[
            ('animal', 'string', 'animal', None),
            ('number_of_legs', 'int', 'animal', None),
            ('weight', 'float', 'animal', None),
            ('habo', 'uid', 'animal', None),
        ], columns=['column', 'type', 'table', 'options'])
    ),
    ###
    (
        'with_prename_on_list_edge',
        {
            'files': {
                "animal": {
                    'subject_fields': ['animal_id'],
                    'type_overrides': {
                        'legs': 'int',
                        'weight': 'float'
                    },
                    'edge_fields': ['habitat_id'],
                    'pre_rename': {
                        'legs': 'number_of_legs',
                        'habitat': 'habo'
                    },
                    'list_edges': ['habitat_id']
                }
            }
        },
        pd.DataFrame(data=[
            ('animal', 'string', 'animal', None),
            ('number_of_legs', 'int', 'animal', None),
            ('weight', 'float', 'animal', None),
            ('habo', '[uid]', 'animal', None),
        ], columns=['column', 'type', 'table', 'options'])
    ),
    ###
    (
        'with_options',
        {
            'files': {
                "animal": {
                    'subject_fields': ['animal_id'],
                    'type_overrides': {
                        'legs': 'int',
                        'weight': 'float'
                    },
                    'edge_fields': ['habitat_id'],
                    'options': {
                        'legs': ['@count', '@index(exact)'],
                        'habitat': ['@reverse']
                    }
                }
            }
        },
        pd.DataFrame(data=[
            ('animal', 'string', 'animal', None),
            ('legs', 'int', 'animal', '@count @index(exact)'),
            ('weight', 'float', 'animal', None),
            ('habitat', 'uid', 'animal', '@reverse'),
        ], columns=['column', 'type', 'table', 'options'])
    ),
    ###
    (
        'with_multiple_files',
        {
            'files': {
                "animal": {
                    'subject_fields': ['animal_id'],
                    'type_overrides': {
                        'legs': 'int',
                        'weight': 'float'
                    },
                    'edge_fields': ['habitat_id'],
                    'options': {
                        'legs': ['@count', '@index(exact)'],
                        'habitat': ['@reverse']
                    }
                },
                'habitat': {
                    'subject_fields': ['habitat_id'],
                    'type_overrides': {
                        'color': 'string',
                        'sea_level': 'int'
                    },
                    'edge_fields': ['country_id'],
                    'options': {
                        'color': ['@index(hash)']
                    }
                }
            }
        },
        pd.DataFrame(data=[
            ('animal', 'string', 'animal', None),
            ('legs', 'int', 'animal', '@count @index(exact)'),
            ('weight', 'float', 'animal', None),
            ('habitat', 'uid', 'animal', '@reverse'),
            ('habitat', 'string', 'habitat', None),
            ('color', 'string', 'habitat', '@index(hash)'),
            ('country', 'uid', 'habitat', None),
            ('sea_level', 'int', 'habitat', None),
        ], columns=['column', 'type', 'table', 'options'])
    ),

    ###
    (
        'with_list_edges',
        {
            'files': {
                "animal": {
                    'subject_fields': ['animal_id'],
                    'type_overrides': {
                        'legs': 'int',
                        'weight': 'float'
                    },
                    'edge_fields': ['habitat_id', 'predator_id'],
                    'list_edges': ['predator_id']
                }
            }
        },
        pd.DataFrame(data=[
            ('animal', 'string', 'animal', None),
            ('legs', 'int', 'animal'),
            ('weight', 'float', 'animal', None),
            ('habitat', 'uid', 'animal'),
            ('predator', '[uid]', 'animal'),
        ], columns=['column', 'type', 'table', 'options'])
    ),

])
def test_create_schema(name, config, expected_frame):
    '''
    Ensures given the parameterized config, the output frame matches the set expected_frame
    '''
    frame = create_schema(config)

    frame = frame.sort_values(by=['table', 'column']).reset_index(drop=True)
    expected_frame = expected_frame.sort_values(by=['table', 'column']).reset_index(drop=True)

    try:
        assert_frame_equal(frame, expected_frame)
    except AssertionError:  # pragma: no cover
        print('Actual Frame \n', frame)  # pragma: no cover
        print('####')  # pragma: no cover
        print('Expected Frame \n', expected_frame)  # pragma: no cover
        raise  # pragma: no cover


@patch('dgraphpandas.strategies.schema.pd.DataFrame.to_csv')
def test_create_schema_export_csv(to_csv_mock: Mock):
    '''
    Ensures when export_csv is passed, then the schema frame
    is passed into to_csv
    '''
    config = {
        'files': {
            "animal": {
                'subject_fields': ['id'],
                'edge_fields': ['habitat']
            }
        }
    }

    result = create_schema(config, export_csv=True)
    assert to_csv_mock.called

    args, kwargs = to_csv_mock.call_args_list[0]
    assert ('./schema.csv',) == args
    assert {'index': False} == kwargs

    expected_frame = pd.DataFrame(
        columns=['column', 'type', 'table', 'options'],
        data=[
            ('id', 'string', 'animal', None),
            ('habitat', 'uid', 'animal', None),
        ])

    assert_frame_equal(expected_frame, result)


def test_create_schema_ensure_xid():
    '''
    Ensures when ensure_xid_predicate is passed,
    then an xid predicate is appended to the schema
    '''
    config = {
        'files': {
            "animal": {
                'subject_fields': ['id'],
                'edge_fields': ['habitat']
            }
        }
    }

    expected_frame = pd.DataFrame(
        columns=['column', 'type', 'table', 'options'],
        data=[
            ('id', 'string', 'animal', None),
            ('habitat', 'uid', 'animal', None),
            ('xid', 'string', None, '@index(exact)'),
        ])

    result = create_schema(config, ensure_xid_predicate=True)
    assert_frame_equal(expected_frame, result)


@patch('builtins.print')
def test_create_schema_console(mock_print):
    '''
    Ensures when console is passed, then the frame is written to stdout.
    '''
    config = {
        'files': {
            "animal": {
                'subject_fields': ['id'],
                'edge_fields': ['habitat']
            }
        }
    }
    expected_frame = pd.DataFrame(
        columns=['column', 'type', 'table', 'options'],
        data=[
            ('id', 'string', 'animal', None),
            ('habitat', 'uid', 'animal', None),
        ])

    result = create_schema(config, console=True)
    assert_frame_equal(expected_frame, result)

    assert mock_print.called
    args, kwargs = mock_print.call_args_list[0]
    assert_frame_equal(args[0], expected_frame)
    assert kwargs == {}
