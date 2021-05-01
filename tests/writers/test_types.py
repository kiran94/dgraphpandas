from unittest.mock import patch, Mock, mock_open

import pytest
from parameterized import parameterized
import pandas as pd

from dgraphpandas.writers.types import generate_types


@parameterized.expand([
    ('without_frame', None),
    ('without_column', pd.DataFrame(columns=['type', 'table', 'options'])),
    ('without_type', pd.DataFrame(columns=['column', 'table', 'options'])),
    ('without_table', pd.DataFrame(columns=['column', 'type', 'options'])),
    ('with_options', pd.DataFrame(columns=['column', 'type', 'table'])),
])
def test_invalid_parameters(name, frame: pd.DataFrame):
    '''
    Ensures when invalid parameters are given,
    then an error is raised
    '''
    with pytest.raises(ValueError, match='frame|column|type|options|table'):
        generate_types(frame)


def test_empty_valid_frame():
    '''
    Ensures when the frame is valid but empty,
    an empty list is returned
    '''
    frame = pd.DataFrame(columns=['column', 'type', 'table', 'options'])
    result = generate_types(frame)
    assert result == []


@parameterized.expand([
    ###
    (
        'single_table',
        pd.DataFrame(
            columns=['column', 'type', 'table', 'options'],
            data=[
                ('id', 'string', 'customer', '@index(exact)'),
                ('age', 'int', 'customer', None),
                ('dob', 'datetime', 'customer', None),
            ]),
        ["type customer { \nid\nage\ndob\n }\n"]
    ),
    ###
    (
        'multiple_tables',
        pd.DataFrame(
            columns=['column', 'type', 'table', 'options'],
            data=[
                ('id', 'string', 'customer', '@index(exact)'),
                ('age', 'int', 'customer', None),
                ('dob', 'datetime', 'customer', None),
                ('id', 'string', 'order', '@index(hash)'),
                ('value', 'double', 'order', None),
                ('product_id', 'string', 'product', '@index(hash)'),
                ('name', 'double', 'product', None),
            ]),
        ["type customer { \nid\nage\ndob\n }\n", "type order { \nid\nvalue\n }\n", "type product { \nproduct_id\nname\n }\n"]
    ),
    ###
    (
        'with_reverse',
        pd.DataFrame(
            columns=['column', 'type', 'table', 'options'],
            data=[
                ('id', 'string', 'customer', '@index(exact)'),
                ('age', 'int', 'customer', None),
                ('order', '[uid]', 'customer', '@reverse'),
                ('gender', '[uid]', 'customer', '@reverse'),
                ('id', 'string', 'order', '@index(hash)'),
                ('value', 'double', 'order', None),
                ('invoice', 'uid', 'order', '@reverse'),
            ]),
        ["type customer { \nid\nage\n<~order>\n<~gender>\n }\n", "type order { \nid\nvalue\n<~invoice>\n }\n"]
    )
])
def test_types_generated(name, frame, expected_result):
    '''
    Ensures types are generated when a valid frame is passed
    '''
    assert generate_types(frame) == expected_result


@patch('builtins.open', callable=mock_open)
def test_types_export_schema(mock_open: Mock):
    '''
    Ensures when export schema is passed then
    the file is written
    '''
    frame = pd.DataFrame(
        columns=['column', 'type', 'table', 'options'],
        data=[
            ('id', 'string', 'customer', '@index(exact)'),
            ('age', 'int', 'customer', None),
            ('order', '[uid]', 'customer', '@reverse'),
            ('gender', '[uid]', 'customer', '@reverse'),
            ('id', 'string', 'order', '@index(hash)'),
            ('value', 'double', 'order', None),
            ('invoice', 'uid', 'order', '@reverse'),
        ])

    expected = ["type customer { \nid\nage\n<~order>\n<~gender>\n }\n", "type order { \nid\nvalue\n<~invoice>\n }\n"]

    result = generate_types(frame, export_schema=True)

    assert result == expected

    args, kwargs = mock_open.call_args_list[0]
    assert args == ('./types.txt', 'w')
    assert kwargs == {'encoding': 'utf-8'}


def test_console(capsys):
    '''
    Ensures when console is provided, then the type builder
    is written to stdout
    '''
    frame = pd.DataFrame(
        columns=['column', 'type', 'table', 'options'],
        data=[
            ('id', 'string', 'customer', '@index(exact)'),
            ('age', 'int', 'customer', None),
            ('order', '[uid]', 'customer', '@reverse'),
            ('gender', '[uid]', 'customer', '@reverse'),
            ('id', 'string', 'order', '@index(hash)'),
            ('value', 'double', 'order', None),
            ('invoice', 'uid', 'order', '@reverse'),
        ])

    expected = ["type customer { \nid\nage\n<~order>\n<~gender>\n }\n", "type order { \nid\nvalue\n<~invoice>\n }\n"]

    result = generate_types(frame, console=True)

    assert result == expected

    out, err = capsys.readouterr()
    for ex in expected:
        assert ex in out
