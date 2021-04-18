import pytest
import pandas as pd
from parameterized import parameterized
from dgraphpandas.writers.schema import generate_schema
from unittest.mock import patch, mock_open, Mock


def test_generate_schema_null_frame():
    '''
    Ensures when the frame is null, an exception is raised
    '''
    with pytest.raises(ValueError, match='frame'):
        generate_schema(None)


@parameterized.expand([
    ('without_column', pd.DataFrame(columns=['type', 'table', 'options']), 'column'),
    ('without_type', pd.DataFrame(columns=['column', 'table', 'options']), 'type'),
    ('without_table', pd.DataFrame(columns=['type', 'column', 'options']), 'table'),
    ('without_options', pd.DataFrame(columns=['type', 'table', 'column']), 'options'),
])
def test_generate_schema_bad_columns(name, frame, missing):
    '''
    Ensures the incoming dataframe does not the have the correct
    columns then an exception is raised
    '''
    with pytest.raises(ValueError, match=missing):
        generate_schema(frame)


@parameterized.expand([
    (
        'with_no_options',
        pd.DataFrame(
            columns=['column', 'type', 'table', 'options'],
            data=[
                ('id', 'string', 'customer', None),
                ('age', 'int', 'customer', None),
                ('subscription', 'uid', 'customer', None)
            ]
        ),
        '\n'.join([
            'id: string .',
            'age: int .',
            'subscription: uid .'
        ])
    ),
    (
        'with_options',
        pd.DataFrame(
            columns=['column', 'type', 'table', 'options'],
            data=[
                ('id', 'string', 'customer', '@index(exact)'),
                ('age', 'int', 'customer', None),
                ('subscription', 'uid', 'customer', '@reverse')
            ]
        ),
        '\n'.join([
            'id: string @index(exact) .',
            'age: int .',
            'subscription: uid @reverse .'
        ])
    ),
    (

        'with_multiple_types',
        pd.DataFrame(
            columns=['column', 'type', 'table', 'options'],
            data=[
                ('id', 'string', 'customer', '@index(exact)'),
                ('age', 'int', 'customer', None),
                ('subscription', 'uid', 'customer', '@reverse'),
                ('quantity', 'int', 'order', None),
                ('value', 'float', 'order', None),
                ('date', 'dateTime', 'order', '@index(day)'),
            ]
        ),
        '\n'.join([
            'id: string @index(exact) .',
            'age: int .',
            'subscription: uid @reverse .',
            'quantity: int .',
            'value: float .',
            'date: dateTime @index(day) .',
        ])
    ),
    (
        'with_duplicates_across_types',
        pd.DataFrame(
            columns=['column', 'type', 'table', 'options'],
            data=[
                ('updated_by', 'string', 'customer', None),
                ('updated_by', 'string', 'order', None),
                ('id', 'int', 'customer', '@index'),
            ]
        ),
        '\n'.join([
            'updated_by: string .',
            'id: int @index .',
        ])
    )
])
@patch('builtins.open', new_callable=mock_open)
def test_generate_schema(name, schema_frame, expected_schema, mock_file_open: Mock):
    '''
    Ensures when the schema frame is given, it matches the expected output.
    '''
    schema = generate_schema(schema_frame)
    assert schema == expected_schema
    assert not mock_file_open.called


@patch('builtins.open', new_callable=mock_open)
def test_generate_schema_export_schema(mock_file_open: Mock):
    '''
    Ensures when export schema is passed the schema is written to file
    '''
    frame = pd.DataFrame(
        columns=['column', 'type', 'table', 'options'],
        data=[
            ('id', 'string', 'customer', '@index(exact)'),
            ('age', 'int', 'customer', None),
            ('subscription', 'uid', 'customer', '@reverse'),
            ('quantity', 'int', 'order', None),
            ('value', 'float', 'order', None),
            ('date', 'dateTime', 'order', '@index(day)'),
        ]
    )

    expected_schema = '\n'.join([
        'id: string @index(exact) .',
        'age: int .',
        'subscription: uid @reverse .',
        'quantity: int .',
        'value: float .',
        'date: dateTime @index(day) .',
    ])

    schema = generate_schema(frame, export_schema=True)
    args, kwargs = mock_file_open.call_args_list[0]

    assert mock_file_open.called
    assert ('./schema.txt', 'w') == args
    assert not kwargs
    assert schema == expected_schema
