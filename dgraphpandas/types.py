import os

from typing import Dict
import logging

logger = logging.getLogger(__name__)

default_rdf_type = os.environ.get('DEFAULT_RDF_TYPE', '<xs:string>')
default_dgraph_type = os.environ.get('DEFAULT_DGRAPH_TYPE', 'string')


_str_to_rdf_types = {
    'string': '<xs:string>',
    'object': '<xs:string>',
    'O': '<xs:string>',
    'int': '<xs:int>',
    'int32': '<xs:int>',
    'int64': '<xs:int>',
    'Int64': '<xs:int>',
    'float': '<xs:float>',
    'float32': '<xs:float>',
    'float64': '<xs:float>',
    'datetime64': '<xs:dateTime>',
    'datetime': '<xs:dateTime>',
    '<M8[ns]': '<xs:dateTime>',
    'bool': '<xs:boolean>',
    'boolean': '<xs:boolean>'
}

_str_to_dgraph_type = {
    'string': 'string',
    'object': 'string',
    'O': 'string',
    'int': 'int',
    'int32': 'int',
    'int64': 'int',
    'Int64': 'int',
    'float': 'float',
    'float32': 'float',
    'float64': 'float',
    'datetime64': 'dateTime',
    'datetime': 'dateTime',
    '<M8[ns]': 'dateTime',
    'bool': 'bool',
    'boolean': 'bool'
}


def find_rdf_types(types: Dict[str, str]) -> Dict[str, str]:
    '''
    Converts a User type into an RDF type.
    '''
    if types is None:
        raise ValueError('types')

    resolved_types: Dict[str, str] = {}
    for col, current_type in types.items():
        resolved_types[col] = _str_to_rdf_types.get(current_type, default_rdf_type)
    return resolved_types


def find_dgraph_type(types: Dict[str, str]) -> Dict[str, str]:
    '''
    Converts a User type into an DGraph type.
    '''
    if types is None:
        raise ValueError('types')

    resolved_types: Dict[str, str] = {}
    for col, current_type in types.items():
        resolved_types[col] = _str_to_dgraph_type.get(current_type, default_dgraph_type)
    return resolved_types
