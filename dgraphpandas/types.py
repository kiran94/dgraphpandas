import os

from typing import Dict
import logging

logger = logging.getLogger(__name__)

default_rdf_type = os.environ.get('DEFAULT_RDF_TYPE', '<xs:string>')


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
    'bool': '<xs:bool>',
    'boolean': '<xs:bool>'
}


def find_rdf_types(types: Dict[str, str]) -> Dict[str, str]:
    '''
    Converts a User type into an RDF type.
    '''
    if types is None:
        raise ValueError('types')

    resolved_types: Dict[str, str] = {}
    for col, type in types.items():
        resolved_types[col] = _str_to_rdf_types.get(type, default_rdf_type)
    return resolved_types
