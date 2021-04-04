import os

from typing import Dict
import logging

logger = logging.getLogger(__name__)

default_rdf_type = os.environ.get('DEFAULT_RDF_TYPE', '<xs:string>')


numpy_str_to_rdf_types = {
    'object': '<xs:string>',
    'O': '<xs:string>',
    'int32': '<xs:int>',
    'float32': '<xs:float>',
    'int64': '<xs:int>',
    'Int64': '<xs:int>',
    'float64': '<xs:float>',
    'datetime64': '<xs:dateTime>',
    '<M8[ns]': '<xs:dateTime>',
    'bool': '<xs:bool>'
}


def find_rdf_types(types: Dict[str, str]):

    if types is None:
        raise ValueError('types')

    resolved_types = {}
    for col, type in types.items():
        resolved_types[col] = numpy_str_to_rdf_types.get(type, default_rdf_type)
    return resolved_types
