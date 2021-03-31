import os

from typing import Dict
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

default_rdf_type = os.environ.get('DEFAULT_RDF_TYPE', '<xs:string>')


numpy_str_to_rdf_types = {
    'object': '<xs:string>',
    'O': '<xs:string>',
    'int32': '<xs:int>',
    'int64': '<xs:int>',
    'Int32': '<xs:int>',
    'Int64': '<xs:int>',
    'float32': '<xs:float>',
    'float64': '<xs:float>',
    'Float32': '<xs:float>',
    'Float64': '<xs:float>',
    'datetime64': '<xs:dateTime>',
    '<M8[ns]': '<xs:dateTime>',
    'bool': '<xs:bool>'
}

numpy_to_rdf_types = {
    key: np.dtype(key) for key, value in numpy_str_to_rdf_types.items()
}


def find_rdf_types(
        frame: pd.DataFrame,
        overrides: Dict[str, str] = None) -> Dict[str, str]:
    '''
    For the columns in the given DataFrame, build a dictionary of column -> rdf type.
    '''
    derived_types = frame.dtypes.to_dict()
    derived_types = {key: numpy_to_rdf_types.get(value, default_rdf_type) for key, value in derived_types.items()}

    if overrides is not None:
        logger.info(f'Overriding rdf types with {overrides}')
        derived_types.update(overrides)

    return derived_types


def find_rdf_types_2(frame: pd.DataFrame, types: Dict[str, str]):

    resolved_types = {}
    for col, type in types.items():
        resolved_types[col] = numpy_str_to_rdf_types.get(type, default_rdf_type)
    return resolved_types
