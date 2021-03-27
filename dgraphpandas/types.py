import os

from typing import Dict
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

default_rdf_type = os.environ.get('DEFAULT_RDF_TYPE', '<xs:string>')

numpy_to_rdf_types = {
    np.dtype('O'): '<xs:string>',
    np.dtype('int32'): '<xs:integer>',
    np.dtype('int64'): '<xs:integer>',
    np.dtype('float32'): '<xs:float>',
    np.dtype('float64'): '<xs:float>',
    np.dtype('datetime64'): '<xs:float>'
}

def find_rdf_types(
    frame: pd.DataFrame,
    overrides: Dict[str, str] = None) -> Dict[str, str]:
    '''
    For the columns in the given DataFrame, build a dictionary of column -> rdf type.
    '''
    derived_types = frame.dtypes.to_dict()
    derived_types = {key:numpy_to_rdf_types.get(value, default_rdf_type) for key, value in derived_types.items()}

    if overrides is not None:
        logger.info(f'Overriding rdf types with {overrides}')
        derived_types.update(overrides)

    return derived_types