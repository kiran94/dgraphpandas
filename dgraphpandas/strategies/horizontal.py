import logging
from typing import Any, Dict, List, Callable, Union

import pandas as pd

from dgraphpandas.config import get_from_config
from dgraphpandas.strategies.vertical import vertical_transform

logger = logging.getLogger(__name__)


def horizontal_transform(
        frame: Union[str, pd.DataFrame],
        config: Dict[str, Any],
        config_file_key: str,
        **kwargs):
    '''
    Horizontally Transform a Pandas DataFrame into Intrinsic and Edge DataFrames.
    '''

    file_config: Dict[str, Any] = config['files'][config_file_key]
    type_overrides: Dict[str, str] = get_from_config('type_overrides', file_config, {}, **(kwargs))
    subject_fields: Union[List[str], Callable[..., List[str]]] = get_from_config('subject_fields', file_config, **(kwargs))

    if isinstance(frame, str):
        logger.debug(f'Reading file {frame}')
        frame = pd.read_csv(frame)

    '''
    Ensure that object values have the correct type according to type_overrides.
    For example, when pandas reads a csv and detects a numerical value it may decide to
    represent them as a float e.g 10.0 so when it's melted into a string it will show as such
    But we really want the value to be just 10 so it matches the corresponding rdf type.
    Therefore before we melt the frame, we enforce these columns have the correct form.
    '''
    logger.debug('Applying Type Overrides %s', type_overrides)
    for col, type in type_overrides.items():
        try:
            logger.debug(f'Converting {col} to {type}')
            frame[col] = frame[col].astype(type)
        except ValueError:
            logger.exception(
                f'''
                Could not convert {col} to {type}.
                Please confirm that the values in the {col} series are convertable to {type}.
                A common scenario here is when we have NA values but the target type does not support them.
                ''')
            exit()

    '''
    Pivot the Horizontal DataFrame based on the given key (subject).
    Change the frame to be 3 columns with triples: subject, predicate, object

    This changes the horizontal frame into a vertical frame as this more closely
    resembles rdf triples.
    '''
    logger.debug(f'Melting frame with subject: {subject_fields}')
    frame = frame.melt(
        id_vars=subject_fields,
        var_name='predicate',
        value_name='object')

    return vertical_transform(frame, config, config_file_key, **(kwargs))
