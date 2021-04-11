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
    if frame is None:
        raise ValueError('frame')
    if not config:
        raise ValueError('config')
    if not config_file_key:
        raise ValueError('config_file_key')

    file_config: Dict[str, Any] = config['files'][config_file_key]
    type_overrides: Dict[str, str] = get_from_config('type_overrides', file_config, {}, **(kwargs))
    subject_fields: Union[List[str], Callable[..., List[str]]] = get_from_config('subject_fields', file_config, **(kwargs))
    date_fields: Dict[str, str] = get_from_config('date_fields', file_config, {}, **(kwargs))

    if not subject_fields:
        raise ValueError('subject_fields')

    if isinstance(frame, str):
        logger.debug(f'Reading file {frame}')
        read_csv_options: Dict[str, Any] = get_from_config('read_csv_options', file_config, {}, **(kwargs))
        frame = pd.read_csv(frame, **(read_csv_options))

    if frame.shape[1] <= len(subject_fields):
        raise ValueError(f'''
            It looks like there are no data fields.
            The subject_fields are {subject_fields}
            The frame columns are {frame.columns}
        ''')

    '''
    Date Fields get special treatment as they can be represented in many different ways
    from different sources. Therefore if the column has been defined in date_fields
    then apply those options to that column.
    '''
    for col, date_format in date_fields.items():
        date_format = date_fields[col]
        logger.debug(f'Converting {col} to datetime: {date_format}')
        frame[col] = pd.to_datetime(frame[col], **(date_format))
        if col not in type_overrides:
            logger.debug(f'Ensuring {col} has datetime64 type')
            type_overrides[col] = 'datetime64'

    '''
    Ensure that object values have the correct type according to type_overrides.
    For example, when pandas reads a csv and detects a numerical value it may decide to
    represent them as a float e.g 10.0 so when it's melted into a string it will show as such
    But we really want the value to be just 10 so it matches the corresponding rdf type.
    Therefore before we melt the frame, we enforce these columns have the correct form.
    '''
    logger.debug('Applying Type Overrides %s', type_overrides)
    for col, current_type in type_overrides.items():
        try:
            logger.debug(f'Converting {col} to {current_type}')
            frame[col] = frame[col].astype(current_type)
        except ValueError:
            logger.exception(
                f'''
                Could not convert {col} to {current_type}.
                Please confirm that the values in the {col} series are convertable to {current_type}.
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
