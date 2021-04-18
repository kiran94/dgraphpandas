import os
import logging
from typing import List
import pandas as pd

logger = logging.getLogger(__name__)


def generate_schema(frame: pd.DataFrame, **kwargs) -> str:
    '''
    Given the pre-processed DataFrame from the schema
    strategy, generate a schema.
    '''
    if frame is None:
        raise ValueError('frame')
    if 'column' not in frame:
        raise ValueError('column')
    if 'type' not in frame:
        raise ValueError('type')
    if 'table' not in frame:
        raise ValueError('table')
    if 'options' not in frame:
        raise ValueError('options')

    output_dir = kwargs.get('output_dir', '.')
    export_schema = kwargs.get('export_schema', False)
    export_file = kwargs.get('export_file', 'schema.txt')

    logger.debug('Concatting columns')
    frame.loc[frame['options'].isnull(), 'joined'] = frame['column'] + ': ' + frame['type'] + ' .'
    frame.loc[~frame['options'].isnull(), 'joined'] = frame['column'] + ': ' + frame['type'] + ' ' + frame['options'] + ' .'

    logger.debug('Joining Expressions into string')
    joined_expression_frame: List[str] = frame['joined'].unique().tolist()
    joined_string = '\n'.join(joined_expression_frame)

    if export_schema:
        export_file = os.path.join(output_dir, export_file)
        logger.info(f'Writing schema to {export_file}')
        with open(export_file, 'w') as f:
            f.write(joined_string)

    return joined_string
