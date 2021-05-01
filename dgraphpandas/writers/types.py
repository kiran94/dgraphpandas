import os
import logging
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


def generate_types(frame: pd.DataFrame, **kwargs) -> List[str]:
    '''
    Given the pre-processed DataFrame from the schema
    strategy, generate types.
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
    export_file = kwargs.get('export_file', 'types.txt')
    console = kwargs.get('console', False)
    encoding = kwargs.get('encoding', 'utf-8')
    line_delimeter = kwargs.get('line_delimeter ', '\n')

    all_types: List[str] = []
    all_types_reverse: List[str] = []

    tables = frame.groupby(by=['table'])
    for name, current_frame in tables:
        logger.debug(f'Creating types for {name}')

        reverse_edge_mask = (~current_frame['options'].isnull()) & current_frame['options'].str.contains('@reverse')
        current_frame.loc[reverse_edge_mask, 'column'] = '<~' + current_frame['column'] + '>'

        type_builder = 'type ' + name
        type_builder += ' { '
        type_builder += line_delimeter
        type_builder += line_delimeter.join(current_frame['column'].unique().tolist())
        type_builder += line_delimeter
        type_builder += ' }'
        type_builder += line_delimeter

        # Split up types with reverse edges so we can gurantee they are applied after other types
        # This is required because if dgraph live encounters a reverse edge for a type defined later in the file
        # then dgraph live will fails.
        # NOTE: There might be a better solution here
        # and we could build a dependency tree based on the references
        # topological sort?
        # also this won't detect circular dependencies
        if current_frame.loc[reverse_edge_mask, 'column'].shape[0]:
            all_types_reverse.append(type_builder)
        else:
            all_types.append(type_builder)

        if console:
            print(type_builder)
            print(line_delimeter)

    if export_schema:
        export_path = os.path.join(output_dir, export_file)
        logger.debug(f'Writing to {export_path} ({encoding})')
        with open(export_path, 'w', encoding=encoding) as f:
            for current_type in all_types:
                f.write(current_type)
                f.write('\n')
            for current_type in all_types_reverse:
                f.write(current_type)
                f.write('\n')

    return all_types + all_types_reverse
