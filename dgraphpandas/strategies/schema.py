import os
import logging
import json
from typing import Union, Dict, Any, List, Set

import pandas as pd

from dgraphpandas.types import find_dgraph_type, default_dgraph_type
from dgraphpandas.config import get_from_config

logger = logging.getLogger(__name__)


def _strip_id(original: Set[str]) -> Set[str]:
    temp_columns = set()
    for i in original:
        if str.endswith(i, '_id'):
            temp_columns.add(i[:-3])
        else:
            temp_columns.add(i)

    return temp_columns


def create_schema(source_config: Union[str, Dict[str, Any]], output_dir='.', **kwargs) -> pd.DataFrame:
    '''
    Creates a Schema DataFrame from the given Configuration.

    Parameters:
        config: a file path or already materialized dictionary
    '''
    if not source_config:
        raise ValueError('source_config')

    if isinstance(source_config, str):
        logger.debug(f'Loading configuration from {source_config}')
        with open(source_config, 'r') as f:
            config: Dict[str, Any] = json.load(f)
    else:
        config: Dict[str, Any] = source_config

    all_frames: List[pd.DataFrame] = []
    files: Dict[str, Any] = config['files']
    strip_id_from_edge_names: bool = get_from_config('strip_id_from_edge_names', config, True, **(kwargs))
    console: bool = get_from_config('console', config, False, **(kwargs))
    export_csv: bool = get_from_config('export_csv', config, False, **(kwargs))
    export_csv_name: str = get_from_config('export_csv_name', config, 'schema.csv', **(kwargs))
    ensure_xid_predicate: bool = get_from_config('ensure_xid_predicate', config, False, **(kwargs))

    for file, file_config in files.items():
        logger.debug(f'Preprocessing schema for {file}')

        subject_fields: List[str] = get_from_config('subject_fields', file_config, None, **(kwargs))
        override_edge_name: Dict[str, Any] = get_from_config('override_edge_name', file_config, {}, **(kwargs))
        list_edges: List[str] = get_from_config('list_edges', file_config, [], **(kwargs))

        if 'subject_fields' not in file_config:
            raise ValueError(f'{file} does not have subject_fields.')

        logger.debug('Building columns and types list')
        columns = set()
        columns.update(set(subject_fields))
        dgraph_types: Dict[str, str] = {}
        edge_fields: List[str] = []

        if 'type_overrides' in file_config:
            type_overrides: List[str] = get_from_config('type_overrides', file_config, None, **(kwargs))
            columns.update(set(type_overrides))
            dgraph_types = find_dgraph_type(file_config['type_overrides'])
        else:
            logger.warning(f'{file} does not have type_overrides skipping schema generation')

        if 'edge_fields' in file_config:
            edge_fields: List[str] = get_from_config('edge_fields', file_config, [], **(kwargs))
            columns.update(set(edge_fields))

        if 'csv_edges' in file_config:
            csv_edges: List[str] = get_from_config('csv_edges', file_config, [], **(kwargs))
            columns.update(set(csv_edges))
            edge_fields.extend(csv_edges)

        if 'ignore_fields' in file_config:
            ignore_fields: List[str] = get_from_config('ignore_fields', file_config, [], **(kwargs))
            columns = columns.difference(set(ignore_fields))

        if 'override_edge_name' in file_config:
            override_edge_name: Dict[str, Any] = get_from_config('override_edge_name', file_config, {}, **(kwargs))
            for options in override_edge_name.values():
                columns.add(options['predicate'])
                edge_fields.append(options['predicate'])

        if strip_id_from_edge_names:
            columns = _strip_id(columns)
            edge_fields = _strip_id(edge_fields)
            list_edges = _strip_id(list_edges)

        if 'pre_rename' in file_config:
            pre_rename: Dict[str, str] = get_from_config('pre_rename', file_config, {}, **(kwargs))
            columns = map(lambda x: pre_rename.get(x, x), columns)

            renamed_dgraph_types: Dict[str, str] = {}
            for original_name, new_name in pre_rename.items():
                renamed_dgraph_types[new_name] = dgraph_types.get(original_name, default_dgraph_type)
                if original_name in list_edges:
                    list_edges.add(new_name)
                if original_name in edge_fields:
                    edge_fields.add(new_name)

                dgraph_types.update(renamed_dgraph_types)

        logger.debug('Building Schema DataFrame')

        frame = pd.DataFrame(columns=['column'])
        frame['column'] = list(columns)
        frame['type'] = frame['column'].map(dgraph_types)

        frame['type'] = frame['type'].fillna(default_dgraph_type)
        frame.loc[frame['column'].isin(edge_fields), 'type'] = 'uid'
        frame.loc[frame['column'].isin(list_edges), 'type'] = '[uid]'

        frame['table'] = file

        if 'options' in file_config:
            options: Dict[str, str] = get_from_config('options', file_config, {}, **(kwargs))
            options = {column: str.join(' ', options) for column, options in options.items() if options is not None}
            frame['options'] = frame['column'].map(options)
        else:
            frame['options'] = None

        all_frames.append(frame)

    if not all_frames:
        logger.warning('No frames were generated')
        return

    frame: pd.DataFrame = pd.concat(all_frames)

    logger.debug('Appending xid declaration')
    if ensure_xid_predicate:
        frame = frame.append({'column': 'xid', 'type': 'string', 'table': None, 'options': '@index(exact)'}, ignore_index=True)

    frame.sort_values(by=['table', 'type'], inplace=True)
    frame.reset_index(inplace=True, drop=True)

    if console:
        print(frame)
    if export_csv:
        path = os.path.join(output_dir, export_csv_name)
        logger.info(f'Writing pre schema file to {path}')
        frame.to_csv(path, index=False)

    return frame
