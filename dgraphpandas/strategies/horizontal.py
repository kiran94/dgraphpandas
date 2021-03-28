import re
import logging
from pprint import pformat
from typing import Any, Dict, List, Callable, Pattern, Union

import numpy as np
import pandas as pd

from dgraphpandas.types import find_rdf_types, default_rdf_type

logger = logging.getLogger(__name__)


def get_from_config(key: str, config: Dict[str, Any], default: Any = None, **kwargs) -> Any:
    return kwargs.get(key, config.get(key, default))


def _prepare_frame(
        frame: pd.DataFrame,
        config: Dict[str, Any],
        config_file_key: str,
        **kwargs):
    '''
    Apply the given configuration options to clean up / restructure the DataFrame.
    To be used before Horizontal Transformations.
    '''
    file_config: Dict[str, Any] = config['files'][config_file_key]
    logger.debug('File Config \n %s', pformat(file_config))

    subject_fields: Union[List[str], Callable[..., List[str]]] = get_from_config('subject_fields', file_config, **(kwargs))
    edge_fields: Union[List[str], Callable[..., List[str]]] = get_from_config('edge_fields', file_config, [], **(kwargs))
    dgraph_type: str = get_from_config('dgraph_type', file_config, config_file_key, **(kwargs))

    if not subject_fields:
        raise ValueError('subject_fields must be defined')

    pre_rename: Dict[str, str] = get_from_config('pre_rename', file_config, {}, **(kwargs))
    type_overrides: Dict[str, str] = get_from_config('type_overrides', file_config, {}, **(kwargs))

    logger.debug('Resolving Potential Callables')
    potential_callables: Dict[str, Union[List[str], Callable]] = {
        'subject_fields': subject_fields,
        'edge_fields': edge_fields,
        'dgraph_type': dgraph_type
    }

    for key, potential_callable in potential_callables.items():
        if callable(potential_callable):
            logger.debug('resolving %s', key)
            potential_callables[key] = potential_callable(frame)
            logger.debug('Resolved %s to %s', key, potential_callables[key])

    logger.debug('Applying Renames %s', pre_rename)
    frame.rename(columns=pre_rename, inplace=True)

    if type_overrides is None:
        logger.debug('Types not specified, deriving')
        type_overrides = find_rdf_types(frame)

    logger.debug('Applying Type Overrides %s', type_overrides)
    for col, type in type_overrides.items():
        try:
            frame[col] = frame[col].astype(type)
        except ValueError:
            logger.exception(
                f'''
                Could not convert {col} to {type}.
                Please confirm that the values in the {col} series are convertable to {type}.
                A common scenario here is when we have NA values but the target type does not support them.
                ''')
            exit()

    key = potential_callables["subject_fields"]
    edges = potential_callables['edge_fields']
    type = potential_callables['dgraph_type']
    return (frame, key, edges, type)


def horizontal_transform(
        frame: pd.DataFrame,
        config: Dict[str, Any],
        config_file_key: str,
        **kwargs):

    file_config: Dict[str, Any] = config['files'][config_file_key]
    frame, key, edges, type = _prepare_frame(frame, config, config_file_key, **(kwargs))

    key_seperator: str = get_from_config('key_separator', config, '_', **(kwargs))
    add_dgraph_type_records: str = get_from_config('add_dgraph_type_records', config, True, **(kwargs))
    strip_id_from_edge_names: str = get_from_config('strip_id_from_edge_names', config, True, **(kwargs))
    drop_na_intrinsic_objects: bool = get_from_config('drop_na_intrinsic_objects', config, True, **(kwargs))
    drop_na_edge_objects: bool = get_from_config('drop_na_edge_objects', config, True, **(kwargs))
    predicate_field: str = get_from_config('predicate_field', config, 'predicate', **(kwargs))
    object_field: str = get_from_config('object_field', config, 'object', **(kwargs))
    illegal_characters: str = get_from_config('illegal_characters', config, ['%', '\\.'], **(kwargs))
    rdf_types: str = get_from_config('rdf_types', file_config, None, **(kwargs))
    ignore_fields: List[str] = get_from_config('ignore_fields', file_config, [], **(kwargs))

    override_edge_name: Dict[str, Any] = get_from_config('override_edge_name', file_config, {}, **(kwargs))

    '''
    For any fields which we don't care about, filter them from the frame.
    '''
    logger.debug(f'Ignoring Columns: {ignore_fields}')
    frame = frame.loc[:, ~frame.columns.isin(ignore_fields)]

    '''
    If there have been no RDF types mapped for the given DataFrame, then
    attempt to derive the types from the DataFrame dtypes.
    '''
    if rdf_types is None:
        logger.debug('Types not specified, deriving')
        rdf_types = find_rdf_types(frame)

    '''
    Pivot the Horizontal DataFrame based on the given key (subject).
    Change the frame to be 3 columns with triples: subject, predicate, object

    This changes the horizontal frame into a vertical frame as this more closely
    resembles rdf triples.
    '''
    logger.debug(f'Melting frame with subject: {key}')
    frame = frame.melt(
        id_vars=key,
        var_name=predicate_field,
        value_name=object_field)

    '''
    If we have a composite key, then join all into a single subject column
    We do this length check as a performance optimisation as apply can take some time
    and there is no need if there is only 1 key.
    '''
    logger.debug(f'Joining Key fields {key} to subject')
    if len(key) > 1:
        frame['subject'] = frame[key].apply(lambda row: key_seperator.join(row.values.astype(str)), axis=1)
    else:
        frame['subject'] = frame[key]

    frame['subject'] = type + key_seperator + frame['subject']

    logger.debug('Dropping keys in favour of subject')
    frame = frame.drop(labels=key, axis=1)

    '''
    Dgraph has a special field called 'dgraph.type', this can be used to query via the type()
    function. If add_dgraph_type_records is enabled, then we add dgraph.type fields
    to the current frame.
    '''
    if add_dgraph_type_records:
        logger.debug('Adding dgraph.type fields')
        add_type_frame = frame.copy()
        add_type_frame['object'] = type
        add_type_frame['predicate'] = 'dgraph.type'
        frame = pd.concat([frame, add_type_frame])

    '''
    If there are edges defined, then break up into intrinsic and edge frames
    '''
    if edges:
        logger.debug(f'Splitting into Intrinsic and edges based on edges {edges}')
        intrinsic = frame.loc[~frame['predicate'].isin(edges)]
        edges = frame.loc[frame['predicate'].isin(edges)]

        '''
        Its common for a data set to have a reference to another 'table' using _id
        convention. For example if you had a Student & School then the student might
        have a 'school_id' field which points to the school. In a Graph Database it might make
        more sense to have (Student) - school -> (School) rather then having an _id in the predicate.
        '''
        if strip_id_from_edge_names:
            edges['predicate'] = edges['predicate'].str.replace('_id', '')

    else:
        logger.debug('No Edges defined, Skipping.')
        intrinsic = frame
        edges = pd.DataFrame(columns=['subject', 'predicate', 'object', 'type'])

    '''
    For each of the vertical rows, map each predicate to a pre-defined rdf type.
    Edges are always uid type, so don't bother filling this in.

    For any types which could not be applied, then default to default_rdf_type
    '''
    logger.debug('Applying RDF Types')
    intrinsic['type'] = intrinsic['predicate'].map(rdf_types)
    intrinsic['type'].fillna(default_rdf_type, inplace=True)
    edges['type'] = np.nan

    '''
    Some characters are illegal in an RDF export and DGraph will not accept them.
    So make sure we filter out any of those characters from our Frame.
    '''
    logger.debug('Filtering Illegal Characters %s', illegal_characters)
    if isinstance(illegal_characters, list):
        logger.debug('Resolving illegal_characters')
        illegal_characters: Pattern = re.compile('|'.join(illegal_characters))

    intrinsic.loc[:, 'subject'] = intrinsic['subject'].replace(illegal_characters, '')
    edges['subject'] = edges['subject'].replace(illegal_characters, '')
    edges['object'] = edges['object'].replace(illegal_characters, '')

    '''
    If the object is NA/Null then the predicate does not exist from this node
    to the target node, so drop those records.
    '''
    if drop_na_intrinsic_objects:
        logger.debug('Dropping records where intrinsic is NA on object')
        intrinsic.dropna(subset=['object'], inplace=True)

    if drop_na_edge_objects:
        logger.debug('Dropping records where edge is NA on object')
        edges.dropna(subset=['object'], inplace=True)

    '''
    For Edges, we want to make sure the objects have the predicate prepended
    so that they map to the corresponding xid for the actual node.

    For example if we had a Student / School relationship and we are
    currently operating on the student then we might have
    <student_45> <school> <1> but we really want <student_45> <school> <school_1>
    because school_1 would be found whenever we are loading our school data.

    If override_edge_name has been specified then do some extra work
    to make the predicate and target object type different.
    '''
    def override_edge_name_apply(row: pd.Series, override_edge_name: Dict[str, Any], key_seperator: str):
        if row['predicate'] not in override_edge_name:
            row['object'] = row['predicate'] + key_seperator + str(row['object'])
        else:
            current_override = override_edge_name[row['predicate']]
            row['predicate'] = current_override['predicate']
            row['object'] = current_override['target_node_type'] + key_seperator + str(row['object'])

        return row

    if any(override_edge_name):
        edges = edges.apply(override_edge_name_apply, axis='columns', args=(override_edge_name, key_seperator))
    else:
        edges['object'] = edges['predicate'].astype(str) + key_seperator + edges['object'].astype(str)

    '''
    Just re-ordering so they are in their natural order
    '''
    intrinsic = intrinsic[['subject', 'predicate', 'object', 'type']]
    edges = edges[['subject', 'predicate', 'object', 'type']]

    return intrinsic, edges
