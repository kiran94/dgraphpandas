import logging
import re
from typing import Dict, List, Callable, Pattern, Tuple, Union
import numpy as np

import pandas as pd

from dgraphpandas.types import find_rdf_types

logger = logging.getLogger(__name__)


def horizontal_transform(
        frame: pd.DataFrame,
        subject_fields: Union[List[str], Callable[..., List[str]]],
        edge_fields: Union[List[str], Callable[..., List[str]]],
        dgraph_type: Union[str, Callable[..., List[str]]],
        types: Dict[str, str] = None,
        illegal_characters: Union[List[str], Pattern] = ['%', '\.'],
        **kwargs) -> Tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Transform an incoming DataFrame (in horizontal format) into
    another DataFrame with columns: subject, predicate, object

    Parameters:
        frame: in horizontal format
        subject_fields: list of field to treat as the subject (key) or callable
        edge_fields: list of field to treat as edges or callable
        dgraph_type: the dgraph.type to map this export into or callable
        types: field to rdf types
        illegal_characters: characters which should be filtered out from non-value fields
    '''
    predicate_field: str = kwargs.get('predicate_field', 'predicate')
    object_field: str = kwargs.get('object_field', 'object')
    key_seperator: str = kwargs.get('key_seperator', '_')
    add_dgraph_type_records: bool = bool(kwargs.get('add_dgraph_type_records', True))
    strip_id_from_edge_names: bool = bool(kwargs.get('strip_id_from_edge_names', True))

    potential_callables: Dict[str, Union[List[str], Callable]] = {
        'subject_fields': subject_fields,
        'edge_fields': edge_fields,
        'dgraph_type': dgraph_type
    }

    if types is None:
        logger.debug('Types not specified, deriving')
        types = find_rdf_types(frame)

    logger.debug('Resolving Potential Callables')
    for key, potential_callable in potential_callables.items():
        if callable(potential_callable):
            logger.debug('resolving %s', key)
            potential_callables[key] = potential_callable(frame)
            logger.debug('Resolved %s to %s', key, potential_callables[key])

    key = potential_callables["subject_fields"]
    edges = potential_callables['edge_fields']
    type = potential_callables['dgraph_type']

    logger.debug(f'Melting frame with subject: {key}')
    frame = frame.melt(
        id_vars=key,
        var_name=predicate_field,
        value_name=object_field)

    logger.debug(f'Joining Key fields {potential_callables["subject_fields"]} to subject')
    frame['subject'] = frame[key].apply(lambda row: key_seperator.join(row.values.astype(str)), axis=1)
    frame['subject'] = type + key_seperator + frame['subject']

    logger.debug('Dropping keys in favour of subject')
    frame = frame.drop(labels=key, axis=1)

    if add_dgraph_type_records:
        logger.debug('Adding dgraph.type fields')
        add_type_frame = frame.copy()
        add_type_frame['object'] = type
        add_type_frame['predicate'] = 'dgraph.type'
        frame = pd.concat([frame, add_type_frame])

    if edges:
        logger.debug(f'Splitting into Intrinsic and edges based on edges {edges}')
        intrinsic = frame.loc[~frame['predicate'].isin(edges)]
        edges = frame.loc[frame['predicate'].isin(edges)]
        if strip_id_from_edge_names:
            edges['predicate'] = edges['predicate'].str.replace('_id', '')

    else:
        logger.debug('No Edges defined, Skipping.')
        intrinsic = frame
        edges = pd.DataFrame(
            columns=['subject', 'predicate', 'object', 'type'])

    logger.debug('Applying Types')
    intrinsic['type'] = intrinsic['predicate'].map(types)
    edges['type'] = np.nan

    logger.debug('Filtering Illegal Characters %s', illegal_characters)
    if isinstance(illegal_characters, list):
        logger.debug('Resolving illegal_characters')
        illegal_characters: Pattern = re.compile('|'.join(illegal_characters))

    intrinsic.loc[:, 'subject'] = intrinsic['subject'].replace(illegal_characters, '')
    edges['subject'] = edges['subject'].replace(illegal_characters, '')
    edges['object'] = edges['object'].replace(illegal_characters, '')
    edges['object'] = edges['predicate'].astype(str) + key_seperator + edges['object'].astype(str)

    intrinsic = intrinsic[['subject', 'predicate', 'object', 'type']]
    edges = edges[['subject', 'predicate', 'object', 'type']]
    return (intrinsic, edges)
