import logging
from typing import List, Tuple

import pandas as pd
from dgraphpandas.types import default_rdf_type

logger = logging.getLogger(__name__)


def _generate_intrinsic(intrinsic: pd.DataFrame) -> List[str]:
    '''
    Generates Intrinsic RDF records from the given frame.
    Intrinsic records are fields for a given node:

    <sloth> <legs> "4"^^<xs:int> .
    <sloth> <color> "grey"^^<xs:string> .
    '''
    intrinsic['subject'] = intrinsic['subject'].astype(str)
    intrinsic['predicate'] = intrinsic['predicate'].astype(str)
    intrinsic['object'] = intrinsic['object'].astype(str)

    intrinsic['type'] = intrinsic['type'].fillna(default_rdf_type)
    intrinsic['dql'] = '<' + intrinsic['subject'] + '>' + ' ' + '<' + intrinsic['predicate'] + \
        '>' + ' ' + '"' + intrinsic['object'] + '"' + '^^' + intrinsic['type'] + ' .'
    intrinsic.drop(columns=['subject', 'predicate', 'object', 'type'], inplace=True)
    intrinsic = intrinsic['dql'].values.tolist()
    return intrinsic


def _generate_edges(edges: pd.DataFrame) -> List[str]:
    '''
    Generates Edge RDF records for the given frame.
    Edge records connect nodes:

    <sloth> <species> <mammal> .
    <sloth> <country> <africa> .
    '''
    edges['subject'] = edges['subject'].astype(str)
    edges['predicate'] = edges['predicate'].astype(str)
    edges['object'] = edges['object'].astype(str)

    edges['dql'] = '<' + edges['subject'] + '>' + ' ' + '<' + edges['predicate'] + '>' + ' ' + '<' + edges['object'] + '>' + ' .'
    edges.drop(columns=['subject', 'predicate', 'object', 'type'], inplace=True)
    edges = edges['dql'].values.tolist()
    return edges


def generate_upserts(
        intrinsic: pd.DataFrame,
        edges: pd.DataFrame, drop_na_objects=True) -> Tuple[List[str], List[str]]:
    '''
    Generates RDF Upsert Statements for the given intrinsic and edges frames.
    '''

    if intrinsic is None:
        raise ValueError('intrinsic')
    if edges is None:
        raise ValueError('edges')

    required_intrinsic_fields = ['subject', 'predicate', 'object', 'type']
    for col in required_intrinsic_fields:
        if col not in intrinsic.columns:
            raise ValueError(f'{col} is not within intrinsic columns {intrinsic.columns}')

    required_edges_fields = ['subject', 'predicate', 'object']
    for col in required_edges_fields:
        if col not in edges.columns:
            raise ValueError(f'{col} is not within edges columns {edges.columns}')

    if drop_na_objects:
        logger.debug('Dropping NA Objects from intrinsic')
        intrinsic.dropna(subset=['object'], inplace=True)

    intrinsic_upserts = _generate_intrinsic(intrinsic)
    edge_upserts = _generate_edges(edges)

    return (intrinsic_upserts, edge_upserts)
