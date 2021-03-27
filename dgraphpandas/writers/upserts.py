import logging
from typing import List, Tuple

import pandas as pd
from dgraphpandas.types import default_rdf_type

logger = logging.getLogger(__name__)


def generate_intrinsic(intrinsic: pd.DataFrame) -> List[str]:
    intrinsic['subject'] = intrinsic['subject'].astype(str)
    intrinsic['predicate'] = intrinsic['predicate'].astype(str)
    intrinsic['object'] = intrinsic['object'].astype(str)

    intrinsic['type'] = intrinsic['type'].fillna(default_rdf_type)
    intrinsic['dql'] = '<' + intrinsic['subject'] + '>' + ' ' + '<' + intrinsic['predicate'] + \
        '>' + ' ' + '"' + intrinsic['object'] + '"' + '^^' + intrinsic['type'] + ' .'
    intrinsic.drop(columns=['subject', 'predicate', 'object', 'type'], inplace=True)
    intrinsic = intrinsic['dql'].values.tolist()
    return intrinsic


def generate_edges(edges: pd.DataFrame) -> List[str]:
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

    if drop_na_objects:
        logger.info('Dropping NA Objects from intrinsic')
        intrinsic.dropna(subset=['object'], inplace=True)

    intrinsic_upserts = generate_intrinsic(intrinsic)
    edge_upserts = generate_edges(edges)

    return (intrinsic_upserts, edge_upserts)
