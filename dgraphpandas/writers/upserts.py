
from typing import List, Tuple
import pandas as pd
from dgraphpandas.types import default_rdf_type


def generate_intrinsic(intrinsic: pd.DataFrame) -> List[str]:
    intrinsic['type'] = intrinsic['type'].fillna(default_rdf_type)
    intrinsic['dql'] = '<' + intrinsic['subject'] + '>' + ' ' + '<' + intrinsic['predicate'] + '>' + ' ' + intrinsic['object'] + '^^' + intrinsic['type'] + ' .'
    intrinsic.drop(columns=['subject', 'predicate', 'object', 'type'], inplace=True)
    intrinsic = intrinsic['dql'].values.tolist()
    return intrinsic


def generate_edges(edges: pd.DataFrame) -> List[str]:
    edges['dql'] = '<' + edges['subject'] + '>' + ' ' + '<' + edges['predicate'] + '>' + ' ' + '<' + edges['object'] + '>' + ' .'
    edges.drop(columns=['subject', 'predicate', 'object', 'type'], inplace=True)#
    edges = edges['dql'].values.tolist()
    return edges


def generate_upserts(intrinsic: pd.DataFrame, edges: pd.DataFrame) -> Tuple[List[str], List[str]]:
    intrinsic_upserts = generate_intrinsic(intrinsic)
    edge_upserts = generate_edges(edges)

    return (intrinsic_upserts, edge_upserts)
