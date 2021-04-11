import logging
from typing import Callable, Dict, Any, List, Union

import pandas as pd

from dgraphpandas.config import get_from_config
from dgraphpandas.strategies.vertical_helpers import (_expand_csv_edges, _join_key_fields, _add_dgraph_type_records,
                                                      _break_up_intrinsic_and_edges, _apply_rdf_types, _format_date_fields,
                                                      _remove_illegal_rdf_characters, _remove_na_objects, _override_edge_name,
                                                      _ignore_fields, _resolve_potential_callables, _rename_fields,
                                                      _find_id_edges)


logger = logging.getLogger(__name__)


def vertical_transform(
        frame: Union[str, pd.DataFrame],
        config: Dict[str, Any],
        config_file_key: str,
        **kwargs):
    '''
    Vertically Transform a Pandas Dataframe into Intrinsic and Edge DataFrames (close to RDF format)
    '''
    if frame is None:
        raise ValueError('frame')
    if not config:
        raise ValueError('config')
    if not config_file_key:
        raise ValueError('config_file_key')

    try:
        file_config: Dict[str, Any] = config['files'][config_file_key]
    except KeyError:
        logger.exception(f'Ensure that {config_file_key} is within the files object in config')
        raise

    if isinstance(frame, str):
        logger.debug(f'Reading file {frame}')
        read_csv_options: Dict[str, Any] = get_from_config('read_csv_options', file_config, {}, **(kwargs))
        frame = pd.read_csv(frame, **(read_csv_options))

    subject_fields: Union[List[str], Callable[..., List[str]]] = get_from_config('subject_fields', file_config, **(kwargs))
    edge_fields: Union[List[str], Callable[..., List[str]]] = get_from_config('edge_fields', file_config, [], **(kwargs))
    dgraph_type: str = get_from_config('dgraph_type', file_config, config_file_key, **(kwargs))

    predicate_field: str = get_from_config('predicate_field', file_config, 'predicate', **(kwargs))
    object_field: str = get_from_config('object_field', file_config, 'object', **(kwargs))
    key_seperator: str = get_from_config('key_separator', config, '_', **(kwargs))
    add_dgraph_type_records: bool = get_from_config('add_dgraph_type_records', config, True, **(kwargs))
    strip_id_from_edge_names: bool = get_from_config('strip_id_from_edge_names', config, True, **(kwargs))
    drop_na_intrinsic_objects = get_from_config('drop_na_intrinsic_objects', config, True, **(kwargs))
    drop_na_edge_objects: bool = get_from_config('drop_na_edge_objects', config, True, **(kwargs))
    illegal_characters: List[str] = get_from_config('illegal_characters', config, ['%', '\\.', '\\s', '\"', '\\n', '\\r\\n'], **(kwargs))
    illegal_characters_intrinsic_object: List[str] = get_from_config('illegal_characters_intrinsic_object', config, ['\"', '\\n', '\\r\\n'], **(kwargs))
    csv_edges: str = get_from_config('csv_edges', file_config, [], **(kwargs))
    csv_edges_seperator: str = get_from_config('csv_edges_seperator', file_config, ',', **(kwargs))
    ignore_fields: List[str] = get_from_config('ignore_fields', file_config, [], **(kwargs))
    override_edge_name: Dict[str, Any] = get_from_config('override_edge_name', file_config, {}, **(kwargs))
    pre_rename: Dict[str, str] = get_from_config('pre_rename', file_config, {}, **(kwargs))
    type_overrides: Dict[str, str] = get_from_config('type_overrides', file_config, {}, **(kwargs))
    date_fields: Dict[str, str] = get_from_config('date_fields', file_config, {}, **(kwargs))
    edge_id_convention: bool = get_from_config('edge_id_convention', file_config, False, **(kwargs))

    if edge_id_convention:
        logger.debug('Override edge_fields with _id convention')
        edge_fields = _find_id_edges

    potential_callables = _resolve_potential_callables(frame, {
        'subject_fields': subject_fields,
        'edge_fields': edge_fields,
        'dgraph_type': dgraph_type,
        'predicate_field': predicate_field,
        'object_field': object_field
    })

    key = potential_callables["subject_fields"]
    edges = potential_callables['edge_fields']
    dgraph_type = potential_callables['dgraph_type']
    predicate_resolved = potential_callables['predicate_field']
    object_resolved = potential_callables['object_field']

    if not key:
        raise ValueError('subject_fields must be defined')
    if predicate_resolved not in frame.columns:
        raise KeyError(f'predicate column {predicate_resolved} must be defined on vertical frame')
    if object_resolved not in frame.columns:
        raise KeyError(f'object column {object_resolved} must be defined on vertical frame')

    frame = frame.rename(columns={predicate_resolved: 'predicate', object_resolved: 'object'})
    frame = _rename_fields(frame, pre_rename)
    frame = _ignore_fields(frame, ignore_fields)
    frame = _expand_csv_edges(frame, csv_edges, seperator=csv_edges_seperator)
    frame = _join_key_fields(frame, key, key_seperator, dgraph_type)
    frame = _add_dgraph_type_records(frame, add_dgraph_type_records, dgraph_type)

    intrinsic, edges = _break_up_intrinsic_and_edges(frame, edges, strip_id_from_edge_names)
    intrinsic = _apply_rdf_types(intrinsic, type_overrides)
    edges['type'] = None

    intrinsic = _format_date_fields(intrinsic, date_fields)
    intrinsic = _remove_illegal_rdf_characters(intrinsic, illegal_characters, 'subject')
    intrinsic = _remove_illegal_rdf_characters(intrinsic, illegal_characters_intrinsic_object, 'object')
    edges = _remove_illegal_rdf_characters(edges, illegal_characters, 'subject')
    edges = _remove_illegal_rdf_characters(edges, illegal_characters, 'object')

    intrinsic = _remove_na_objects(intrinsic, drop_na_intrinsic_objects)
    edges = _remove_na_objects(edges, drop_na_edge_objects)

    _override_edge_name(edges, override_edge_name, key_seperator)

    intrinsic = intrinsic[['subject', 'predicate', 'object', 'type']]
    edges = edges[['subject', 'predicate', 'object', 'type']]

    return intrinsic, edges
