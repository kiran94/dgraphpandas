import re
import logging
from typing import Callable, Dict, Any, List, Pattern, Tuple, Union

import pandas as pd

from dgraphpandas.types import default_rdf_type
from dgraphpandas.types import find_rdf_types


logger = logging.getLogger(__name__)


def _expand_csv_edges(frame: pd.DataFrame, csv_edges: List[str], seperator=',') -> pd.DataFrame:
    '''
    Sometimes fields will be delivered in CSV format.
    For example a Movie record might have a cast field with the value: actor1, actor2, actor3
    In this kind of situation, we want the actors to be broken up into 3 different nodes

    When a field is declared within csv_edges, then we break up each of the values in the csv into
    it's own record.
    '''

    if frame is None:
        raise ValueError('frame')

    if csv_edges:
        logger.debug(f'Detected csv_edges {csv_edges}. Breaking up those columns')
        csv_edge_frame = frame[frame['predicate'].isin(csv_edges)]
        csv_edge_frame['object'] = csv_edge_frame['object'].str.split(seperator)
        csv_edge_frame = csv_edge_frame.explode(column='object')
        csv_edge_frame.dropna(subset=['object'], inplace=True)
        csv_edge_frame['object'] = csv_edge_frame['object'].str.strip()

        frame = frame[~(frame['predicate'].isin(csv_edges))]
        frame = pd.concat([frame, csv_edge_frame])

    return frame


def _join_key_fields(frame: pd.DataFrame, key: List[str], key_seperator: str, dgraph_type: str) -> pd.DataFrame:
    '''
    If we have a composite key, then join all into a single subject column
    We do this length check as a performance optimization as apply can take some time
    and there is no need if there is only 1 key.
    '''
    if frame is None:
        raise ValueError('frame')
    elif key is None:
        raise ValueError('key')
    elif key_seperator is None:
        raise ValueError('key_seperator')
    elif dgraph_type is None:
        raise ValueError('type')

    logger.debug(f'Joining Key fields {key} to subject')
    if len(key) > 1:
        frame['subject'] = frame[key].apply(lambda row: key_seperator.join(row.values.astype(str)), axis=1)
    else:
        frame[key] = frame[key].astype(str)
        frame['subject'] = frame[key]

    frame['subject'] = dgraph_type + key_seperator + frame['subject']

    logger.debug('Dropping keys in favour of subject')
    frame = frame.drop(labels=key, axis=1)
    return frame


def _add_dgraph_type_records(frame: pd.DataFrame, add_dgraph_type_records: bool, dgraph_type: str) -> pd.DataFrame:
    '''
    Dgraph has a special field called 'dgraph.type', this can be used to query via the type()
    function. If add_dgraph_type_records is enabled, then we add dgraph.type fields
    to the current frame.
    '''
    if add_dgraph_type_records:
        logger.debug('Adding dgraph.type fields')
        add_type_frame = frame.copy()
        add_type_frame['object'] = dgraph_type
        add_type_frame['predicate'] = 'dgraph.type'
        add_type_frame.drop_duplicates(keep='first', inplace=True)
        frame = pd.concat([frame, add_type_frame])

    return frame


def _break_up_intrinsic_and_edges(frame: pd.DataFrame, edges: List[str], strip_id_from_edge_names: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
    '''
    If there are edges defined, then break up into intrinsic and edge frames
    Otherwise still break up but return an empty edges frame

    For strip_id_from_edge_names, Its common for a data set to have a reference to another 'table' using _id
    convention. For example if you had a Student & School then the student might
    have a 'school_id' field which points to the school. In a Graph Database it might make
    more sense to have (Student) - school -> (School) rather then having an _id in the predicate.
    '''
    if frame is None:
        raise ValueError('frame')

    if edges:
        logger.debug(f'Splitting into Intrinsic and edges based on edges {edges}')
        intrinsic = frame.loc[~frame['predicate'].isin(edges)]
        edges = frame.loc[frame['predicate'].isin(edges)]

        if strip_id_from_edge_names:
            edges['predicate'] = edges['predicate'].str.replace('_id', '')

    else:
        logger.debug('No Edges defined, Skipping.')
        intrinsic = frame
        edges = pd.DataFrame(columns=['subject', 'predicate', 'object', 'type'])

    return intrinsic, edges


def _apply_rdf_types(frame: pd.DataFrame, types: Dict[str, str]):
    '''
    For each of the vertical rows, map each predicate to a pre-defined rdf type.
    Edges are always uid type, so don't bother filling this in.

    For any types which could not be applied, then default to default_rdf_type
    '''

    if frame is None:
        raise ValueError('frame')
    if types is None:
        raise ValueError('types')

    logger.debug('Applying RDF Types')
    rdf_types = find_rdf_types(types)
    frame['type'] = frame['predicate'].map(rdf_types)
    frame['type'].fillna(default_rdf_type, inplace=True)

    return frame


def _format_date_fields(frame: pd.DataFrame, date_formats: Dict[str, str] = None) -> pd.DataFrame:
    '''
    Ensure that DateTime fields are formatted in ISO format
    And any fields are which NaT are filtered out.
    '''
    if frame is None:
        raise ValueError('frame')

    if date_formats:
        logger.debug(f'Applying date_formats {date_formats}')
        for col, format_options in date_formats.items():
            logger.debug(f'Applying {format_options} to {col}')
            mask = frame['predicate'] == col
            frame.loc[mask, 'object'] = pd.to_datetime(frame.loc[mask, 'object'], **(format_options))
            frame.loc[mask, 'type'] = '<xs:dateTime>'

    logger.debug('Ensuring Date Time fields are in ISO format')
    intrinsic_with_datetime = frame.loc[frame['type'] == '<xs:dateTime>']
    frame = frame.loc[frame['type'] != '<xs:dateTime>']

    try:
        intrinsic_with_datetime['object'] = intrinsic_with_datetime['object'].apply(lambda x: x.isoformat())
    except AttributeError:
        logger.error('It looks like a value being declared as a datetime is not actually a datetime')
        raise

    intrinsic_with_datetime = intrinsic_with_datetime.loc[intrinsic_with_datetime['object'] != 'NaT']
    frame = pd.concat([frame, intrinsic_with_datetime])
    return frame


def _compile_illegal_characters_regex(characters: List[str]) -> Union[Pattern, None]:
    if not characters:
        return None

    return re.compile('|'.join(characters))


def _remove_illegal_rdf_characters(frame: pd.DataFrame, illegal_characters: Union[List[str], Pattern], field: str):
    '''
    Some characters are illegal in an RDF export and DGraph will not accept them.
    So make sure we filter out any of those characters from our Frame.

    There is a differentiation between illegal characters in rdf in general and the values
    which can be inside the subject fields for intrinsic values since these are quoted and are
    actual data items which may be visible to clients.
    '''

    if frame is None:
        raise ValueError('frame')
    if not field:
        raise ValueError('field')

    logger.debug('Compiling Illegal Characters %s', illegal_characters)
    if isinstance(illegal_characters, list):
        logger.debug('Resolving illegal_characters')
        illegal_characters: Pattern = _compile_illegal_characters_regex(illegal_characters)

    if illegal_characters:
        frame[field] = frame[field].replace(illegal_characters, '')

    return frame


def _remove_na_objects(frame: pd.DataFrame, drop_na: bool = True):
    '''
    If the object is NA/Null then the predicate does not exist from this node
    to the target node, so drop those records.
    '''
    if frame is None:
        raise ValueError('frame')

    if drop_na:
        logger.debug('Dropping records where NA on object')
        frame.dropna(subset=['object'], inplace=True)

    return frame


def _override_edge_name(edges: pd.DataFrame, override_edge_name: Dict[str, Any], key_seperator: str):
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
    if edges is None:
        raise ValueError('edges')
    if not key_seperator:
        raise ValueError('key_seperator')

    def _override_edge_name_apply(row: pd.Series, override_edge_name: Dict[str, Any], key_seperator: str):
        if row['predicate'] not in override_edge_name:
            row['object'] = row['predicate'] + key_seperator + str(row['object'])
        else:
            current_override = override_edge_name[row['predicate']]
            row['predicate'] = current_override['predicate'] if 'predicate' in current_override else row['predicate']
            row['object'] = current_override['target_node_type'] + key_seperator + str(row['object'])

        return row

    if override_edge_name is not None and any(override_edge_name):
        edges = edges.apply(_override_edge_name_apply, axis='columns', args=(override_edge_name, key_seperator))
    else:
        edges['object'] = edges['predicate'].astype(str) + key_seperator + edges['object'].astype(str)

    return edges


def _ignore_fields(frame: pd.DataFrame, ignore_fields: List[str]) -> pd.DataFrame:
    '''
    For any predicates within the ignore fields list, remove them
    from our frame.
    '''
    if frame is None:
        raise ValueError('frame')

    if ignore_fields:
        frame = frame[~frame['predicate'].isin(ignore_fields)]

    return frame


def _resolve_potential_callables(frame: pd.DataFrame, potential_callables: Dict[str, Union[List[str], str, Callable]]) -> Dict[str, Union[List[str], str, Callable]]:
    '''
    Some user defined parameters are Callable so that they can be derived from the frame dynamically.
    For example, if you wanted to apply a convention that all your edges always end with _id
    then for the edge_fields parameter, you could provide a lambda which resolves into a list given the frame
    '''
    if frame is None:
        raise ValueError('frame')

    for key, potential_callable in potential_callables.items():
        if callable(potential_callable):
            logger.debug('resolving %s', key)
            potential_callables[key] = potential_callable(frame)
            logger.debug('Resolved %s to %s', key, potential_callables[key])

    return potential_callables


def _rename_fields(frame: pd.DataFrame, pre_rename: Dict[str, str]) -> pd.DataFrame:
    '''
    For any renames defined in the incoming pre_rename, then update the predicate names
    according to that mapping. If the predicate does not exist in the mapping
    then leave untouched.
    '''
    if frame is None:
        raise ValueError('frame')

    if pre_rename:
        frame['predicate'] = frame['predicate'].apply(lambda x: pre_rename.get(x, x))

    return frame


def _find_id_edges(frame: pd.DataFrame) -> List[str]:
    '''
    Gets a unique list of predicates in the given column which have _id suffix.
    '''
    if frame is None:
        raise ValueError('frame')
    return frame.loc[frame['predicate'].str.endswith('_id'), 'predicate'].unique().tolist()
