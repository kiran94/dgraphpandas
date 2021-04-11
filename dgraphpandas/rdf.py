import os
import logging
import gzip
from typing import Any, Dict, Union, Tuple, List, Callable

import pandas as pd

from dgraphpandas.config import get_from_config, _get_config
from dgraphpandas.writers.upserts import generate_upserts
from dgraphpandas.strategies.vertical import vertical_transform
from dgraphpandas.strategies.horizontal import horizontal_transform

logger = logging.getLogger(__name__)


def _resolve_transform(config: Dict[str, Any]):
    '''
    Based on the transform configuration, choose
    a transform function
    '''
    if config is None:
        raise ValueError('config')
    if 'transform' not in config:
        return horizontal_transform

    if config['transform'] == 'horizontal':
        transform_func = horizontal_transform
    elif config['transform'] == 'vertical':
        transform_func = vertical_transform
    else:
        logger.debug('Transform not set within configuration, defaulting to horizontal')
        transform_func = horizontal_transform

    return transform_func


def to_rdf(
        frame: Union[str, pd.DataFrame],
        config: Union[Dict[str, Any], str],
        config_key: str,
        output_dir: Union[str, None] = None,
        **kwargs) -> Union[None, List[Tuple[List[str], List[str]]]]:
    '''
    Converts a Pandas DataFrame into RDF Exports.

    Parameters:
        frame: A Pandas DataFrame or file path to a CSV to be converted.
        config: A Configuration Dictionary or file path
        config_key: The file (key) to use in the configuration
        output_dir: The output directory to push exports. If none, don't export

    Returns:
        If chunking was applied then a list of tuples
        If no chunking then just a tuple
        Each tuple has two items: intrinsic and edges
    '''
    if frame is None:
        raise ValueError('frame')
    if not config:
        raise ValueError('config')
    if not config_key:
        raise ValueError('config_key')

    config = _get_config(config)
    transform_func = _resolve_transform(config)

    '''
    The Frame may be a file path or already loaded DataFrame.
    If it's a string then attempt to load the file.
    '''
    if isinstance(frame, str):
        file_config = config['files'][config_key]
        read_csv_options: Dict[str, Any] = get_from_config('read_csv_options', file_config, {}, **(kwargs))
        chunk_size: int = get_from_config('chunk_size', config, 10_000_000, **(kwargs))
        source_file_name = os.path.basename(frame).split('.')[0]

        result = []
        for index, frame in enumerate(pd.read_csv(frame, chunksize=chunk_size, **(read_csv_options))):
            result.append(to_rdf_from_frame(frame, config, config_key, transform_func, source_file_name, output_dir, index, **(kwargs)))
        return result
    else:
        return to_rdf_from_frame(frame, config, config_key, transform_func, config_key, output_dir, 0, **(kwargs))


def to_rdf_from_frame(
        frame: pd.DataFrame,
        config: Dict[str, Any],
        config_key,
        transform_func: Callable,
        source_file_name: str,
        output_dir: str,
        index: int = 0,
        **kwargs):

    file_config = config['files'][config_key]
    console: bool = get_from_config('console', config, False, **(kwargs))
    export_csv: bool = get_from_config('export_csv', file_config, False, **(kwargs))
    export_rdf: bool = get_from_config('export_rdf', file_config, False, **(kwargs))
    encoding: str = get_from_config('encoding', file_config, 'utf-8', **(kwargs))
    gz_compression_level: int = get_from_config('gz_compression_level', file_config, 9, **(kwargs))

    logger.info('Transforming Source Frame to Rdf Frame')
    intrinsic, edges = transform_func(frame, config, config_key, **(kwargs))
    if console:
        print('Intrinsic \n', intrinsic)
        print('Edges \n', edges)

    intrinsic_upserts, edges_upserts = generate_upserts(intrinsic, edges)
    if output_dir is not None:
        os.makedirs(output_dir, exist_ok=True)
        if index == 0:
            intrinsic_base_path = os.path.join(output_dir, source_file_name + '_intrinsic')
            edges_base_path = os.path.join(output_dir, source_file_name + '_edges')
        else:
            intrinsic_base_path = os.path.join(output_dir, source_file_name + '_intrinsic_' + str(index+1))
            edges_base_path = os.path.join(output_dir, source_file_name + '_edges_' + str(index+1))

        if export_csv:
            intrinsic_csv_path = intrinsic_base_path + '.csv'
            edges_csv_path = edges_base_path + '.csv'

            logger.info(f'Writing to {intrinsic_csv_path}')
            intrinsic.to_csv(intrinsic_csv_path, index=False, encoding=encoding)

            logger.info(f'Writing to {edges_csv_path}')
            edges.to_csv(edges_csv_path, index=False, encoding=encoding)

        if export_rdf:
            logger.info('Generating Rdf Upserts from Frames')

            intrinsic_gz_path = intrinsic_base_path + '.gz'
            logger.info(f'Writing to {len(intrinsic_upserts)} upserts to {intrinsic_gz_path}')
            with gzip.open(intrinsic_gz_path, mode='wb', compresslevel=gz_compression_level) as zip_file:
                s = '\n'.join(intrinsic_upserts)
                s = s.encode(encoding=encoding)
                zip_file.write(s)

            edges_gz_path = edges_base_path + '.gz'
            logger.info(f'Writing to {len(edges_upserts)} upserts to {edges_gz_path}')
            with gzip.open(edges_gz_path, mode='wb', compresslevel=gz_compression_level) as zip_file:
                s = '\n'.join(edges_upserts)
                s = s.encode(encoding=encoding)
                zip_file.write(s)

    return intrinsic_upserts, edges_upserts
