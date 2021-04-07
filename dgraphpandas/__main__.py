import logging
import argparse
import os
import gzip
import json
from typing import Any, Dict
from pprint import pformat

import pandas as pd

from dgraphpandas import __version__, __description__
from dgraphpandas.strategies.horizontal import horizontal_transform
from dgraphpandas.strategies.vertical import vertical_transform
from dgraphpandas.writers.upserts import generate_upserts

pd.set_option('mode.chained_assignment', None)


def main():
    parser = argparse.ArgumentParser(description=__description__)
    parser.add_argument('-f', '--file', required=True, help='The Data File (CSV) to convert into RDF.')
    parser.add_argument('-c', '--config', required=True, help='The DgraphPandas Configuration. See Documentation for options/examples.')
    parser.add_argument('-ck', '--config_file_key', required=True, help='The Entry in the Configuration to use for this passed file.')
    parser.add_argument('-o', '--output_dir', default='.', help='The output directory to write files.')
    parser.add_argument('--console', action='store_true', default=False, help='Write the Preprocessed DataFrames to console (for debugging)')
    parser.add_argument('--pre_csv', action='store_true', default=False, help='Write the Preprocessed DataFrame to CSV (for debugging)')
    parser.add_argument('--skip_upsert_generation', action='store_true', default=False, help="Don't generate RDF files")
    parser.add_argument('--encoding', default=os.environ.get('DGRAPHPANDAS_ENCODING', 'utf-8'), help='The Encoding to write files.')
    parser.add_argument('--chunk_size', default=10_000_000, type=int, help='Process and output in chunks rather all at once')
    parser.add_argument('--gz_compression_level', default=9, type=int, help='Compression level to set output gzip files to')
    parser.add_argument('--key_separator')
    parser.add_argument('--add_dgraph_type_records', default=True)
    parser.add_argument('--drop_na_intrinsic_objects', default=True)
    parser.add_argument('--drop_na_edge_objects', default=True)
    parser.add_argument('--illegal_characters', default=['%', '\\.', '\\s', '\"', '\\n', '\\r\\n'])
    parser.add_argument('--illegal_characters_intrinsic_object', default=['\"', '\\n', '\\r\\n'])
    parser.add_argument('--version', action='version', version=__version__)
    parser.add_argument('-v', '--verbosity',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'NOTSET'],
                        default=os.environ.get('DGRAPHPANDAS_LOG', 'INFO'))

    args = parser.parse_args()

    logging.basicConfig(level=args.verbosity)
    logger = logging.getLogger(__name__)

    try:
        import coloredlogs
        coloredlogs.install(level=args.verbosity)
    except ImportError as e:
        logger.debug(e)

    with open(args.config, 'r') as f:
        global_config: Dict[str, Any] = json.load(f)
        logger.debug('Global Config \n %s', pformat(global_config))

    options = {
        'key_separator': args.key_separator,
        'add_dgraph_type_records': args.add_dgraph_type_records,
        'drop_na_intrinsic_objects': args.drop_na_intrinsic_objects,
        'drop_na_edge_objects': args.drop_na_edge_objects,
        'illegal_characters': args.illegal_characters,
        'illegal_characters_intrinsic_object': args.illegal_characters_intrinsic_object
    }
    options = {key: value for key, value in options.items() if value is not None and value is not False}

    if global_config['transform'] == 'horizontal':
        transform_func = horizontal_transform
    elif global_config['transform'] == 'vertical':
        transform_func = vertical_transform
    else:
        raise NotImplementedError(global_config['transform'])

    for index, frame in enumerate(pd.read_csv(args.file, chunksize=args.chunk_size)):
        index += 1
        logger.info(f'Processing Chunk {index} with {frame.shape[0]} rows')

        intrinsic, edges = transform_func(args.file, global_config, args.config_file_key, **(options))

        if args.console and index == 1:
            print('Intrinsic:')
            print(intrinsic)
            print('Edges:')
            print(edges)
        else:
            logger.debug('Console only enabled for first chunk, skipping.')

        source_file_name = os.path.basename(args.file).split('.')[0]
        if index == 1:
            intrinsic_base_path = os.path.join(args.output_dir, source_file_name + '_intrinsic')
            edges_base_path = os.path.join(args.output_dir, source_file_name + '_edges')
        else:
            intrinsic_base_path = os.path.join(args.output_dir, source_file_name + '_intrinsic_' + str(index))
            edges_base_path = os.path.join(args.output_dir, source_file_name + '_edges_' + str(index))

        if args.pre_csv:
            os.makedirs(args.output_dir, exist_ok=True)

            intrinsic_csv_path = intrinsic_base_path + '.csv'
            edges_csv_path = edges_base_path + '.csv'

            logger.info(f'Writing to {intrinsic_csv_path}')
            intrinsic.to_csv(intrinsic_csv_path, index=False, encoding=args.encoding)

            logger.info(f'Writing to {edges_csv_path}')
            edges.to_csv(edges_csv_path, index=False, encoding=args.encoding)

        if not args.skip_upsert_generation:
            logger.info('Generating Upsert Queries')
            intrinsic_upserts, edges_upserts = generate_upserts(intrinsic, edges)

            os.makedirs(args.output_dir, exist_ok=True)
            intrinsic_gz_path = intrinsic_base_path + '.gz'
            edges_gz_path = edges_base_path + '.gz'

            logger.info(f'Writing to {len(intrinsic_upserts)} upserts to {intrinsic_gz_path}')
            with gzip.open(intrinsic_gz_path, mode='wb', compresslevel=args.gz_compression_level) as zip:
                s = '\n'.join(intrinsic_upserts)
                s = s.encode(encoding=args.encoding)
                zip.write(s)

            logger.info(f'Writing to {len(edges_upserts)} upserts to {edges_gz_path}')
            with gzip.open(edges_gz_path, mode='wb', compresslevel=args.gz_compression_level) as zip:
                s = '\n'.join(edges_upserts)
                s = s.encode(encoding=args.encoding)
                zip.write(s)

        else:
            logger.warning('skip_upsert_generation was set, skipping')


if __name__ == '__main__':
    main()
