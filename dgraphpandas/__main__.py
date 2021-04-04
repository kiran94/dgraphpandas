import logging
import argparse
import os
import gzip
import json
from typing import Any, Dict, List
from pprint import pformat

import pandas as pd

from dgraphpandas.strategies.horizontal import horizontal_transform
from dgraphpandas.strategies.vertical import vertical_transform
from dgraphpandas.writers.upserts import generate_upserts

logger = logging.getLogger(__name__)

try:
    import coloredlogs
    coloredlogs.install(level='DEBUG')
except ImportError as e:
    logger.warning(e)

pd.set_option('mode.chained_assignment', None)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', required=True, help='The Data File (CSV) to convert into RDF.')
    parser.add_argument('-c', '--config', required=True, help='The DgraphPandas Configuration. See Documentation for options/examples.')
    parser.add_argument('-ck', '--config_file_key', required=True, help='The Entry in the Configuration to use for this passed file.')
    parser.add_argument('-o', '--output_dir', default='.', help='The output directory to write files.')
    parser.add_argument('--console', action='store_true', default=False, help='Write the Preprocessed DataFrames to console (for debugging)')
    parser.add_argument('--pre_csv', action='store_true', default=False, help='Write the Preprocessed DataFrame to CSV (for debugging)')
    parser.add_argument('--skip_upsert_generation', action='store_true', default=False, help="Don't generate RDF files")
    parser.add_argument('--encoding', default=os.environ.get('DGRAPH_PANDAS_ENCODING', 'utf-8'), help='The Encoding to write files.')

    parser.add_argument('--key_separator')
    parser.add_argument('--add_dgraph_type_records', default=True)
    parser.add_argument('--drop_na_intrinsic_objects', default=True)
    parser.add_argument('--drop_na_edge_objects', default=True)
    parser.add_argument('--illegal_characters', default=['%', '\\.', '\\s', '\"', '\\n', '\\r\\n'])
    parser.add_argument('--illegal_characters_intrinsic_object', default=['\"', '\\n', '\\r\\n'])

    args = parser.parse_args()

    with open(args.config, 'r') as f:
        global_config: Dict[str, Any] = json.load(f)
        logger.debug('Global Config \n %s', pformat(global_config))

    all_intrinsic: List[pd.DataFrame] = []
    all_edges: List[pd.DataFrame] = []
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
        intrinsic, edges = horizontal_transform(args.file, global_config, args.config_file_key, **(options))
        all_intrinsic.append(intrinsic)
        all_edges.append(edges)

    elif global_config['transform'] == 'vertical':
        intrinsic, edges = vertical_transform(args.file, global_config, args.config_file_key, **(options))
        all_intrinsic.append(intrinsic)
        all_edges.append(edges)

    else:
        raise NotImplementedError(global_config['transform'])

    logger.debug(f'Concatting {len(all_intrinsic)} intrinsic frames and {len(all_edges)} frames')
    intrinsic = pd.concat(all_intrinsic)
    edges = pd.concat(all_edges)

    if args.console:
        print('Intrinsic:')
        print(intrinsic)
        print('Edges:')
        print(edges)

    if args.pre_csv:
        os.makedirs(args.output_dir, exist_ok=True)
        source_file_name = os.path.basename(args.file).split('.')[0]

        intrinsic_path = os.path.join(args.output_dir, source_file_name + '_intrinsic.csv')
        edges_path = os.path.join(args.output_dir, source_file_name + '_edges.csv')

        logger.info(f'Writing to {intrinsic_path}')
        intrinsic.to_csv(intrinsic_path, index=False, encoding=args.encoding)

        logger.info(f'Writing to {edges_path}')
        edges.to_csv(edges_path, index=False, encoding=args.encoding)

    if not args.skip_upsert_generation:
        logger.info('Generating Upsert Queries')
        intrinsic_upserts, edges_upserts = generate_upserts(intrinsic, edges)

        os.makedirs(args.output_dir, exist_ok=True)
        intrinsic_path = os.path.join(args.output_dir, os.path.basename(args.file).split('.')[0] + '_intrinsic.gz')
        edges_path = os.path.join(args.output_dir, os.path.basename(args.file).split('.')[0] + '_edges.gz')

        logger.info(f'Writing to {len(intrinsic_upserts)} upserts to {intrinsic_path}')
        with gzip.open(intrinsic_path, mode='wb', compresslevel=9) as zip:
            s = '\n'.join(intrinsic_upserts)
            s = s.encode(encoding=args.encoding)
            zip.write(s)

        logger.info(f'Writing to {len(edges_upserts)} upserts to {edges_path}')
        with gzip.open(edges_path, mode='wb', compresslevel=9) as zip:
            s = '\n'.join(edges_upserts)
            s = s.encode(encoding=args.encoding)
            zip.write(s)
    else:
        logger.warning('skip_upsert_generation was set, skipping')


if __name__ == '__main__':
    main()
