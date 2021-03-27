import io
import logging
import argparse
import os
import gzip
from typing import List

import pandas as pd

from dgraphpandas.strategies.horizontal import horizontal_transform
from dgraphpandas.writers.upserts import generate_upserts

logger = logging.getLogger(__name__)

try:
    import coloredlogs
    coloredlogs.install(level='DEBUG')
except ImportError as e:
    logger.warning(e)

pd.set_option('mode.chained_assignment', None)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file')
    parser.add_argument('-s', '--subject', nargs='+', required=True)
    parser.add_argument('-e', '--edges', nargs='+', required=False)
    parser.add_argument('-t', '--type', required=True)
    parser.add_argument('-o', '--output_dir', default='.')
    parser.add_argument('-c', '--chunksize', default=None, type=int)
    parser.add_argument('--console', action='store_true', default=False)
    parser.add_argument('--pre_csv', action='store_true', default=False)
    parser.add_argument('--generate_upsert', action='store_true', default=False)
    parser.add_argument('--encoding', default='utf-8')

    args = parser.parse_args()

    all_intrinsic: List[pd.DataFrame] = []
    all_edges: List[pd.DataFrame] = []

    logger.info(f'Reading from {args.file}')
    if args.chunksize:
        for index, frame in enumerate(pd.read_csv(args.file, chunksize=args.chunksize)):
            logger.info(f'Running Chunk {index}')
            intrinsic, edges = horizontal_transform(frame, args.subject, args.edges, args.type)
            all_intrinsic.append(intrinsic)
            all_edges.append(edges)
    else:
        frame = pd.read_csv(args.file)
        intrinsic, edges = horizontal_transform(frame, args.subject, args.edges, args.type)
        all_intrinsic.append(intrinsic)
        all_edges.append(edges)

    logger.debug(f'Concatting {len(all_intrinsic)} intrinsic frames and {len(all_edges)} frames')
    intrinsic = pd.concat(all_intrinsic)
    edges = pd.concat(all_edges)

    if args.console:
        print(intrinsic)
        print(edges)

    if args.pre_csv:
        os.makedirs(args.output_dir, exist_ok=True)
        intrinsic_path = os.path.join(args.output_dir, args.type + '_intrinsic.csv')
        edges_path = os.path.join(args.output_dir, args.type + '_edges.csv')

        logger.info(f'Writing to {intrinsic_path}')
        intrinsic.to_csv(intrinsic_path, index=False)

        logger.info(f'Writing to {edges_path}')
        edges.to_csv(edges_path, index=False)

    if args.generate_upsert:
        logger.info('Generating Upsert Queries')
        intrinsic_upserts, edges_upserts = generate_upserts(intrinsic, edges)

        os.makedirs(args.output_dir, exist_ok=True)
        intrinsic_path = os.path.join(args.output_dir, args.type + '_intrinsic.gz')
        edges_path = os.path.join(args.output_dir, args.type + '_edges.gz')

        logger.info(f'Writing to {intrinsic_path}')
        with gzip.open(intrinsic_path, mode='wb', compresslevel=9) as zip:
            s = '\n'.join(intrinsic_upserts)
            s = s.encode(encoding=args.encoding)
            zip.write(s)

        logger.info(f'Writing to {edges_path}')
        with gzip.open(edges_path, mode='wb', compresslevel=9) as zip:
            s = '\n'.join(edges_upserts)
            s = s.encode(encoding=args.encoding)
            zip.write(s)
    else:
        logger.warning('generate_upsert not set, skipping')
