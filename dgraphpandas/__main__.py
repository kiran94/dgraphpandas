from ast import parse
import logging
import argparse
import os
from typing import List

import pandas as pd

from dgraphpandas.strategies.horizontal import horizontal_transform
from dgraphpandas.writers.upserts import generate_upsert

logger = logging.getLogger(__name__)

try:
    import coloredlogs
    coloredlogs.install(level='DEBUG')
except ImportError as e:
    logger.warning(e)


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

    args = parser.parse_args()

    all_intrinsic: List[pd.DataFrame] = []
    all_edges: List[pd.DataFrame] = []

    if args.chunksize:
        for index, frame in enumerate(pd.read_csv(args.file, compression='infer', chunksize=args.chunksize)):

            logger.info(f'Running Chunk {index}')

            intrinsic, edges = horizontal_transform(frame, args.subject, args.edges, args.type)
            all_intrinsic.append(intrinsic)
            all_edges.append(edges)
    else:
        frame = pd.read_csv(args.file, compression='infer')
        intrinsic, edges = horizontal_transform(frame, args.subject, args.edges, args.type)
        all_intrinsic.append(intrinsic)
        all_edges.append(edges)

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

    # frame.to_csv('data/pokemon_horizontal.zip', compression={'method': 'zip', 'archive_name': 'pokemon_horizontal.csv'}, index=False)

    # generate_upsert()
