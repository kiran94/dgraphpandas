import logging
import argparse

import pandas as pd

from dgraphpandas.strategies.horizontal import horizontal_transform

logger = logging.getLogger(__name__)

try:
    import coloredlogs
    coloredlogs.install(level='DEBUG')
except ImportError as e:
    logger.warning(e)


if __name__ == '__main__':

    path = 'data/pokemon_horizontal.zip'
    type = 'pokemon'
    subject_fields = ['No', 'Name']
    edges = ['Type', 'Generation']

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file')
    parser.add_argument('-s', '--subject', nargs='+', required=True)
    parser.add_argument('-e', '--edges', nargs='+', required=False)
    parser.add_argument('-t', '--type', required=True)

    args = parser.parse_args()

    frame = pd.read_csv(args.file, compression='infer')
    print(frame)

    intrinsic, edges = horizontal_transform(
        frame, args.subject, args.edges, args.type)
    print(intrinsic)
    print(edges)

    # intrinsic.to_csv('output/intrinsic.csv', index=False)
    # edges.to_csv('output/edges.csv', index=False)
    # frame.to_csv('data/pokemon_horizontal.zip', compression={'method': 'zip', 'archive_name': 'pokemon_horizontal.csv'}, index=False)
