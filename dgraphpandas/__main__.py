import logging
import argparse
import os
import json
from typing import Any, Dict
from pprint import pformat

import pandas as pd

from dgraphpandas import __version__, __description__, to_rdf

pd.set_option('mode.chained_assignment', None)


def main():
    parser = argparse.ArgumentParser(description=__description__)
    parser.add_argument('-f', '--file', required=True, help='The Data File (CSV) to convert into RDF.')
    parser.add_argument('-c', '--config', required=True, help='The DgraphPandas Configuration. See Documentation for options/examples.')
    parser.add_argument('-ck', '--config_file_key', required=True, help='The Entry in the Configuration to use for this passed file.')
    parser.add_argument('-o', '--output_dir', default='.', help='The output directory to write files.')
    parser.add_argument('--console', action='store_true', default=False, help='Write the Preprocessed DataFrames to console (for debugging)')
    parser.add_argument('--export_csv', action='store_true', default=False, help='Write the Preprocessed DataFrame to CSV (for debugging)')
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

    options = {
        'key_separator': args.key_separator,
        'add_dgraph_type_records': args.add_dgraph_type_records,
        'drop_na_intrinsic_objects': args.drop_na_intrinsic_objects,
        'drop_na_edge_objects': args.drop_na_edge_objects,
        'illegal_characters': args.illegal_characters,
        'illegal_characters_intrinsic_object': args.illegal_characters_intrinsic_object,
        'console': args.console,
        'export_csv': args.export_csv,
        'chunk_size': args.chunk_size
    }
    options = {key: value for key, value in options.items() if value is not None and value is not False}

    with open(args.config, 'r') as f:
        global_config: Dict[str, Any] = json.load(f)
        logger.debug('Global Config \n %s', pformat(global_config))

    to_rdf(args.file, global_config, args.config_file_key, args.output_dir, export_rdf=True, **(options))


if __name__ == '__main__':
    main()
