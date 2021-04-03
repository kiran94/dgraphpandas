#!/bin/bash
set -e

# Run me from root of the repository

CONFIG=samples/pokemon/dgraphpandas.json
INPUT_DIR=samples/pokemon/input
OUTPUT_DIR=samples/pokemon/output

python -m dgraphpandas -c $CONFIG -o $OUTPUT_DIR -ck version -f $INPUT_DIR/versions.csv
python -m dgraphpandas -c $CONFIG -o $OUTPUT_DIR -ck region -f $INPUT_DIR/regions.csv
python -m dgraphpandas -c $CONFIG -o $OUTPUT_DIR -ck pokemon -f $INPUT_DIR/pokemon.csv
python -m dgraphpandas -c $CONFIG -o $OUTPUT_DIR -ck move -f $INPUT_DIR/moves.csv

