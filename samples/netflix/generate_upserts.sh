#!/bin/bash
set -e

# Run me from root of the repository

INPUT_DIR=samples/netflix/input
OUTPUT_DIR=samples/netflix/output
CONFIG=samples/netflix/dgraphpandas.json

python -m dgraphpandas -c $CONFIG -ck show_types -f $INPUT_DIR/show_types.csv -o $OUTPUT_DIR
python -m dgraphpandas -c $CONFIG -ck cast -f $INPUT_DIR/cast.csv -o $OUTPUT_DIR
python -m dgraphpandas -c $CONFIG -ck genre -f $INPUT_DIR/genre.csv -o $OUTPUT_DIR
python -m dgraphpandas -c $CONFIG -ck rating -f $INPUT_DIR/rating.csv -o $OUTPUT_DIR
python -m dgraphpandas -c $CONFIG -ck directors -f $INPUT_DIR/directors.csv -o $OUTPUT_DIR
python -m dgraphpandas -c $CONFIG -ck titles -f $INPUT_DIR/netflix_titles.csv -o $OUTPUT_DIR

echo Done