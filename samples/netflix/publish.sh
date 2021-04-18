#!/bin/bash
set -e

DGRAPH_TEMP_BUFFERS=dgraph_temp_buffers
UPSERT_PREDICATE=xid
XIDMAP_LOCATION=xidmap
INPUT_DIR=input
OUTPUT_DIR=output
CONFIG=dgraphpandas.json

echo Generating Schema
dgraphpandas -c $CONFIG -x schema

echo Generating Exports
dgraphpandas -c $CONFIG -ck show_types -f $INPUT_DIR/show_types.csv -o $OUTPUT_DIR
dgraphpandas -c $CONFIG -ck cast -f $INPUT_DIR/cast.csv -o $OUTPUT_DIR
dgraphpandas -c $CONFIG -ck genre -f $INPUT_DIR/genre.csv -o $OUTPUT_DIR
dgraphpandas -c $CONFIG -ck rating -f $INPUT_DIR/rating.csv -o $OUTPUT_DIR
dgraphpandas -c $CONFIG -ck director -f $INPUT_DIR/directors.csv -o $OUTPUT_DIR
dgraphpandas -c $CONFIG -ck title -f $INPUT_DIR/netflix_titles.csv -o $OUTPUT_DIR

echo Ensuring xid has index
curl -sX POST localhost:8080/alter -d 'xid: string @index(hash) .' | jq .

echo Applying Data Files
for f in $OUTPUT_DIR/*.gz; do
    echo "$f"
    dgraph live \
        --schema schema.txt \
        --tmp $DGRAPH_TEMP_BUFFERS \
        --files "$f" \
        --upsertPredicate $UPSERT_PREDICATE \
        --xidmap $XIDMAP_LOCATION \
        --format rdf \
        --batch 500
done
