#!/bin/bash
set -e

# Run me from samples/pokemon

DGRAPH_TEMP_BUFFERS=dgraph_temp_buffers
UPSERT_PREDICATE=xid
XIDMAP_LOCATION=xidmap
INPUT_DIR=input
OUTPUT_DIR=output
CONFIG=dgraphpandas.json

echo Generating Schema
dgraphpandas -c $CONFIG -x schema

echo Generating Exports

# dgraphpandas -c $CONFIG -o $OUTPUT_DIR -ck generations -f $INPUT_DIR/generations.csv
dgraphpandas -c $CONFIG -o $OUTPUT_DIR -ck pokemon_species -f $INPUT_DIR/pokemon_species.csv
dgraphpandas -c $CONFIG -o $OUTPUT_DIR -ck version -f $INPUT_DIR/versions.csv
dgraphpandas -c $CONFIG -o $OUTPUT_DIR -ck region -f $INPUT_DIR/regions.csv
dgraphpandas -c $CONFIG -o $OUTPUT_DIR -ck pokemon -f $INPUT_DIR/pokemon.csv
dgraphpandas -c $CONFIG -o $OUTPUT_DIR -ck move -f $INPUT_DIR/moves.csv

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
        --format rdf

done
