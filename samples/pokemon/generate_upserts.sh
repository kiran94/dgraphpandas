#!/bin/bash
set -e

# Run me from root of the repository

INPUT_DIR=samples/pokemon/input
OUTPUT_DIR=samples/pokemon/output

###########################
echo Generating Generations
python -m dgraphpandas \
    --file $INPUT_DIR/generations.csv \
    --subject id \
    --type generation \
    --edges main_region_id \
    --output_dir $OUTPUT_DIR \
    --generate_upsert

###########################
echo Generating Types
python -m dgraphpandas \
    --file $INPUT_DIR/types.csv \
    --subject id \
    --type type \
    --edges generation_id damage_class_id \
    --output_dir $OUTPUT_DIR \
    --generate_upsert

###########################
echo Generating Version
python -m dgraphpandas \
    --file $INPUT_DIR/versions.csv \
    --subject id \
    --type version \
    --edges version_group_id \
    --output_dir $OUTPUT_DIR \
    --generate_upsert

###########################
echo Generating Regions
python -m dgraphpandas \
    --file $INPUT_DIR/regions.csv \
    --subject id \
    --type region \
    --output_dir $OUTPUT_DIR \
    --generate_upsert

###########################
echo Generating Pokemon
python -m dgraphpandas \
    --file $INPUT_DIR/pokemon.csv \
    --subject id \
    --type pokemon \
    --output_dir $OUTPUT_DIR \
    --generate_upsert

###########################
echo Generating Moves
python -m dgraphpandas \
    --file $INPUT_DIR/moves.csv \
    --subject id \
    --type move \
    --edges generation_id type_id target_id damage_class_id effect_id contest_type_id contest_effect_id super_contest_effect_id \
    --output_dir $OUTPUT_DIR \
    --generate_upsert

###########################
echo Generating Pokemon
python -m dgraphpandas \
    --file $INPUT_DIR/pokemon_species.csv \
    --subject id \
    --type pokemon \
    --edges generation_id evolution_chain_id color_id shape_id habitat_id evolves_from_species_id growth_rate_id \
    --output_dir $OUTPUT_DIR \
    --generate_upsert
