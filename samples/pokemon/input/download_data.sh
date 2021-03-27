#!/bin/bash
set -e

files=(
   "https://raw.githubusercontent.com/veekun/pokedex/master/pokedex/data/csv/regions.csv"
   "https://raw.githubusercontent.com/veekun/pokedex/master/pokedex/data/csv/pokemon.csv"
   "https://raw.githubusercontent.com/veekun/pokedex/master/pokedex/data/csv/types.csv"
   "https://raw.githubusercontent.com/veekun/pokedex/master/pokedex/data/csv/pokemon_moves.csv"
   "https://raw.githubusercontent.com/veekun/pokedex/master/pokedex/data/csv/moves.csv"
   "https://raw.githubusercontent.com/veekun/pokedex/master/pokedex/data/csv/versions.csv"
   "https://raw.githubusercontent.com/veekun/pokedex/master/pokedex/data/csv/generations.csv"
   "https://raw.githubusercontent.com/veekun/pokedex/master/pokedex/data/csv/abilities.csv"
   "https://raw.githubusercontent.com/veekun/pokedex/master/pokedex/data/csv/regions.csv"
)

for file in "${files[@]}"
do
   echo Downloading $file
   curl -s $file -O
done
