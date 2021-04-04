# dgraphpandas

[![Python Build](https://github.com/kiran94/dgraphpandas/actions/workflows/python-package.yml/badge.svg)](https://github.com/kiran94/dgraphpandas/actions/workflows/python-package.yml) ![PyPI](https://img.shields.io/pypi/v/dgraphpandas?color=blue&style=flat-square) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Library (with accompanying cli tool) to transform [Pandas](https://pandas.pydata.org/pandas-docs/stable/user_guide/index.html#user-guide) DataFrames into Exports ([RDF](https://en.wikipedia.org/wiki/Resource_Description_Framework)) to be sent to [DGraph Live Loader](https://dgraph.io/docs/deploy/fast-data-loading/live-loader/)

- [dgraphpandas](#dgraphpandas)
  - [Usage](#usage)
    - [Command Line](#command-line)
    - [Module](#module)
  - [Configuration](#configuration)
    - [Additional Configuration](#additional-configuration)
  - [Samples](#samples)
  - [Local Setup](#local-setup)

## Usage

```sh
python -m pip install dgraphpandas
```

### Command Line

This is a real example which you can find in the samples folder and run from the root of this repository.

```sh
python -m dgraphpandas \
  --config samples/planets/dgraphpandas.json \
  --config_file_key planet \
  --file samples/planets/solar_system.csv \
  --output samples/planets/output
```

### Module

```py
from dgraphpandas.strategies.horizontal import horizontal_transform
from dgraphpandas.strategies.vertical import vertical_transform
from dgraphpandas.writers.upserts import generate_upserts

# Define a Configuration for your data files(s). Explained further in the Configuration section.
config = {
  "transform": "horizontal",
  "files": {
    "planet": {
      "subject_fields": ["id"],
      "edge_fields": ["type"],
      "type_overrides": {
        "order_from_sun": "int32",
        "diameter_earth_relative": "float32",
        "diameter_km": "float32",
        "mass_earth_relative": "float32",
        "mean_distance_from_sun_au": "float32",
        "orbital_period_years": "float32",
        "orbital_eccentricity": "float32",
        "mean_orbital_velocity_km_sec": "float32",
        "rotation_period_days": "float32",
        "inclination_axis_degrees": "float32",
        "mean_temperature_surface_c": "float32",
        "gravity_equator_earth_relative": "float32",
        "escape_velocity_km_sec": "float32",
        "mean_density": "float32",
        "number_moons": "int32",
        "rings": "bool"
      },
      "ignore_fields": ["image", "parent"]
    }
  }
}

# Perform a Horizontal Transform on the passed file using the config/key
intrinsic, edges = horizontal_transform('solar_system.csv', config, "planet")

# Generate RDF Upsert statements
intrinsic_upserts, edges_upserts = generate_upserts(intrinsic, edges)

# Do something with these statements e.g write to zip and ship to DGraph
# The cli will zip this output automatically
print(intrinsic)
print(edges)
```

## Configuration

A Configuration file influences how we transform a DataFrame. It consists of:

- Global configuration options
  - Options which will be applied to files
  - These can either be defined in the configuration or as `kwargs` in the transform method.
  - A collection of `files`

- File configuration options
  - Options which will be applied only to this entry
  - `subject_fields` is required so the unique identifier for a row in the DataFrame can be found
  - `edge_fields` are optional and if provided will generate edge output
  - `type_overrides` are optional but recommended to ensure the correct type is attached in RDF

*If you are running this with the module and passing via `kwargs` then these options may also be lambda callable with takes the dataframe. For example if you didn't want to hard code all your edge fields and were following a convention that all edge fields have suffix `_id` then you could set the edge_fields to `lambda frame: frame.loc[frame['predicate'].str.endswith('_id'), 'predicate'].unique().tolist()`
`*

```py
config = {
  "transform": "horizontal",
  "files": {
    "planet": {
      "subject_fields": ["id"],
      "edge_fields": ["type"],
      "type_overrides": {
        "order_from_sun": "int32",
        "diameter_earth_relative": "float32",
        "diameter_km": "float32",
        "mass_earth_relative": "float32",
        "mean_distance_from_sun_au": "float32",
        "orbital_period_years": "float32",
        "orbital_eccentricity": "float32",
        "mean_orbital_velocity_km_sec": "float32",
        "rotation_period_days": "float32",
        "inclination_axis_degrees": "float32",
        "mean_temperature_surface_c": "float32",
        "gravity_equator_earth_relative": "float32",
        "escape_velocity_km_sec": "float32",
        "mean_density": "float32",
        "number_moons": "int32",
        "rings": "bool"
      },
      "ignore_fields": ["image", "parent"]
    }
  }
}
```

### Additional Configuration

**Global Level**

These options can be placed on the root of the config or passed as `kwargs` directly.

- `add_dgraph_type_records`
    - DGraph has a special field called `dgraph.type`, this can be used to query via the `type()`
    function. If `add_dgraph_type_records` is enabled, then we add `dgraph.type` fields
    to the current frame.
- `strip_id_from_edge_names`
    - Its common for a data set to have a reference to another 'table' using `_id` convention
    - For example if you had a Student & School then the student might more sense to have (Student) - school -> (School) rather then having an `_id` in the predicate.
- `drop_na_intrinsic_objects`
    - Automatically drop intrinsic records where the object is NA. In a relational model, you might have a column with a null entry however in a graph model you may want to omit the attribute altogether
- `drop_na_edge_objects`
    - Same as `drop_na_intrinsic_objects` but for edges
- `key_separator`
    - Separator used to combine key fields. For example if the key separator was `_` and we were operating on an intrinsic attribute for a customer with id 1 then the `xid` would be `customer_1`
- `illegal_characters`
    - Characters to strip from intrinsic and edge subjects. if the unique identifier has a character not supported by RDF/DGraph then strip them away or they will not be accepted by live loading.
- `illegal_characters_intrinsic_object`
    - Same as `illegal_characters` but for the subject on intrinsic fields. These have a different set of illegal characters because subjects on intrinsic records are actual data values and are quoted. They therefore can accept many more characters then the subject.

**File Level**
- `type_overrides`
    - Recommended. This ensures that data types are being treated as a type and the output RDF has the correct type mapped into it. Without this fields will go under the default rdf type `<xs:string>` but you may want a field to be a true int in RDF.
    - Additionally certain data types such as `datetime64` will activate special handling to ensure the output in RDF is within the correct format to be ingested into DGraph.
    - Supported Types can be found [here](https://github.com/kiran94/dgraphpandas/blob/main/dgraphpandas/types.py)
- `csv_edges`
    - Sometimes a vendor will provide a data file where a single column is actually a csv list and each csv value should be broken into multiple RDF statements (because they relate to independent entities). Adding that column into this list will do that.
    - For example in the [Netflix sample / title file](https://github.com/kiran94/dgraphpandas/blob/main/samples/netflix/dgraphpandas.json) we have a `cast` column where the values are `actor_1, actor2` then adding to csv_edges will ensure that the movie has 2 different relationships for each cast member.
- `ignore_fields`
    - Add fields in the input that we don't are about to this list so they aren't present in the output
- `override_edge_name`
    - Ensure that the edge name as a different predicate and/or target_node_type to what is defined in the file.
    - For example in the [Pokemon sample / pokemon_species](https://github.com/kiran94/dgraphpandas/blob/main/samples/pokemon/dgraphpandas.json) file you will see a column called `evolves_from_species` which tells us for a given pokemon which other pokemon does it evolve from. If we were to use the raw data here we would get a `evolves_from_species` edge with an incorrect target xid. Instead we want to override the `target_node_type` to `pokemon` so the edge correctly loops back to a node of the same type.
- `pre_rename`
    - Rename intrinsic predicates or edge names to something else

## Samples

Samples can be found [here](https://github.com/kiran94/dgraphpandas/tree/main/samples). They follow a convention where the download script can be found within the `input` directory and the config, generate_upsert, publish scripts can be found root of each respective sample.

There are also [Jupyter Notebooks](https://github.com/kiran94/dgraphpandas/tree/main/samples/notebooks) which should show step by step examples.

## Local Setup

Assuming you have already cloned the repo and have a terminal in the root of the project.

```sh
# Create Virtual Environment and Activate it
conda create -n dgraphpandas python=3.6 # or venv
conda activate dgraphpandas

# Restore packages
python -m pip install -r requirements-dev.txt
python -m pip install -r requirements.txt

# Run Flake
flake8 --count .

# Run Tests
python -m unittest

# Create & Run DGraph
docker-compose up

# Try a Sample
# See Sample section for more details
# It should help getting some data,
# generating rdf and publishing to your
# local DGraph

# Install a Local Copy of the Library
python -m pip install -e .

# Remember to Uninstall once ready
python -m pip uninstall dgraphpandas -y
```
