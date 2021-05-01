# dgraphpandas

[![Build](https://github.com/kiran94/dgraphpandas/actions/workflows/python-package.yml/badge.svg)](https://github.com/kiran94/dgraphpandas/actions/workflows/python-package.yml) ![PyPI](https://img.shields.io/pypi/v/dgraphpandas?color=blue&style=flat-square) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![Coverage Status](https://coveralls.io/repos/github/kiran94/dgraphpandas/badge.svg)](https://coveralls.io/github/kiran94/dgraphpandas) [![Codacy Badge](https://app.codacy.com/project/badge/Grade/3484574402e0408c97849301b354be8d)](https://www.codacy.com/gh/kiran94/dgraphpandas/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=kiran94/dgraphpandas&amp;utm_campaign=Badge_Grade)

A Library (with accompanying cli tool) to transform [Pandas](https://pandas.pydata.org/pandas-docs/stable/user_guide/index.html#user-guide) DataFrames into Exports ([RDF](https://en.wikipedia.org/wiki/Resource_Description_Framework)) to be sent to [DGraph Live Loader](https://dgraph.io/docs/deploy/fast-data-loading/live-loader/)

```sh
python -m pip install dgraphpandas
```

## Usage

### Command Line

```sh
❯ dgraphpandas --help
usage: dgraphpandas [-h] [-x {upserts,schema,types}] [-f FILE] -c CONFIG
                    [-ck CONFIG_FILE_KEY] [-o OUTPUT_DIR] [--console]
                    [--export_csv] [--encoding ENCODING]
                    [--chunk_size CHUNK_SIZE]
                    [--gz_compression_level GZ_COMPRESSION_LEVEL]
                    [--key_separator KEY_SEPARATOR]
                    [--add_dgraph_type_records ADD_DGRAPH_TYPE_RECORDS]
                    [--drop_na_intrinsic_objects DROP_NA_INTRINSIC_OBJECTS]
                    [--drop_na_edge_objects DROP_NA_EDGE_OBJECTS]
                    [--illegal_characters ILLEGAL_CHARACTERS]
                    [--illegal_characters_intrinsic_object ILLEGAL_CHARACTERS_INTRINSIC_OBJECT]
                    [--version] [-v {DEBUG,INFO,WARNING,ERROR,NOTSET}]
```

This is a real example which you can find in the [samples folder](https://github.com/kiran94/dgraphpandas/tree/main/samples) and run from the root of this repository:

```sh
dgraphpandas \
  --config samples/planets/dgraphpandas.json \
  --config_file_key planet \
  --file samples/planets/solar_system.csv \
  --output samples/planets/output
```

### Module

This example can also be found in [Notebook](https://github.com/kiran94/dgraphpandas/blob/main/samples/notebooks/PlanetSample.ipynb) form.

```py
import dgraphpandas as dpd

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
# Generate RDF Upsert statements
intrinsic, edges = dpd.to_rdf('solar_system.csv', config, 'planet', output_dir='.', export_rdf=True)

# Do something with these statements e.g write to zip and ship to DGraph
# The cli will zip this output automatically
# In module mode when you provide output_dir and export_rdf it will automatically zip and write to disk
print(intrinsic)
print(edges)
```

Alternatively, you could call the underlying methods

```py
# Perform a Horizontal Transform on the passed file using the config/key
intrinsic, edges = horizontal_transform('solar_system.csv', config, "planet")
# Generate RDF Upsert statements
intrinsic_upserts, edges_upserts = generate_upserts(intrinsic, edges)
```