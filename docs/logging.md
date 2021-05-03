
Logging is used throughout dgraphpandas using the standard [logging module](https://docs.python.org/3/library/logging.html).

## Command Line

If you are using the command line tool then setting the logging level is as simple as passing an additional parameter.

```sh
dgraphpandas -v DEBUG
```

Where the following levels are supported: `DEBUG,INFO,WARNING,ERROR,NOTSET`

By default these will log to stdout with no color but you can also add coloring by installing the [`coloredlogs`](https://pypi.org/project/coloredlogs/) module.

```sh
python -m pip install coloredlogs
```

## Module

If you are using dgraphpandas in your own script then you can set logging on a more granular level like so:

```py
import logging
import dgraphpandas as dpd

logging.basicConfig()
logging.getLogger('dgraphpandas.rdf').setLevel(logging.DEBUG)
logging.getLogger('dgraphpandas.types').setLevel(logging.DEBUG)
logging.getLogger('dgraphpandas.config').setLevel(logging.DEBUG)
logging.getLogger('dgraphpandas.strategies.horizontal').setLevel(logging.DEBUG)
logging.getLogger('dgraphpandas.strategies.vertical').setLevel(logging.DEBUG)
logging.getLogger('dgraphpandas.strategies.vertical_helpers').setLevel(logging.DEBUG)
logging.getLogger('dgraphpandas.strategies.schema').setLevel(logging.DEBUG)
logging.getLogger('dgraphpandas.writers.upserts').setLevel(logging.DEBUG)
logging.getLogger('dgraphpandas.writers.schema').setLevel(logging.DEBUG)

# your logic
dpd.to_rdf('input.csv', config, 'config_key')
```
