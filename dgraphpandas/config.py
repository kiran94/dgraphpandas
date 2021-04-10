import json
import logging
from pprint import pformat
from typing import Dict, Any, Union

logger = logging.getLogger(__name__)


def get_from_config(key: str, config: Dict[str, Any], default: Any = None, **kwargs) -> Any:
    '''
    For the given configuration key,
    first try to pull from kwargs (if there is an override)
    and secondly pull from the passed config dictionary
    else return default
    '''
    if not key:
        raise ValueError('key')
    if not config:
        raise ValueError('config')

    return kwargs.get(key, config.get(key, default))


def _get_config(config: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    '''
    The Configuration may be a file path or already loaded config.
    If it's a string then attempt to load it first.
    '''
    if config is None:
        raise ValueError('config')

    if isinstance(config, str):
        logger.info(f'Reading from configuration {config}')
        with open(config, 'r') as f:
            config: Dict[str, Any] = json.load(f)
            logger.debug('Global Config \n %s', pformat(config))
        return config
    else:
        return config
