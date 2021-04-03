
from typing import Dict, Any


def get_from_config(key: str, config: Dict[str, Any], default: Any = None, **kwargs) -> Any:
    '''
    For the given configuration key,
    first try to pull from kwargs (if there is an override)
    and secondly pull from the passed config dictionary
    else return default
    '''
    if not key:
        raise ValueError(key)
    if not config:
        raise ValueError(config)

    return kwargs.get(key, config.get(key, default))
