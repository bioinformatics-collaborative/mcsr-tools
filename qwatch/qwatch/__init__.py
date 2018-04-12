__version__ = '0.1.0'
from .core import Qwatch, NotRequiredIf
from .cli import qwatch


def represent_dictionary_order(cls, dict_data):
    return cls.represent_mapping('tag:yaml.org,2002:map', dict_data.items())


def _setup_yaml():
    """ https://stackoverflow.com/a/8661021 """
    yaml.add_representer(OrderedDict, represent_dictionary_order)

__all__ = (
    "Qwatch",
    "NotRequiredIf",
    "utils",
    "qwatch",
    "_setup_yaml",
           )