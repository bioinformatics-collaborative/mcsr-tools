__version__ = '0.1.0'
from .core import Qwatch, NotRequiredIf
from .cli import qwatch
__all__ = (
    "Qwatch",
    "NotRequiredIf",
    "utils",
    "qwatch",
           )