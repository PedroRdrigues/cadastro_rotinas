# dentro de __init__.py
import sys

if sys.version_info < (3, 14, 3):
    raise ImportError("Este pacote exige Python 3.14.3 ou superior.")

from ._rotinas import *
from ._utils import *
