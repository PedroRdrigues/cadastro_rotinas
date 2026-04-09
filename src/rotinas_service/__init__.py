# dentro de __init__.py
import sys

if sys.version_info < (3, 14, 3):
    raise ImportError("Este pacote exige Python 3.14.3 ou superior.")

from ._rotinas import RoutineService
from ._utils import check_and_update_log_file, create_essential_folders
