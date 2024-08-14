# try:
import perspective

assert tuple(map(int, perspective.__version__.split("."))) >= (2, 8, 0)

from ._perspective import *
from ._perspetive_publish import *
from ._perspective_adaptor import *

# except:
#     pass
