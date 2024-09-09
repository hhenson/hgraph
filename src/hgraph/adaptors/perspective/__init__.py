# try:
import perspective

assert tuple(map(int, perspective.__version__.split("."))) >= (2, 10, 1)

from ._perspective import *
from ._perspetive_publish import *
from ._perspective_adaptor import *

# except:
#     pass
