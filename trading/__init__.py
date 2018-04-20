import sys

__version__ = '0.0.0'

if sys.version_info < (3, 6, 0):
    print("Python 3.6.0 or higher is required")
    sys.exit()

from .a1 import *


#__all__ = ['util']
# for _m in (objects, contract, order, ticker, ib, client, wrapper,
#         flexreport, ibcontroller):
#     __all__ += _m.__all__
