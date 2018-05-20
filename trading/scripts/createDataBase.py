"""
defines a command-line utility for the creation of the database
"""


import sys
from ib_insync import IB, Forex, Contract, Index, util
import ibapi
import argparse
import asyncio
from configparser import ConfigParser, ExtendedInterpolation
import os
from trading import database, utils
import pandas as pd
import logging
import numpy as np

util.patchAsyncio()

# the following does not work: it is never entered upon any error
@ibapi.utils.iswrapper
def error(self, reqId: int, errorCode: int, errorString: str, contract):
    """ibapi error callback. Does not work. Don't know why"""
    super().error(reqId, errorCode, errorString, contract)
    print("Error. Id: ", reqId, " Code: ", errorCode, " Msg: ", errorString, contract)



def createDataBase(args):
    """creates the database"""


    # log to a file
    util.logToFile(f'createDataBase.log')

    # increase width of pandas printing
    pd.set_option('display.width',200)

    mydb = database.instantiateMyDB(args)

    pass


parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-c', '--configFile', help='Config File Name', required=True, type=str)
parser.add_argument('--clientId', help='clientId to connect to TWS/gateway', required=False, default=None,  type=int)
parser.add_argument('--timeOutTime', help='maximum time to wait for simple Requests to IB (not requests for historical bars)', required=False, default=None,  type=int)

if __name__ == '__main__':
    args = parser.parse_args()


    apschedulerLogger = logging.getLogger('apscheduler')
    apschedulerLogger.setLevel(logging.ERROR)
    tradingLogger = logging.getLogger('trading')
    tradingLogger.setLevel(logging.INFO)


    sys.exit(createDataBase(args))

