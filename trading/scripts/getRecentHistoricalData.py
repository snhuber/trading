import pandas as pd
import sys
import argparse
from trading import database
from ib_insync import util
from configparser import ConfigParser, ExtendedInterpolation
import os
from trading import myWatchdog, utils
from ib_insync.ibcontroller import IBC
from ib_insync import IB, Contract
import logging
from ib_insync import ibcontroller
import pprint
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import datetime
from sqlalchemy import desc
import dateutil
from trading import marketDataIB, exceptions
from apscheduler.events import *

def runProg(args):
    """run program"""

    util.patchAsyncio()

    # log to a file
    utils.logToFile(f'getRecentHistoricalData.log')
    # utils.logToConsole()

    apschedulerLogger = logging.getLogger('apscheduler')
    apschedulerLogger.setLevel(logging.ERROR)
    tradingLogger = logging.getLogger('trading')
    tradingLogger.setLevel(logging.WARNING)



    pd.set_option('display.width', 200)

    # load the config file
    configFile = args.configFile
    config = ConfigParser(interpolation=ExtendedInterpolation(), defaults=os.environ)
    config.read(configFile)

    # load data from configFile
    host = config.get('InteractiveBrokers', 'host')
    port = config.getint('InteractiveBrokers', 'port')
    DBType = config.get('DataBase', 'DBType')
    DBFileName = config.get('DataBase', 'DBFileName')
    clientId = config.get('InteractiveBrokers', 'clientId')



    # for production mode: watchdog
    if 1:
        # start watchdog
        # ibc = IBC(963, gateway=True, tradingMode='paper',ibcIni='/home/bn/IBController/configPaper.ini')
        ibcIni = config.get('InteractiveBrokers', 'ibcIni')
        tradingMode = config.get('InteractiveBrokers', 'tradingMode')
        ibc = IBC(970, gateway=True, tradingMode=tradingMode, ibcIni=ibcIni)
        myWatchdogapp = myWatchdog.myWatchdog(ibc, appStartupTime=15, port=4002)
        myWatchdogapp.start()
        ib = myWatchdogapp.ib
        pass

    if 0:
        # faster way for now
        ib = IB()
        ib.connect(host=host, port=port, clientId=clientId)
        pass

    pass

    # create database class
    mydb = database.tradingDB(DBType=DBType, DBFileName=DBFileName)
    # load existing database
    mydb.instantiateExistingTablesAndClasses(ib=ib)
    # set log level of sqlalchemy
    mydb._loggerSQLAlchemy.setLevel(logging.WARNING)

    qcs = mydb.MarketDataInfoTableDataFrame.qualifiedContract
    for qc in qcs:
        print(qc,type(qc))
        ib.reqMktData(contract=qc,
                      genericTickList='',
                      snapshot=False,
                      regulatorySnapshot=False,
                      mktDataOptions=None)

        pass



    df = pd.DataFrame(columns='symbol bidSize bid ask askSize high low close'.split())
    df['symbol'] = [qc.localSymbol for qc in qcs]
    contract2Row = {qc: i for (i, qc) in enumerate(qcs)}
    pprint.pprint(contract2Row)

    def onPendingTickers(tickers):
        for t in tickers:
            iRow = contract2Row[t.contract]
            localSymbol = t.contract.localSymbol
            if localSymbol  == "EUR.USD":
                nowUTC = pd.to_datetime(pd.datetime.utcnow()).tz_localize(None)
                nowUTCRounded = nowUTC.floor('1 min')
                dateTime = pd.to_datetime(t.time).tz_localize(None)
                print(localSymbol, nowUTCRounded, ((dateTime - nowUTCRounded)/pd.Timedelta('1 sec')),t.close)

    #         df.iloc[iRow, 1:] = (t.bidSize, t.bid, t.ask, t.askSize, t.high, t.low, t.close)
    #     print(df)

    ib.setCallback('pendingTickers', onPendingTickers)

    # ib.sleep(300)

    if 1:
        util.allowCtrlC()
        # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
        try:
            asyncio.get_event_loop().run_forever()
        except (KeyboardInterrupt, SystemExit):
            pass

#
#     for qc in qcs:
#         ib.cancelMktData(qcs)
#
#
#     ib.disconnect()
#
#
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-c', '--configFile', help='Config File Name', required=True, type=str)
if __name__ == '__main__':
    args = parser.parse_args()
    sys.exit(runProg(args))
