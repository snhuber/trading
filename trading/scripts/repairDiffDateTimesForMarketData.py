from trading import database
import argparse
import sys
from ib_insync import util, IB
from configparser import ConfigParser, ExtendedInterpolation
import os
import logging
import pandas as pd



def runProg(args):
    """run program"""

    pd.set_option('display.width', 300)

    # log to a file
    util.logToFile(f'createTraingData.log')

    # load the config file
    configFile = args.configFile
    config = ConfigParser(interpolation=ExtendedInterpolation(), defaults=os.environ)
    config.read(configFile)

    # load data from configFile
    host = config.get('InteractiveBrokers', 'host')
    port = config.getint('InteractiveBrokers', 'port')
    DBType = config.get('DataBase', 'DBType')
    DBFileName = config.get('DataBase', 'DBFileName')
    clientId = config.getint('InteractiveBrokers', 'clientId')

    # override configFile if clientId is given on the command line
    if args.clientId is not None:
        clientId = args.clientId


    # create database class
    mydb = database.tradingDB(DBType=DBType, DBFileName=DBFileName)
    # mydb = database.tradingDB(DBType='mysql', DBFileName=DBFileName)

    # load existing database
    mydb.instantiateExistingTablesAndClasses()
    # set log level
    mydb._loggerSQLAlchemy.setLevel(logging.ERROR)

    tbls = mydb.MarketDataInfoTableDataFrame['tableORM']

    if 0:
        # loop over entire table and repair
        df = None
        for idx in mydb.MarketDataInfoTableDataFrame.index:
            tableORM = mydb.MarketDataInfoTableDataFrame.at[idx,'tableORM']
            tableName = tableORM.__tablename__

            ssn = mydb.Session()
            print(tableName,ssn.query(tableORM).count())
            ssn.close()

            # if tableName not in ['MarketData_IND_N225_JPY_OSE.JPN', 'MarketData_IND_INDU_USD_CME']:
            # if tableName not in ['MarketData_IND_N225_JPY_OSE.JPN']:
            if False:
                continue
            dfLoop = mydb.correctDiffDateTimesForMarketDataTable(tableName=tableName,
                                                   startDateTime=None,
                                                   endDateTime=None,
                                                   doCorrection = True)
            print(dfLoop)
            if df is None:
                df = dfLoop.copy()
            else:
                if dfLoop is not None:
                    df = df.append(dfLoop)
                    pass
                pass
            pass
        df.sort_index(inplace=True)
        print(df)

    if 1:
        # loop over entire table and check for specific values in any of the columns [close, low, high, open]
        df = None
        valuesToFind = [None, 0]
        for idx in mydb.MarketDataInfoTableDataFrame.index:
            tableORM = mydb.MarketDataInfoTableDataFrame.at[idx,'tableORM']
            tableName = tableORM.__tablename__
            if tableName not in ['MarketData_CFD_IBDE30_EUR_SMART']:
                # continue
                pass
            ssn = mydb.Session()
            print(f'tableName: {tableName}; nRows: {ssn.query(tableORM).count()}; ',end='',flush=True)
            ssn.close()
            dfLoop = mydb.findEntriesInMarketDataTable(tableName=tableName,
                                                       valuesToFind=valuesToFind,
                                                       startDateTime=None,
                                                       endDateTime=None,
                                                       doCorrection = False)
            # print(dfLoop)
            nRowsLoop = 0
            if dfLoop is not None:
                nRowsLoop = len(dfLoop)
                pass
            print(f'nRows with {valuesToFind}: {nRowsLoop}',end='\n',flush=True)
            if df is None:
                df = dfLoop.copy()
            else:
                if dfLoop is not None:
                    df = df.append(dfLoop)
                    pass
                pass
            pass
        df.sort_index(inplace=True)
        df = df.sort_values(by=['tableName', 'datetime'])
        print(df)




parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-c', '--configFile', help='Config File Name', required=True, type=str)
parser.add_argument('--clientId', help='clientId to connect to TWS/gateway', required=False, default=None, type=int)
if __name__ == '__main__':
    args = parser.parse_args()
    sys.exit(runProg(args))
