from trading import database
import argparse
import sys
from ib_insync import util, IB
from configparser import ConfigParser, ExtendedInterpolation
import os
import logging
import numpy as np
import pandas as pd
import time
import multiprocessing as mp
from multiprocessing import managers
import sqlalchemy
from collections import OrderedDict
import functools

def takeMyTime(N):
    for i in range(N):
        a = np.exp(2.1)
        pass
    pass


def parallelizeDataBaseRequest(tableNames: list,
                               simpleFunc: callable=None,
                               ):
    # tableNames contains a list of string representing the tableNames for which we want to call simpleFunc
    # simpleFunc has one argument: the name of the table
    # it returns one item (whatever that is)

    # Setup a list of processes that we want to run
    manager = mp.Manager()
    # setup a container for the results of each call to simpleFunc
    results = manager.dict()
    # print(type(results))

    # the list of processes to run
    processes = []

    # set up a function that is to be called from the processes that fills the results Dict
    def multiProcessSimpleFunc(tableName,results):
        resultOfSimpleFunc = simpleFunc(tableName=tableName)
        results[tableName] = resultOfSimpleFunc
        pass

    # set up the argss to pass to the callable
    argss = [(tableName, results) for tableName in tableNames]

    for args in argss:
        process = mp.Process(target=multiProcessSimpleFunc,
                             args=args)

        processes.append(process)
        pass

    # Run processes
    for p in processes:
        p.start()
        pass

    # Exit the completed processes
    for p in processes:
        p.join()
        pass

    resultsDict = dict(results)

    return resultsDict


def funcMultiProcessingSharedList(
        tableName: str,
        mydb: database.tradingDB,
        dateRange: pd.DatetimeIndex,
        results: managers.DictProxy) -> None:

    dfNew = getCompleteDFFromDateRangeAndTableName(tableName=tableName,
                                                   mydb=mydb,
                                                   dateRange=dateRange)

    results[tableName] = dfNew

    pass


def getCompleteDFFromDateRangeAndTableName(
        tableName: str=None,
        mydb: database.tradingDB=None,
        dateRange: pd.DatetimeIndex=None):
    """retrieve training data for one one table"""

    tableORM = mydb.getTableORMByTablename(tableName)

    df = getDataFromDateRange(tableSchema=tableORM.__table__,
                              mydb=mydb,
                              dateRange=dateRange)

    dfNew = processRawDataFrameAsResultFromGetDataFromRangeToTrainginData(
        tableName=tableName,
        df=df,
        referenceDateTime=dateRange[-1])

    return dfNew


def processRawDataFrameAsResultFromGetDataFromRangeToTrainginData(
        tableName: str,
        df: pd.DataFrame,
        referenceDateTime: pd.datetime) -> pd.DataFrame:
    # check if we have data
    dataExists = True

    if df.achieved.isnull().any():
        dataExists = False
        pass

    # add year
    df.loc[:, 'year'] = df.achieved.dt.year if dataExists else None
    df.loc[:, 'quarter'] = df.achieved.dt.quarter if dataExists else None
    df.loc[:, 'month'] = df.achieved.dt.month if dataExists else None
    df.loc[:, 'yearweek'] = df.achieved.dt.weekofyear if dataExists else None
    df.loc[:, 'weekday'] = df.achieved.dt.weekday if dataExists else None
    df.loc[:, 'minutesSinceMidnight'] = (df.achieved.dt.hour * 60 + df.achieved.dt.minute) if dataExists else None

    # calculate the difference in time between a row and the next row
    df.loc[:, 'diffToNextRowInMinutes'] = (df.achieved.diff().shift(-1) / pd.Timedelta(1, 'm')) if dataExists else None
    # the last difference is zero
    df.iloc[-1, df.columns.get_loc('diffToNextRowInMinutes')] = 0 if dataExists else None

    df.loc[:, 'tableName'] = tableName
    referenceDateTime = referenceDateTime
    lastDateTimeFound = df.iloc[-1, df.columns.get_loc('achieved')] if dataExists else None
    df.loc[:, 'requestReferenceDateTime'] = referenceDateTime
    diffToReferenceDateTimeInMinutes = (referenceDateTime - lastDateTimeFound) / pd.Timedelta(1,
                                                                                              'm') if dataExists else None
    df.iloc[-1, df.columns.get_loc('diffToNextRowInMinutes')] = diffToReferenceDateTimeInMinutes if dataExists else None

    if dataExists:
        df.loc[:, 'diffToNextRowInMinutes'] = df.diffToNextRowInMinutes.astype(int)
        pass

    # repair missing data in open, high, low
    df.loc[df['open'].isnull(), 'open'] = df['close']
    df.loc[df['low'].isnull(), 'low'] = df['close']
    df.loc[df['high'].isnull(), 'high'] = df['close']

    dfNew = df.copy()
    return dfNew


def getDataFromDateRange(
        tableSchema: sqlalchemy.Table,
        mydb: database.tradingDB,
        dateRange: pd.DatetimeIndex)-> pd.DataFrame:

    tt1 = time.time()

    eng = mydb.DBEngine
    eng.dispose()

    ssn = mydb.Session()
    results = []

    for _DT in dateRange:
        __DT = _DT.to_pydatetime()
        _close = None
        _low = None
        _high = None
        _open = None


        tm = None
        qsf = (ssn.query(tableSchema.c.close, tableSchema.c.low, tableSchema.c.high, tableSchema.c.high, tableSchema.c.datetime).order_by(tableSchema.c.datetime.desc()).filter(
            tableSchema.c.datetime <= __DT)).first()
        if qsf is not None:
            _close, _low, _high, _open, tm = qsf
        else:
            qsf = (
                ssn.query(tableSchema.c.close, tableSchema.c.low, tableSchema.c.high, tableSchema.c.high, tableSchema.c.datetime).order_by(tableSchema.c.datetime).filter(tableSchema.c.datetime > __DT)).first()
            if qsf is not None:
                _close, _low, _high, _open, tm = qsf
                pass
            pass
        tDiff = None
        try:
            tDiff = tm - __DT
        except:
            pass
        ordrdDct = OrderedDict(
            (
                ('target', __DT),
                ('achieved', tm),
                ('mismatchInMinutes', tDiff / pd.Timedelta(1, 'm')),
                ('close', _close),
                ('low', _low),
                ('high', _high),
                ('open', _open)
            )
        )
        results.append(ordrdDct)
        pass

    df = pd.DataFrame(results)
    df.sort_index(inplace=True)

    tt2 = time.time()
    ttdiff = tt2 - tt1

    nRowsTotal = nRowsTotal = ssn.query(tableSchema).count()

    ssn.close()

    print(f'Getting data from table: {tableSchema}: rowsTotal: {nRowsTotal}; {ttdiff}')

    return (df)


def createDateRange(startDate: pd.datetime) -> pd.DatetimeIndex:
    tt1 = time.time()

    a = pd.date_range(start=startDate - pd.DateOffset(minutes=20), end=startDate, freq='1 min')
    a = a.append(
        pd.date_range(start=startDate - pd.DateOffset(hours=1), end=a[0], freq='5 min'))
    a = a.append(
        pd.date_range(start=startDate - pd.DateOffset(hours=4), end=a[0], freq='20 min'))
    a = a.append(
        pd.date_range(start=startDate - pd.DateOffset(days=1), end=a[0], freq='1 H'))
    a = a.append(
        pd.date_range(start=startDate - pd.DateOffset(weeks=1), end=a[0], freq='4 H'))
    a = a.append(
        pd.date_range(start=startDate - pd.DateOffset(weeks=4), end=a[0], freq='12 H'))
    a = a.append(
        pd.date_range(start=startDate - pd.DateOffset(weeks=12), end=a[0], freq='1 D'))
    a = a.append(
        pd.date_range(start=startDate - pd.DateOffset(weeks=3 * 4 * 12), end=a[0], freq='1 W'))


    # a = pd.date_range(start=startDate - pd.DateOffset(minutes=20), end=startDate, freq='1 min')
    # a = a.append(
    #     pd.date_range(start=startDate - pd.DateOffset(hours=1), end=a[0], freq='1 min'))
    # a = a.append(
    #     pd.date_range(start=startDate - pd.DateOffset(hours=4), end=a[0], freq='2 min'))
    # a = a.append(
    #     pd.date_range(start=startDate - pd.DateOffset(days=1), end=a[0], freq='5 min'))
    # a = a.append(
    #     pd.date_range(start=startDate - pd.DateOffset(weeks=1), end=a[0], freq='10 H'))
    # a = a.append(
    #     pd.date_range(start=startDate - pd.DateOffset(weeks=4), end=a[0], freq='1 H'))
    # a = a.append(
    #     pd.date_range(start=startDate - pd.DateOffset(weeks=12), end=a[0], freq='4 H'))
    # a = a.append(
    #     pd.date_range(start=startDate - pd.DateOffset(weeks=3 * 4 * 12), end=a[0], freq='8 H'))

    b = a.drop_duplicates().sort_values()

    tt2 = time.time()
    ttdiff = tt2 - tt1
    print(f'Generation of date range: {ttdiff}')

    return (b)




def runProg(args):
    """run program"""

    ###
    # conclusion: for fast processes, it is beneficial to use the serial approach
    ##

    pd.set_option('display.width', 300)

    # log to a file
    util.logToFile(f'createTraingData.log')

    # load the config file
    configFile = args.configFile
    config = ConfigParser(interpolation=ExtendedInterpolation(), defaults=os.environ)
    config.read(configFile)

    # load data from configFile
    DBType = config.get('DataBase', 'DBType')
    DBFileName = config.get('DataBase', 'DBFileName')
    configIB = config['InteractiveBrokers']
    barSizePandasTimeDelta = pd.Timedelta(**eval(configIB.get('densityTimeDelta', '{"minutes":1}')))

    # create database class
    mydb = database.tradingDB(DBType=DBType, DBFileName=DBFileName)
    # mydb = database.tradingDB(DBType='mysql', DBFileName=DBFileName)

    # load existing database
    mydb.instantiateExistingTablesAndClasses()
    # set log level
    mydb._loggerSQLAlchemy.setLevel(logging.ERROR)

    nowUTCFloor = pd.to_datetime(pd.datetime.utcnow()).floor(barSizePandasTimeDelta) - barSizePandasTimeDelta
    startDate = nowUTCFloor
    # startDate = pd.to_datetime('2018-05-17 15:00:00').tz_localize('Europe/Berlin').tz_convert('UTC').tz_localize(None).round('1 min')
    print(startDate)
    dateRange = createDateRange(startDate)

    tableNames = mydb.MarketDataInfoTableDataFrame.tableName

    if 1:
        # multiprocessing Shared List using wrapper
        tt1 = time.time()
        def simpleFunc (tableName: str=None) -> None:
            return getCompleteDFFromDateRangeAndTableName(tableName=tableName,
                                                          mydb=mydb,
                                                          dateRange=dateRange)

        resultsDict = parallelizeDataBaseRequest(tableNames=tableNames,
                                                 simpleFunc=simpleFunc)

        df = resultsDict['MarketData_CASH_EUR_JPY_IDEALPRO']
        # df = resultsDict['MarketData_CFD_IBUS500_USD_SMART']
        print(df)

        tt2 = time.time()
        ttdiff = tt2 - tt1
        print(f'retrieved data multi processing Shared Variable using wrapper: {ttdiff}')
        print()


        pass

    if 1:
        # serial processing, long function
        tt1 = time.time()

        def simpleFunc (tableName: str=None) -> None:
            return getCompleteDFFromDateRangeAndTableName(tableName=tableName,
                                                          mydb=mydb,
                                                          dateRange=dateRange)

        resultsDict = {}
        for tableName in tableNames:
            resultsDict[tableName] = simpleFunc(tableName)
            pass

        df = resultsDict['MarketData_CASH_EUR_JPY_IDEALPRO']
        # df = resultsDict['MarketData_CFD_IBUS500_USD_SMART']
        # print(df)


        tt2 = time.time()
        ttdiff = tt2 - tt1
        print(f'retrieved data serial long function: {ttdiff}')
        print()

        pass

    if 1:
        # multiprocessing Shared List using wrapper, fast function
        tt1 = time.time()
        def simpleFunc (tableName: str=None) -> None:
            tableORM = mydb.getTableORMByTablename(tableName)
            ssn = mydb.Session()
            myDT = ssn.query(sqlalchemy.func.min(tableORM.datetime)).scalar()
            ssn.close()
            return myDT

        resultsDict = parallelizeDataBaseRequest(tableNames=tableNames,
                                                 simpleFunc=simpleFunc)

        print(resultsDict)


        tt2 = time.time()
        ttdiff = tt2 - tt1
        print(f'retrieved data multi processing Shared Variable using wrapper fast function: {ttdiff}')
        print()


        pass

    if 1:
        # serial processing, fast function
        tt1 = time.time()
        def simpleFunc (tableName: str=None) -> None:
            tableORM = mydb.getTableORMByTablename(tableName)
            ssn = mydb.Session()
            myDT = ssn.query(sqlalchemy.func.min(tableORM.datetime)).scalar()
            ssn.close()
            return myDT

        resultsDict={}
        for tableName in tableNames:
            resultsDict[tableName] = simpleFunc(tableName)
            pass

        print(resultsDict)

        tt2 = time.time()
        ttdiff = tt2 - tt1
        print(f'retrieved data serial fast function: {ttdiff}')
        print()


        pass

    if 0:
        # multiprocessing Shared List
        tt1 = time.time()

        # Setup a list of processes that we want to run
        manager = mp.Manager()
        results = manager.dict()
        # print(type(results))

        processes = []
        argss = [(tableName,mydb,dateRange,results) for tableName in tableNames]

        for args in argss:
            tableName = args[0]
            process = mp.Process(target=funcMultiProcessingSharedList,
                                 args=args)

            processes.append(process)
            pass

        # Run processes
        for p in processes:
            p.start()
            pass

        # Exit the completed processes
        for p in processes:
            p.join()
            pass

        resultsDict = dict(results)
        # print(resultsDict)

        df = resultsDict['MarketData_CASH_EUR_JPY_IDEALPRO']
        # df = resultsDict['MarketData_CFD_IBUS500_USD_SMART']

        print(df)

        tt2 = time.time()
        ttdiff = tt2 - tt1
        print(f'retrieved data multi processing Shared Variable: {ttdiff}')
        print()


        pass
    pass





parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-c', '--configFile', help='Config File Name', required=True, type=str)
if __name__ == '__main__':
    args = parser.parse_args()
    sys.exit(runProg(args))
