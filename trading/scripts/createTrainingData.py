from trading import database
import argparse
import sys
from ib_insync import util, IB
from configparser import ConfigParser, ExtendedInterpolation
import os
import logging
import numpy as np
import pandas as pd
import pandas
import time
import multiprocessing as mp
from multiprocessing import managers
import sqlalchemy
from collections import OrderedDict

def takeMyTime(N):
    for i in range(N):
        a = np.exp(2.1)
        pass
    pass


def funcMultiProcessingSharedList(i: int,
                        dateRange: pandas.core.indexes.datetimes.DatetimeIndex,
                        tbl: sqlalchemy.sql.schema.Table,
                        mydb: database.tradingDB,
                        results: managers.DictProxy) -> None:

    df = getDataFromDateRange(dateRange=dateRange,
                              tbl=tbl,
                              mydb=mydb)

    # check if we have data
    dataExists = True

    if df.achieved.isnull().any():
        dataExists = False
        pass

    # add year
    df.loc[:,'year'] = df.achieved.dt.year if dataExists else None
    df.loc[:,'quarter'] = df.achieved.dt.quarter  if dataExists else None
    df.loc[:,'month'] = df.achieved.dt.month  if dataExists else None
    df.loc[:,'yearweek'] = df.achieved.dt.weekofyear  if dataExists else None
    df.loc[:,'weekday'] = df.achieved.dt.weekday  if dataExists else None
    df.loc[:,'minutesSinceMidnight'] = (df.achieved.dt.hour*60 + df.achieved.dt.minute) if dataExists else None

    # calculate the difference in time between a row and the next row
    df.loc[:, 'diffToNextRowInMinutes'] = (df.achieved.diff().shift(-1)/pd.Timedelta(1,'m')) if dataExists else None
    # the last difference is zero
    df.iloc[-1, df.columns.get_loc('diffToNextRowInMinutes')] = 0 if dataExists else None


    df.loc[:,'tableName'] = tbl.name
    referenceDateTime = dateRange[-1]
    lastDateTimeFound = df.iloc[-1, df.columns.get_loc('achieved')] if dataExists else None
    df.loc[:,'requestReferenceDateTime'] = referenceDateTime
    diffToReferenceDateTimeInMinutes = (referenceDateTime - lastDateTimeFound) / pd.Timedelta(1,'m') if dataExists else None
    df.iloc[-1, df.columns.get_loc('diffToNextRowInMinutes')] = diffToReferenceDateTimeInMinutes if dataExists else None

    if dataExists:
        df.loc[:, 'diffToNextRowInMinutes'] = df.diffToNextRowInMinutes.astype(int)

    results[tbl.name] = df

    pass


def getDataFromDateRange(dateRange: pandas.core.indexes.datetimes.DatetimeIndex,
                         tbl: pandas.core.indexes.datetimes.DatetimeIndex,
                         mydb: database.tradingDB) \
        -> pandas.DataFrame:
    tt1 = time.time()

    eng = mydb.DBEngine
    eng.dispose()

    ssn = mydb.Session()
    results = []


    for _DT in dateRange:
        __DT = _DT.to_pydatetime()
        close = None
        tm = None
        qsf = (ssn.query(tbl.c.close, tbl.c.datetime).order_by(tbl.c.datetime.desc()).filter(
            tbl.c.datetime <= __DT)).first()
        if qsf is not None:
            close, tm = qsf
        else:
            qsf = (
                ssn.query(tbl.c.close, tbl.c.datetime).order_by(tbl.c.datetime).filter(tbl.c.datetime > __DT)).first()
            if qsf is not None:
                close, tm = qsf
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
                ('mismatchInMinutes', tDiff/pd.Timedelta(1,'m')),
                ('close', close),
            )
        )
        results.append(ordrdDct)
        pass



    df = pd.DataFrame(results)
    df.sort_index(inplace=True)

    tt2 = time.time()
    ttdiff = tt2 - tt1

    nRowsTotal = nRowsTotal = ssn.query(tbl).count()

    ssn.close()

    print(f'Getting data from table: {tbl}: rowsTotal: {nRowsTotal}; {ttdiff}')


    return (df)


def createDateRange(startDate: pd.datetime) -> pandas.core.indexes.datetimes.DatetimeIndex:
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

    # create database class
    mydb = database.tradingDB(DBType=DBType, DBFileName=DBFileName)
    # mydb = database.tradingDB(DBType='mysql', DBFileName=DBFileName)

    # load existing database
    mydb.instantiateExistingTablesAndClasses()
    # set log level
    mydb._loggerSQLAlchemy.setLevel(logging.ERROR)
    ssn = mydb.Session()

    tbls = mydb.MarketDataInfoTableDataFrame['tableORM']
    tbl = tbls.iloc[0].__table__

    nowUTC = pd.to_datetime(pd.datetime.utcnow()).floor('1 min') - pd.Timedelta(minutes=1)
    startDate = nowUTC
    startDate = pd.to_datetime('2018-05-17 15:00:00').tz_localize('Europe/Berlin').tz_convert('UTC').tz_localize(None).round('1 min')
    print(startDate)
    dateRange = createDateRange(startDate)

    tt1 = time.time()
    dfs1 = []

    nTables = len(tbls)

    N = 3000000

    if 1:
        # multiprocessing Shared List
        tt1 = time.time()

        # Setup a list of processes that we want to run
        manager = mp.Manager()
        results = manager.dict()
        # print(type(results))

        processes = []
        for i in range(nTables):
            process = mp.Process(target=funcMultiProcessingSharedList,
                                 args=(i,
                                       dateRange,
                                       tbls.iloc[i].__table__,
                                       mydb,
                                       results))

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
        df = resultsDict['MarketData_CASH_EUR_AUD_IDEALPRO']
        df = resultsDict['MarketData_IND_N225_JPY_OSE.JPN']
        df = resultsDict['MarketData_IND_INDU_USD_CME']

        print(df)

        tt2 = time.time()
        ttdiff = tt2 - tt1
        print(f'retrieved data multi processing Shared Variable: {ttdiff}')


        pass



    ssn.commit()


    a = ssn.query(mydb.MarketDataInfoTable.earliestDateTime).all()
    print(a)

    ssn.close()





parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-c', '--configFile', help='Config File Name', required=True, type=str)
if __name__ == '__main__':
    args = parser.parse_args()
    sys.exit(runProg(args))
