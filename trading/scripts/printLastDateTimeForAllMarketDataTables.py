from trading import database
import argparse
import sys
from ib_insync import util, IB
from configparser import ConfigParser, ExtendedInterpolation
import os
import logging
import pandas as pd
import sqlalchemy
from collections import OrderedDict
import numpy as np
import time


def getInfoAboutTables(mydb:database.tradingDB=None):

    ssn = mydb.Session()

    resultList = []

    for idx in mydb.MarketDataInfoTableDataFrame.index:
        tableORM = mydb.MarketDataInfoTableDataFrame.at[idx, 'tableORM']
        tableName = tableORM.__tablename__
        lastDateTimeOnDisk = ssn.query(sqlalchemy.func.max(tableORM.datetime)).scalar()
        largestDiffDateTimeInMinutes = ssn.query(sqlalchemy.func.max(tableORM.diffToNextRowInMinutes)).scalar()
        # largestDiffDateTimeRow = ssn.query(tableORM).order_by(tableORM.diffToNextRowInMinutes.desc()).first()
        dateTimeOflargestDiffDateTimeInMinutesRecord = ssn.query(tableORM.datetime).order_by(tableORM.datetime).filter(tableORM.diffToNextRowInMinutes==largestDiffDateTimeInMinutes).first()
        if dateTimeOflargestDiffDateTimeInMinutesRecord is not None:
            dateTimeOflargestDiffDateTimeInMinutes = dateTimeOflargestDiffDateTimeInMinutesRecord[0]
            pass
        else:
            dateTimeOflargestDiffDateTimeInMinutes = pd.NaT
            pass


        nRows = ssn.query(tableORM).count()
        ordrdDct = OrderedDict(
            (
                ('tableName', tableName),
                ('nRows', nRows),
                ('lastDateTimeOnDisk', lastDateTimeOnDisk),
                ('largestDiffDateTimeInMinutes', largestDiffDateTimeInMinutes),
                ('dateTimeOflargestDiffDateTimeInMinutes', dateTimeOflargestDiffDateTimeInMinutes),
            )
        )
        resultList.append(ordrdDct)
        pass

    # t0 = time.time()
    # for idx in mydb.MarketDataInfoTableDataFrame.index:
    #     tableORM = mydb.MarketDataInfoTableDataFrame.at[idx, 'tableORM']
    #     tableName = tableORM.__tablename__
    #
    #     lastDateTimeOnDisk = ssn.query(sqlalchemy.func.max(tableORM.datetime)).scalar()
    #     # largestDiffDateTime = ssn.query(sqlalchemy.func.max(tableORM.diffToNextRowInMinutes)).scalar()
    #     # nRows = ssn.query(tableORM).count()
    #
    #     t1 = time.time()
    #     print(idx,t1-t0,tableName, lastDateTimeOnDisk)
    #     pass

    ssn.commit()
    ssn.close()

    df = pd.DataFrame(resultList)

    return df

def printState(df: pd.DataFrame=None):

    nowUTCExact = pd.to_datetime(pd.datetime.utcnow())
    nowUTC = nowUTCExact.floor('1 min')
    print(f'now: {nowUTCExact} {nowUTC}')


    df.loc[:, 'diffTimeToNow'] = (df.loc[:, 'lastDateTimeOnDisk'] - nowUTC) / pd.Timedelta(1, 'm')
    df.loc[:, 'diffTimeToNow'] = df.diffTimeToNow.replace(np.nan, -999999).astype(int)
    df.loc[:, 'largestDiffDateTimeInMinutes'] = df.largestDiffDateTimeInMinutes.replace(np.nan, -999999).astype(int)
    df = df.drop(columns=['lastDateTimeOnDisk'])

    print(df)
    pass


def runProg(args):
    """run program"""

    pd.set_option('display.width', 300)

    # log to a file
    util.logToFile(f'printLastDateTimeForAllMarketDataTables.log')

    # load the config file
    configFile = args.configFile
    config = ConfigParser(interpolation=ExtendedInterpolation(), defaults=os.environ)
    config.read(configFile)

    # load data from configFile
    DBType = config.get('DataBase', 'DBType')
    DBFileName = config.get('DataBase', 'DBFileName')


    # create database class
    mydb = database.tradingDB(DBType=DBType, DBFileName=DBFileName, sqlalchemyLoggingLevel=logging.ERROR)
    # mydb = database.tradingDB(DBType='mysql', DBFileName=DBFileName)

    # load existing database
    mydb.instantiateExistingTablesAndClasses()

    # set log level
    tradingLogger = logging.getLogger('trading')
    tradingLogger.setLevel(logging.ERROR)

    df = getInfoAboutTables(mydb=mydb)
    printState(df=df)

    pass




parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-c', '--configFile', help='Config File Name', required=True, type=str)
if __name__ == '__main__':
    args = parser.parse_args()
    sys.exit(runProg(args))
