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

    if 1:
        # loop over entire table and repair
        nowUTCExact  = pd.to_datetime(pd.datetime.utcnow())
        nowUTC  = nowUTCExact.floor('1 min')
        print(f'now: {nowUTCExact} {nowUTC}')
        resultList = []
        for idx in mydb.MarketDataInfoTableDataFrame.index:
            tableORM = mydb.MarketDataInfoTableDataFrame.at[idx,'tableORM']
            tableName = tableORM.__tablename__
            lastDateTimeOnDisk = ssn.query(sqlalchemy.func.max(tableORM.datetime)).scalar()
            largestDiffDateTime = ssn.query(sqlalchemy.func.max(tableORM.diffToNextRowInMinutes)).scalar()
            nRows = ssn.query(tableORM).count()
            diffTime = None
            if not pd.isnull(lastDateTimeOnDisk):
                diffTime = (lastDateTimeOnDisk - nowUTC)
                diffTime = diffTime / pd.Timedelta(1,'m')
                pass
            ordrdDct = OrderedDict(
                (
                    ('tableName',tableName),
                    ('nRows', nRows),
                    ('diffTimeToNow', diffTime),
                    ('largestDiffDateTime', largestDiffDateTime),
                )
            )
            resultList.append(ordrdDct)
            pass
        pass

    ssn.commit()

    ssn.close()
    df = pd.DataFrame(resultList)
    df.loc[:,'diffTimeToNow'] = df.diffTimeToNow.replace(np.nan,-999999).astype(int)
    df.loc[:,'largestDiffDateTime'] = df.largestDiffDateTime.replace(np.nan,-999999).astype(int)
    print(df)
    print(df.dtypes)


parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-c', '--configFile', help='Config File Name', required=True, type=str)
if __name__ == '__main__':
    args = parser.parse_args()
    sys.exit(runProg(args))
