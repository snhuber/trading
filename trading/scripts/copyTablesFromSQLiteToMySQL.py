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
import dateutil
import threading




def runProg(args):
    """run program"""

    pd.set_option('display.width', 200)

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
    clientId = config.get('InteractiveBrokers', 'clientId')

    # override configFile if clientId is given on the command line
    if args.clientId is not None:
        clientId = args.clientId

    if 1:
        # faster way for now
        ib = IB()
        ib.connect(host=host, port=port, clientId=clientId)
        pass

    pass

    # create database class
    mydbSQLite = database.tradingDB(DBType=DBType, DBFileName=DBFileName)
    # load existing database
    mydbSQLite.instantiateExistingTablesAndClasses(ib=ib)
    # set log level
    mydbSQLite._loggerSQLAlchemy.setLevel(logging.ERROR)

    # create database class
    mydbMySQL = database.tradingDB(DBType='mysql', DBFileName=DBFileName)
    # load existing database
    mydbMySQL.instantiateExistingTablesAndClasses(ib=ib)
    # set log level
    mydbMySQL._loggerSQLAlchemy.setLevel(logging.ERROR)


    tblsSQLiteORM = mydbSQLite.MarketDataInfoTableDataFrame['tableORM']
    tblsMySQLORM = mydbMySQL.MarketDataInfoTableDataFrame['tableORM']

    nTables = len(tblsMySQLORM)

    ssnSQLite = mydbSQLite.Session()
    ssnMySQL = mydbMySQL.Session()

    for i in range(0,nTables):
        tt1 = time.time()

        tblSQLiteSchema = tblsSQLiteORM.iloc[i].__table__
        tblMySQLSchema = tblsMySQLORM.iloc[i].__table__


        print(tblSQLiteSchema.name)
        print(tblMySQLSchema)

        qs = ssnSQLite.query(tblSQLiteSchema)
        df_read = pd.read_sql(qs.statement, qs.session.bind)

        mydbMySQL.upsertDataFrame(df_read,tblMySQLSchema)
        ssnMySQL.commit()

        tt2 = time.time()
        ttdiff = tt2 - tt1
        print(f'bla {tblSQLiteSchema.name}: {ttdiff}')
        pass

    ssnSQLite.close()
    ssnMySQL.close()

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-c', '--configFile', help='Config File Name', required=True, type=str)
parser.add_argument('--clientId', help='clientId to connect to TWS/gateway', required=False, default=None, type=int)
if __name__ == '__main__':
    args = parser.parse_args()
    sys.exit(runProg(args))
