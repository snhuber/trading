from trading import database, utils
import argparse
import sys
from configparser import ConfigParser, ExtendedInterpolation
import os
import logging
import pandas as pd
import sqlalchemy



def runProg(args):
    """run program"""

    pd.set_option('display.width', 300)

    # log to a file
    utils.logToFile(f'createTraingData.log')

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

    if 1:
        if (mydb.DBEngine.name == 'sqlite'):
            print ('starting to perform vacuum ...')
            stmt = sqlalchemy.text('vacuum')
            mydb.DBEngine.execute(stmt)
            print('... done')

    ssn.commit()
    ssn.close()



parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-c', '--configFile', help='Config File Name', required=True, type=str)
if __name__ == '__main__':
    args = parser.parse_args()
    sys.exit(runProg(args))
