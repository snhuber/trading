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
    utils.logToFile(f'mainProg01.log')
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
        ib = IB()
        myWatchdogapp = myWatchdog.myWatchdog(ibc, ib=ib, appStartupTime=15, port=4002)
        myWatchdogapp.start()
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

    # get a list of qualified contracts that correspond to each row in mydb.MarketDataInfoTableDataFrame
    if 1:
        qcs = list(mydb.MarketDataInfoTableDataFrame.qualifiedContract.values)

    scheduler = AsyncIOScheduler()

    # define scheduled task to get historical data
    if 1:
        job = marketDataIB.asyncioJobGetHistoricalData
        i = 0
        job_id = f'id_getHistoricalData_{i}'
        job_name = job_id

        # duration time delta
        durationPandasTimeDelta = pd.Timedelta(weeks=4)
        # bar size delta
        barSizePandasTimeDelta = pd.Timedelta(minutes=1)

        # startDateTime
        earliestDateTimeUTC = pd.to_datetime('2018-05-05').tz_localize('UTC')

        # additionalArgs will overwrite the defaults
        additionalArgs = {
            'durationPandasTimeDelta': durationPandasTimeDelta,
            'barSizezePandasTimeDelta': barSizePandasTimeDelta,
            'earliestDateTimeUTC': earliestDateTimeUTC,
            'timeOutTimeHistoricalBars': 1800,
        }
        # the default is earliestDateTime. If we do not want
        # to overwrite this, we have to delete the item (or not specify it above)
        del additionalArgs['earliestDateTimeUTC']

        # _qcs = qcs[6:]
        _qcs = qcs

        scheduler.add_job(job, trigger='cron',
                          minute='*',
                          # second='*/5',
                          name=job_name,
                          id=job_id,
                          coalesce=True,
                          max_instances=1,
                          misfire_grace_time=30,
                          args=[ib, mydb, _qcs, additionalArgs])

    # define scheduled tasks to get recent historical data
    if 1:
        def my_listener_executed(event):
            a = (f'executed: {event.code}, {event.job_id}, {event.jobstore}')
            logging.info(a)
            print(a)
            pass

        def my_listener_submitted(event):
            a = (f'submitted: {event.code}, {event.job_id}, {event.jobstore}')
            logging.info(a)
            print(a)
            pass

        def my_listener_missed(event):
            a = (f'missed: {event.code}, {event.job_id}, {event.jobstore}')
            logging.info(a)
            print(a)
            pass

        def my_listener_max_instances(event):
            a = (f'max_instances: {event.code}, {event.job_id}, {event.jobstore}')
            logging.info(a)
            print(a)
            pass

        def my_listener_else(event):
            if event.exception:
                print(f'else: The job {event.job_id} crashed :(')
            else:
                print(f'else: The job {event.job_id} worked :)')

        # scheduler.add_listener(my_listener_executed, EVENT_JOB_EXECUTED)
        # scheduler.add_listener(my_listener_submitted, EVENT_JOB_SUBMITTED)
        # scheduler.add_listener(my_listener_missed, EVENT_JOB_MISSED)
        # scheduler.add_listener(my_listener_max_instances, EVENT_JOB_MAX_INSTANCES)

        i = 0
        for idx, row in mydb.MarketDataInfoTableDataFrame.iterrows():
            qc = row.qualifiedContract

            # duration time delta
            durationMaximumPandasTimeDelta = pd.Timedelta(weeks=4)
            # bar size delta
            barSizePandasTimeDelta = pd.Timedelta(minutes=1)

            # additionalArgs will overwrite the defaults
            additionalArgs = {
                'durationMaximumPandasTimeDelta': durationMaximumPandasTimeDelta,
                'barSizezePandasTimeDelta': barSizePandasTimeDelta,
                'timeOutTimeHistoricalBars': 1800,
            }

            job = marketDataIB.createAsyncioJobGetRecentData()
            job_bla = job
            job_id = f'id_getRecentHistoricalData_{i:02}'
            job_name = job_id

            # if qc.localSymbol not in ['SPX', 'DJI']:
            # if qc.localSymbol in ['EUR.JPY']:
            if True:
                # print(i, qc)
                scheduler.add_job(job_bla, trigger='cron',
                                  minute='*',
                                  second='5-20',
                                  # second='10',
                                  # second='*',
                                  # second='*/2',
                                  name=job_name,
                                  id=job_id,
                                  coalesce=True,
                                  max_instances=1,
                                  misfire_grace_time=30,
                                  args=[ib, mydb, qc, additionalArgs])
                i = i + 1
                pass
            pass
        pass

    # start scheduler
    if 1:
        schedules = []
        # check if the watchdoc app exists
        try:
            myWatchdogapp
        except NameError:
            app_exists = False
        else:
            app_exists = True
        if app_exists:
            myWatchdogapp.schedulerList = [[scheduler,schedules]]
            pass
        util.allowCtrlC()
        scheduler.start()
        print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
        # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
        try:
            asyncio.get_event_loop().run_forever()
        except (KeyboardInterrupt, SystemExit):
            pass

    ib.disconnect()


parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-c', '--configFile', help='Config File Name', required=True, type=str)
if __name__ == '__main__':
    args = parser.parse_args()
    sys.exit(runProg(args))
