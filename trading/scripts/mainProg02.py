import pandas as pd
import sys
import argparse
from trading import database
from ib_insync import util
from ib_insync import objects
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
    utils.logToFile(f'mainProg02.log')
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
        myWatchdogApp = myWatchdog.myWatchdog(ibc, ib=ib, appStartupTime=15, port=4002)
        myWatchdogApp.start()
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
        pass

    # define scheduled tasks to get recent historical data
    if 0:
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


    # define some callbacks
    if 1:
        def myBarUpdateCallback(bars, hasNewBar):
            localSymbol = bars.contract.localSymbol
            secType = bars.contract.secType
            b0 = bars[0]
            if isinstance(b0, objects.RealTimeBar):
                dateTimeAttributeName = 'time'
                barType = 'RealTimeBar'
            else:
                dateTimeAttributeName = 'date'
                barType = 'BarData'
                pass
            dt0 = pd.to_datetime(getattr(b0, dateTimeAttributeName)).tz_localize(None)
            bm1 = bars[-1]
            dtm1 = pd.to_datetime(getattr(bm1, dateTimeAttributeName)).tz_localize(None)
            nowUTC = pd.to_datetime(pd.datetime.utcnow()).tz_localize(None)
            diffDateTIme = (nowUTC - dtm1) / pd.Timedelta('1 sec')
            if (hasNewBar or True):
                print(
                    f'local Symbol: {localSymbol}, hasNewBar: {hasNewBar}; barType: {barType}, nBars: {len(bars)}, diffDateTime: {diffDateTIme}, close: {bm1.close}')


        def myErrorCallback(reqId, errorCode, errorString, contract):
            print("myErrorCallback", reqId, errorCode, errorString, contract)

        def myConnectedCallback():
            print('connected')

            barss = requestHistoricalBars(qcs)


            print('connected 2')

            ib.barUpdateEvent.clear()
            ib.barUpdateEvent += myBarUpdateCallback

            print('connected 3')

            pass

        def myDisconnectedCallback():
            print('disconnected')

        def myTimeoutCallback(timeout):
            print(f'timeout {timeout}')
        pass

        def mySoftTimeoutCallback(watchdogApp):
            print (f'soft time out {watchdogApp}')

        def myHardTimeoutCallback(watchdogApp):
            print (f'hard time out {watchdogApp}')
            logging.info(f'hard timeout')
            watchdogApp.flush()


    # request historical bars
    if 1:
        # list of qualified contracts
        qcs = mydb.MarketDataInfoTableDataFrame.qualifiedContract

        # function to request historical bars (makes use of qcs and ib)
        def requestHistoricalBars(qcs):
            # request historical bars
            barss = []
            for qc in qcs:
                whatToShow = 'TRADES' if qc.secType == 'IND' else 'MIDPOINT'
                bars = ib.reqHistoricalData(
                    qc,
                    endDateTime='',
                    durationStr='1 D',
                    barSizeSetting='1 min',
                    whatToShow=whatToShow,
                    useRTH=False,
                    formatDate=2,
                    keepUpToDate=True)
                barss.append(bars)
                pass
            return barss

        # request the bars
        barss = requestHistoricalBars(qcs)
        pass

    # register callbacks with ib
    if 1:
        ib.connectedEvent.clear()
        ib.connectedEvent += myConnectedCallback

        ib.disconnectedEvent.clear
        ib.disconnectedEvent = myDisconnectedCallback

        ib.errorEvent.clear()
        ib.errorEvent += myErrorCallback

        ib.timeoutEvent.clear()
        ib.timeoutEvent += myTimeoutCallback

        ib.barUpdateEvent.clear()
        ib.barUpdateEvent += myBarUpdateCallback

        myWatchdogApp.softTimeoutEvent.clear()
        myWatchdogApp.softTimeoutEvent += mySoftTimeoutCallback

        myWatchdogApp.hardTimeoutEvent.clear()
        myWatchdogApp.hardTimeoutEvent += myHardTimeoutCallback

    # start scheduler
    if 1:
        schedules = []
        # check if the watchdoc app exists
        try:
            myWatchdogApp
        except NameError:
            app_exists = False
        else:
            app_exists = True
        if app_exists:
            myWatchdogApp.schedulerList = [[scheduler,schedules]]
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
