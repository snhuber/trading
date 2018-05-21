import pandas as pd
import sys
import argparse
from configparser import ConfigParser, ExtendedInterpolation
import os
from trading import utils
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import operator
import dateutil
from ib_insync import objects, Contract
from ib_insync import util
from ib_insync.ibcontroller import IBC, Watchdog
from ib_insync import IB
from trading import database
from trading import containerClass
from trading import marketDataIB

def runProg(args):
    """run program"""

    util.patchAsyncio()

    # log to a file
    utils.logToFile(f'mainProg05.log',level=logging.INFO)
    # utils.logToConsole()

    apschedulerLogger = logging.getLogger('apscheduler')
    apschedulerLogger.setLevel(logging.INFO)
    tradingLogger = logging.getLogger('trading')
    tradingLogger.setLevel(logging.INFO)


    pd.set_option('display.width', 200)
    # local timezone
    tzlocal = dateutil.tz.tzlocal()

    # load the config file
    configFile = args.configFile
    config = ConfigParser(interpolation=ExtendedInterpolation(), defaults=os.environ)
    config.read(configFile)

    # load data from configFile
    host = config.get('InteractiveBrokers', 'host')
    port = config.getint('InteractiveBrokers', 'port')
    clientId = config.get('InteractiveBrokers', 'clientId')
    DBType = config.get('DataBase', 'DBType')
    DBFileName = config.get('DataBase', 'DBFileName')


    # for production mode: watchdog
    watchdogApp = None
    if 1:
        # start watchdog
        # ibc = IBC(963, gateway=True, tradingMode='paper',ibcIni='/home/bn/IBController/configPaper.ini')
        ibcIni = config.get('InteractiveBrokers', 'ibcIni')
        tradingMode = config.get('InteractiveBrokers', 'tradingMode')
        ibc = IBC(970, gateway=True, tradingMode=tradingMode, ibcIni=ibcIni)
        ib = IB()
        watchdogApp = Watchdog(ibc, ib=ib, appStartupTime=15, host=host, port=port, clientId=clientId)
        watchdogApp.start()

    if 0:
        # faster way for now
        ib = IB()
        ib.connect(host=host, port=port, clientId=clientId)
        class myWatchdog(object):
            def __init__(self):
                self.ib = ib
                pass
            pass
        watchdogApp = myWatchdog()
        pass

    pass


    # create database class
    mydb = database.tradingDB(DBType=DBType, DBFileName=DBFileName)
    # load existing database
    mydb.instantiateExistingTablesAndClasses(ib=ib)
    # set log level of sqlalchemy
    mydb._loggerSQLAlchemy.setLevel(logging.WARNING)



    # set the list of qualified contracts
    # get a list of qualified contracts that correspond to each row in mydb.MarketDataInfoTableDataFrame
    __qcs__ = list(mydb.MarketDataInfoTableDataFrame.qualifiedContract.values)
    # qcs = __qcs__[0:2]
    # qcs = operator.itemgetter(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12)(__qcs__)
    # qcs = operator.itemgetter(0, 13, 1, 11, 7)(__qcs__)
    # qcs = operator.itemgetter(0, 12, 13, 2, 10, 3)(__qcs__)
    # qcs = operator.itemgetter(0, 1, 10)(__qcs__)
    qcs = __qcs__
    if isinstance(qcs,Contract):
        qcs = [qcs]
        pass
    if isinstance(qcs,tuple):
        qcs = list(qcs)
        pass


    if None in qcs:
        print('problem with connecting to IB. Now exiting')
        sys.exit()

    # define the container class
    cc = containerClass.ContainerClass()

    # add config
    cc.config = config

    # set watchdogapp
    cc.watchdogApp = watchdogApp

    # set database
    cc.mydb = mydb
    cc.qcs = qcs

    # register callbacks with ib
    cc.registerCallbacks()

    # define a scheduler
    useScheduler = True
    if useScheduler:
        scheduler = AsyncIOScheduler()
        cc.scheduler = scheduler
        cc.scheduler.start()
        pass


    # general setting for the density of data for historical requests
    configIB = config['InteractiveBrokers']
    barSizePandasTimeDelta = pd.Timedelta(**eval(configIB.get('densityTimeDelta', '{"minutes":1}')))


    ##############################################################
    # request recent historical bars
    ##############################################################
    # settings to request the bars (for the qcs that are a member of cc)
    # the 'short' settings are the default ones to be applied during trading hours

    durationPandasTimeDelta = pd.Timedelta(**eval(configIB.get('durationTimeDeltaRecentHistoricalDataShort', '{"hours":1}')))
    timeOutTime = configIB.getint('timeOutTimeShortRequests', 10)

    recentHistoricalDataSettingsShort = {
        'durationPandasTimeDelta': durationPandasTimeDelta,
        'barSizePandasTimeDelta': barSizePandasTimeDelta,
        'timeOutTime': timeOutTime,
        'maximumBarsLengthFactor': 2,
    }

    # settings for recent historical bars to be requested during off-trading hours.
    # due to performance reasons, during the trading hours we want to request
    # very short bars; during off-trading hours, we can request longer bars
    # which fill up possible gaps left by the shorter setting.

    durationPandasTimeDelta = pd.Timedelta(**eval(configIB.get('durationTimeDeltaRecentHistoricalDataLong', '{"days":1}')))
    timeOutTime = configIB.getint('timeOutTimeMediumRequests', 60)

    recentHistoricalDataSettingsLong = {
        'durationPandasTimeDelta': durationPandasTimeDelta,
        'barSizePandasTimeDelta': barSizePandasTimeDelta,
        'timeOutTime': timeOutTime,
        'maximumBarsLengthFactor': 2,
    }


    # set the current settings in the containerClass
    a = (f'Now updating the settings for the request of recent historical bars')
    logging.info(a)
    print(a)
    # set the settings
    cc.recentHistoricalDataSettings = recentHistoricalDataSettingsShort

    # request the bars
    a = (f'Now requesting initial recent historical bars')
    logging.info(a)
    print(a)
    orderedDictOfBars = cc.requestRecentHistoricalOrderedDictOfBars()
    cc.orderedDictOfBars = orderedDictOfBars


    for (tableName, bars) in cc.orderedDictOfBars.items():
        nBars = None
        if isinstance(bars,objects.BarDataList):
            nBars = len(bars)
        print(tableName,type(bars),nBars)
    ##############################################################


    ##############################################################
    # request historical bars
    ##############################################################

    # add the job requesting historical data to the scheduler
    # this setting starts at the earliestDateTime given by IB

    earliestPandasTimeDelta = pd.Timedelta(**eval(configIB.get('earliestTimeDeltaHistoricalData', '{"weeks":4}')))
    durationPandasTimeDelta = pd.Timedelta(**eval(configIB.get('durationTimeDeltaHistoricalData', '{"days":1}')))
    timeOutTime = configIB.getint('timeOutTimeLongRequests', 1800)
    # timeOutTime = configIB.getint('timeOutTimeMediumRequests', 60)

    if earliestPandasTimeDelta.total_seconds() < 0:
        earliestDateTimeUTCNaive = None
    else:
        earliestDateTimeUTCNaive = pd.to_datetime(pd.datetime.utcnow()).floor('1 min') - earliestPandasTimeDelta
        pass

    historicalDataGetterSettings={
        'ib': cc.watchdogApp.ib,
        'mydb': cc.mydb,
        'qcs': cc.qcs,
        'durationPandasTimeDelta': durationPandasTimeDelta,
        'barSizePandasTimeDelta': barSizePandasTimeDelta,
        'earliestDateTime': earliestDateTimeUTCNaive,
        'timeOutTime': timeOutTime,
        'jitterSpanFraction': 0.02,
    }

    jobSettings = {
        'job': marketDataIB.asyncioJobGetHistoricalData,
        'args': [],
        'kwargs': historicalDataGetterSettings,
        'jobRootName': None,
        'minute': '*',
        'second': '*/5',
        'coalesce': True,
        'misfire_grace_time': 30,
        'trigger': 'cron',
        'max_instances': 1,
    }

    if useScheduler:
        cc.addJobToScheduler(jobSettings=jobSettings)
        pass
    ##############################################################


    ##############################################################
    # change the request of recent historical bars to a longer setting during off-trading hours
    ##############################################################
    # add a scheduled job that switches from the short to the long settings
    jobSettings = {
        'job': cc.schedulerJobSwitchRequestForRecentHistoricalDataFromOneSettingToOther,
        'args': [],
        'kwargs': recentHistoricalDataSettingsLong,
        'jobRootName': 'schedulerJobSwitchRequestForRecentHistoricalDataFromShortToLong',
        'hour': '22',
        # 'hour': '*',
        'minute': '02',
        # 'minute': '*',
        'second': '00',
        # 'second': '5-59/10',
        'coalesce': True,
        'misfire_grace_time': 30,
        'trigger': 'cron',
        'max_instances': 1,
    }

    if useScheduler:
        cc.addJobToScheduler(jobSettings=jobSettings)

    # add a scheduled job that switches from the long to the short settings
    jobSettings = {
        'job': cc.schedulerJobSwitchRequestForRecentHistoricalDataFromOneSettingToOther,
        'args': [],
        'kwargs': recentHistoricalDataSettingsShort,
        'jobRootName': 'schedulerJobSwitchRequestForRecentHistoricalDataFromLongToShort',
        'hour': '04',
        # 'hour': '*',
        'minute': '02',
        # 'minute': '*',
        'second': '00',
        # 'second': '*/10',
        'coalesce': True,
        'misfire_grace_time': 30,
        'trigger': 'cron',
        'max_instances': 1,
    }

    if useScheduler:
        cc.addJobToScheduler(jobSettings=jobSettings)

    ##############################################################

    if 1:
        if useScheduler:
            print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
            # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
            try:
                asyncio.get_event_loop().run_forever()
            except (KeyboardInterrupt, SystemExit):
                pass
            pass
        else:
            util.allowCtrlC()
            ib.run()
            pass
        pass

    ib.disconnect()



parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-c', '--configFile', help='Config File Name', required=True, type=str)
if __name__ == '__main__':
    args = parser.parse_args()
    sys.exit(runProg(args))
