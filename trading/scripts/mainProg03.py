import pandas as pd
import sys
import argparse
from trading import database
from ib_insync import util
from configparser import ConfigParser, ExtendedInterpolation
import os
from trading import utils
from ib_insync.ibcontroller import IBC, Watchdog
from ib_insync import IB
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
from trading import containerClass
import operator
import dateutil
from ib_insync import objects, Contract

def runProg(args):
    """run program"""

    util.patchAsyncio()

    # log to a file
    utils.logToFile(f'mainProg03.log',level=logging.INFO)
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
    qcs = operator.itemgetter(0, 13)(__qcs__)
    # qcs = __qcs__
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


    ##############################################################
    # request recent historical bars
    ##############################################################
    # settings to request the bars (for the qcs that are a member of cc)
    # the 'short' settings are the default ones to be applied during trading hours
    configIB = config['InteractiveBrokers']

    recentHistoricalDataSettingsShort = {
        'durationPandasTimeDelta': pd.Timedelta(hours=1),
        'barSizePandasTimeDelta': pd.Timedelta(minutes=1),
        'timeOutTime': configIB.getint('timeOutTimeShortRequests', 10),
        'maximumBarsLengthFactor': 2,
    }

    # settings for recent historical bars to be requested during off-trading hours.
    # due to performance reasons, during the trading hours we want to request
    # very short bars; during off-trading hours, we can request longer bars
    # which fill up possible gaps left by the shorter setting.
    recentHistoricalDataSettingsLong = {
        'durationPandasTimeDelta': pd.Timedelta(days=1),
        'barSizePandasTimeDelta': pd.Timedelta(minutes=1),
        'timeOutTime': configIB.getint('timeOutTimeMediumRequests', 60),
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
        if isinstance(bars,objects.BarDataList): nBars = len(bars)
        print(tableName,type(bars),nBars)
    ##############################################################


    ##############################################################
    # request historical bars
    ##############################################################

    # add the job requesting historical data to the scheduler
    # this setting starts at the earliestDateTime given by IB
    earliestDateTimeUTCNaive = None
    # this setting starts at a short period before now
    # earliestDateTimeUTCNaive= pd.to_datetime(pd.datetime.utcnow()) - pd.Timedelta(weeks=6)
    reqHistoricalDataSettings = {
        'durationPandasTimeDelta': pd.Timedelta(days=1),
        'barSizePandasTimeDelta': pd.Timedelta(minutes=1),
        'earliestDateTimeUTCNaive': earliestDateTimeUTCNaive,
        'timeOutTimeHistoricalBars': 1800,
        'jitterSpanFraction': 0.02,
        'minute': '*',
        'second': '0',
        'coalesce': True,
        'misfire_grace_time': 30
    }
    cc.reqHistoricalDataSettings = reqHistoricalDataSettings
    if useScheduler:
        cc.addAsyncioJobGetHistoricalDataToScheduler(i=None)
        pass
    ##############################################################

    # # # add a scheduled job that switches from the short to the long settings
    # # strTitle = 'switch_request_for_recent_historical_data_from_short_to_long_settings'
    # # scheduler.add_job(cc.switchRequestForRecentHistoricalDataFromOneSettingToOther,
    # #                   trigger='cron',
    # #                   hour='23',
    # #                   minute='00',
    # #                   second='00',
    # #                   name=strTitle,
    # #                   id=strTitle,
    # #                   coalesce=True,
    # #                   max_instances=1000,
    # #                   misfire_grace_time=180,
    # #                   args=[recentHistoricalDataSettingsLong])
    # #
    # # # add a scheduled job that switches from the long to the short settings
    # # strTitle = 'switch_request_for_recent_historical_data_from_long to short_settings'
    # # scheduler.add_job(cc.switchRequestForRecentHistoricalDataFromOneSettingToOther,
    # #                   trigger='cron',
    # #                   hour='05',
    # #                   minute='00',
    # #                   second='00',
    # #                   name=strTitle,
    # #                   id=strTitle,
    # #                   coalesce=True,
    # #                   max_instances=1000,
    # #                   misfire_grace_time=180,
    # #                   args=[recentHistoricalDataSettingsShort])


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
