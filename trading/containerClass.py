import pandas as pd
import sys
from trading import database
from ib_insync import objects
from trading import utils
from ib_insync import Contract
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from  apscheduler.job import Job as apschedulerJob
from trading import marketDataIB
from collections import OrderedDict
import asyncio
from ib_insync.contract import Forex
import datetime



class ContainerClass(object):
    """class that combines several objects necessary for the trading app"""

    __log = logging.getLogger(__name__)

    def __init__(self):
        """constructor of the containerClass"""

        self.config = None
        self.watchdogApp = None
        self.qcs = None
        self.scheduler = None
        self.schedulerJobList = []
        self.mydb = None
        self.orderedDictOfBars = OrderedDict()
        self.getHistoricalDataCounter = 0
        self._errorThatTriggersWatchdogRestart = False
        # now comes a little hack. When the system is disconnecting (because gateway crashed, or some other reconnecting thing),
        # some scheduled tasks might still be running. Or at least, the scheduler thinks they are.
        # We remove these tasks, but still the scheduler thinks these tasks are running, so checking if they are running in
        # the scheduler and giving them a new ID has no effect.
        # a solution is to keep track of the number of reconnections.
        # so each time the system is coming up again, scheduled tasks will get an incremented counter.
        # this counter is then added to the counter that is appended to the rootName of a function for the scheduler
        self.reconnectionCounter = 0


        self.recentHistoricalDataSettings = {
            'durationPandasTimeDelta': pd.Timedelta(days=1),
            'barSizePandasTimeDelta': pd.Timedelta(minutes=1),
            'timeOutTime': 10,
            'maximumBarsLengthFactor': 2,
        }


        pass


    def myCreateFileJob(self, strTitle: str = "A_Title") -> None:
        """a method that simply creates files on disk

        This method was created to be called from a scheduler for test reasons"""
        dateTimeString = pd.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        FN = f'{strTitle}-{dateTimeString}'
        file = open(FN, 'w')
        file.close()
        pass

    def requestRecentHistoricalBars(self, qc: Contract) -> objects.BarDataList:
        """a method that requests historical bars for one qualified contract

        A method that requests recent historical bars for one qualified contract.
        The settings to be used for the request are given by the property "recentHistoricalDataSettings" in the containerClass
        These settings can (and should) be dynamically updated

        """

        a = (f'requesting recent historical data for {qc.localSymbol}. settings: {self.recentHistoricalDataSettings}')
        self.__log.info(a)
        # print(a)

        ib = self.watchdogApp.ib

        # get the settings from the ones stored currently in the containerClass; set some defaults if those do
        # not have the required attributes
        d = self.recentHistoricalDataSettings
        durationPandasTimeDelta = d.get('durationPandasTimeDelta', pd.Timedelta(days=1))
        barSizePandasTimeDelta = d.get('barSizePandasTimeDelta', pd.Timedelta(minutes=1))
        timeOutTime = d.get('timeOutTime', 10)

        # a = (f'duration: {durationPandasTimeDelta}')
        # self.__log.info(a)
        # print(a)
        #
        # a = (f'barSize: {barSizePandasTimeDelta}')
        # self.__log.info(a)
        # print(a)

        dTD = utils.CTimeDelta(durationPandasTimeDelta)
        bTD = utils.CTimeDelta(barSizePandasTimeDelta)

        # a = (f'duration: {dTD.IB_Duration_String}')
        # self.__log.info(a)
        # a = (f'barSize: {bTD.IB_Bar_Size_String}')
        # self.__log.info(a)

        whatToShow = utils.dictSecTypeToWhatToShow.get(qc.secType, None)

        [bars, timeOutOccurred] = marketDataIB.getHistoricalDataBars(qc=qc,
                                                                     ib=self.watchdogApp.ib,
                                                                     endDateTime='',
                                                                     durationStr=dTD.IB_Duration_String,
                                                                     barSizeSetting=bTD.IB_Bar_Size_String,
                                                                     whatToShow=whatToShow,
                                                                     formatDate=2,
                                                                     keepUpToDate=True,
                                                                     useRTH=False,
                                                                     timeOutTime=timeOutTime)


        
        
        # bars is None, an empty list, or a filled list.
        # if bars is None, a timeout Ocurred for that contract
        # if bars is empty, no historical data is available
        # if it is non-empty, everything is ok

        return bars

    def requestRecentHistoricalOrderedDictOfBars(self) -> OrderedDict:
        """a method that requests all historical bars for all the contracts in qc.

        The method returns orderedDictOfBars:
          each item in orderedDictOfBars is one BarDataList that contains the historical bars for one contract


        The settings are given by the property recentHistoricalDataSettings
        This method is designed to be called on the start of the app and whenever
        there is a reconnection to IB"""

        a = (f'requesting recent historical data for all qcs. settings: {self.recentHistoricalDataSettings}')
        self.__log.info(a)

        orderedDictOfBars = self.orderedDictOfBars
        qcs = self.qcs
        ib = self.watchdogApp.ib
        mydb = self.mydb
        n = len(qcs)

        for i,qc in enumerate(qcs):
            tableName = mydb.getValueFromMarketDataInfoTable(whereColumn='conId', whereValue=qc.conId,
                                                             getColumn='tableName')
            # a = (f'{i+1}/{n}: requesting recent historical data for contract: {qc.localSymbol}')
            # self.__log.info(a)
            # print(a)

            bars = self.requestRecentHistoricalBars(qc)
            orderedDictOfBars[tableName] = bars
            pass
        # note: the orderedDictOfBars attribute is continually updated
        # therefore, it is growing over time
        # the up-to-date orderedDictOfBars can therefore always be accessed using the orderedDictOfBars attribute

        return orderedDictOfBars

    def cancelAllRecentHistoricalBars(self):
        """method to cancel all the requests for the historical bars"""

        listOfTableNames = [tableName for (tableName,bars) in self.orderedDictOfBars.items()]
        a = (f'Canceling the historical bars for: {listOfTableNames}')
        self.__log.info(a)
        for (tableName, bars) in self.orderedDictOfBars.items():
            if self.watchdogApp.ib.isConnected():
                try:
                    self.watchdogApp.ib.cancelHistoricalData(bars)
                except:
                    pass
                pass
            pass
        pass


    def myBarUpdateCallback(self, bars: objects.BarDataList, hasNewBar: bool, forceUpdate: bool=False)-> None:
        """method that defines what is to be done with the bars that are received using the
        method "requestRecentHistoricalBars" """

        # a = (f'bar update callback. {bars[0].date}; {bars[-1].date} {len(bars)}')
        # self.__log.info(a)
        # print(a)


        # get the current settings for the recent historical bars
        d = self.recentHistoricalDataSettings
        durationPandasTimeDelta = d.get('durationPandasTimeDelta', pd.Timedelta(days=1))
        barSizePandasTimeDelta = d.get('barSizePandasTimeDelta', pd.Timedelta(
            minutes=1))  # this is the setting that specifies the frequency of the requested data
        maximumBarsLengthFactor = d.get('maximumBarsLengthFactor',
                                        2)  # cancel the request and restart a new request when the bars cover a stretch of time that is longer than durationPandasTimeDelta * maximumBarsLengthFactor

        # main part
        mydb = self.mydb
        assert (isinstance(mydb, database.tradingDB))
        qc = bars.contract
        tableName = mydb.getValueFromMarketDataInfoTable(whereColumn='conId', whereValue=qc.conId,
                                                         getColumn='tableName')
        tableORM = mydb.getTableORMByTablename(tableName=tableName)
        tableSchema = tableORM.__table__
        localSymbol = qc.localSymbol
        firstBar = bars[0]
        lastBar = bars[-1]
        # the type of the BarDataList can be either RealTimeBar or BarData.
        # these have a different name for the attribute that codes the datetime
        if isinstance(firstBar, objects.RealTimeBar):
            dateTimeAttributeName = 'time'
            barType = 'RealTimeBar'
        else:
            dateTimeAttributeName = 'date'
            barType = 'BarData'
            pass

        # datetime of first and last bar
        firstBarDateTimeUTCNaive = pd.to_datetime(getattr(firstBar, dateTimeAttributeName)).tz_localize(
            None)  # should be UTC because we set formatDate=2 when requesting data
        lastBarDateTimeUTCNaive = pd.to_datetime(getattr(lastBar, dateTimeAttributeName)).tz_localize(
            None)  # should be UTC because we set formatDate=2 when requesting data

        # number of bars
        nBars = len(bars)

        # difference between UTC and the DateTime of the last bar in seconds
        nowUTCNaive = pd.to_datetime(pd.datetime.utcnow())
        diffDateTime = (nowUTCNaive - lastBarDateTimeUTCNaive) / pd.Timedelta('1 sec')

        # some datetimes that help deciding if we have to persist the bars
        nowUTCNaiveFloor = nowUTCNaive.floor(barSizePandasTimeDelta)
        lastBarDateTimeUTCNaiveFloor = lastBarDateTimeUTCNaive.floor(barSizePandasTimeDelta)
        lastDiskDateTimeUTCNaive = marketDataIB.getLastDateTimeOnDiskUTCNaive(mydb=mydb,
                                                                         tableSchema=tableSchema)
        if not pd.isnull(lastDiskDateTimeUTCNaive):
            lastDiskDateTimeUTCNaiveFloor = lastDiskDateTimeUTCNaive.floor(barSizePandasTimeDelta)
            pass
        else:
            lastDiskDateTimeUTCNaiveFloor = (firstBarDateTimeUTCNaive - barSizePandasTimeDelta).floor(barSizePandasTimeDelta)
            pass

        firstDateTimeToTakeUTCNaive = lastBarDateTimeUTCNaiveFloor - durationPandasTimeDelta

        # a = (f'nowUTCNaiveFloor: {nowUTCNaiveFloor}; firstBarDateTimeUTCNaive: {firstBarDateTimeUTCNaive}; firstBarDateTimeUTCNaive: {firstBarDateTimeUTCNaive}; lastBarDateTimeUTCNaiveFloor {lastBarDateTimeUTCNaiveFloor}')
        # self.__log.info(a)
        # print(a)

        # persist bars on disk if new information is available
        if (nowUTCNaiveFloor > lastDiskDateTimeUTCNaiveFloor + barSizePandasTimeDelta) or forceUpdate:
            # the current time is larger than the last time on disk + barSizePandasTimeDelta
            # that means we are lagging behind.
            # persist the bars on disk if the lastBarDateTimeFloor == nowUTCNaiveFloor
            # only if this last condition is met, the bars for the last unit are complete
            if (lastBarDateTimeUTCNaiveFloor == nowUTCNaiveFloor) or forceUpdate:
                # we are now in the case where
                #   * the current bar datetime is equal to nowUTCNaive --> the previous bar is complete
                # we persist the bars (except the last value which is incomplete) on disk
                if forceUpdate and (lastBarDateTimeUTCNaiveFloor < nowUTCNaiveFloor):
                    bars2=bars
                else:
                    bars2=bars[:-1]
                    pass
                # we only take the bars that have times larger than the first value according to the durationString
                # used for requesting the historical bars
                bars3 = []
                for bar in bars2:
                    barDateTimeUTCNaive = pd.to_datetime(bar.date).tz_localize(None)
                    if (barDateTimeUTCNaive >= firstDateTimeToTakeUTCNaive) and (bar.close>0):
                        bars3.append(bar)
                        pass
                    pass

                a = (f'bar update callback before persist: {qc.localSymbol}; {bars3[0].date}; {bars3[-1].date} {len(bars3)}; {lastDiskDateTimeUTCNaiveFloor}; {nowUTCNaiveFloor}; {forceUpdate}')
                # def blabla(dt):
                #     return datetime.datetime.strftime(dt,'%H:%M:%S')
                # a = (f'bar update callback before persist: {qc.localSymbol}; firstBar: {blabla(bars3[0].date)}; lastBar: {blabla(bars3[-1].date)} {len(bars3)}; lastDisk: {blabla(lastDiskDateTimeUTCNaiveFloor)}; now: {blabla(nowUTCNaiveFloor)}; {forceUpdate}')
                # print(a)
                self.__log.info(a)

                marketDataIB.persistMarketDataBarsOnDisk(bars=bars3,
                                                         mydb=mydb,
                                                         tableSchema=tableSchema,
                                                         doCorrection=True)
                pass
            pass

        # cancel and re-request the recent historical bars if they are longer
        # than maximumBarsLengthFactor the duration for which they have been requested originally
        barsSpanTimeDeltaOnline = lastBarDateTimeUTCNaive - firstBarDateTimeUTCNaive
        if (barsSpanTimeDeltaOnline > maximumBarsLengthFactor * durationPandasTimeDelta) or (nowUTCNaiveFloor - lastBarDateTimeUTCNaiveFloor) > 4*barSizePandasTimeDelta:
            self.watchdogApp.ib.cancelHistoricalData(bars)
            bars = self.requestRecentHistoricalBars(qc)
            self.orderedDictOfBars[tableName] = bars
            a = (f'canceled and renewed recent historical bars for: {tableName}')
            self.__log.info(a)
            pass

        if (hasNewBar):
            a = (
                f'localSymbol: {localSymbol}, nBars: {nBars}, diffDateTime: {diffDateTime}, close: {lastBar.close}, last: {lastDiskDateTimeUTCNaiveFloor}')
            self.__log.info(a)
            pass

        pass

    def myErrorCallback(self, reqId: int, errorCode: str, errorString: str, contract: Contract) -> None:
        """a Callback for error handling from IB/ib_insync"""

        funcName = sys._getframe().f_code.co_name
        localSymbol = None if contract is None else contract.localSymbol
        a = (f'reqId: {reqId}, errorCode: {errorCode}, errorString: {errorString}, localSymbol: {localSymbol}')
        self.__log.info(a)
        # print(f'{funcName}: {a}')

        if errorCode in [10182, 1100, 504]:
            # indicates broken network, but working gateway
            self._errorThatTriggersWatchdogRestart = True
            pass

        if errorCode == 2106:
            # indicates working internet connection in conjunction with gateway
            if self._errorThatTriggersWatchdogRestart:
                # if we had a network problem prior to this event, restart watchdog
                self._errorThatTriggersWatchdogRestart = False
                # self.watchdogApp.flush() # if we use this, we run into error 504 that triggers continuously watchdog restarts
                a = (f'disconnecting ib and thereby triggering a watchdog.flush()')
                self.__log.info(a)
                self.watchdogApp.ib.disconnect()  # this also triggers watchdog restarts, but seems to be more gracefully than watchdog.flush. In particular, error 504 does not appear and watchdog will not be starte continuously after this.
                pass
            pass
        pass

    def myConnectedCallback(self):
        """a Callback that is called when a reconnection happens (connected to the event connectedEvent).
        Note that this event is not fired upon the first connection """

        funcName = sys._getframe().f_code.co_name
        a = (f'connected callback')
        self.__log.info(a)
        # print(f'{funcName}: {a}')

        # this counts the number of reconnections. Used for finding unique Ids for tasks that were still running when
        # there was a disconnection
        self.reconnectionCounter +=1

        ib = self.watchdogApp.ib
        scheduler = self.scheduler

        # request the historical bars again
        a = (f're-requesting historical bars')
        orderedDictOfBars = self.requestRecentHistoricalOrderedDictOfBars()
        self.orderedDictOfBars = orderedDictOfBars

        # # persist the initial result on disk
        # a = (f'persisting the bars on disk')
        # self.__log.info(a)
        # for (tableName, bars) in self.orderedDictOfBars.items():
        #     if bars is not None and len(bars) > 0:
        #         self.myBarUpdateCallback(bars, hasNewBar=True, forceUpdate=True)
        #         pass
        #     pass

        # add all jobs in the schedulerJobList to the scheduler
        if isinstance(scheduler,AsyncIOScheduler) and scheduler.running:
            for i,jobSettings in enumerate(self.schedulerJobList):

                jobRootName = jobSettings.get('jobRootName', None)

                # a = (f'{i}: about to add job {jobRootName} to the scheduler: {scheduler}')
                # self.__log.info(a)
                # print(a)

                # the job is already in schedulerJobList, we only need to add it to the scheduler
                job = self.addJobToScheduler(jobSettings, addToSchedulerJobList=False)

                # a = (f'added job {job.id} to the scheduler: {scheduler}')
                # self.__log.info(a)
                # print(a)

                pass
            pass
        pass

    def myDisconnectedCallback(self):
        """a Callback that is called when a disconnection happens (connected to the event disconnectedEvent)."""

        funcName = sys._getframe().f_code.co_name
        a = (f'disconnected callback')
        self.__log.info(a)
        # print(f'{funcName}: {a}')

        # remove jobs from the scheduler.
        # this should be ok because the corresponding jobs are still in schedulerJobList
        # so they can be re-added in the connected Callback
        scheduler = self.scheduler
        a = (f'removing jobs from the scheduler')
        self.__log.info(a)
        if isinstance(scheduler, AsyncIOScheduler) and scheduler.running:
            for job in scheduler.get_jobs():
                self.__log.info(f'scheduler: removing job {job}')
                job.remove()
                pass
            pass

        a = (f'removing requests for recent historical bars')
        self.__log.info(a)
        self.cancelAllRecentHistoricalBars()
        pass


    def myTimeoutCallback(self, timeout):
        """a Callback that is called when a timout happens (connected to the event timeoutEvent)."""

        funcName = sys._getframe().f_code.co_name
        a = (f'timeout callback {timeout}')
        self.__log.info(a)
        # print(f'{funcName}: {a}')

    def myStoppingCallback(self, watchdogApp):
        """a Callback that is called when watchdog is stopping (connected to the event watchdog.stoppingEvent)."""

        funcName = sys._getframe().f_code.co_name
        a = (f'stopping watchdog callback: {watchdogApp}')
        self.__log.info(a)
        # print(f'{funcName}: {a}')


    def myStoppedCallback(self, watchdogApp):
        """a Callback that is called when watchdog is stopped (connected to the event watchdog.stoppedEvent)."""

        funcName = sys._getframe().f_code.co_name
        a = (f'stopped watchdog callback {watchdogApp}')
        self.__log.info(a)
        # print(f'{funcName}: {a}')

    def myStartingCallback(self, watchdogApp):
        """a Callback that is called when watchdog is starting (connected to the event watchdog.startingEvent)."""

        funcName = sys._getframe().f_code.co_name
        a = (f'starting watchdog callback {watchdogApp}')
        self.__log.info(a)
        # print(f'{funcName}: {a}')
        pass

    def myStartedCallback(self, watchdogApp):
        """a Callback that is called when watchdog is started (connected to the event watchdog.startedEvent)."""

        funcName = sys._getframe().f_code.co_name
        a = (f'started watchdog callback {watchdogApp}')
        self.__log.info(a)
        # print(f'{funcName}: {a}')
        pass

    def mySoftTimeoutCallback(self, watchdogApp):
        """a Callback that is called when watchdog is emitting soft time out (connected to watchdog.softTimeoutEvent)."""

        funcName = sys._getframe().f_code.co_name
        a = (f'soft time out watchdog callback {watchdogApp}')
        self.__log.info(a)
        # print(f'{funcName}: {a}')

    def myHardTimeoutCallback(self, watchdogApp):
        """a Callback that is called when watchdog is emitting hard time out (connected to watchdog.hardTimeoutEvent)."""

        funcName = sys._getframe().f_code.co_name
        a = (f'hard time out watchdog callback {watchdogApp}')
        self.__log.info(a)
        # print(f'{funcName}: {a}')
        # the watchdog restart is handled by the errorCallback
        pass


    def removeJobFromScheduler(self,
                               jobRootName: str=None):
        """
        remove the job with the given rootName from the scheduler

        This function removes the job from the scheduler as well as the corresponding jobSettings from the
        schedulerJobList in containerClass

        :param jobRootName:
        :type jobRootName:
        :return:
        :rtype:
        """
        scheduler = self.scheduler
        if not isinstance(scheduer,AsyncIOScheduler):
            return None
        if not scheduler.running:
            return None
        if not isinstance(jobRootName,str):
            return None

        # search for the job in all jobs
        jobsToRemove = []
        for job in scheduler.get_jobs:
            if job.id.startswith(jobRootName):
                # job exists
                jobsToRemove.append(job)
                pass
            pass

        # loop over jobsToRemove and remove the jobs
        for job in jobsToRemove:
            job.remove()
            pass

        # search for the job in the jobList of containerClass
        indices = []
        for indx, jobSetting in enumerate(self.schedulerJobList):
            if jobRootName == jobSetting.get('jobRootName',None):
                indices.append(indx)
                pass
            pass

        # loop over jobSettingsToRemove and remove the jobs from the JobList of containerClass
        for indx in indices[::-1]:
            del self.schedulerJobList[indx]
            pass
        pass

    # wrapper to add jobs to scheduler
    def addJobToScheduler(self, jobSettings: dict=None, addToSchedulerJobList: bool=True) -> apschedulerJob:
        """a method to add a cron job to to the scheduler

        This function adds the job that is specified in the jobSettings to the scheduler of containerClass.
        If addToJobList==True, it also adds the job to the schedulerJobList of containerClass

        . The main purpose of this function is to wrap
        function adding to be able to do this in a generic way as it has to be done in more than one place.
        For example, when IB is re-establishing connection, scheduled jobs should be re-added.
        All jobs will have a counter appended to make them unique such that the scheduler will not think that any
        job is already running. This is done because some long-running tasks might exist when the network is interrupted
        or gateway is restarted; these long-running tasks will not be continued by the IB-side of things, but the scheduler
        still seese them as running.
        In this case, re-scheduling the same task with a different number will make the job unique.

        :param job: the function that should be executed. If None, nothing happens.
        :param jobName: the name of the job. If None, job.__name__ will be taken.
        :param jobCounter: the counter of the job. If None, a counter will be automatically appended.
        :param additionalArgs: a dictionary that is passed on to the scheduler.
        :type job: function
        :type jobName: str
        :type jobCounter: int
        :type additionalArgs: dict
        :returns: Nothing
        :rtype: None
        """

        _jobSettings = jobSettings.copy()

        job = _jobSettings.pop('job', None)
        # return if job is not present or not callable
        if (job is None) or (not callable(job)):
            return

        scheduler = self.scheduler

        assert(isinstance(scheduler,AsyncIOScheduler))

        # jobRootName is the name of the job without the counter appendix
        jobRootName = _jobSettings.pop('jobRootName', None)
        if not isinstance(jobRootName,str):
            jobRootName = job.__name__
            pass



        # find out whether this job is already in the scheduler
        jobExistsInScheduler = False
        schedulerJob = None
        for _job in scheduler.get_jobs():
            _jobName = _job.id
            assert(isinstance(_jobName,str))
            if _jobName.startswith(jobRootName):
                # this job is already contained in the scheduler
                jobExistsInScheduler = True
                schedulerJob = _job
                break
                pass
            pass

        # find the counter of the job
        schedulerJobCounter = self.reconnectionCounter # this makes sure we get new unique Ids after a reconnection
        if jobExistsInScheduler:
            jobCounterFromScheduler = int(schedulerJob.id.split('_')[-1])
            schedulerJobCounter = jobCounterFromScheduler + 1
            pass
        jobCounter = schedulerJobCounter

        # generate the job Id (job Name)
        jobName = (f'{jobRootName}_{jobCounter:06}')


        # a = (f'addJobToScheduler: jobName={jobName}; args={jobSettings.get("args"),None}, kwargs={jobSettings.get("kwargs",None)}')
        # self.__log.info(a)
        # print(a)

        schedulerJob = scheduler.add_job(
            job,
            name=jobName,
            id=jobName,
            **_jobSettings
        )

        if addToSchedulerJobList:
            # the most important part: put all the necessary settings for this
            # job in the schedulerJobList
            # first, add jobRootName which might have been empty to the jobSettings
            jobSettings['jobRootName'] = jobRootName
            self.schedulerJobList.append(jobSettings)
            pass

        return schedulerJob




    async def schedulerJobSwitchRequestForRecentHistoricalDataFromOneSettingToOther(self, **kwargs):
        """this function cancels the current requests for recent historical data,
        and then relaunches a similar request with different settings"""


        # fetch default kwargs using the settings in the containerClass. make a copy to not change the settings in containerClass
        _kwargs = self.recentHistoricalDataSettings.copy()

        # a = (f'schedulerJobSwitch: before; _kwargs={_kwargs}')
        # self.__log.info(a)
        # print(a)

        # update the kwargs using the passed arguments
        _kwargs.update(kwargs)

        # a = (f'schedulerJobSwitch: after; _kwargs={_kwargs}')
        # self.__log.info(a)
        # print(a)


        # cancel the current request for recent historical data
        a = (f'cancelling request for recent historical bars')
        self.__log.info(a)
        # print(a)
        self.cancelAllRecentHistoricalBars()

        # re-initialize the ordered dict of bars
        a = (f're-initializing the ordered dict of bars')
        self.__log.info(a)
        # print(a)
        self.orderedDictOfBars = OrderedDict()

        # relaunch the request with the new settings
        a = (f'renewing request for recent historical bars with new settings')
        self.__log.info(a)
        # print(a)
        self.recentHistoricalDataSettings = _kwargs.copy()
        self.orderedDictOfBars = self.requestRecentHistoricalOrderedDictOfBars()

        pass




    # register callbacks with ib
    def registerCallbacks(self):
        """a method to register callbacks"""
        ib = self.watchdogApp.ib

        ib.connectedEvent.clear()
        ib.connectedEvent += self.myConnectedCallback

        ib.disconnectedEvent.clear
        ib.disconnectedEvent += self.myDisconnectedCallback

        ib.errorEvent.clear()
        ib.errorEvent += self.myErrorCallback

        ib.timeoutEvent.clear()
        ib.timeoutEvent += self.myTimeoutCallback

        ib.barUpdateEvent.clear()
        ib.barUpdateEvent += self.myBarUpdateCallback

        # register callbacks with watchdog
        watchdogApp = self.watchdogApp

        watchdogApp.startingEvent.clear()
        watchdogApp.startingEvent += self.myStartingCallback

        watchdogApp.startedEvent.clear()
        watchdogApp.startedEvent += self.myStartedCallback

        watchdogApp.stoppingEvent.clear()
        watchdogApp.stoppingEvent += self.myStoppingCallback

        watchdogApp.stoppedEvent.clear()
        watchdogApp.stoppedEvent += self.myStoppedCallback

        watchdogApp.softTimeoutEvent.clear()
        watchdogApp.softTimeoutEvent += self.mySoftTimeoutCallback

        watchdogApp.hardTimeoutEvent.clear()
        watchdogApp.hardTimeoutEvent += self.myHardTimeoutCallback

    pass

