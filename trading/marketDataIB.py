from trading import utils, database
import logging
from ib_insync import util, IB, Object, IBC, Contract, objects
from sqlalchemy import desc, func, update
import pandas as pd
from random import randint
import asyncio
import dateutil
import pprint
import sqlalchemy
from sqlalchemy.ext.declarative.api import DeclarativeMeta as TableORM
import datetime

_logger = logging.getLogger(__name__)


def getHistoricalDataBars(ib,
                          qc,
                          endDateTime: str = '',
                          durationStr: str = '600 S',
                          barSizeSetting: str = '1 min',
                          whatToShow: str=None,
                          formatDate: int=2,
                          keepUpToDate: bool=False,
                          useRTH: bool=False,
                          timeOutTime: int=600,
                          ) -> list:
    """

    :param ib:
    :type ib:
    :param qc:
    :type qc:
    :param endDateTime:
    :type endDateTime:
    :param durationStr:
    :type durationStr:
    :param barSizeSetting:
    :type barSizeSetting:
    :param whatToShow:
    :type whatToShow:
    :param formatDate:
    :type formatDate:
    :param keepUpToDate:
    :type keepUpToDate:
    :param useRTH:
    :type useRTH:
    :param timeOutTime:
    :type timeOutTime:
    :return:
    :rtype:
    """

    if whatToShow is None:
        whatToShow = utils.dictSecTypeToWhatToShow.get(qc.secType, None)
        pass

    assert (whatToShow is not None)

    # a = (f'started getHistoricalDataBars: timeouttime={timeOutTime}')
    # _logger.info(a)
    # print(a)

    bars = None
    timeOutOccurred = False
    try:
        req = ib.reqHistoricalDataAsync(qc,
                                           endDateTime=endDateTime,
                                           durationStr=durationStr,
                                           barSizeSetting=barSizeSetting,
                                           whatToShow=whatToShow,
                                           formatDate=formatDate,
                                           keepUpToDate=keepUpToDate,
                                           useRTH=useRTH)
        bars = ib.run(asyncio.wait_for(req, timeOutTime))

    except asyncio.TimeoutError:
        a = (f'Timeout while requesting historical bars for contract {qc}')
        _logger.warn(a)
        timeOutOccurred = True
        pass

    # a = (f'finished getHistoricalDataBars')
    # _logger.info(a)
    # print(a)

    return [bars, timeOutOccurred]

def getLastDateTimeOnDiskUTCNaive(mydb, tableSchema):
    """get last Date Time in the tableSchema on disk in UTC Naive format

    :param mydb:
    :param tableSchema:
    :return:
    """
    #*80
    # all data on disk are timezone-unaware times that are in UTC
    result = None
    ssn = mydb.Session()
    lDT = ssn.query(tableSchema.c.datetime).order_by(desc(tableSchema.c.datetime)).first()
    if lDT is not None and len(lDT) == 1:
        lDT = lDT[0]
    else:
        return (result)

    lDT = pd.to_datetime(lDT)
    result = lDT

    ssn.close()

    return (result)

def getEarliestDateTimeUTCNaiveFromDataFrame(mydb: database.tradingDB, conId):
    # all data on disk (and therefore in the dataframe) are timezone-unaware times that are in UTC
    result = None
    df = mydb.MarketDataInfoTableDataFrame

    eDTs = df.loc[df.conId==conId,'earliestDateTime'].values

    if len(eDTs) == 1:
        eDT = eDTs[0]
    else:
        return (result)

    # eDT can be a list containing None
    if eDT is None:
        return (result)

    eDT = pd.to_datetime(eDT)
    result = eDT

    return (result)


def persistMarketDataBarsOnDisk(bars, mydb: database.tradingDB, tableSchema, doCorrection=True):
    """write the bars to disk.
    update the data.
    also calculate time differences between successive rows"""
    if bars is not None and len(bars) > 0:
        tStart = pd.datetime.now()
        # convert bars to dataframe
        df = util.df(bars)
        # rename columns (the 'date' column defined in the bars is the 'datetime' column in the tables)
        df.rename(columns={'date': 'datetime'}, inplace=True)
        # calculate the time difference between rows

        # calculate the difference in time between a row and the next row
        df.loc[:, 'diffToNextRowInMinutes'] = df.datetime.diff().shift(-1) / pd.Timedelta(1,'m')
        # the last difference is zero
        df.iloc[-1, df.columns.get_loc('diffToNextRowInMinutes')] = 0


        # upsert the dataframe
        if tableSchema is not None:


            mydb.upsertDataFrame(df, tableSchema)
            # print(df.tail(1))


            # repair datetime on disk.
            # it can happen that the data fetched contains less or more information
            # than the data on disk (That has been fetched before).
            # therefore, the diffDateTImes need to be corrected for all entries between
            # the first and the last entries

            firstDateTime = df.iloc[0,df.columns.get_loc('datetime')].tz_localize(None)
            lastDateTime = df.iloc[-1,df.columns.get_loc('datetime')].tz_localize(None)


            # find previous DateTime
            ssn = mydb.Session()
            previousDateTime = ssn.query(func.max(tableSchema.c.datetime)).filter(tableSchema.c.datetime < firstDateTime).scalar()
            ssn.close()
            # this is None if no such value exists
            if previousDateTime is None:
                previousDateTime = firstDateTime


            ttStart = pd.datetime.now()
            if doCorrection:
                df = mydb.correctDiffDateTimesForMarketDataTable(tableName=tableSchema.name,
                                                                 startDateTime=previousDateTime,
                                                                 endDateTime=lastDateTime,
                                                                 doCorrection=doCorrection)
                pass
            ttEnd = pd.datetime.now()
            a = (f'finished correcting data. Table: {tableSchema.name}; nRows: {len(bars)}; elapsed time: {ttEnd-ttStart}')
            _logger.info(a)

            pass

        tEnd = pd.datetime.now()
        a = (f'finished treating data. Table: {tableSchema.name}; nRows: {len(bars)}; elapsed time: {tEnd-tStart}')
        _logger.info(a)
        # print(a)
        pass
    pass




def getCurrentInfoForHistoricalDataRequests(mydb, tablePos):
    # retrieve the current information about where we are in the historical data request task
    tableNameOnDisk = None
    startDateTimeOnDiskUTCNaive = None
    ssn = mydb.Session()
    for row in ssn.query(tablePos).order_by(tablePos.c.tableName):
        tableNameOnDisk = row.tableName
        startDateTimeOnDiskUTCNaive = pd.to_datetime(row.endDateTime)
        break
    ssn.close()
    return([tableNameOnDisk, startDateTimeOnDiskUTCNaive])



def getNewStartDateTime(ssn, tableORM, oldStartDateTimeUTCNaive, diffTimeInMinutes):
    """finds the next startDateTime at the beginning of a gap"""
    startDateTimeIsOnDisk = bool(ssn.query(tableORM).filter(tableORM.datetime == oldStartDateTimeUTCNaive).count() == 1)
    dateTimeAtStartOfNextGapUTCNaive = None
    if startDateTimeIsOnDisk:
        a = (f'startDateTIme {oldStartDateTimeUTCNaive} is on Disk')
        _logger.info(a)
    else:
        a = (f'startDateTIme {oldStartDateTimeUTCNaive} is not on Disk')
        _logger.info(a)
        pass
    if startDateTimeIsOnDisk:
        # advance startDateTime to the beginning of the next gap (if a next gap is found)
        rowAtEndOfContiguousData = ssn.query(tableORM).filter(tableORM.diffToNextRowInMinutes > diffTimeInMinutes).filter(
            tableORM.datetime >= oldStartDateTimeUTCNaive).order_by(tableORM.datetime).first()
        dateTimeEndOfCongiguousDataFound = False
        if rowAtEndOfContiguousData is not None:
            dateTimeEndOfCongiguousData = rowAtEndOfContiguousData.datetime
            dateTimeEndOfCongiguousDataFound = True
            # we found a row at the end of the contiguous data
            a = (f'end of contiguous data found: {dateTimeEndOfCongiguousData}')
            _logger.info(a)
        else:
            # we did not find a row at the end of the contiguous data
            dateTimeEndOfCongiguousDataFound = False
            a = (f'end of contiguous data not found')
            _logger.info(a)
            pass
        if dateTimeEndOfCongiguousDataFound:
            # advance the startDateTime to the next dateTime that is not on disk
            dateTimeAtStartOfNextGap = dateTimeEndOfCongiguousData + pd.Timedelta(diffTimeInMinutes, 'm')
            a = (f'start of next gap: {dateTimeAtStartOfNextGap}')
            _logger.info(a)
            dateTimeAtStartOfNextGapUTCNaive = pd.to_datetime(dateTimeAtStartOfNextGap)
            pass
        pass
    return (dateTimeAtStartOfNextGapUTCNaive)


class HistoricalDataGetterCurrentState(object):
    """class to hold the current state of the Historical Data Getter"""

    __slots__ = [
        'indexIntoQCS',
        'tableName',
        'qc',
        'conId',
        'tableORM',
        'startDateTime',
        'endDateTime',
        'durationStringOnLastUpdateForIB',
        'nRowsReadOnLastUpdate',
        'nRowsTotal'
    ]
    def __init__(self):
        for k in self.__slots__:
            setattr(self,k,None)
            pass
        pass

    def advanceCurrentStateToNextQC(self,
                                   mydb: database.tradingDB = None,
                                   qcs: list = [],
                                   tableNames: list = []) -> None:
        """advancint the current State to the next qualified contract"""

        assert (isinstance(mydb, database.tradingDB))
        assert (len(qcs) > 0)
        assert (len(qcs) == len(tableNames))

        ssn = mydb.Session()

        self.indexIntoQCS = (self.indexIntoQCS + 1) % len(tableNames)
        self.tableName = tableNames[self.indexIntoQCS]
        self.qc = qcs[self.indexIntoQCS]
        self.conId = self.qc.conId
        self.tableORM = utils.getValueFromDataFrame(
            mydb.MarketDataInfoTableDataFrame,
            whereColumn='conId',
            whereValue=self.conId,
            getColumn='tableORM')
        # the following means we are starting the new qc
        self.startDateTime = None
        self.endDateTime = None
        self.durationStringOnLastUpdateForIB = None
        self.nRowsReadOnLastUpdate = None
        self.nRowsTotal = ssn.query(self.tableORM).count()

        ssn.close()
        pass

    def updateCurrentStateFromDisk(self,
                                   mydb: database.tradingDB=None,
                                   qcs: list=[],
                                   tableNames: list=[]) -> None:
        """using the state read from disk, set the corresponding internal variables"""

        assert (isinstance(mydb, database.tradingDB))
        assert (len(qcs) > 0)
        assert (len(qcs) == len(tableNames))

        tablePos = mydb.MarketDataHistoricalRequestsPositionTable

        ssn = mydb.Session()

        # the following is None if there is no content
        stateOnDisk = ssn.query(tablePos).first()
        assert (isinstance(stateOnDisk, mydb.MarketDataHistoricalRequestsPositionTable))
        indexIntoQCS = 0
        try:
            indexIntoQCS = tableNames.index(stateOnDisk.tableName)
        except:
            pass

        self.indexIntoQCS = indexIntoQCS
        self.tableName = stateOnDisk.tableName
        self.qc = qcs[indexIntoQCS]
        self.conId = self.qc.conId
        self.tableORM = utils.getValueFromDataFrame(
            mydb.MarketDataInfoTableDataFrame,
            whereColumn='conId',
            whereValue=self.conId,
            getColumn='tableORM')
        # the current state aims to start at the endDateTIme given on Disk
        self.startDateTime = utils.conformDateTimeToPandasUTCNaive(stateOnDisk.endDateTime)
        self.endDateTime = None
        self.durationStringOnLastUpdateForIB = stateOnDisk.durationStringOnLastUpdate
        self.nRowsReadOnLastUpdate = stateOnDisk.numberOfTicksRetrieved
        self.nRowsTotal = ssn.query(self.tableORM).count()


        ssn.close()
        pass

    def writeCurrentStateToDisk(self, mydb: database.tradingDB) -> None:
        """writes the current State to disk"""

        ssn = mydb.Session()
        tablePos = mydb.MarketDataHistoricalRequestsPositionTable

        # the following is None if there is no content
        stateOnDisk = ssn.query(tablePos).first()
        if stateOnDisk is None:
            stateOnDisk = tablePos()
            pass

        assert (isinstance(stateOnDisk, tablePos))

        # update the ORM instance
        stateOnDisk.endDateTime = self.endDateTime
        stateOnDisk.tableName = self.tableName
        stateOnDisk.durationStringOnLastUpdate = self.durationStringOnLastUpdateForIB
        stateOnDisk.lastUpdateDateTimeLocal = pd.datetime.now()
        stateOnDisk.numberOfTicksRetrieved = self.nRowsReadOnLastUpdate

        # persist on disk
        ssn.commit()
        ssn.close()


    def updateCurrentStateFromBarsAndStartEndDateTImes(self,
                                                       mydb: database.tradingDB=None,
                                                       startDateTime: pd.datetime=None,
                                                       endDateTime: pd.datetime=None,
                                                       durationStringOnLastUpdateForIB: str=None,
                                                       bars: list=[]) -> None:
        """update the current state using the information in the bars as well as startDateTime and endDateTIme

        this means that we did not switch the qualified Contract.
        All information about qc, the indexIntoQCs, tables should be up-to-date"""

        ssn = mydb.Session()

        self.startDateTime = utils.conformDateTimeToPandasUTCNaive(startDateTime)
        self.endDateTime = utils.conformDateTimeToPandasUTCNaive(endDateTime)
        nBars = 0
        if bars is not None:
            nBars = len(bars)
            pass
        self.durationStringOnLastUpdateForIB = durationStringOnLastUpdateForIB
        self.nRowsReadOnLastUpdate = nBars
        self.nRowsTotal = ssn.query(self.tableORM).count()

        ssn.close()
        pass


    def __repr__(self):
        clsName = self.__class__.__name__
        attributeDict = {}
        for k in self.__slots__:
            attributeDict[k] = getattr(self, k)
            pass
        attributeString = pprint.pformat(attributeDict)
        return f'{clsName}:\n{attributeString}'

    pass



class HistoricalDataGetter(Object):
    """class to get historical Data from IB

    """
    defaults = dict(
        ib=None,
        mydb=None,
        qcs=[],
        durationPandasTimeDelta=pd.Timedelta(weeks=1),
        barSizePandasTimeDelta=pd.Timedelta(minutes=1),
        earliestDateTime=None,
        timeOutTime=1800,
        jitterSpanFraction=0.02,
    )

    __slots__ = list(defaults.keys()) + [
        'tableNames',
        'currentState',
        'tzlocal'
    ]

    def __init__(self, *args, **kwargs):
        Object.__init__(self, *args, **kwargs)

        self.tableNames = [
            self.mydb.getValueFromMarketDataInfoTable(whereColumn='conId', whereValue=qc.conId, getColumn='tableName') for qc
            in self.qcs]


        self.currentState = HistoricalDataGetterCurrentState()
        self.tzlocal = dateutil.tz.tzlocal()


        # self.writeCurrentStateToDisk()
        # self.fetchTimeseriesFromIB()
        # self.persistTimeseriesReadFromIBonDisk()
        # self.calculateNextStepParameters()
        # self.performStep()
        #

        pass

    def persistBarsOnDisk(self, bars: list=[]):
        """persist bars on Disk.

        we only save the bars with dateTime < datetime.now

        :param bars:
        :type bars:
        :return:
        :rtype:
        """
        if bars is not None and len(bars) > 0:
            lastBarDateTime = utils.conformDateTimeToPandasUTCNaive(pd.to_datetime(bars[-1].date))
            _nowUTCNaiveFloor = pd.to_datetime(pd.datetime.utcnow()).floor(self.barSizePandasTimeDelta)
            if lastBarDateTime == _nowUTCNaiveFloor:
                bars = bars[:-1]
                pass
            pass

        # upsert the data
        persistMarketDataBarsOnDisk(bars=bars,
                                    mydb=self.mydb,
                                    tableSchema=self.currentState.tableORM.__table__,
                                    doCorrection=True)

        pass

    def getEarliestDateTimeFromIB(self,
                                  useCurrentQC: bool=True,
                                  qc: Contract=None,
                                  timeOutTime: int=30) ->pd.datetime:
        """retrieve the earliestDateTime from IB for a contract using some timeOutTIme"""

        if useCurrentQC:
            _qc = self.currentState.qc
            pass
        if not useCurrentQC:
            _qc = qc
            pass
        earliestDT = utils.conformDateTimeToPandasUTCNaive(
                utils.getEarliestDateTimeFromIBAsDateTime(ib=self.ib,
                                                          qualifiedContract=_qc,
                                                          timeOutTime=timeOutTime))
        return earliestDT



    pass


    def runForever(self):
        """iterate over all qualifiedContracts in qcs and get the data starting from the position saved on disk"""
        timeOutErrorCounterMax = 10
        timeOutErrorCounter = 0
        while True:

            tStart = pd.datetime.now()

            # get the current state on Disk
            self.currentState.updateCurrentStateFromDisk(mydb=self.mydb,
                                                         qcs=self.qcs,
                                                         tableNames=self.tableNames)


            # check if the current conId allows the retrieval of historical data
            # to do that, we check if we can retrieve the earliestDateTime directly from IB using reqHeadTimeStamp with a short timeOut
            # a = (f'about to try to get the earliestDateTime')
            # _logger.info(a)
            earliestDTFromIB = None
            try:
                # a = (f'in try part trying to get the earliestDateTime')
                # _logger.info(a)
                earliestDTFromIB = self.getEarliestDateTimeFromIB()
            except:
                # we might be disconnected. This should be taken care of by watchdog
                # a = (f'in except part trying to get the earliestDateTime')
                # _logger.info(a)
                return

            # if there is no earliestDateTime according to IB, it makes no sense to try to retrieve data
            if pd.isnull(earliestDTFromIB):
                a = (
                    f'attempting to get historical data: {self.currentState.qc.symbol}, {self.currentState.qc.currency}: not performed because earliestDateTime as queried directly from IB is NULL.')
                timeOutErrorCounter += 1
            else:
                a = (
                    f'attempting to get historical data: {self.currentState.qc.symbol}, {self.currentState.qc.currency}: is going to be performed because earliestDateTime as queried directly from IB is: {earliestDTFromIB}')
                timeOutErrorCounter = 0
                pass

            _logger.info(a)
            # print(a)

            # we did not break the for loop in the rows above because we wanted to log some items.
            # we are now breaking the loop if no earliestDateTime is found
            if pd.isnull(earliestDTFromIB):
                # it makes no sense to continue if IB has no earliestDateTime for this conId
                # we therefore skip all code below and continue the for loop
                # we will also go to the next conId if a certain number of timeOuts has been reached
                if timeOutErrorCounter >= timeOutErrorCounterMax:
                    self.currentState.advanceCurrentStateToNextQC(mydb=self.mydb,
                                                             qcs=self.qcs,
                                                             tableNames=self.tableNames)
                    timeOutErrorCounter = 0
                    pass

                self.currentState.writeCurrentStateToDisk(mydb=self.mydb)
                self.ib.sleep(0.1)
                continue
            else:
                # all ok, we can retrieve data and will continue
                pass

            # we have made sure that our current position is such that we have a conId for which we can retrieve data
            assert(not pd.isnull(earliestDTFromIB))

            # now get the current valid earliestDateTime.
            # this is either earliestDateTimeFromIB or earliestDateTIme as given by the parameters if tht is not None
            # we want to do this here because we want to update this value dynamically.
            # if self.earliestDateTIme is None, we need to dynamically fetch the earliestDateTime each time
            # we run this loop to be sure we have up-to-date information
            earliestDT = earliestDTFromIB
            if self.earliestDateTime is not None:
                earliestDT = self.earliestDateTime
                pass

            # now get the startDateTime.
            # this is either earliestDT or the endDateTime as found on disk if that exists and if it is a later date
            startDateTime = earliestDT
            if not pd.isnull(self.currentState.startDateTime):
                startDateTime = max(self.currentState.startDateTime, earliestDT)
                pass


            # we now have the startDateTime. Calculate the endDateTime
            endDateTime = startDateTime + self.durationPandasTimeDelta

            # add some jitter to make sure we are not always requesting the same period
            if self.jitterSpanFraction > 0:
                durationTimeInMinutes = int((endDateTime - startDateTime) / pd.Timedelta(1, 'm'))
                jitterSpan = durationTimeInMinutes * self.jitterSpanFraction
                jitterInMinutes = randint(0,int(jitterSpan))
                jitterTimeDelta = pd.Timedelta(jitterInMinutes, 'm')
                startDateTime = startDateTime - jitterTimeDelta

                # re-calculate endDateTime
                endDateTime = startDateTime + self.durationPandasTimeDelta
                pass




            # we now have startDateTime and endDateTime. Now retrieve the corresponding data
            endDateTimeLocal = endDateTime.tz_localize('UTC').tz_convert(self.tzlocal).tz_localize(None)
            dTD = utils.CTimeDelta(self.durationPandasTimeDelta)
            bTD = utils.CTimeDelta(self.barSizePandasTimeDelta)

            # print(f'localSymbol: {self.currentState.qc.localSymbol}; '
            #       f'earDT: {earliestDT.strftime("%Y-%m-%d %H:%M")}; '
            #       f'sttDT: {startDateTime.strftime("%Y-%m-%d %H:%M")}; '
            #       f'endDT: {endDateTime.strftime("%Y-%m-%d %H:%M")}; '
            #       f'endDTLocal: {endDateTimeLocal.strftime("%Y-%m-%d %H:%M")}; '
            #       f'dTD: {dTD.IB_Duration_String}; '
            #       f'bTD: {bTD.IB_Bar_Size_String}')

            bars = None
            timeOutOccured = None
            try:
                [bars, timeOutOccured] = getHistoricalDataBars(ib=self.ib,
                                                           qc=self.currentState.qc,
                                                           endDateTime=endDateTimeLocal,
                                                           durationStr=dTD.IB_Duration_String,
                                                           barSizeSetting=bTD.IB_Bar_Size_String,
                                                           timeOutTime=self.timeOutTime)

            except:
                # there might be other problems that have occurred. ib could be disconnected.
                # watchdog should take care of that
                return

            if timeOutOccured is not None and timeOutOccured:
                a = (f'Timeout while requesting historical bars for contract {self.currentState.qc}; timeOutTime: {self.timeOutTime}')
                _logger.warn(a)
                # print(a)
                pass

            # persist the bars on disk
            self.persistBarsOnDisk(bars=bars)



            # now update the currentState
            self.currentState.updateCurrentStateFromBarsAndStartEndDateTImes(mydb=self.mydb,
                                                                             startDateTime=startDateTime,
                                                                             endDateTime=endDateTime,
                                                                             durationStringOnLastUpdateForIB=dTD.IB_Duration_String,
                                                                             bars=bars)





            # advance to next qc if we have retrieved all data up to now
            utcnow = pd.to_datetime(pd.datetime.utcnow())
            latestDateTimeThatShouldBeOnDisk = utcnow.floor(self.barSizePandasTimeDelta) - self.barSizePandasTimeDelta
            if endDateTime > latestDateTimeThatShouldBeOnDisk:
                self.currentState.advanceCurrentStateToNextQC(mydb=self.mydb,
                                                          qcs=self.qcs,
                                                          tableNames=self.tableNames)
                pass


            # write current State to Disk
            self.currentState.writeCurrentStateToDisk(mydb=self.mydb)




            # log info about the end of bar data retrieval
            tEnd = pd.datetime.now()
            tDelta = tEnd - tStart
            a = (
                f'finished to get historical data chunk: '
                f'{self.currentState.qc.symbol}, {self.currentState.qc.currency}; '
                f'startDT: {self.currentState.startDateTime}; '
                f'endDT: {self.currentState.endDateTime}; '
                f'durationString: {dTD.IB_Duration_String}; '
                f'elapsedTime: {tDelta}; '
                f'rows: {self.currentState.nRowsReadOnLastUpdate}; '
                f'rowsTotal: {self.currentState.nRowsTotal}')
            _logger.info(a)
            # print(a)


            self.ib.sleep(0.1)
            pass
        pass
    pass



async def asyncioJobGetHistoricalData(*args, **kwargs):
    """get all available historical data for the contracts specified in qcs"""
    success = False

    a = (f'starting function to get historical data')
    _logger.info(a)
    # print(a)


    historicalDataGetter = HistoricalDataGetter(**kwargs)
    historicalDataGetter.runForever()


    a = (f'ending function to get historical data')
    _logger.info(a)
    # print(a)

    pass

class TradingHourParser():
    def __init__(self, cds: objects.ContractDetails):
        self.timeZoneId = cds.timeZoneId
        if self.timeZoneId == 'CST':
            self.timeZoneId = 'CST6CDT'
            pass
        if self.timeZoneId == 'JST':
            self.timeZoneId = 'Asia/Tokyo'
            pass
        if self.timeZoneId == 'GB':
            self.timeZoneId = 'GMT'
            pass
        self.tradingHours = cds.tradingHours
        self.liquidHours = cds.liquidHours
        pass

    def parseToDF(self) -> pd.DataFrame:
        tradingHoursParsed = [item for item in self._parse_trading_hours_iterator(hours=self.tradingHours)]
        liquidHoursParsed = [item for item in self._parse_trading_hours_iterator(hours=self.liquidHours)]
        tradingHoursParsedDataFrame = pd.DataFrame.from_records(tradingHoursParsed, columns=['open','close'])
        tradingHoursParsedDataFrame['typeOfHours'] = 'trading'
        liquidHoursParsedDataFrame = pd.DataFrame.from_records(liquidHoursParsed,columns=['open','close'])
        liquidHoursParsedDataFrame['typeOfHours'] = 'liquid'



        dfTrading = pd.melt(tradingHoursParsedDataFrame, value_vars=['open', 'close'], var_name='typeOfAction',
                            value_name='datetime', id_vars='typeOfHours')
        dfLiquid = pd.melt(liquidHoursParsedDataFrame, value_vars=['open', 'close'], var_name='typeOfAction',
                            value_name='datetime', id_vars='typeOfHours')

        df = pd.concat([dfTrading,dfLiquid])
        df.sort_values(by=['typeOfHours','datetime'], ascending=[False,True], inplace=True)
        return df

    def _parse_trading_hours_iterator(self, hours: str=None) -> pd.DataFrame:
        """:Return: A dataframe with naive (tz-unaware) :class:`pandas.Timestamp` objects giving the time ranges
        parsed from an IB trading hours string.
        The information of the timezone given in the contractdetails is used to convert all times
        to UTC naive.

        Example:
            '20170621:1700-20170621:1515;20170622:1700-20170622:1515;'
            '20180625:0500-20180625:0800;'
            '20180610:CLOSED;'
        """

        for daystr in hours.split(';'):  # Regex can't handle varying number of repeated capture groups
            if daystr.find('CLOSED') > 0:
                dateStr1, closedStr = daystr.split(':')
                closedStr = closedStr.split(',')[0].strip()
                date1 = datetime.datetime.strptime(dateStr1, '%Y%m%d').date()
                time1 = datetime.time(0, 0, 0)
                datetime1 = datetime.datetime.combine(date1, time1)
                pdTS1 = pd.to_datetime(datetime1)
                pdTS2 = pdTS1
                pass
            else:
                datetimeStr1, datetimeStr2 = daystr.split('-')
                dateStr1, timeStr1 = datetimeStr1.split(':')
                timeStr1 = timeStr1.split(',')[0].strip()
                dateStr2, timeStr2 = datetimeStr2.split(':')
                timeStr2 = timeStr2.split(',')[0].strip()
                date1 = datetime.datetime.strptime(dateStr1, '%Y%m%d').date()
                date2 = datetime.datetime.strptime(dateStr2, '%Y%m%d').date()
                start = datetime.datetime.strptime(timeStr1, '%H%M').time()
                end = datetime.datetime.strptime(timeStr2, '%H%M').time()
                datetime1 = datetime.datetime.combine(date1, start)
                datetime2 = datetime.datetime.combine(date2, end)
                pdTS1 = pd.to_datetime(datetime1)
                pdTS2 = pd.to_datetime(datetime2)
                pass
            pass
            if self.timeZoneId=='GMT':
                a=2
                pass
            pdTS1 = pdTS1.tz_localize(self.timeZoneId).tz_convert('UTC').tz_localize(None)
            pdTS2 = pdTS2.tz_localize(self.timeZoneId).tz_convert('UTC').tz_localize(None)
            yield pdTS1, pdTS2
        pass


pass

## old stuff

# async def asyncioJobGetHistoricalData(ib, mydb: database.tradingDB, qcs, additionalArgs={}):
#     """get all available historical data for the contracts specified in qcs"""
#     success = False
#     if not ib.isConnected():
#         return success
#
#     # a = (f'starting function to get historical data: {[qc.localSymbol for qc in qcs]}')
#     # _logger.info(a)
#     # print(a)
#
#     # create a session for this task
#     ssn = mydb.Session()
#
#     # the table with the information about the position in the historical data request task
#     tablePos = mydb.MarketDataHistoricalRequestsPositionTable.__table__
#
#     # get the info about where we are in the historical data request task
#     [tableNameOnDisk, startDateTimeOnDiskUTCNaiveFloor] = getCurrentInfoForHistoricalDataRequests(mydb, tablePos)
#
#     # find the index for the for-loop below
#     tableNames = [mydb.getValueFromMarketDataInfoTable(whereColumn='conId', whereValue=qc.conId, getColumn='tableName') for qc in qcs]
#     indexToStart = 0
#     try:
#         indexToStart = tableNames.index(tableNameOnDisk)
#     except:
#         pass
#
#     # get the additional args
#     # duration time delta
#     durationPandasTimeDeltaDefault = pd.Timedelta(weeks=4)
#     # bar size delta
#     barSizePandasTimeDeltaDefault = pd.Timedelta(minutes=1)
#
#     # override durations and bar size settings if arguments are given
#     durationPandasTimeDelta = additionalArgs.get('durationPandasTimeDelta', durationPandasTimeDeltaDefault)
#     barSizePandasTimeDelta = additionalArgs.get('barSizePandasTimeDelta', barSizePandasTimeDeltaDefault)
#     timeOutTimeHistoricalBars = additionalArgs.get('timeOutTimeHistoricalBars', 600)
#     jitterSpanFraction = additionalArgs.get('jitterSpanFraction', 0.02)
#
#
#     # if we are finished with all data retrieval, remove table content such
#     # that we can start over
#     startOver = False
#     if indexToStart == len(qcs)-1:
#         if pd.isnull(tableNameOnDisk):
#             # no content in the table. Start over
#             startOver = True
#             pass
#         else:
#             if pd.isnull(startDateTimeOnDiskUTCNaiveFloor):
#                 # there was a tablename but no time. This probably means we wrote before a tableName that cannot retrieve historical data. Start over.
#                 startOver = True
#                 pass
#             else:
#                 # there is a tableName and a dateTime. We have to check if we are finished with fethcing all data for this table
#                 if startDateTimeOnDiskUTCNaiveFloor >= pd.to_datetime(pd.datetime.utcnow()).floor(barSizePandasTimeDelta) - barSizePandasTimeDelta:
#                     # we are finished fetching data. Start over
#                     startOver = True
#                     pass
#                 pass
#             pass
#         pass
#
#     if startOver:
#         delStmt = tablePos.delete()
#         results = ssn.bind.execute(delStmt)
#         ssn.commit()
#         indexToStart = 0
#         pass
#
#     # a = (f'index to start: {indexToStart}')
#     # _logger.info(a)
#     # print(a)
#
#     # retrieve historical data, starting at the position where we were last time this program ran
#     for qc in qcs[indexToStart:]:
#         # refresh the info about where we are in the historical data request task
#         [tableNameOnDisk, startDateTimeOnDiskUTCNaiveFloor] = getCurrentInfoForHistoricalDataRequests(mydb, tablePos)
#
#
#         # this can be a long task if it is not interrupted.
#
#         # we record the starting time
#         tStart1 = pd.datetime.now()
#
#         # the conId for this contract
#         conId = qc.conId
#
#         # the table into which we want to insert
#         tableORM = utils.getValueFromDataFrame(mydb.MarketDataInfoTableDataFrame,whereColumn='conId', whereValue=conId, getColumn='tableORM')
#
#         # get the earliest dateTime for this conId defined by IB
#         # get it directly from IB as this is a check whether we can ask for historical Data for this conId
#         earliestDateTimeUTCNaiveAccordingToIBQueriedDirectly = utils.conformDateTimeToPandasUTCNaive(utils.getEarliestDateTimeFromIBAsDateTime(ib=ib, qualifiedContract=qc, timeOutTime=10))
#         # if there is no earliestDateTime according to IB, it makes no sense to try to retrieve data
#         if pd.isnull(earliestDateTimeUTCNaiveAccordingToIBQueriedDirectly):
#             a = (f'attempting to get historical data: {qc.symbol}, {qc.currency}: not performed because earliestDateTime as queried directly from IB is NULL.')
#         else:
#             a = (f'attempting to get historical data: {qc.symbol}, {qc.currency}; is going to be performed because earliestDateTime as queried directly from IB is: {earliestDateTimeUTCNaiveAccordingToIBQueriedDirectly}')
#             pass
#
#         _logger.info(a)
#
#         # we did not break the for loop in the rows above because we wanted to log some items.
#         # we are now breaking the loop if no earliestDateTime is found
#         if pd.isnull(earliestDateTimeUTCNaiveAccordingToIBQueriedDirectly):
#             # it makes no sense to continue if IB has no earliestDateTime for this conId
#             # we therefore skip all code below and continue the for loop, essentially meaning that the next conId
#             # will be treated
#             # we must update the positiontable of course
#             # remove the old row by removing the table content
#
#             delStmt = tablePos.delete()
#             results = ssn.bind.execute(delStmt)
#             ssn.commit()
#
#             insStmt = tablePos.insert().values(
#                 tableName=tableORM.__tablename__,
#                 endDateTime=None,
#                 durationStringOnLastUpdate=None,
#                 numberOfTicksRetrieved=None,
#                 lastUpdateDateTimeLocal=pd.datetime.now(),
#             )
#             results = ssn.bind.execute(insStmt)
#             ssn.commit()
#
#             continue
#             pass
#
#
#         # local timezone
#         tzlocal = dateutil.tz.tzlocal()
#
#
#
#         # override the earliestDateTime if an argument is given
#         earliestDateTimeUTCNaiveAccordingToAdditionalArgs = additionalArgs.get('earliestDateTimeUTCNaive', None)
#         if (not pd.isnull(earliestDateTimeUTCNaiveAccordingToAdditionalArgs)):
#             earliestDateTimeUTCNaive = earliestDateTimeUTCNaiveAccordingToAdditionalArgs
#         else:
#             earliestDateTimeUTCNaive = earliestDateTimeUTCNaiveAccordingToIBQueriedDirectly
#             pass
#
#         startDateTimeUTCNaive = earliestDateTimeUTCNaive
#         # if there is information in the position table, take this information
#
#         if not pd.isnull(startDateTimeOnDiskUTCNaiveFloor):
#             # only take the information if it concerns the table that we are currently treating
#             if tableNameOnDisk == tableORM.__tablename__:
#                 if startDateTimeOnDiskUTCNaiveFloor > startDateTimeUTCNaive:
#                     # only correct the information from disk if the disk value is larger than the current value
#                     startDateTimeUTCNaive = startDateTimeOnDiskUTCNaiveFloor
#                     pass
#                 pass
#             pass
#
#
#
#         # create the timeDelta objects that generate the IB strings
#         dTDRegular = utils.CTimeDelta(durationPandasTimeDelta)
#         dTD = dTDRegular
#         bTD = utils.CTimeDelta(barSizePandasTimeDelta)
#         diffTimeInMinutes = int(bTD._timeDelta / pd.Timedelta(minutes=1)) # necessary in case we use the algorithm to search for the newest datetime on disk
#
#
#         # now make sure that startDateTime is not larger than nowUTCNaive
#         # this makes sure that we actually retrieve data
#         # this is ok as the loop for a given contract starts below such that we do not interfere with the ending conditions for that loop
#         #           and the loop over all contracts is initialized above such that the conditions for that loop are not impacted
#         if startDateTimeUTCNaive >= pd.to_datetime(pd.datetime.utcnow()).floor(barSizePandasTimeDelta) - durationPandasTimeDelta:
#             startDateTimeUTCNaive = pd.to_datetime(pd.datetime.utcnow()).floor(barSizePandasTimeDelta) - durationPandasTimeDelta
#             pass
#
#
#         conditionOnStartDateTime = startDateTimeUTCNaive <= pd.to_datetime(pd.datetime.utcnow()).ceil(barSizePandasTimeDelta)
#         while (not pd.isnull(startDateTimeUTCNaive)) and conditionOnStartDateTime:
#
#             # a = (f'while start: startDateTimeUTCNaive {startDateTimeUTCNaive}, now {pd.to_datetime(pd.datetime.utcnow()).ceil(barSizePandasTimeDelta)}, condition {conditionOnStartDateTime}')
#             # _logger.info(a)
#
#             a = (f'startDateTime Original {startDateTimeUTCNaive}')
#             _logger.info(a)
#
#             # calculate the regular endDateTIme
#             endDateTimeUTCNaive = startDateTimeUTCNaive + dTD._timeDelta
#             endDateTimeUTCNaiveRegular = endDateTimeUTCNaive # necessary in case we use the algorithm to search for the newest datetime on disk
#             a = (f'calculated endDateTime {endDateTimeUTCNaive}')
#             _logger.info(a)
#
#             # initialize the durationTimeDelta Object
#             dTD = dTDRegular
#
#             if 0: # search for the newest datetime on disk and only request data for periods that start from that point
#                 ## skipping data that already exists
#                 # Idea: if the startDateTime is on disk, then
#                 #          advance startDateTime to the beginning of the next gap (if a next gap is found)
#                 newStartDateTimeUTCNaive = getNewStartDateTime(ssn=ssn, tableORM=tableORM, oldStartDateTimeUTCNaive=startDateTimeUTCNaive, diffTimeInMinutes=diffTimeInMinutes)
#                 if newStartDateTimeUTCNaive is not None:
#                     a = (f'advancing to new StartDateTime {newStartDateTimeUTCNaive}')
#                     _logger.info(a)
#                     startDateTimeUTCNaive = newStartDateTimeUTCNaive
#                     # continue the while loop
#                     continue
#                     pass
#
#
#                 # the recalculated startDateTime above has placed us in a gap by design
#                 # test that this is correct
#                 startDateTimeIsOnDisk = bool(
#                     ssn.query(tableORM).filter(tableORM.datetime == startDateTimeUTCNaive).count() == 1)
#                 assert(not startDateTimeIsOnDisk)
#
#                 # now find the end of the gap
#                 rowWhereGapStops = ssn.query(tableORM).filter(tableORM.diffToNextRowInMinutes<=diffTimeInMinutes).filter(tableORM.datetime>=startDateTimeUTCNaive).order_by(tableORM.datetime).first()
#                 if rowWhereGapStops is not None:
#                     # we found a datetime where the gap stops
#                     a = (f'found a row where the gap stops. datetime: {rowWhereGapStops.datetime}')
#                     _logger.info(a)
#                     pass
#                 else:
#                     # we found no datetime where the gap stops
#                     a = (f'found no row where the gap stops.')
#                     _logger.info(a)
#                     pass
#
#                 if rowWhereGapStops is not None:
#                     # because we found a row where the gap stops, we will only request data until that timepoint (if the resulting duration is smaller than the current regular duration)
#                     endDateTimeWhereGapStopsUTCNaive = pd.to_datetime(rowWhereGapStops.datetime)
#                     a = (f'endDatetime calculated using the end of the gap: {endDateTimeWhereGapStopsUTCNaive}')
#                     _logger.info(a)
#                     endDateTimeUTCNaive = min(endDateTimeUTCNaiveRegular, endDateTimeWhereGapStopsUTCNaive)
#                     a = (f'now setting a new endDateTime that is the minumum of the endDateTime where the gap stops and the regular endDateTime : {endDateTimeUTCNaive}')
#                     _logger.info(a)
#                     # because we have redefined the endDateTime, we have to generate a revised TimeDelta object
#                     pandasTimeDelta = pd.Timedelta(seconds=(endDateTimeUTCNaive - startDateTimeUTCNaive).total_seconds())
#                     dTD = utils.CTimeDelta(pandasTimeDelta)
#                     a = (
#                         f'generated a new TimeDelta object. IBDurationString :{dTD.IB_Duration_String}')
#                     _logger.info(a)
#
#                     pass
#                 else:
#                     # we found no row where the gap stops
#                     a = (f'no new endDatetime calculated using the end of the gap because no end of gap was found')
#                     _logger.info(a)
#                     pass
#
#
#
#             # add some jitter to the startDate to make sure we are not always requesting the same period
#             durationTimeInMinutes = int((endDateTimeUTCNaive - startDateTimeUTCNaive) / pd.Timedelta(1, 'm'))
#             jitterSpan = durationTimeInMinutes * jitterSpanFraction
#             jitterInMinutes = randint(0,int(jitterSpan))
#             jitterTimeDelta = pd.Timedelta(jitterInMinutes,'m')
#
#             dTD = utils.CTimeDelta(durationPandasTimeDelta - jitterTimeDelta)
#
#             # specify the endDateTime
#             endDateTimeUTCNaive = startDateTimeUTCNaive + dTD._timeDelta
#
#             # log info about chunk getting
#             tStart2 = pd.datetime.now()
#             a = (
#                 f'attempting to get historical data chunk: {qc.symbol}, {qc.currency}; startDT: {startDateTimeUTCNaive}; endDT: {endDateTimeUTCNaive}; durationString: {dTD.IB_Duration_String}; timeout: {timeOutTimeHistoricalBars}')
#             _logger.info(a)
#             # print(a)
#
#             # get the historical bars
#             endDateTimeLocalNaive = endDateTimeUTCNaive.tz_localize('UTC').tz_convert(tzlocal).tz_localize(None)
#
#             bars = None
#             timeOutOccured = None
#             [bars, timeOutOccured] = getHistoricalDataBars(ib,
#                                                            qc,
#                                                            endDateTime=endDateTimeLocalNaive,
#                                                            durationStr=dTD.IB_Duration_String,
#                                                            barSizeSetting=bTD.IB_Bar_Size_String,
#                                                            timeOutTime=timeOutTimeHistoricalBars)
#
#             if timeOutOccured is not None and timeOutOccured:
#                 a = (f'Timeout while requesting historical bars for contract {qc}')
#                 _logger.warn(a)
#                 pass
#
#             # it could be that the bars contain the current datetime.
#             # we do not want to use this value in the update as it is still submect to change.
#             # we therefore check if the date of last bars is the current date and remove the corresponding row if that
#             # is the case
#
#             if bars is not None and len(bars) >0:
#                 lastBarDateTime = utils.conformDateTimeToPandasUTCNaive(pd.to_datetime(bars[-1].date))
#                 _nowUTCNaiveFloor = pd.to_datetime(pd.datetime.utcnow()).floor(barSizePandasTimeDelta)
#                 if lastBarDateTime == _nowUTCNaiveFloor:
#                     bars = bars[:-1]
#                     pass
#                 pass
#
#
#             # persist the result on disk
#             # upsert the data
#             persistMarketDataBarsOnDisk(bars, mydb, tableORM.__table__, doCorrection=True)
#
#             # calculate the number of rows that have been retrieved
#
#             nRows = 0
#             if bars is not None:
#                 nRows = len(bars)
#                 pass
#
#             # calculate the total number of rows on disk
#             nRowsTotal = ssn.query(tableORM).count()
#
#             # # write the information about where we are to disk
#             # endDateTimeUTCNaiveAsDateTime = endDateTimeUTCNaive.to_pydatetime()
#
#             # ib.sleep(2)
#             # a = f'before removing: {ssn.query(tablePos).all()}'
#             # print(a)
#             # _logger.info(a)
#             # # ib.sleep(2)
#
#             # remove the old row by removing the table content
#             delStmt = tablePos.delete()
#             results = ssn.bind.execute(delStmt)
#             ssn.commit()
#
#             # a = f'after removing: {ssn.query(tablePos).all()}'
#             # print(a)
#             # _logger.info(a)
#             # # ib.sleep(2)
#
#             # add this row to update the position in the positiongtable
#             insStmt = tablePos.insert().values(
#                 tableName=tableORM.__tablename__,
#                 endDateTime=endDateTimeUTCNaive,
#                 durationStringOnLastUpdate=dTD.IB_Duration_String,
#                 numberOfTicksRetrieved=nRows,
#                 lastUpdateDateTimeLocal=pd.datetime.now(),
#             )
#             results = ssn.bind.execute(insStmt)
#             ssn.commit()
#
#             # a = f'after udpating: {ssn.query(tablePos).all()}'
#             # print(a)
#             # _logger.info(a)
#             # # ib.sleep(2)
#
#             # log info about the end of bar data retrieval
#             tEnd2 = pd.datetime.now()
#             tDelta2 = tEnd2 - tStart2
#             a = (
#                 f'finished to get historical data chunk: {qc.symbol}, {qc.currency}; startDT: {startDateTimeUTCNaive}; endDT: {endDateTimeUTCNaive}; durationString: {dTD.IB_Duration_String}; elapsedTime: {tDelta2}; rows: {nRows}; rowsTotal: {nRowsTotal}')
#             _logger.info(a)
#
#             # update the startDateTime for the next iteration of the loop
#             startDateTimeUTCNaive = endDateTimeUTCNaive
#
#             # as this ia a potentially very long running loop, make a little pause to
#             # allow other asyncio processes (for example, the processes that tet recent market data)
#             # to also get a piece of the processor
#             ib.sleep(0.01)
#
#
#             conditionOnStartDateTime = startDateTimeUTCNaive <= pd.to_datetime(pd.datetime.utcnow()).ceil(barSizePandasTimeDelta)
#
#             # a = (f'while end: startDateTimeUTCNaive {startDateTimeUTCNaive}, now {pd.to_datetime(pd.datetime.utcnow()).ceil(barSizePandasTimeDelta)}, condition {conditionOnStartDateTime}')
#             # _logger.info(a)
#             # print(a)
#
#             pass
#
#         tEnd1 = pd.datetime.now()
#         tDelta1 = tEnd1 - tStart1
#         a = (f'finished to get historical data: {qc.symbol}, {qc.currency}; elapsedTime: {tDelta1}')
#         _logger.info(a)
#         # print(a)
#         pass
#     success = True
#     ssn.close()
#     # a = (f'finished function to get historical data')
#     # _logger.info(a)
#     # print(a)
#
#     return success
#
#
# def createAsyncioJobGetRecentData():
#     async def asyncioJobGetRecentData(ib: IB, mydb: database.tradingDB, qc, additionalArgs={}):
#         """get recent historical data for the contract specified in qc"""
#         success = False
#         a = (f'attempting to get recent historical data chunk: {qc.symbol}, {qc.currency}')
#         _logger.info(a)
#
#         if not (isinstance(ib, IB) and  ib.isConnected()):
#             return success
#
#
#         conId = qc.conId
#
#         tableORM = utils.getValueFromDataFrame(mydb.MarketDataInfoTableDataFrame, whereColumn='conId', whereValue=conId,
#                                                getColumn='tableORM')
#
#
#         lastDateTimeOnDiskUTCNaive = getLastDateTimeOnDiskUTCNaive(mydb, tableORM.__table__)
#         earliestDateTimeUTCNaiveAccordingToIB = getEarliestDateTimeUTCNaiveFromDataFrame(mydb, conId)
#
#         # if there is no earliestDateTime according to IB, it makes no sense to try to retrieve data
#         if pd.isnull(earliestDateTimeUTCNaiveAccordingToIB):
#             success = True
#             return(success)
#
#
#         # duration time delta
#         durationMaximumPandasTimeDeltaDefault = pd.Timedelta(weeks=4)
#         # bar size delta
#         barSizePandasTimeDeltaDefault = pd.Timedelta(minutes=1)
#
#         # override defaults if values have been passed in additionalArgs
#         durationMaximumPandasTimeDelta = additionalArgs.get('durationMaximumPandasTimeDelta', durationMaximumPandasTimeDeltaDefault)
#         barSizePandasTimeDelta = additionalArgs.get('barSizePandasTimeDelta', barSizePandasTimeDeltaDefault)
#         timeOutTimeHistoricalBars = additionalArgs.get('timeOutTimeHistoricalBars',600)
#
#         # we wish to get the most recent historical data
#         # the end time is therefore nowUTCNaive
#         nowUTCNaive = pd.to_datetime(pd.datetime.utcnow()).floor(barSizePandasTimeDelta)
#         endDateTimeUTCNaive = nowUTCNaive
#
#         # the earliest possible start date time is endtime minus the maximum allowed time delta
#         earliestStartDateTimeUTCNaive = endDateTimeUTCNaive - durationMaximumPandasTimeDelta
#
#         # by default, the startDateTime is this earliest possible datetime
#         startDateTimeUTCNaive = earliestStartDateTimeUTCNaive
#
#         # if we found a lastDateTime on Disk, the startDateTime is modified
#         if not pd.isnull(lastDateTimeOnDiskUTCNaive):
#             startDateTimeUTCNaive = max(lastDateTimeOnDiskUTCNaive, earliestStartDateTimeUTCNaive)
#             pass
#
#         # for some reason, it is possible that the time on disk is in the future.
#         # we therefore determine the startDateTime as the minimum between the startDateTime as calculated before
#         # and the endDateTime
#         startDateTimeUTCNaive = min(startDateTimeUTCNaive, endDateTimeUTCNaive)
#
#         # we can now calculate the time delta that we actually want to fetch
#         durationPandasTimeDelta = endDateTimeUTCNaive - startDateTimeUTCNaive
#
#         # define the objects used to calculate the IB strings for time deltas and bar size settings
#         dTD = utils.CTimeDelta(durationPandasTimeDelta)
#         bTD = utils.CTimeDelta(barSizePandasTimeDelta)
#
#         # log info about the start of bar data retrieval
#         tStart2 = pd.datetime.now()
#         if not pd.isnull(earliestDateTimeUTCNaiveAccordingToIB):
#             a = (
#                 f'attempting to get recent historical data chunk: {qc.symbol}, {qc.currency}; startDT: {startDateTimeUTCNaive} (durationString: {dTD.IB_Duration_String})')
#         else:
#             a = (
#                 f'attempting to get recent historical data chunk: {qc.symbol}, {qc.currency}; not performed because earliestDateTime is not given by IB; time: {tStart2}')
#             pass
#         _logger.info(a)
#
#
#         # get the historical data bars
#         # only get bars if the last value on disk is not utcnow
#         bars = None
#         timeOutOccured = None
#         if not (nowUTCNaive == lastDateTimeOnDiskUTCNaive):
#             [bars, timeOutOccured] = getHistoricalDataBars(ib,
#                                          qc,
#                                          endDateTime='',
#                                          durationStr=dTD.IB_Duration_String,
#                                          barSizeSetting=bTD.IB_Bar_Size_String,
#                                          timeOutTime=timeOutTimeHistoricalBars)
#
#         if timeOutOccured is not None and timeOutOccured:
#             a = (f'Timeout while requesting historical bars for contract {qc}')
#             _logger.warn(a)
#             pass
#
#         # it could be that the bars contain the current datetime.
#         # we do not want to use this value in the update as it is still submect to change.
#         # we therefore check if the date of last bars is the current date and remove the corresponding row if that
#         # is the case
#
#         if bars is not None and len(bars) >0:
#             lastBarDateTime = utils.conformDateTimeToPandasUTCNaive(pd.to_datetime(bars[-1].date))
#             _nowUTCNaiveFloor = pd.to_datetime(pd.datetime.utcnow()).floor(barSizePandasTimeDelta)
#             if lastBarDateTime == _nowUTCNaiveFloor:
#                 bars = bars[:-1]
#                 pass
#             pass
#
#         # calculate the number of rows that have been retrieved
#         nRows = 0
#         if bars is not None:
#             nRows = len(bars)
#             pass
#
#         # persist result on disk
#         # upsert the data
#         persistMarketDataBarsOnDisk(bars,mydb,tableORM.__table__, doCorrection=True)
#
#
#         # calculate the total number of rows on disk
#         ssn = mydb.Session()
#         nRowsTotal = ssn.query(tableORM.__table__).count()
#         ssn.close()
#
#         # log info about the end of bar data retrieval
#         tEnd2 = pd.datetime.now()
#         tDelta2 = tEnd2 - tStart2
#         a = (f'finished to get recent historical data chunk: {qc.symbol}, {qc.currency}; startDT: {startDateTimeUTCNaive} (durationString: {dTD.IB_Duration_String}); elapsedTime: {tDelta2}; rows: {nRows}; rowsTotal: {nRowsTotal}')
#         _logger.info(a)
#         success = True
#         ssn.close()
#         return success
#
#
#     return asyncioJobGetRecentData
