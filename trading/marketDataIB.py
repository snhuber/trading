from trading import utils, database
import logging
from ib_insync import util, IB
from sqlalchemy import desc, func, update
import pandas as pd
from random import randint
import asyncio
import dateutil

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


def createAsyncioJobGetRecentData():
    async def asyncioJobGetRecentData(ib: IB, mydb: database.tradingDB, qc, additionalArgs={}):
        """get recent historical data for the contract specified in qc"""
        success = False
        a = (f'attempting to get recent historical data chunk: {qc.symbol}, {qc.currency}')
        _logger.info(a)

        if not (isinstance(ib, IB) and  ib.isConnected()):
            return success


        conId = qc.conId

        tableORM = utils.getValueFromDataFrame(mydb.MarketDataInfoTableDataFrame, whereColumn='conId', whereValue=conId,
                                               getColumn='tableORM')


        lastDateTimeOnDiskUTCNaive = getLastDateTimeOnDiskUTCNaive(mydb, tableORM.__table__)
        earliestDateTimeUTCNaiveAccordingToIB = getEarliestDateTimeUTCNaiveFromDataFrame(mydb, conId)

        # if there is no earliestDateTime according to IB, it makes no sense to try to retrieve data
        if pd.isnull(earliestDateTimeUTCNaiveAccordingToIB):
            success = True
            return(success)


        # duration time delta
        durationMaximumPandasTimeDeltaDefault = pd.Timedelta(weeks=4)
        # bar size delta
        barSizePandasTimeDeltaDefault = pd.Timedelta(minutes=1)

        # override defaults if values have been passed in additionalArgs
        durationMaximumPandasTimeDelta = additionalArgs.get('durationMaximumPandasTimeDelta', durationMaximumPandasTimeDeltaDefault)
        barSizePandasTimeDelta = additionalArgs.get('barSizePandasTimeDelta', barSizePandasTimeDeltaDefault)
        timeOutTimeHistoricalBars = additionalArgs.get('timeOutTimeHistoricalBars',600)

        # we wish to get the most recent historical data
        # the end time is therefore nowUTCNaive
        nowUTCNaive = pd.to_datetime(pd.datetime.utcnow()).floor(barSizePandasTimeDelta)
        endDateTimeUTCNaive = nowUTCNaive

        # the earliest possible start date time is endtime minus the maximum allowed time delta
        earliestStartDateTimeUTCNaive = endDateTimeUTCNaive - durationMaximumPandasTimeDelta

        # by default, the startDateTime is this earliest possible datetime
        startDateTimeUTCNaive = earliestStartDateTimeUTCNaive

        # if we found a lastDateTime on Disk, the startDateTime is modified
        if not pd.isnull(lastDateTimeOnDiskUTCNaive):
            startDateTimeUTCNaive = max(lastDateTimeOnDiskUTCNaive, earliestStartDateTimeUTCNaive)
            pass

        # for some reason, it is possible that the time on disk is in the future.
        # we therefore determine the startDateTime as the minimum between the startDateTime as calculated before
        # and the endDateTime
        startDateTimeUTCNaive = min(startDateTimeUTCNaive, endDateTimeUTCNaive)

        # we can now calculate the time delta that we actually want to fetch
        durationPandasTimeDelta = endDateTimeUTCNaive - startDateTimeUTCNaive

        # define the objects used to calculate the IB strings for time deltas and bar size settings
        dTD = utils.CTimeDelta(durationPandasTimeDelta)
        bTD = utils.CTimeDelta(barSizePandasTimeDelta)

        # log info about the start of bar data retrieval
        tStart2 = pd.datetime.now()
        if not pd.isnull(earliestDateTimeUTCNaiveAccordingToIB):
            a = (
                f'attempting to get recent historical data chunk: {qc.symbol}, {qc.currency}; startDT: {startDateTimeUTCNaive} (durationString: {dTD.IB_Duration_String})')
        else:
            a = (
                f'attempting to get recent historical data chunk: {qc.symbol}, {qc.currency}; not performed because earliestDateTime is not given by IB; time: {tStart2}')
            pass
        _logger.info(a)


        # get the historical data bars
        # only get bars if the last value on disk is not utcnow
        bars = None
        timeOutOccured = None
        if not (nowUTCNaive == lastDateTimeOnDiskUTCNaive):
            [bars, timeOutOccured] = getHistoricalDataBars(ib,
                                         qc,
                                         endDateTime='',
                                         durationStr=dTD.IB_Duration_String,
                                         barSizeSetting=bTD.IB_Bar_Size_String,
                                         timeOutTime=timeOutTimeHistoricalBars)

        if timeOutOccured is not None and timeOutOccured:
            a = (f'Timeout while requesting historical bars for contract {qc}')
            _logger.warn(a)
            pass

        # calculate the number of rows that have been retrieved
        nRows = 0
        if bars is not None:
            nRows = len(bars)
            pass

        # persist result on disk
        # upsert the data
        persistMarketDataBarsOnDisk(bars,mydb,tableORM.__table__, doCorrection=True)


        # calculate the total number of rows on disk
        ssn = mydb.Session()
        nRowsTotal = ssn.query(tableORM.__table__).count()
        ssn.close()

        # log info about the end of bar data retrieval
        tEnd2 = pd.datetime.now()
        tDelta2 = tEnd2 - tStart2
        a = (f'finished to get recent historical data chunk: {qc.symbol}, {qc.currency}; startDT: {startDateTimeUTCNaive} (durationString: {dTD.IB_Duration_String}); elapsedTime: {tDelta2}; rows: {nRows}; rowsTotal: {nRowsTotal}')
        _logger.info(a)
        success = True
        ssn.close()
        return success


    return asyncioJobGetRecentData


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


async def asyncioJobGetHistoricalData(ib, mydb: database.tradingDB, qcs, additionalArgs={}):
    """get all available historical data for the contracts specified in qcs"""
    success = False
    if not ib.isConnected():
        return success

    # a = (f'starting function to get historical data: {[qc.localSymbol for qc in qcs]}')
    # _logger.info(a)
    # print(a)

    # create a session for this task
    ssn = mydb.Session()

    # the table with the information about the position in the historical data request task
    tablePos = mydb.MarketDataHistoricalRequestsPositionTable.__table__

    # get the info about where we are in the historical data request task
    [tableNameOnDisk, startDateTimeOnDiskUTCNaiveFloor] = getCurrentInfoForHistoricalDataRequests(mydb, tablePos)

    # find the index for the for-loop below
    tableNames = [mydb.getValueFromMarketDataInfoTable(whereColumn='conId', whereValue=qc.conId, getColumn='tableName') for qc in qcs]
    indexToStart = 0
    try:
        indexToStart = tableNames.index(tableNameOnDisk)
    except:
        pass

    # if we are finished with all data retrieval, remove table content such
    # that we can start over
    if indexToStart == len(qcs)-1:
        if not pd.isnull(startDateTimeOnDiskUTCNaiveFloor):
            if startDateTimeOnDiskUTCNaiveFloor >= pd.to_datetime(pd.datetime.utcnow()):
                delStmt = tablePos.delete()
                results = ssn.bind.execute(delStmt)
                ssn.commit()
                indexToStart = 0
                pass
            pass
        pass

    # a = (f'index to start: {indexToStart}')
    # _logger.info(a)
    # print(a)

    # retrieve historical data, starting at the position where we were last time this program ran
    for qc in qcs[indexToStart:]:
        # refresh the info about where we are in the historical data request task
        [tableNameOnDisk, startDateTimeOnDiskUTCNaiveFloor] = getCurrentInfoForHistoricalDataRequests(mydb, tablePos)

        # this can be a long task if it is not interrupted.

        # we record the starting time
        tStart1 = pd.datetime.now()

        # the conId for this contract
        conId = qc.conId

        # the table into which we want to insert
        tableORM = utils.getValueFromDataFrame(mydb.MarketDataInfoTableDataFrame,whereColumn='conId', whereValue=conId, getColumn='tableORM')

        # get the earliest dateTime for this conId defined by IB
        earliestDateTimeUTCNaiveAccordingToIB = getEarliestDateTimeUTCNaiveFromDataFrame(mydb, conId)
        # if there is no earliestDateTime according to IB, it makes no sense to try to retrieve data
        if pd.isnull(earliestDateTimeUTCNaiveAccordingToIB):
            a = (f'attempting to get historical data: {qc.symbol}, {qc.currency}; not performed because earliestDateTime is not given by IB.')
        else:
            a = (f'attempting to get historical data: {qc.symbol}, {qc.currency}; is going to be performed because earliestDateTime is given by IB: {earliestDateTimeUTCNaiveAccordingToIB}')
            pass

        _logger.info(a)

        # we did not break the for loop in the rows above because we wanted to log some items.
        # we are now breaking the loop if no earliestDateTime is found
        if pd.isnull(earliestDateTimeUTCNaiveAccordingToIB):
            # it makes no sense to continue if IB has no earliestDateTime for this conId
            # we therefore skip all code below and continue the for loop, essentially meaning that the next conId
            # will be treated
            continue
            pass



        # local timezone
        tzlocal = dateutil.tz.tzlocal()



        # override the earliestDateTime if an argument is given
        earliestDateTimeUTCNaiveAccordingToAdditionalArgs = additionalArgs.get('earliestDateTimeUTCNaive', None)
        if (not pd.isnull(earliestDateTimeUTCNaiveAccordingToAdditionalArgs)):
            earliestDateTimeUTCNaive = earliestDateTimeUTCNaiveAccordingToAdditionalArgs
        else:
            earliestDateTimeUTCNaive = earliestDateTimeUTCNaiveAccordingToIB
            pass

        startDateTimeUTCNaive = earliestDateTimeUTCNaive
        # if there is information in the position table, take this information

        if not pd.isnull(startDateTimeOnDiskUTCNaiveFloor):
            # only take the information if it concerns the table that we are currently treating
            if tableNameOnDisk == tableORM.__tablename__:
                if startDateTimeOnDiskUTCNaiveFloor > startDateTimeUTCNaive:
                    # only correct the information from disk if the disk value is larger than the current value
                    startDateTimeUTCNaive = startDateTimeOnDiskUTCNaiveFloor
                    pass
                pass
            pass




        # duration time delta
        durationPandasTimeDeltaDefault = pd.Timedelta(weeks=4)
        # bar size delta
        barSizePandasTimeDeltaDefault = pd.Timedelta(minutes=1)

        # override durations and bar size settings if arguments are given
        durationPandasTimeDelta = additionalArgs.get('durationPandasTimeDelta', durationPandasTimeDeltaDefault)
        barSizePandasTimeDelta = additionalArgs.get('barSizePandasTimeDelta', barSizePandasTimeDeltaDefault)
        timeOutTimeHistoricalBars = additionalArgs.get('timeOutTimeHistoricalBars',600)
        jitterSpanFraction = additionalArgs.get('jitterSpanFraction',0.02)

        # create the timeDelta objects that generate the IB strings
        dTDRegular = utils.CTimeDelta(durationPandasTimeDelta)
        dTD = dTDRegular
        bTD = utils.CTimeDelta(barSizePandasTimeDelta)
        diffTimeInMinutes = int(bTD._timeDelta / pd.Timedelta(minutes=1)) # necessary in case we use the algorithm to search for the newest datetime on disk


        # now make sure that startDateTime is not larger than nowUTCNaive
        # this makes sure that we actually retrieve data
        # this is ok as the loop for a given contract starts below such that we do not interfere with the ending conditions for that loop
        #           and the loop over all contracts is initialized above such that the conditions for that loop are not impacted
        if startDateTimeUTCNaive >= pd.to_datetime(pd.datetime.utcnow()).ceil(barSizePandasTimeDelta) - durationPandasTimeDelta:
            startDateTimeUTCNaive = pd.to_datetime(pd.datetime.utcnow()).ceil(barSizePandasTimeDelta) - durationPandasTimeDelta
            pass


        conditionOnStartDateTime = startDateTimeUTCNaive <= pd.to_datetime(pd.datetime.utcnow()).ceil(barSizePandasTimeDelta)
        while (not pd.isnull(startDateTimeUTCNaive)) and conditionOnStartDateTime:

            # a = (f'while start: startDateTimeUTCNaive {startDateTimeUTCNaive}, now {pd.to_datetime(pd.datetime.utcnow()).ceil(barSizePandasTimeDelta)}, condition {conditionOnStartDateTime}')
            # _logger.info(a)

            a = (f'startDateTime Original {startDateTimeUTCNaive}')
            _logger.info(a)

            # calculate the regular endDateTIme
            endDateTimeUTCNaive = startDateTimeUTCNaive + dTD._timeDelta
            endDateTimeUTCNaiveRegular = endDateTimeUTCNaive # necessary in case we use the algorithm to search for the newest datetime on disk
            a = (f'calculated endDateTime {endDateTimeUTCNaive}')
            _logger.info(a)

            # initialize the durationTimeDelta Object
            dTD = dTDRegular

            if 0: # search for the newest datetime on disk and only request data for periods that start from that point
                ## skipping data that already exists
                # Idea: if the startDateTime is on disk, then
                #          advance startDateTime to the beginning of the next gap (if a next gap is found)
                newStartDateTimeUTCNaive = getNewStartDateTime(ssn=ssn, tableORM=tableORM, oldStartDateTimeUTCNaive=startDateTimeUTCNaive, diffTimeInMinutes=diffTimeInMinutes)
                if newStartDateTimeUTCNaive is not None:
                    a = (f'advancing to new StartDateTime {newStartDateTimeUTCNaive}')
                    _logger.info(a)
                    startDateTimeUTCNaive = newStartDateTimeUTCNaive
                    # continue the while loop
                    continue
                    pass


                # the recalculated startDateTime above has placed us in a gap by design
                # test that this is correct
                startDateTimeIsOnDisk = bool(
                    ssn.query(tableORM).filter(tableORM.datetime == startDateTimeUTCNaive).count() == 1)
                assert(not startDateTimeIsOnDisk)

                # now find the end of the gap
                rowWhereGapStops = ssn.query(tableORM).filter(tableORM.diffToNextRowInMinutes<=diffTimeInMinutes).filter(tableORM.datetime>=startDateTimeUTCNaive).order_by(tableORM.datetime).first()
                if rowWhereGapStops is not None:
                    # we found a datetime where the gap stops
                    a = (f'found a row where the gap stops. datetime: {rowWhereGapStops.datetime}')
                    _logger.info(a)
                    pass
                else:
                    # we found no datetime where the gap stops
                    a = (f'found no row where the gap stops.')
                    _logger.info(a)
                    pass

                if rowWhereGapStops is not None:
                    # because we found a row where the gap stops, we will only request data until that timepoint (if the resulting duration is smaller than the current regular duration)
                    endDateTimeWhereGapStopsUTCNaive = pd.to_datetime(rowWhereGapStops.datetime)
                    a = (f'endDatetime calculated using the end of the gap: {endDateTimeWhereGapStopsUTCNaive}')
                    _logger.info(a)
                    endDateTimeUTCNaive = min(endDateTimeUTCNaiveRegular, endDateTimeWhereGapStopsUTCNaive)
                    a = (f'now setting a new endDateTime that is the minumum of the endDateTime where the gap stops and the regular endDateTime : {endDateTimeUTCNaive}')
                    _logger.info(a)
                    # because we have redefined the endDateTime, we have to generate a revised TimeDelta object
                    pandasTimeDelta = pd.Timedelta(seconds=(endDateTimeUTCNaive - startDateTimeUTCNaive).total_seconds())
                    dTD = utils.CTimeDelta(pandasTimeDelta)
                    a = (
                        f'generated a new TimeDelta object. IBDurationString :{dTD.IB_Duration_String}')
                    _logger.info(a)

                    pass
                else:
                    # we found no row where the gap stops
                    a = (f'no new endDatetime calculated using the end of the gap because no end of gap was found')
                    _logger.info(a)
                    pass



            # add some jitter to the startDate to make sure we are not always requesting the same period
            durationTimeInMinutes = int((endDateTimeUTCNaive - startDateTimeUTCNaive) / pd.Timedelta(1, 'm'))
            jitterSpan = durationTimeInMinutes * jitterSpanFraction
            jitterInMinutes = randint(0,int(jitterSpan))
            jitterTimeDelta = pd.Timedelta(jitterInMinutes,'m')

            dTD = utils.CTimeDelta(durationPandasTimeDelta - jitterTimeDelta)

            # specify the endDateTime
            endDateTimeUTCNaive = startDateTimeUTCNaive + dTD._timeDelta

            # log info about chunk getting
            tStart2 = pd.datetime.now()
            a = (
                f'attempting to get historical data chunk: {qc.symbol}, {qc.currency}; startDT: {startDateTimeUTCNaive}; endDT: {endDateTimeUTCNaive}; durationString: {dTD.IB_Duration_String}')
            _logger.info(a)
            # print(a)

            # get the historical bars
            endDateTimeLocalNaive = endDateTimeUTCNaive.tz_localize('UTC').tz_convert(tzlocal).tz_localize(None)

            bars = None
            timeOutOccured = None
            [bars, timeOutOccured] = getHistoricalDataBars(ib,
                                                           qc,
                                                           endDateTime=endDateTimeLocalNaive,
                                                           durationStr=dTD.IB_Duration_String,
                                                           barSizeSetting=bTD.IB_Bar_Size_String,
                                                           timeOutTime=timeOutTimeHistoricalBars)

            if timeOutOccured is not None and timeOutOccured:
                a = (f'Timeout while requesting historical bars for contract {qc}')
                _logger.warn(a)
                pass


            # persist the result on disk
            # upsert the data
            persistMarketDataBarsOnDisk(bars, mydb, tableORM.__table__, doCorrection=True)

            # calculate the number of rows that have been retrieved

            nRows = 0
            if bars is not None:
                nRows = len(bars)
                pass

            # calculate the total number of rows on disk
            nRowsTotal = ssn.query(tableORM).count()

            # # write the information about where we are to disk
            # endDateTimeUTCNaiveAsDateTime = endDateTimeUTCNaive.to_pydatetime()

            # ib.sleep(2)
            # a = f'before removing: {ssn.query(tablePos).all()}'
            # print(a)
            # _logger.info(a)
            # # ib.sleep(2)

            # remove the old row by removing the table content
            delStmt = tablePos.delete()
            results = ssn.bind.execute(delStmt)
            ssn.commit()

            # a = f'after removing: {ssn.query(tablePos).all()}'
            # print(a)
            # _logger.info(a)
            # # ib.sleep(2)

            # add this row
            insStmt = tablePos.insert().values(
                tableName=tableORM.__tablename__,
                endDateTime=endDateTimeUTCNaive,
                durationStringOnLastUpdate=dTD.IB_Duration_String,
                numberOfTicksRetrieved=nRows,
                lastUpdateDateTimeLocal=pd.datetime.now(),
            )
            results = ssn.bind.execute(insStmt)
            ssn.commit()

            # a = f'after udpating: {ssn.query(tablePos).all()}'
            # print(a)
            # _logger.info(a)
            # # ib.sleep(2)

            # log info about the end of bar data retrieval
            tEnd2 = pd.datetime.now()
            tDelta2 = tEnd2 - tStart2
            a = (
                f'finished to get historical data chunk: {qc.symbol}, {qc.currency}; startDT: {startDateTimeUTCNaive}; endDT: {endDateTimeUTCNaive}; durationString: {dTD.IB_Duration_String}; elapsedTime: {tDelta2}; rows: {nRows}; rowsTotal: {nRowsTotal}')
            _logger.info(a)

            # update the startDateTime for the next iteration of the loop
            startDateTimeUTCNaive = endDateTimeUTCNaive

            # as this ia a potentially very long running loop, make a little pause to
            # allow other asyncio processes (for example, the processes that tet recent market data)
            # to also get a piece of the processor
            ib.sleep(0.01)


            conditionOnStartDateTime = startDateTimeUTCNaive <= pd.to_datetime(pd.datetime.utcnow()).ceil(barSizePandasTimeDelta)

            # a = (f'while end: startDateTimeUTCNaive {startDateTimeUTCNaive}, now {pd.to_datetime(pd.datetime.utcnow()).ceil(barSizePandasTimeDelta)}, condition {conditionOnStartDateTime}')
            # _logger.info(a)
            # print(a)

            pass

        tEnd1 = pd.datetime.now()
        tDelta1 = tEnd1 - tStart1
        a = (f'finished to get historical data: {qc.symbol}, {qc.currency}; elapsedTime: {tDelta1}')
        _logger.info(a)
        # print(a)
        pass
    success = True
    ssn.close()
    # a = (f'finished function to get historical data')
    # _logger.info(a)
    # print(a)

    return success


