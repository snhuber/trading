from trading import utils, database
import logging
from ib_insync import util, IB
from sqlalchemy import desc, func, update
import pandas as pd
from random import randint
import asyncio
import dateutil

_logger = logging.getLogger(__name__)


def getHistoricalDataBars(ib, qc, endDateTime: str = '', durationStr: str = '600 S', barSizeSetting: str = '1 min', timeOutTime=600) -> list:
    """request historical data async"""
    whatToShow = utils.dictSecTypeToWhatToShow.get(qc.secType, None)

    bars = None
    timeOutOccurred = False
    try:
        req = ib.reqHistoricalDataAsync(qc,
                                           endDateTime=endDateTime,  # pd.to_datetime("2018-04-01"),
                                           durationStr=durationStr,
                                           barSizeSetting=barSizeSetting,
                                           whatToShow=whatToShow,
                                           formatDate=2,
                                           keepUpToDate=False,
                                           useRTH=False)
        bars = ib.run(asyncio.wait_for(req, timeOutTime))
    except asyncio.TimeoutError:
        a = (f'Timeout while requesting historical bars for contract {qc}')
        _logger.warn(a)
        timeOutOccurred = True
        pass

    return [bars, timeOutOccurred]

def getLastDateTimeOnDiskUTC(mydb, table):
    # all data on disk are timezone-unaware times that are in UTC
    result = None
    ssn = mydb.Session()
    lDT = ssn.query(table.c.datetime).order_by(desc(table.c.datetime)).first()
    if lDT is not None and len(lDT) == 1:
        lDT = lDT[0]
    else:
        return (result)

    lDT = pd.to_datetime(lDT)
    lDT = lDT.tz_localize('UTC')
    result = lDT

    ssn.close()

    return (result)

def getEarliestDateTimeUTCFromDataFrame(mydb: database.tradingDB, conId):
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
    eDT = eDT.tz_localize('UTC')
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

            if doCorrection:
                df = mydb.correctDiffDateTimesForMarketDataTable(tableName=tableSchema.name,
                                                                 startDateTime=previousDateTime,
                                                                 endDateTime=lastDateTime,
                                                                 doCorrection=doCorrection)
                pass

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


        lastDateTimeOnDiskUTC = getLastDateTimeOnDiskUTC(mydb, tableORM.__table__)
        earliestDateTimeUTCAccordingToIB = getEarliestDateTimeUTCFromDataFrame(mydb, conId)

        # if there is no earliestDateTime according to IB, it makes no sense to try to retrieve data
        if pd.isnull(earliestDateTimeUTCAccordingToIB):
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
        # the end time is therefore utcnow
        nowUTC = pd.to_datetime(pd.datetime.utcnow()).tz_localize('UTC').floor('1 Min')
        endDateTimeUTC = nowUTC

        # the earliest possible start date time is endtime minus the maximum allowed time delta
        earliestStartDateTimeUTC = endDateTimeUTC - durationMaximumPandasTimeDelta

        # by default, the startDateTime is this earliest possible datetime
        startDateTimeUTC = earliestStartDateTimeUTC

        # if we found a lastDateTime on Disk, the startDateTime is modified
        if not pd.isnull(lastDateTimeOnDiskUTC):
            startDateTimeUTC = max(lastDateTimeOnDiskUTC, earliestStartDateTimeUTC)
            pass

        # for some reason, it is possible that the time on disk is in the future.
        # we therefore determine the startDateTime as the minimum between the startDateTime as calculated before
        # and the endDateTime
        startDateTimeUTC = min(startDateTimeUTC, endDateTimeUTC)

        # we can now calculate the time delta that we actually want to fetch
        durationPandasTimeDelta = endDateTimeUTC - startDateTimeUTC

        # define the objects used to calculate the IB strings for time deltas and bar size settings
        dTD = utils.CTimeDelta(durationPandasTimeDelta)
        bTD = utils.CTimeDelta(barSizePandasTimeDelta)

        # log info about the start of bar data retrieval
        tStart2 = pd.datetime.now()
        if not pd.isnull(earliestDateTimeUTCAccordingToIB):
            a = (
                f'attempting to get recent historical data chunk: {qc.symbol}, {qc.currency}; startDT: {startDateTimeUTC} (durationString: {dTD.IB_Duration_String}); time: {tStart2}')
        else:
            a = (
                f'attempting to get recent historical data chunk: {qc.symbol}, {qc.currency}; not performed because earliestDateTime is not given by IB; time: {tStart2}')
            pass
        _logger.info(a)


        # get the historical data bars
        # only get bars if the last value on disk is not utcnow
        bars = None
        timeOutOccured = None
        if not (nowUTC == lastDateTimeOnDiskUTC):
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
        a = (f'finished to get recent historical data chunk: {qc.symbol}, {qc.currency}; startDT: {startDateTimeUTC} (durationString: {dTD.IB_Duration_String}); time: {tEnd2}; elapsedTime: {tDelta2}; rows: {nRows}; rowsTotal: {nRowsTotal}')
        _logger.info(a)
        success = True
        ssn.close()
        return success


    return asyncioJobGetRecentData


def getCurrentInfoForHistoricalDataRequests(mydb, tablePos):
    # retrieve the current information about where we are in the historical data request task
    tableNameOnDisk = None
    startDateTimeOnDiskUTC = None
    ssn = mydb.Session()
    for row in ssn.query(tablePos).order_by(tablePos.c.tableName):
        tableNameOnDisk = row.tableName
        startDateTimeOnDiskUTCNaive = row.endDateTime
        startDateTimeOnDiskUTC = pd.to_datetime(startDateTimeOnDiskUTCNaive).tz_localize('UTC')
        break
    ssn.close()
    return([tableNameOnDisk, startDateTimeOnDiskUTC])



def getNewStartDateTime(ssn, tableORM, oldStartDateTimeUTC, diffTimeInMinutes):
    """finds the next startDateTime at the beginning of a gap"""
    startDateTimeIsOnDisk = bool(ssn.query(tableORM).filter(tableORM.datetime == oldStartDateTimeUTC).count() == 1)
    dateTimeAtStartOfNextGapUTC = None
    if startDateTimeIsOnDisk:
        a = (f'startDateTIme {oldStartDateTimeUTC} is on Disk')
        _logger.info(a)
    else:
        a = (f'startDateTIme {oldStartDateTimeUTC} is not on Disk')
        _logger.info(a)
        pass
    if startDateTimeIsOnDisk:
        # advance startDateTime to the beginning of the next gap (if a next gap is found)
        rowAtEndOfContiguousData = ssn.query(tableORM).filter(tableORM.diffToNextRowInMinutes > diffTimeInMinutes).filter(
            tableORM.datetime >= oldStartDateTimeUTC).order_by(tableORM.datetime).first()
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
            dateTimeAtStartOfNextGapUTC = pd.to_datetime(dateTimeAtStartOfNextGap).tz_localize('UTC')
            pass
        pass
    return (dateTimeAtStartOfNextGapUTC)


async def asyncioJobGetHistoricalData(ib, mydb: database.tradingDB, qcs, additionalArgs={}):
    """get all available historical data for the contracts specified in qcs"""
    success = False
    if not ib.isConnected():
        return success


    # create a session for this task
    ssn = mydb.Session()

    # the table with the information about the position in the historical data request task
    tablePos = mydb.MarketDataHistoricalRequestsPositionTable.__table__

    # get the info about where we are in the historical data request task
    [tableNameOnDisk, startDateTimeOnDiskUTC] = getCurrentInfoForHistoricalDataRequests(mydb,tablePos)

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
        if startDateTimeOnDiskUTC is not None:
            if startDateTimeOnDiskUTC >= pd.to_datetime(pd.datetime.utcnow()).tz_localize('UTC'):
                delStmt = tablePos.delete()
                results = ssn.bind.execute(delStmt)
                ssn.commit()
                pass
            pass
        pass

    # retrieve historical data, starting at the position where we were last time this program ran
    for qc in qcs[indexToStart:]:
        # this can be a long task if it is not interrupted.

        # we record the starting time
        tStart1 = pd.datetime.now()

        # the conId for this contract
        conId = qc.conId

        # the table into which we want to insert
        tableORM = utils.getValueFromDataFrame(mydb.MarketDataInfoTableDataFrame,whereColumn='conId', whereValue=conId, getColumn='tableORM')

        # get the earliest dateTime for this conId defined by IB
        earliestDateTimeUTCAccordingToIB = getEarliestDateTimeUTCFromDataFrame(mydb, conId)
        # if there is no earliestDateTime according to IB, it makes no sense to try to retrieve data
        if pd.isnull(earliestDateTimeUTCAccordingToIB):
            a = (f'attempting to get historical data: {qc.symbol}, {qc.currency}; not performed because earliestDateTime is not given by IB; time: {tStart1}')
        else:
            a = (f'attempting to get historical data: {qc.symbol}, {qc.currency}; is going to be performed because earliestDateTime is given by IB; time: {tStart1}')
            pass

        _logger.info(a)

        # we did not break the for loop in the rows above because we wanted to log some items.
        # we are now breaking the loop if no earliestDateTime is found
        if pd.isnull(earliestDateTimeUTCAccordingToIB):
            # it makes no sense to continue if IB has no earliestDateTime for this conId
            # we therefore skip all code below and continue the for loop, essentially meaning that the next conId
            # will be treated
            continue
            pass



        # local timezone
        tzlocal = dateutil.tz.tzlocal()



        # override the earliestDateTime if an argument is given
        earliestDateTimeUTC = additionalArgs.get('earliestDateTimeUTC', earliestDateTimeUTCAccordingToIB)

        startDateTimeUTC = earliestDateTimeUTC
        # if there is inofrmation in the position table, take this information
        if startDateTimeOnDiskUTC is not None:
            # only take the information if it concerncs the table that we are currently treating
            if tableNameOnDisk == tableORM.__tablename__:
                startDateTimeUTC = startDateTimeOnDiskUTC
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
        jitterSpanDivider = additionalArgs.get('jitterSpanDivider',50)

        # create the timeDelta objects that generate the IB strings
        dTDRegular = utils.CTimeDelta(durationPandasTimeDelta)
        dTD = dTDRegular
        bTD = utils.CTimeDelta(barSizePandasTimeDelta)
        diffTimeInMinutes = int(bTD._timeDelta / pd.Timedelta(minutes=1))



        while (startDateTimeUTC is not None) and ((startDateTimeUTC) <= pd.to_datetime(pd.datetime.utcnow()).tz_localize('UTC').ceil('1 Min')):

            a = (f'startDateTIme Original {startDateTimeUTC}')
            _logger.info(a)

            # calculate the regular endDateTIme
            endDateTimeUTC = startDateTimeUTC + dTD._timeDelta
            endDateTimeUTCRegular = endDateTimeUTC
            a = (f'calculated endDateTime {endDateTimeUTC}')
            _logger.info(a)

            # initialize the durationTimeDelta Object
            dTD = dTDRegular

            if 0:
                ## skipping data that already exists
                # Idea: if the startDateTime is on disk, then
                #          advance startDateTime to the beginning of the next gap (if a next gap is found)
                newStartDateTimeUTC = getNewStartDateTime(ssn=ssn, tableORM=tableORM, oldStartDateTimeUTC=startDateTimeUTC, diffTimeInMinutes=diffTimeInMinutes)
                if newStartDateTimeUTC is not None:
                    a = (f'advancing to new StartDateTime {newStartDateTimeUTC}')
                    _logger.info(a)
                    startDateTimeUTC = newStartDateTimeUTC
                    # continue the while loop
                    continue
                    pass


                # the recalculated startDateTime above has placed us in a gap by design
                # test that this is correct
                startDateTimeIsOnDisk = bool(
                    ssn.query(tableORM).filter(tableORM.datetime == startDateTimeUTC).count() == 1)
                assert(not startDateTimeIsOnDisk)

                # now find the end of the gap
                rowWhereGapStops = ssn.query(tableORM).filter(tableORM.diffToNextRowInMinutes<=diffTimeInMinutes).filter(tableORM.datetime>=startDateTimeUTC).order_by(tableORM.datetime).first()
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
                    endDateTimeWhereGapStopsUTC = pd.to_datetime(rowWhereGapStops.datetime).tz_localize('UTC')
                    a = (f'endDatetime calculated using the end of the gap: {endDateTimeWhereGapStopsUTC}')
                    _logger.info(a)
                    endDateTimeUTC = min(endDateTimeUTCRegular, endDateTimeWhereGapStopsUTC)
                    a = (f'now setting a new endDateTime that is the minumum of the endDateTime where the gap stops and the regular endDateTime : {endDateTimeUTC}')
                    _logger.info(a)
                    # because we have redefined the endDateTime, we have to generate a revised TimeDelta object
                    pandasTimeDelta = pd.Timedelta(seconds=(endDateTimeUTC - startDateTimeUTC).total_seconds())
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
            durationTimeInMinutes = int((endDateTimeUTC - startDateTimeUTC) / pd.Timedelta(1, 'm'))
            jitterSpan = durationTimeInMinutes / jitterSpanDivider
            jitterInMinutes = randint(0,int(jitterSpan))
            jitterTimeDelta = pd.Timedelta(jitterInMinutes,'m')
            startDateTimeUTC = startDateTimeUTC - jitterTimeDelta
            # because we now have a startDateTime that is prior to the original one, we are extending the duration for which we want to fetch data
            # such that the original endDateTime is met
            pandasTimeDelta = dTD._timeDelta
            pandasTimeDelta = pandasTimeDelta + jitterTimeDelta
            dTD = utils.CTimeDelta(pandasTimeDelta)

            # specify the endDateTime
            endDateTimeUTC = startDateTimeUTC + dTD._timeDelta

            # log info about chunk getting
            tStart2 = pd.datetime.now()
            a = (
                f'attempting to get historical data chunk: {qc.symbol}, {qc.currency}; startDT: {startDateTimeUTC}; endDT: {endDateTimeUTC}; durationString: {dTD.IB_Duration_String}; time: {tStart2}')
            _logger.info(a)

            # get the historical bars
            endDateTimeLocal = endDateTimeUTC.tz_convert(tzlocal)
            endDateTimeLocalNaive = endDateTimeLocal.tz_localize(None)

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

            # write the information about where we are to disk
            endDateTimeUTCNaiveAsDateTime = endDateTimeUTC.tz_localize(None).to_pydatetime()
            # remove the old row
            delStmt = tablePos.delete()
            results = ssn.bind.execute(delStmt)
            ssn.commit()
            # add this row
            insStmt = tablePos.insert().values(
                tableName=tableORM.__tablename__,
                endDateTime=endDateTimeUTCNaiveAsDateTime,
                durationStringOnLastUpdate=dTD.IB_Duration_String,
                numberOfTicksRetrieved=nRows,
                lastUpdateDateTimeLocal=pd.datetime.now(),
            )
            results = ssn.bind.execute(insStmt)
            ssn.commit()

            # log info about the end of bar data retrieval
            tEnd2 = pd.datetime.now()
            tDelta2 = tEnd2 - tStart2
            a = (
                f'finished to get historical data chunk: {qc.symbol}, {qc.currency}; startDT: {startDateTimeUTC}; endDT: {endDateTimeUTC}; durationString: {dTD.IB_Duration_String}; time: {tEnd2}; elapsedTime: {tDelta2}; rows: {nRows}; rowsTotal: {nRowsTotal}')
            _logger.info(a)

            # update the startDateTime for the next iteration of the loop
            startDateTimeUTC = endDateTimeUTC

            # as this ia a potentially very long running loop, make a little pause to
            # allow other asyncio processes (for example, the processes that tet recent market data)
            # to also get a piece of the processor
            await asyncio.sleep(0.01)

            pass

        tEnd1 = pd.datetime.now()
        tDelta1 = tEnd1 - tStart1
        a = (f'finished to get historical data: {qc.symbol}, {qc.currency}; time: {tEnd1}; elapsedTime: {tDelta1}')
        _logger.info(a)
        pass
    success = True
    ssn.close()
    return success


