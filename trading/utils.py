import logging
import datetime
from sqlalchemy.orm.util import class_mapper
from sqlalchemy.orm.base import object_mapper
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.sql.schema import Table
from ib_insync import Contract, IB
import asyncio
from trading import exceptions
import pandas as pd
import numpy as np
_logger = logging.getLogger(__name__)

dictSecTypeToWhatToShow = {
    'IND': 'TRADES',
    'STK': 'MIDPOINT',
    'CASH': 'MIDPOINT',
    'CFD': 'MIDPOINT',
    'FUT': 'MIDPOINT',
}

class CTimeDelta(object):
    """creates IB date strings based on pandasTimeDelta"""
    def __init__(self, pandasTimeDelta: pd.Timedelta=None):
        self.__possibleDeltas = ['years','months','weeks','days','hours','minutes','seconds']
        self._timeDelta = pandasTimeDelta
        self._seconds = self._timeDelta.total_seconds()
        self._IB_Duration_String = None
        self._IB_Bar_String_String = None
        self._allowedSeconds = [1,5,10,15,30]
        self._allowedMinutes = [1,2,3,5,10,15,20,30]
        self._allowedHours = [1,2,3,4,8]
        self._allowedDays = [1]
        self._allowedWeeks = [1]


        pass

    @property
    def IB_Duration_String(self):
        """I'm the 'IB_Duration_String' property."""
        strIBDuration = ''
        scds = (self._seconds)
        if scds <= 60*60*24: # IB does not allow to request more than 24*3600 seconds
            secs = int(np.ceil(scds))
            strIBDuration = f'{secs} S'
        elif scds <= 60*60*24*7:
            # IB has a strange way of interpreting days: 2 D is not 2*24*3600, but 1*24*3600 plus whatever there is that IB counts as the last day
            # to make sure that the entire date range is found we have to add +1
            days = int(np.ceil((scds)/(60*60*24)))+1
            strIBDuration = f'{days} D'
        else:
            weeks = int(np.ceil((scds+1)/(60*60*24*7)))+1
            strIBDuration = f'{weeks} W'
        return strIBDuration

    @property
    def IB_Bar_Size_String(self):
        """I'm the 'IB_Bar Size String' property."""
        strIBBarSize = ''
        scds = int(self._seconds)
        if scds < 1:
            strIBBarSize = '1 secs'
            pass
        elif scds < 60:
            aa = np.array(self._allowedSeconds)
            seconds = aa[aa<=scds][-1]
            strIBBarSize = f'{seconds} secs'
            pass
        elif scds<60*60:
            aa = np.array(self._allowedMinutes)
            minutes = aa[aa<=scds/60][-1]
            strIBBarSize = f'{minutes} mins'
            if minutes == 1:
                strIBBarSize = f'{minutes} min'
                pass
            pass
        elif scds < 60*60*24:
            aa = np.array(self._allowedHours)
            hours = aa[aa<=scds/(60*60)][-1]
            strIBBarSize = f'{hours} hours'
            if minutes == 1:
                strIBBarSize = f'{hours} hour'
                pass
            pass
        elif scds < 60*60*24*7:
            aa = np.array(self._allowedDays)
            days = aa[aa<=scds/(60*60*24)][-1]
            strIBBarSize = f'{days} day'
            pass
        else:
            aa = np.array(self._allowedWeeks)
            weeks = aa[aa<=scds/(60*60*24*7)][-1]
            strIBBarSize = f'{weeks} week'
            pass
        return(strIBBarSize)

# def more_logging():
#     # just example code
#     # # create a file handler
#     # handler = logging.FileHandler('hello.log', mode='w')  # recreate file on each run
#     # handler.setLevel(logging.INFO)
#     #
#     # # create a logging format
#     # FORMAT = "%(filename)s:%(lineno)s - %(asctime)s - %(funcName)20s()  %(message)s"
#     # # FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     # formatter = logging.Formatter(FORMAT)
#     # handler.setFormatter(formatter)
#     #
#     # # add the handlers to the logger
#     # logger.addHandler(handler)
#     #
#     # # create console handler and set level to debug
#     # ch = logging.StreamHandler()
#     # ch.setLevel(logging.DEBUG)
#     #
#     # # add formatter to ch
#     # ch.setFormatter(formatter)
#     #
#     # # add ch to logger
#     # logger.addHandler(ch)
#     pass


# def load_df(strPicklePath: str = 'df3.pickle') -> pd.DataFrame():
#     """
#     Loads a dataframe
#     :return:
#     """
#     df = pd.read_pickle(strPicklePath)
#     df.date = df.date.dt.tz_localize(None)
#     df.columns = ['datetime', 'close', 'cur']
#     df.drop(columns=['cur'],inplace=True)
#     df.set_index('datetime', inplace=True, drop=True)
#     return (df)
#
#
# def load_dfs():
#     curs = ['USD','GBP','CHF','AUD','CAD','CNH','JPY','RUB']
#     # curs = ['USD', 'GBP', 'CHF']
#     dfs = []
#     if 1:
#         for cur in curs:
#             for f in glob.glob(f'df_{cur}_05.pickle'):
#                 df = load_df(f)
#                 df.cur = cur # metadata for the dataframe; this is an added attribute that is only present because I add it here
#                 dfs.append(df)
#                 pass
#             pass
#     return dfs

def _is_sa_class_mapped(cls):
    try:
        class_mapper(cls)
        return True
    except:
        return False


def _is_sa_object_mapped(obj):
    try:
        object_mapper(obj)
    except UnmappedInstanceError:
        return False
    return True

def _is_sa_table(obj):
    return isinstance(obj,Table)

def convertTableRowToDict(tableRow):
    d = tableRow.__dict__.copy()
    keys = d.keys()
    keys2 = tableRow.__table__.columns.keys()
    for superfluousKey in (set(keys) - set(keys2)):
        del (d[superfluousKey])
        pass
    return (d)

def getQualifiedContractFromConId(ib,conId, timeOutTime=10):
    # ib must exist and connected

    if not (isinstance(ib,IB) and ib.isConnected()):
        return(None)

    c = Contract(conId=conId)
    qcs = None
    try:
        req = ib.qualifyContractsAsync(c)
        qcs = ib.run(asyncio.wait_for(req, timeOutTime))
    except asyncio.TimeoutError:
        a = (f'Timeout while requesting the qualification of contract: {c}')
        _logger.warn(a)
    except Exception as excp:
        pass

    if qcs is None or len(qcs) != 1:
        raise exceptions.CouldNotQualifyContract(c)
    else:
        qc = qcs[0]
        pass

    return (qc)

def conformDateTimeToPandasUTCNaive(a):
    pdDT = None
    pdDTUTC = None
    pdDTUTCNaive = None

    if pd.isnull(a):
        return pdDTUTCNaive

    if hasattr(a,'tzinfo'):
        if a.tzinfo is not None:
            pdDTUTCNaive = pd.to_datetime(a).tz_convert('UTC').tz_localize(None)
            pass
        else:
            pdDTUTCNaive = pd.to_datetime(a)
            pass
        pass
    return pdDTUTCNaive

def getValueFromDataFrame(df, whereColumn, whereValue, getColumn):
    results = df.loc[df[whereColumn]==whereValue,getColumn].values
    if len(results) != 1:
        return None
    return (results[0])

def getEarliestDateTimeFromIBAsDateTime(ib, qualifiedContract=None, **kwargs):

    if qualifiedContract is None:
        return (None)

    if not (isinstance(ib,IB) and ib.isConnected()):
        return(None)

    whatToShow = dictSecTypeToWhatToShow.get(qualifiedContract.secType,None)
    useRTH = kwargs.pop('useRTH',False)
    # formatDate = kwargs.pop('formatDate',2)
    timeOutTime = kwargs.pop('timeOutTime', 1)

    # sometimes, this request just hangs.
    # we implement a safeguard against that by only giving the request
    # a given amount of time
    # if the amount of time is exceeded, the program returns None

    eDT = None
    try:
        req = ib.reqHeadTimeStampAsync(qualifiedContract, whatToShow=whatToShow, useRTH=useRTH, formatDate=2, **kwargs)
        eDT = ib.run(asyncio.wait_for(req, timeOutTime))
    except asyncio.TimeoutError:
        a = (f'Timeout while requesting the earliestDateTime for contract {qualifiedContract}')
        _logger.warn(a)
    except Exception as excp:
        pass

    return(eDT)

def logToFile(path, loggerName = None, level=logging.INFO, ibapiLevel=logging.ERROR):
    """
    Create a log handler that logs to the given file.
    """
    logger = logging.getLogger(loggerName)
    logger.setLevel(level)
    setIBAPILogLevel(ibapiLevel)
    formatter = logging.Formatter(
            '%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s')
    handler = logging.FileHandler(path)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def logToConsole(loggerName = None, level=logging.INFO, ibapiLevel=logging.ERROR):
    """
    Create a log handler that logs to the console.
    """
    logger = logging.getLogger(loggerName)
    logger.setLevel(level)
    setIBAPILogLevel(ibapiLevel)
    formatter = logging.Formatter(
            '%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def setIBAPILogLevel(level=logging.ERROR):
    """
    The IBAPI uses excessive debug logging on the root logger.
    This function patches IBAPI to give it its own private logger.
    Setting a sticter log level will then cut the amount of logging.
    """
    from ibapi import client, decoder, reader
    logger = logging.getLogger('ibapi')
    logger.getLogger = lambda: logger
    logger.INFO = logging.INFO
    logger.setLevel(level)
    client.logging = decoder.logging = reader.logging = logger