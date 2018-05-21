import sys
import pandas as pd
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base, declared_attr, AbstractConcreteBase
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Float, ForeignKey, func, Numeric, Table
from sqlalchemy.orm import scoped_session, sessionmaker, relationship, configure_mappers, joinedload
import logging
import time
import os
import os.path
from ib_insync import IB
from trading import utils
from trading import upsert as MyUpsert
from enum import Enum
from ib_insync import Contract, IB
from configparser import ConfigParser, ExtendedInterpolation
from collections import OrderedDict


from sqlalchemy.ext.automap import automap_base

# the following is entered sometimes; but not, for example, when ib.connect() fails due to the clientID already taken
def myErrorCallback(reqId, errorCode, errorString , contract):
    """ib_insycn error callback. Is not called when ib.connect() is called with clientID already in use"""
    # print("myErrorCallback", reqId,errorCode,errorString,contract)
    # print("myErrorCallback", reqId, errorCode, errorString, contract)

class tableCategories(Enum):
    MarketData = 'MarketData'


class tradingDB(object):
    """Class for database management"""

    __slots__ = [
        '__DBType',
        '__DBFileName',
        '__DBEngine',
        '__classRegistry',
        '__DBDeclarativeBase',
        '_logger',
        '_loggerSQLAlchemy',
        '__MarketDataInfoTableDataFrame',
        'tableCategories',
        'MarketDataInfoTable',
        'MarketDataHistoricalRequestsPositionTable',
        'MarketDataTableBase',
        'Session',
        'upsert',
     ]


    def __init__(self, **kwargs):
        """Constructor for myDB"""
        DBType = kwargs.get('DBType','sqlite')
        DBFileName = kwargs.get('DBFileName',os.path.abspath(os.path.expanduser('~/tradingData/ttest.sqlite')))
        self.__DBType = DBType
        self.__DBFileName = DBFileName
        self.__DBEngine = self.__createDBEngine()
        self.__DBEngine.echo = False
        self.__classRegistry = {}
        self.__DBDeclarativeBase = self.__createDBDeclarativeBase()
        self.Session = self.__createSession()
        self.Session = self.__createSession()
        self.upsert = self.__createUpsert()

        self.MarketDataTableBase = self.__createMarketDataTableBaseClass()
        self.MarketDataInfoTable = self.__createMarketDataInfoTableClass()
        self.MarketDataHistoricalRequestsPositionTable = self.__createMarketDataHistoricalRequestsPositionTableClass()

        # if no tables have been created on disk, the following will be None
        self.__MarketDataInfoTableDataFrame = self.createMarketDataInfoTableDataFrameFromMarketDataInfoTable()


        self.tableCategories = tableCategories

        self.__setupLogging()

        pass



    def getTableORMByTablename(self, tableName):
        """Return class reference mapped to table.

        :param tablename: String with name of table.
        :return: Class reference or None.
        """
        tableORM = self.__classRegistry.get(tableName,None)
        if tableORM is None:
            tableORM = self.createMarketDataTableClass(tableName=tableName)
            pass

        return (tableORM)


    def createMarketDataInfoTableDataFrameFromMarketDataInfoTable(self, ib = None, timeOutTime=10):
        """this DataFrame has the same columns as the MarketDataInfoTable.
        In addition, we add the columns
        'talbeORM´, 'qualifiedContract'

        Make sure that this function is called each time the MarketDataInfoTable is updated
        """

        # if the table does not exist on disk, return None
        if not self.DBEngine.has_table('MarketDataInfoTable'):
            return None

        ssn = self.Session()

        # if the table contains no rows, return None
        if ssn.query(self.MarketDataInfoTable).count() == 0:
            ssn.close()
            return None


        table = self.MarketDataInfoTable
        query = ssn.query(table).order_by(table.tableName)
        df = pd.read_sql(query.statement, ssn.bind)
        tableNames = [r.tableName for r in ssn.query(table.tableName).order_by(table.tableName).all()]
        tableORMs = []
        for tableName in tableNames:
            tableORMs.append(self.getTableORMByTablename(tableName))
            pass
        df.loc[:, 'tableORM'] = tableORMs
        qcs = None
        if (isinstance(ib,IB) and ib.isConnected()):
            qcs = [utils.getQualifiedContractFromConId(ib=ib, conId=row.conId, timeOutTime=timeOutTime)  for (indx,row) in df.iterrows()]
        df.loc[:, 'qualifiedContract'] = qcs

        ssn.close()
        return (df)

    def upsertDataFrame(self, df, tableSchema, chunkSize=100000):
        """upsert a dataframe df to the table tableSchema (reflect table)

        * remove all columns from df that are not in tableSchema
        * convert date/datetime columns to datetime.datetime objects
        """
        # drop all columns not in tbl
        df = df.drop(columns=df.columns.difference(tableSchema.c.keys()))
        # date columns
        dateColumns = list(df.select_dtypes(include=['datetimetz','datetime64']).head(1).columns.values)
        # remove timezone info; data is always in UTC
        for cH in dateColumns:
            df[cH] = pd.to_datetime(df[cH]).dt.tz_localize(None)
            pass
        # convert to dict
        d = df.to_dict(orient='records')
        # convert datetime columns to datetime.datetime values (necessary for mysql)
        for dd in d:
            for cH in dateColumns:
                dd[cH] = dd[cH].to_pydatetime()
                pass
            pass
        if tableSchema is not None and len(d) > 0:
            ssn = self.Session()
            if chunkSize is not None and chunkSize > 1: chunks = [d[x:x + chunkSize] for x in range(0, len(d), chunkSize)]
            else: chunks = [d]
            for i,chunk in enumerate(chunks):
                # print(i,len(chunks))
                ssn.execute(self.upsert(tableSchema, chunk))
                pass
            ssn.commit()
            ssn.close()
            pass
        pass


    def getValueFromMarketDataInfoTable(self,
                                            whereColumn: str=None,
                                            whereValue=None,
                                            getColumn: str=None,
                                        ):
        """ gets the value in the column getColumn from the row for which the column whereColumn has the value whereValue
        raises an error if more than one matching row is found
        returns None if no matching row is found"""

        table = self.MarketDataInfoTable

        ssn = self.Session()
        # raises error if more than one row found
        retVal = ssn.query(getattr(table, getColumn)).order_by(table.tableName).filter(getattr(table,whereColumn)==whereValue).scalar()
        return retVal


    def updateMarketDataInfoTableWithEarliestDateTimeFromIB(self,ib, timeOutTime=10):
        """updates earliestDateTime in the MarketDataInfoTable
        Does not use MarketDataInfoTableDataFrame for the looping.
        Updates MarketDataInfoTableDataFrame at the end"""

        if not (isinstance(ib, IB) and ib.isConnected()):
            return None

        ssn = self.Session()
        table = self.MarketDataInfoTable

        # loop over the rows of MarketDataInfoTable
        for row in ssn.query(table).order(table.tableName):

            tableName = row.tableName
            tableORM = self.getTableORMByTablename(tableName)

            conId = row.conId
            # get the qualified Contract for this conId from IB
            qc = utils.getQualifiedContractFromConId(ib=ib,conId=conId,timeOutTime=timeOutTime)
            # get the earliest DateTime for this conId from IB
            eDT = utils.getEarliestDateTimeFromIBAsDateTime(ib, qualifiedContract=qc, timeOutTime=timeOutTime)

            row.earliestDateTime = eDT
            pass

        ssn.commit()
        ssn.close()

        self.MarketDataInfoTableDataFrame = self.createMarketDataInfoTableDataFrameFromMarketDataInfoTable(ib=ib,timeOutTime=timeOutTime)
        pass


    def instantiateExistingTablesAndClasses(self, ib=None, timeOutTime=10):
        """recreate all the SA classes based on the tables found on disk and fills the MarketDataInfoTableDataFrame"""
        # the table MarketDataInfoTable contains all tables with market data info
        tableMDIT = self.MarketDataInfoTable
        ssn = self.Session()
        # loop over rows on disk (do not use MarketDataInfoTableDataFrame)
        for row in ssn.query(tableMDIT).order_by(tableMDIT.tableName).all():
            tableName = row.tableName
            tableORM = self.getTableORMByTablename(tableName)
            conId = row.conId
            qc = utils.getQualifiedContractFromConId(ib=ib, conId=conId, timeOutTime=timeOutTime)
        ssn.close()
        self.MarketDataInfoTableDataFrame = self.createMarketDataInfoTableDataFrameFromMarketDataInfoTable(ib=ib,timeOutTime=timeOutTime)
        pass


    def calculateTableName(self, **kwargs):

        category = kwargs.get('category', None)
        secType = kwargs.get('secType', None)
        symbol = kwargs.get('symbol', None)
        currency = kwargs.get('currency', None)
        exchange = kwargs.get('exchange', None)

        tableName = f'{category}_{secType}_{symbol}_{currency}_{exchange}'
        return (tableName)



    def __setupLogging(self):
        self._logger = logging.getLogger('tradingDB')
        self._logger.setLevel(logging.INFO)
        # sqlalchemy logging
        # logger.disabled=True does not work when using 'sqlalchemy'
        # looger.disabled=True does work when using 'sqlalchemy.engine.base.Engine'
        self._loggerSQLAlchemy = logging.getLogger('sqlalchemy')
        self._loggerSQLAlchemy.setLevel(logging.INFO)

        pass


    def __createSession(self):
        """creates a Session that is ready for multithreaded access to the databse"""
        session_factory = sessionmaker(bind=self.__DBEngine)
        return (scoped_session(session_factory))


    # the tables

    def __createMarketDataHistoricalRequestsPositionTableClass(self):
        """Creates the sqlalchemy class that holds the table that indicates where we are
        within the retrieval of the historical market data."""
        Base = self.__DBDeclarativeBase

        class MarketDataHistoricalRequestsPositionTable(Base):
            """Class that holds the information about where we are within the retrieval of historical market data."""

            __tablename__ = 'MarketDataHistoricalRequestsPositionTable'

            tableName = Column(String(1000), primary_key=True) # this is the name of the table where we are at the moment
            endDateTime = Column(DateTime(timezone=False)) # this is the last datetime for which we retrieved data
            numberOfTicksRetrieved = Column(Integer) # the number of Ticks that were retrieved on last Update
            durationStringOnLastUpdate = Column(String(1000)) # the durationString passed to IB on last update
            lastUpdateDateTimeLocal = Column(DateTime(timezone=False)) # this is the last datetime at which we retrieved data

            def __repr__(self):
                a = f'<tableName={self.tableName}, endDateTime={self.endDateTime}, lastUpdateTimeLocal={lastUpdateTimeLocal}'
                return (a)

            pass
        self.__classRegistry[MarketDataHistoricalRequestsPositionTable.__tablename__] = MarketDataHistoricalRequestsPositionTable
        return MarketDataHistoricalRequestsPositionTable

    def __createMarketDataInfoTableClass(self):
        """Creates the sqlalchemy class that holds the market data info table."""
        Base = self.__DBDeclarativeBase

        class MarketDataInfoTable(Base):
            """Class that holds the market data info table.
            Each row in this table corresponds to one table that contains market data"""

            __tablename__ = 'MarketDataInfoTable'

            tableName = Column(String(1000), primary_key=True)
            description = Column(String(10000))
            conId = Column(Integer)
            symbol = Column(String(1000))
            currency = Column(String(1000))
            exchange = Column(String(1000))
            category = Column(String(1000))
            secType = Column(String(1000))
            earliestDateTime = Column(DateTime(timezone=False))

            @classmethod
            def setEarliestDateTime(cls, ssn, tableName, earliestDateTime):
                member = ssn.query(Member).filter_by(tableName=tableName).one()
                member.earliestDateTime = earliestDateTime
                ssn.flush()


            def __repr__(self):
                a = f'<tableName={self.tableName}, description={self.description}, conId={self.conId}, ' + \
                    f'symbol={self.symbol}, currency={self.currency}, exchange={self.description}, category={self.category}, ' + \
                    f'secType={self.secType}, earliestDateTime={self.earliestDateTime}'
                return (a)


            pass
        self.__classRegistry[MarketDataInfoTable.__tablename__] = MarketDataInfoTable
        return MarketDataInfoTable

    def __createMarketDataTableBaseClass(self):
        """Creates the base class from which all classes with market data inherit"""
        Base = self.__DBDeclarativeBase
        class MarketDataTableBase(Base):
            """Base class for tables that contain market data.
            The actual tables that contain market data inherit from this class"""
            __abstract__ = True
            __tablename__ = 'MarketDataTableBase'

            datetime = Column(DateTime(timezone=False), primary_key=True, index=True)
            diffToNextRowInMinutes = Column(Integer) # for each row, indicates the time in minutes before the ncxt closing value is availabe in the table
                                                    # in a table ordered by datetime ASC, this means diffToNextRowInMinutes[i] = datetime[i+1] - datetime [i]
                                                    # the last entry in the table has diffToNextRowInMinutes = 0
            close = Column(Float)
            pass
        self.__classRegistry[MarketDataTableBase.__tablename__] = MarketDataTableBase
        return MarketDataTableBase


    def createMarketDataTableClass(self, tableName):
        """Creates the individual sqlalchemy classes that hold the market data."""
        class MarketDataTable(self.MarketDataTableBase):
            __tablename__ = tableName
            __name__ = tableName
        self.__classRegistry[tableName] = MarketDataTable
        return MarketDataTable



    def createAllTables(self, **kwargs):
        engine = kwargs.pop('engine', self.__DBEngine)
        self.__DBDeclarativeBase.metadata.create_all(bind=engine, **kwargs)

    def dropAllTables(self, **kwargs):
        engine = kwargs.pop('engine', self.__DBEngine)
        self.__DBDeclarativeBase.metadata.drop_all(bind=engine, **kwargs)


    def __createUpsert(self):
        if self.__DBEngine.name == 'sqlite':
            upsert = MyUpsert.UpsertSQLite
        elif self.__DBEngine.name == 'mysql':
            upsert = MyUpsert.UpsertMySQL
            pass
        return (upsert)


    def __createDBEngine(self, echo=False):
        retVal = None
        if self.__DBType.lower() == 'mysql':
            retVal = create_engine('mysql+pymysql://bn:basket05@127.0.0.1/trading', echo=echo)
        elif self.__DBType.lower() == 'sqlite':
            retVal = create_engine('sqlite:///' + self.__DBFileName, echo=echo)
        return retVal

    def __createDBDeclarativeBase(self):
        retVal = None
        if isinstance(self.__DBEngine,sqlalchemy.engine.base.Engine):
            retVal = declarative_base(bind=self.__DBEngine)
        return retVal

    def correctDiffDateTimesForMarketDataTable(self,
                                               tableName: str,
                                               startDateTime: pd.datetime = None,
                                               endDateTime: pd.datetime = None,
                                               doCorrection=True):
        """ correct diffDateTime for a marketDataTable on Disk.
        The function corrects the diffDateTimes with
          startDateTime <= dateTime <= endDateTime
        to correct these rows, we will fetch all data up to the following Row after endDateTime
        if any given row does not have a successor, its diffDateTime is 0
        if endDateTime is not given, apply the corrections for all rows >= startDateTime
        if startDateTime is not given, set startDateTime = earliest DateTime on Disk"""

        # the following will always return a table
        # the table might be empty and not exist on disk if
        # the string "tableName" is not within the strings that represent a table that
        # has been instantiated
        tableORM = self.getTableORMByTablename(tableName)

        ssn = self.Session()
        nRows = ssn.query(tableORM).count()
        if nRows == 0:
            ssn.close()
            return None

        if pd.isnull(startDateTime):
            startDateTime = ssn.query(func.min(tableORM.datetime)).scalar()
            pass

        if pd.isnull(endDateTime):
            endDateTime = ssn.query(func.max(tableORM.datetime)).scalar()

        if pd.isnull(startDateTime) or pd.isnull(endDateTime):
            ssn.close()
            return None

        assert (endDateTime >= startDateTime)

        # If the endDateTime is larger than the last dateTime on Disk,
        # then the diffMismatch calculated below for the last row
        # will be wrong (it will be the difference between the last dateTime on Disk and the endDateTime).
        # we therefore set endDateTime to the largest value on disk if it is larger than that value
        lastDateTimeOnDisk = ssn.query(func.max(tableORM.datetime)).scalar()
        if (not pd.isnull(lastDateTimeOnDisk)) and (endDateTime>lastDateTimeOnDisk):
            endDateTime = lastDateTimeOnDisk
            pass

        # find the next row after endDateTime
        followingDateTime = ssn.query(func.min(tableORM.datetime)).filter(tableORM.datetime > endDateTime).scalar()
        # print(followingDateTime)
        if followingDateTime is None:
            # no following row found.
            # we use followingDateTime below to start the loop over the rows
            # therefore, we set followingDateTime here to endDateTIme which will
            # result in the last diffDateTime to be set to 0
            followingDateTime = endDateTime
        else:
            # we found a followingDateTime
            # nothing to do
            pass

        queryWithSelectedRows = ssn.query(tableORM).filter(tableORM.datetime >= startDateTime).filter(
            tableORM.datetime <= endDateTime)
        nRows = queryWithSelectedRows.count()

        # return if no rows are found
        if nRows == 0:
            ssn.close()
            return None


        # start the repair loop by iterating over the rows in table2
        mismatchList = []
        currentDateTime = followingDateTime
        # loop from end to start
        for row in queryWithSelectedRows.order_by(tableORM.datetime.desc()):
            diffDateTimeInMinutes = int((currentDateTime - row.datetime) / pd.Timedelta(1, 'm'))
            diffToNextRowInMinutesOnDisk = row.diffToNextRowInMinutes
            diffMismatch = diffToNextRowInMinutesOnDisk - diffDateTimeInMinutes
            if diffMismatch != 0:

                row.diffToNextRowInMinutes = diffDateTimeInMinutes
                ordrdDct = OrderedDict(
                    (
                        ('tableName', tableName),
                        ('datetime', row.datetime),
                        ('diffToNextRowInMinutesOnDisk', diffToNextRowInMinutesOnDisk),
                        ('diffToNextRowInMinutesCorrected', diffDateTimeInMinutes),
                        ('diffMismatch', diffMismatch),
                    )
                )
                mismatchList.append(ordrdDct)
            currentDateTime = row.datetime
            pass

        if doCorrection:
            ssn.commit()

        ssn.close()

        df = pd.DataFrame(mismatchList)
        return (df)


    @property
    def DBType(self):
        return self.__DBType

    @DBType.setter
    def DBType(self, DBType):
        if self.__DBType != DBType:
            del self.__DBEngine
            self.__DBType = DBType
            self.__DBEngine = self.__createDBEngine()
            del self.__DBDeclarativeBase
            self.__DBDeclarativeBase = self.__createDBDeclarativeBase()
            self.upsert = self.__createUpsert()

    @DBType.deleter
    def DBType(self):
        del self.__DBType


    @property
    def DBFileName(self):
        return self.__DBFileName

    @DBFileName.setter
    def DBFileName(self, DBFileName):
        self.__DBFileName = DBFileName

    @DBFileName.deleter
    def DBFileName(self):
        del self.__DBFileName

    @property
    def DBEngine(self):
        return self.__DBEngine

    @DBEngine.setter
    def DBEngine(self, DBEngine):
        self.__DBEngine = DBEngine

    @DBEngine.deleter
    def DBEngine(self):
       del self.__DBEngine

    @property
    def DBDeclarativeBase(self):
        return self.__DBDeclarativeBase

    @DBDeclarativeBase.setter
    def DBDeclarativeBase(self, DBDeclarativeBase):
        self.__DBDeclarativeBase = DBDeclarativeBase

    @DBDeclarativeBase.deleter
    def DBDeclarativeBase(self):
       del self.__DBDeclarativeBase

    @property
    def MarketDataInfoTableDataFrame(self):
        return self.__MarketDataInfoTableDataFrame

    @MarketDataInfoTableDataFrame.setter
    def MarketDataInfoTableDataFrame(self, value):
        self.__MarketDataInfoTableDataFrame = value

    @MarketDataInfoTableDataFrame.deleter
    def MarketDataInfoTableDataFrame(self):
        del self.__MarketDataInfoTableDataFrame

def instantiateMyDB(args):
    """instantiate all SQ ORM classes using a config file passed in the arguments"""

    # load the config file
    configFile = args.configFile
    config = ConfigParser(interpolation=ExtendedInterpolation(),defaults=os.environ)
    config.read(configFile)


    # create connection to IB
    ib = IB()
    assert(isinstance(ib,IB))

    ib.errorEvent += myErrorCallback

    # load data from configFile
    a = config.get('MarketData', 'ConIdList')
    conIdList = eval(a)
    host = config.get('InteractiveBrokers', 'host')
    port = config.getint('InteractiveBrokers', 'port')
    clientId = config.getint('InteractiveBrokers', 'clientId')
    DBType = config.get('DataBase', 'DBType')
    DBFileName = config.get('DataBase', 'DBFileName')
    timeOutTime = config.getint('InteractiveBrokers', 'timeOutTimeShortRequests')

    # override configFile if clientId is given on the command line
    if args.clientId is not None:
        clientId = args.clientId

    # override configFile if timeOutTime is given on the command line
    if args.timeOutTime is not None:
        timeOutTime = args.TimeOutTime

    # connect to interactive brokers
    try:
        ib.connect(host=host, port=port, clientId=clientId)
    except:
        import random
        clientId = clientId + random.randint(1, 100000)
        ib.connect(host=host, port=port, clientId=clientId)
        pass

    # create database class
    mydb = tradingDB(DBType=DBType,DBFileName=DBFileName)

    # loop over all conIds defined in the config File and create the sqlalchemy ORM classes
    # these tables will appear in the metadata of the DBDeclarativeBase attribute of mydb
    # prepare a dataframe that holds all infos that should be put into the MarketDataInfoTable on disk
    # and the MarketDataInfoTableDataFrame in memory
    nTables = len(conIdList)
    featureList = [
        'conId',
        'qualifiedContract',
        'earliestDateTime',
        'category',
        'kwargs',
        'tableName',
        'tableORM',
    ]
    dfWithInfoAboutTables = pd.DataFrame(None, index=range(nTables), columns=featureList)
    dfWithInfoAboutTables.loc[:,'conId'] = conIdList


    df = dfWithInfoAboutTables
    for indx in df.index:
        conId = df.at[indx, 'conId']

        a = (f'about to qualify contract: conId: {conId}')
        logging.info(a)
        print(a)

        qc = utils.getQualifiedContractFromConId(ib=ib,conId=conId,timeOutTime=timeOutTime)
        df.at[indx, 'qualifiedContract'] = qc

        # calculate the earliest Date for this contract
        earliestDateTime = utils.getEarliestDateTimeFromIBAsDateTime(ib=ib, qualifiedContract=qc, timeOutTime=timeOutTime)
        df.at[indx, 'earliestDateTime'] = earliestDateTime

        # set the category that should be MarketData for the tables to be generated in this loop
        category = mydb.tableCategories.MarketData.value
        df.at[indx, 'category'] = category

        # set the keyword arguments for the call to calculateTableName
        kwargs = {}
        kwargs['category'] = category
        kwargs['earliestDateTime'] = earliestDateTime
        kwargs.update(qc.dict())
        df.at[indx, 'kwargs'] = kwargs

        # calculate the tableName
        tableName = mydb.calculateTableName(**kwargs)
        df.at[indx, 'tableName'] = tableName

        # create the sqlalchemy ORM class; this will write the class to the mydb.DBDeclarativeBase.metadata object
        a = (f'creating MarketData Table: conId: {conId}; tableName: {tableName}')
        logging.info(a)
        print(a)
        tableORM = mydb.getTableORMByTablename(tableName=tableName)
        df.at[indx, 'tableORM'] = tableORM

        pass


    # now all the ORM tables should be defined.
    # they are not yet created on disk.
    # also, the MarketDataInfoTable is not populated and the MarketDataInfoTableDataFrame is not populated

    # loop over all conIds defined in the config File and create a row in the Market Data Info Table
    # also, populate the corresponding dataframe

    # create all tables on disk if they do not yet exist
    mydb.createAllTables()



    ssn = mydb.Session()
    for indx, row in dfWithInfoAboutTables.iterrows():
        tableName = row.tableName
        print(f'upserting a row for {tableName} to the MarketDataInfoTable')
        # create a row for each conId in the MDIT table
        # first, instantiate a row in the MarketDataInfoTable
        MDIT = mydb.MarketDataInfoTable(tableName=tableName)
        # set all available column values
        kwargs = row.kwargs
        for k,v in kwargs.items():
            if k in MDIT.__table__.columns:
                setattr(MDIT, k, v)
                pass
            pass
        # upsert this table Row to the table
        d = utils.convertTableRowToDict(MDIT)
        # # only update values that are not none
        # rowOfTableOnDisk = ssn.query(mydb.MarketDataInfoTable).filter(mydb.MarketDataInfoTable.tableName==tableName).first()
        # for k,v in d.items():
        #     if v is None:
        #         a = None
        #         print(k,v)
        #         try:
        #             a = getattr(rowOfTableOnDisk, 'earliestDateTime', None)
        #         except:
        #             pass
        #
        #         d[k] = a
        #         print(a)
        #         pass
        #     pass
        ssn.execute(mydb.upsert(mydb.MarketDataInfoTable, [d]))

    ssn.commit()
    ssn.close()

    mydb.MarketDataInfoTableDataFrame = mydb.createMarketDataInfoTableDataFrameFromMarketDataInfoTable(ib=ib,timeOutTime=timeOutTime)


    # disconnect from interactive brokers
    ib.disconnect()

    return(mydb)



def doitNew():
    mydb = tradingDB()
    utils.logToFile('ttest.log')
    # utils.logToConsole()
    # if we want to reduce salAlchemy Logging
    # mydb._loggerSQLAlchemy.setLevel(logging.ERROR)


    mydb.dropAllTables()
    # create all tables that are defined now. This is just the table that contains a single market data table in each row
    mydb.createAllTables()



    pass

def main():
    doitNew()

if __name__ == "__main__":
    sys.exit(main())
