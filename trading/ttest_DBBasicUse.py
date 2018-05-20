import pandas as pd
import sqlalchemy as sa
from sqlalchemy import event
import pymysql

# from urllib import quote_plus as urlquote
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric, DateTime, Float, TIMESTAMP
from sqlalchemy.orm import scoped_session, sessionmaker

import time
import threading
import numpy as np

from pandas import Timestamp
import datetime

Base = declarative_base()

#Declaration of the class in order to write into the database. This structure is standard and should align with SQLAlchemy's doc.
class Current(Base):
    __tablename__ = 'df_test'

    id = Column(Integer, primary_key=True)
    date = sa.types.DateTime(timezone=False)
    close = Column(Numeric)
    Cur = Column(String(500))

    def __repr__(self):
        return (f'(id= {self.id}, date={self.date}, close={self.close}, cur={self.Cur}')

class Current2(Base):
    __tablename__ = 'df_test2'

    id = Column(Integer, primary_key=True)
    date = sa.types.DateTime(timezone=False)
    close = Column(Float)
    Cur = Column(String(500))

    def __repr__(self):
        return (f'(id= {self.id}, date={self.date}, close={self.close}, cur={self.Cur}')
        # return (f'(id= {self.id}, close = {self.close}, cur={self.Cur}')




def test():
    """Stupid test function"""
    L = []
    for i in range(100):
        L.append(i)


def get_mysql_engine():
    """
    gets a connection to the mysql database
    :return:
    """
    engine = sa.create_engine('mysql+pymysql://bn:basket05@127.0.0.1/trading', echo=True)
    return engine


def load_df(strPicklePath: str='df3.pickle') -> pd.DataFrame():
    """
    Loads a dataframe
    :return:
    """
    df = pd.read_pickle(strPicklePath)
    df.date = df.date.dt.tz_localize(None)
    return(df)

# create function for thread
def Tfunc(i,indices, df, results):
    print("Thread no.:%d" % (i + 1))
    session = Session()
    results[i] = i*2
    if i == 0:
        Current2.__table__.drop(session.bind)
        Current2.__tablename__ = f'df_test_{i}'
        Current2.__table__.create(session.bind)
    a = df.to_dict(orient='records')
    # [d.update({'date':d['date'].to_pydatetime()}) for d in a]
    session.bulk_insert_mappings(Current2, a)
    session.commit()
    print("%d finished sleeping from thread\n" % i)


if __name__ == '__main__':
    import timeit
    import time
    # a = timeit.timeit("test()", setup="from __main__ import test")
    df_all = load_df('df3.pickle')
    df_single = load_df('df_USD_05.pickle')

    engine = get_mysql_engine()
    engine.echo=False
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    Session = scoped_session(session_factory)
    session = Session()



    # @event.listens_for(engine, 'before_cursor_execute')
    # def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    #     if executemany:
    #         cursor.fast_executemany = True

    N = int(2e4)
    df_to_write = df_all.head(n=N).copy()
    df_to_write.reset_index(inplace=True)
    df_to_write.index.name = 'id'
    print(df_to_write.head())

    if 1:
        t1 = time.time()
        df_to_write.to_sql("df_test", session.bind, if_exists='replace', index=True, chunksize=100000)
        print('to_sql {} records: {} seconds'.format(N,time.time()-t1))
    if 0:
        t1 = time.time()
        df = df_to_write.copy()
        a = df.to_dict(orient='records')
        # [d.update({'date': d['date'].to_pydatetime()}) for d in a]
        session.bulk_insert_mappings(Current2, a)
        session.commit()
        print('sa bulk insert {} records: {} seconds'.format(N, time.time() - t1))

    if 1:
        t1 = time.time()
        tsks = []
        jobRange = range(8)
        results = [i for i in jobRange]
        for i in jobRange:
            indices = np.arange(start=0,stop=10,step=1)
            tsk = threading.Thread(target=Tfunc, args=(i,indices,df_to_write,results))
            tsks.append(tsk)
            tsk.start()

            # check thread is alive or not
            # c = tsk.isAlive()

            # fetch the name of thread
            # c1 = tsk.getName()
            # print('\n', c1, "is Alive:", c)

            # get toatal number of thread in execution
            # count = threading.active_count()
            # print("Total No of threads:", count)
        for i in jobRange:
            tsk = tsks[i]
            tsk.join()

        print(results)

        print('parallel write: {} records: {} seconds'.format(N,time.time()-t1))
    if 0:
        t1 = time.time()

        df_read = pd.read_sql('SELECT * FROM df_test', con=engine)
        print(df_read.shape)
        print('pandas read: {} records: {} seconds'.format(N, time.time() - t1))
    if 0:
        t1 = time.time()
        hours = 60;
        days = 24 * hours;
        weeks = 7 * days;
        months = 4 * weeks;
        years = 12 * months;
        a = np.concatenate([
            np.arange(start=0, stop=20, step=1),
            np.arange(start=20, stop=1 * hours, step=5),
            np.arange(start=1 * hours, stop=4 * hours, step=20),
            np.arange(start=4 * hours, stop=1 * days, step=1 * hours),
            np.arange(start=1 * days, stop=1 * weeks, step=4 * hours),
            np.arange(start=1 * weeks, stop=1 * months, step=12 * hours),
            np.arange(start=1 * months, stop=3 * months, step=1 * days),
            np.arange(start=3 * months, stop=36 * months, step=1 * weeks),
        ])
        query = session.query(Current).filter_by(Cur='USD').filter(Current.id.in_(a.tolist()))
        df_read = pd.read_sql(query.statement, query.session.bind)
        print(df_read.head())
        print('sa read: {} records: {} seconds'.format(N, time.time() - t1))
        pass

    Session.remove()

    if 0:
        t1 = time.time()
        df_to_write = df_all.head(n=1000)
        write_table(engine=engine,df=df_to_write,tableName='df_all',chunkSize=1000)




