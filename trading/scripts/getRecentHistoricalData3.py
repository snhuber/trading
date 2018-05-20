import pandas as pd
import sys
from ib_insync import util
from ib_insync import objects
from ib_insync.ibcontroller import IBC
from ib_insync import IB, Contract
from ib_insync import ibcontroller
from trading import utils


def runProg():
    """run program"""

    util.patchAsyncio()

    # log to a file
    utils.logToFile(f'getRecentHistoricalData3.log')
    # utils.logToConsole()

    # set pandas option
    pd.set_option('display.width', 200)

    # specify connection details
    host = '127.0.0.1'
    port = 4002
    ibcIni = '/home/bn/IBController/configPaper.ini'
    tradingMode = 'paper'
    clientId = 12

    # start watchdog
    ibc = IBC(970, gateway=True, tradingMode=tradingMode, ibcIni=ibcIni)
    ib = IB()
    watchdogApp = ibcontroller.Watchdog(ibc, ib=ib,appStartupTime=15, host=host, port=port, clientId=clientId)


    def mySoftTimeoutCallback():
        print ('soft time out')

    def myHardTimeoutCallback():
        print ('hard time out')

    def myStoppingCallback(sthg):
        print (f'Stopping Event {sthg}')

    def myStoppedCallback(sthg):
        print (f'Stopped Event {sthg}')

    watchdogApp.hardTimeoutEvent.clear()
    watchdogApp.hardTimeoutEvent += myHardTimeoutCallback

    watchdogApp.softTimeoutEvent.clear()
    watchdogApp.softTimeoutEvent += mySoftTimeoutCallback

    watchdogApp.stoppingEvent.clear()
    watchdogApp.stoppingEvent += myStoppingCallback

    watchdogApp.stoppedEvent.clear()
    watchdogApp.stoppedEvent += myStoppedCallback

    watchdogApp.start()

    # create some contracts
    qcs = []
    c = Contract(symbol='EUR',currency='USD',exchange='IDEALPRO',secType='CASH')
    qc = ib.qualifyContracts(c)[0]
    qcs.append(qc)

    # function to request historical bars
    def requestHistoricalBars(qcs):
        # request historical bars
        barss = []
        for qc in qcs:
            bars = ib.reqHistoricalData(
                qc,
                endDateTime='',
                durationStr='900 S',
                barSizeSetting='10 secs',
                whatToShow='MIDPOINT',
                useRTH=False,
                formatDate=2,
                keepUpToDate=True)
            barss.append(bars)
            pass
        return barss

    def requestMarketData(qcs):
        for qc in qcs:
            ib.reqMktData(contract=qc,
                          genericTickList='',
                          snapshot=False,
                          regulatorySnapshot=False,
                          mktDataOptions=None)
            pass
        pass

    def requestRealTimeBars(qcs):
        barss = []
        for qc in qcs:
            bars = ib.reqRealTimeBars(contract=qc,
                                   barSize='',
                                   whatToShow='MIDPOINT',
                                    useRTH=False,
                                   realTimeBarsOptions=None)
            barss.append(bars)
            pass
        return (barss)

    # define some callback
    def onBarUpdate(bars, hasNewBar):
        localSymbol = bars.contract.localSymbol
        b0 = bars[0]
        if isinstance(b0, objects.RealTimeBar):
            dateTimeAttributeName = 'time'
            barType = 'RealTimeBar'
        else:
            dateTimeAttributeName = 'date'
            barType = 'BarData'
            pass
        dt0 = pd.to_datetime(getattr(b0,dateTimeAttributeName)).tz_localize(None)
        bm1 = bars[-1]
        dtm1 = pd.to_datetime(getattr(bm1,dateTimeAttributeName)).tz_localize(None)
        nowUTC = pd.to_datetime(pd.datetime.utcnow()).tz_localize(None)
        diffDateTIme = (nowUTC - dtm1) / pd.Timedelta('1 sec')

        print(f'local Symbol: {localSymbol}, barType: {barType}, nBars: {len(bars)}, diffDateTime: {diffDateTIme}, close: {bm1.close}')

    def onPendingTickers(tickers):
        for t in tickers:
            localSymbol = t.contract.localSymbol
            if localSymbol  == "EUR.USD":
                nowUTC = pd.to_datetime(pd.datetime.utcnow()).tz_localize(None)
                nowUTCRounded = nowUTC.floor('1 min')
                dateTime = pd.to_datetime(t.time).tz_localize(None)
                print(localSymbol, nowUTCRounded, ((dateTime - nowUTCRounded)/pd.Timedelta('1 sec')),t.close)
                pass
            pass
        pass

    def myErrorCallback(reqId, errorCode, errorString, contract):
        # print("myErrorCallback", reqId,errorCode,errorString,contract)
        if errorCode == 322:
            print("myErrorCallback", reqId, errorCode, errorString, contract)

            # more than 50 simultaneous historical data requests
            app.ib.client.cancelHistoricalData(reqId)

    def onConnectedCallback():
        print('connected')

        barss = requestHistoricalBars(qcs)
        # barss = requestRealTimeBars(qcs)
        ib.barUpdateEvent.clear()
        ib.barUpdateEvent += onBarUpdate

        print('connected 2')
        # requestMarketData(qcs)
        # ib.pendingTickersEvent.clear()
        # ib.pendingTickersEvent += onPendingTickers

        print('connected 3')


        pass

    def onDisconnectedCallback():
        print ('disconnected')

    def myTimeoutCallback(timeout):
        print (f'timeout {timeout}')

    # request the bars
    barss = requestHistoricalBars(qcs)
    # request market data
    # requestMarketData(qcs)
    # request real time bars
    # requestRealTimeBars(qcs)

    # register the callbacks with ib

    ib.connectedEvent.clear()
    ib.connectedEvent += onConnectedCallback

    ib.disconnectedEvent.clear
    ib.disconnectedEvent = onDisconnectedCallback

    ib.barUpdateEvent.clear()
    ib.barUpdateEvent += onBarUpdate

    ib.pendingTickersEvent.clear()
    # ib.pendingTickersEvent += onPendingTickers

    ib.errorEvent.clear()
    ib.errorEvent += myErrorCallback

    ib.timeoutEvent.clear()
    # ib.timeoutEvent += myTimeoutCallback


    # run and never stop
    ib.run()



if __name__ == '__main__':
    sys.exit(runProg())
