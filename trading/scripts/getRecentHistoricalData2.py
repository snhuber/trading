import pandas as pd
import sys
from ib_insync import util
from ib_insync.ibcontroller import IBC
from ib_insync import IB, Contract
from ib_insync import ibcontroller


def runProg():
    """run program"""

    util.patchAsyncio()

    # log to a file
    util.logToFile(f'getRecentHistoricalData2.log')
    # util.logToConsole()

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
    watchdogApp.start()
    pass

    # create some contracts
    qcs = []
    c = Contract(symbol='EUR',currency='USD',exchange='IDEALPRO',secType='CASH')
    qc = ib.qualifyContracts(c)[0]
    qcs.append(qc)

    # request market data
    for qc in qcs:
        ib.reqMktData(contract=qc,
                      genericTickList='',
                      snapshot=False,
                      regulatorySnapshot=False,
                      mktDataOptions=None)

        pass

    # define some callback
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

    # register the callbacks with ib
    ib.setCallback('error', myErrorCallback)
    ib.setCallback('pendingTickers', onPendingTickers)

    # run and never stop
    ib.run()



if __name__ == '__main__':
    sys.exit(runProg())
