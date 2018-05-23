"""
defines a command-line utility for finding conIds
"""
import sys
from ib_insync import IB, Forex, Contract, Index, util
import ibapi
import click
import asyncio
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'],
                        token_normalize_func=lambda x: x.lower())
print(CONTEXT_SETTINGS)

util.patchAsyncio()
util.logToFile(f'{__name__}.log')

# the following does not work: it is never entered upon any error
@ibapi.utils.iswrapper
def error(self, reqId: int, errorCode: int, errorString: str, contract):
    """ibapi error callback. Does not work. Don't know why"""
    super().error(reqId, errorCode, errorString, contract)
    print("Error. Id: ", reqId, " Code: ", errorCode, " Msg: ", errorString, contract)

# the following is entered sometimes; but not, for example, when ib.connect() fails due to the clientID already taken
def myErrorCallback(reqId, errorCode, errorString , contract):
    """ib_insycn error callback. Is not called when ib.connect() is called with clientID already in use"""
    # print("myErrorCallback", reqId,errorCode,errorString,contract)
    print("myErrorCallback", reqId, errorCode, errorString, contract)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--clientid','-c',default=241, show_default=True, help='clientId to connect to running gateway/TWS app')
def findConIds(**kwargs):
    """prints a list of conIds for a hard-coded list of currencies"""
    ib = IB()
    ib.errorEvent += myErrorCallback
    clientId = kwargs.get('clientId'.lower())
    ib.connect('127.0.0.1', 4002, clientId=clientId)
    # build a list of valid contracts
    # define contracts
    ForexStrs = [
        # 'AUD',
        # 'CAD',
        'CHF',
        'CNH',
        'GBP',
        'JPY',
        'USD',
        'RUB',
        # 'CZK',
        # 'DKK',
        # 'HUF',
        # 'ILS',
        # 'MXN',
        # 'NOK',
        # 'NZD',
        # 'PLN',
        # 'SEK',
        # 'SGD',
        # 'TRY',
        # 'ZAR',
        ]

    # Indices and CFDs
    others = [
        # {
        #     'secType': 'IND',
        #     'symbol': 'DAX',
        #     'exchange': 'DTB',
        #     'currency': 'EUR'
        # },
        # {
        #     'secType': 'IND',
        #     'symbol': 'INDU',
        #     'exchange': 'CME',
        #     'currency': 'USD'
        # },
        # {
        #     'secType': 'IND',
        #     'symbol': 'HSC50',
        #     'exchange': 'HKFE',
        #     'currency': 'HKD'
        # },
        # {
        #     'secType': 'IND',
        #     'symbol': 'N225',
        #     'exchange': 'OSE.JPN',
        #     'currency': 'JPY'
        # },
        # {
        #     'secType': 'IND',
        #     'symbol': 'SPX',
        #     'exchange': 'CBOE',
        #     'currency': 'USD'
        # },
        {
            'secType': 'CFD',
            'symbol': 'IBCH20',
            'exchange': 'SMART',
            'currency': 'CHF'
        },
        {
            'secType': 'CFD',
            'symbol': 'IBDE30',
            'exchange': 'SMART',
            'currency': 'EUR'
        },
        {
            'secType': 'CFD',
            'symbol': 'IBGB100',
            'exchange': 'SMART',
            'currency': 'GBP'
        },
        {
            'secType': 'CFD',
            'symbol': 'IBJP225',
            'exchange': 'SMART',
            'currency': 'JPY'
        },
        {
            'secType': 'CFD',
            'symbol': 'IBHK50',
            'exchange': 'SMART',
            'currency': 'HKD'
        },
        {
            'secType': 'CFD',
            'symbol': 'IBUS30',
            'exchange': 'SMART',
            'currency': 'USD'
        },
        {
            'secType': 'CFD',
            'symbol': 'IBUS500',
            'exchange': 'SMART',
            'currency': 'USD'
        },

    ]

    contractsQualified = []

    # Forex
    for s in ForexStrs:
        try:
            contractsQualified.append(ib.qualifyContracts(Contract(secType='CASH',
                                                                   symbol='EUR',
                                                                   exchange='IDEALPRO',
                                                                   currency=s))[0])
            pass
        except:
            print('could not qualify the contract for {}'.format(s))
            pass
        pass

    # others
    for d in others:
        try:
            contractsQualified.append(ib.qualifyContracts(Contract(**d))[0])
            pass
        except:
            print('could not qualify the contract for {}'.format(s))
            pass
        pass


    # get contract information
    conIds = []
    for c in contractsQualified:
        if c.secType in ['CASH','CFD']:
            whatToShow = 'MIDPOINT'
        else:
            whatToShow = 'TRADES'

        eDT = None
        try:
            req = ib.reqHeadTimeStampAsync(c, whatToShow=whatToShow, useRTH=False, formatDate=2)
            try:
                eDT = ib.run(asyncio.wait_for(req,10))
            except asyncio.TimeoutError:
                print('timeout')
                pass
            pass
        except:
            pass
        print (c.symbol,  c.currency, c.localSymbol, c.exchange, c.conId, c.secType, eDT)

        # cD = ib.reqContractDetails(c)[0]
        # secType=cD.summary.secType
        # print (c.symbol, c.currency, c.localSymbol, c.exchange, c.conId, eDT, c.secType)
        conIds.append(c.conId)
    print(conIds)


if __name__ == '__main__':
    sys.exit(findConIds())
