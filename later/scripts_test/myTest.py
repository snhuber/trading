"""sets up the database"""
import sys
from configparser import ConfigParser, ExtendedInterpolation
import os
import json


def main(argv=None) -> str:
    argv= sys.argv if argv is None else argv
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read('config.ini')
    print(config)
    print (config.get('DEFAULT', 'DataDir',vars=os.environ))
    print(config.get('DEFAULT', 'DBFileName', vars=os.environ))
    print(config.options('MarketData'))
    # a = json.loads(config.get('MarketData','ConIdList'))
    # print(a)
    # print(type(a))
    a = config.get('MarketData','ConIdList')
    print(eval(a))
    print(type(eval(a)))

    # function to find earliest Date for given ConID
    # function to find symbol/currency for given ConID
    # functions should assume that gateway is running
    # functions should connect to IB without the need for a given clientID
    # or they should request a clientID


if __name__ == "__main__":
    sys.exit(main(sys.argv))
