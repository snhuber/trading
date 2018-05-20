from setuptools import setup
import os
import sys
import codecs
from warnings import warn

import trading

if sys.version_info < (3, 0, 0):
    raise RuntimeError("trading is for Python 3")

if sys.version_info < (3, 6, 0):
    warn("trading requires Python 3.6 or higher")

here = os.path.abspath(os.path.dirname(__file__))
print(here)
with codecs.open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='trading',
    version=trading.__version__,
    description='Python automated trading package',
    long_description=long_description,
    url='https://github.com/bjrnfrdnnd/trading',
    author='Björn Nadrowski',
    entry_points={
        'console_scripts': ['trading-getListOfConIds=trading.scripts.findConIds:findConIds'],
    },
    license='None',
    author_email='bjrnfrdnnd@gmail.com',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: just me',
        'Topic :: Office/Business :: Financial :: Investment :: making money',
        'License :: None',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3 :: Only',
    ],
    keywords='ibapi ib_insync asyncio jupyter interactive brokers async',
    packages=['trading']
)
