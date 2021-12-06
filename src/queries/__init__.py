# Stores the content of the queries to be re-used

import re
from os import path

_HERE = path.dirname(path.abspath(__file__))

_QUERY_CLEANUP = None
_QUERY_FETCH_COMMODITIES = None
_QUERY_INSERT_PRICES = None
_REGEX_INSERT_PRICES = re.compile(r"\/\*(.+)\*\/")
_MTXES_INSERT_PRICES = None

with open(path.join(_HERE, "Cleanup.sql"), "r") as f:
    _QUERY_CLEANUP = f.read()

with open(path.join(_HERE, "FetchCommodities.sql"), "r") as f:
    _QUERY_FETCH_COMMODITIES = f.read()

with open(path.join(_HERE, "InsertPrices.sql"), "r") as f:
    _QUERY_INSERT_PRICES = f.read()
    _MTXES_INSERT_PRICES = _REGEX_INSERT_PRICES.search(_QUERY_INSERT_PRICES)

def cleanup():
    return _QUERY_CLEANUP

def fetch_commodities():
    return _QUERY_FETCH_COMMODITIES

def insert_prices(prices):
    if len(prices) == 0:
        return None # Nothing to do

    return _QUERY_INSERT_PRICES.replace(
        _MTXES_INSERT_PRICES.group(0),
        (f"{_MTXES_INSERT_PRICES.group(1)}, " * len(prices))[:-2]
    ), prices
