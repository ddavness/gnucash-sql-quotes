# Stores the content of the queries to be re-used

import re, uuid
from datetime import datetime, time
from os import path

_HERE = path.dirname(path.abspath(__file__))
_REGEX = re.compile(r"\/\*(.+)\*\/")

_QUERY_CLEANUP = None
_QUERY_FETCH_COMMODITIES = None
_QUERY_QC_SETUP = None
_QUERY_QC_TDOWN = None

_QUERY_INSERT_PRICES = None
_MTXES_INSERT_PRICES = None

_QUERY_SETUP_COMMODITIES = None
_MTXES_SETUP_COMMODITIES = None

def _repeat_replace(target, pattern, n):
    return target.replace(pattern.group(0), (f"{pattern.group(1)}, " * n)[:-2])
def _flatten(t):
    return [item for sublist in t for item in sublist]

with open(path.join(_HERE, "Cleanup.sql"), "r") as f:
    _QUERY_CLEANUP = f.read()

with open(path.join(_HERE, "FetchCommodities.sql"), "r") as f:
    _QUERY_FETCH_COMMODITIES = f.read()

with open(path.join(_HERE, "QuoteCurrencyTableSetup.sql"), "r") as f:
    _QUERY_QC_SETUP = f.read()

with open(path.join(_HERE, "QuoteCurrencyTableTeardown.sql"), "r") as f:
    _QUERY_QC_TDOWN = f.read()

with open(path.join(_HERE, "InsertPrices.sql"), "r") as f:
    _QUERY_INSERT_PRICES = f.read()
    _MTXES_INSERT_PRICES = _REGEX.search(_QUERY_INSERT_PRICES)

with open(path.join(_HERE, "SetupCommodities.sql"), "r") as f:
    _QUERY_SETUP_COMMODITIES = f.read()
    _MTXES_SETUP_COMMODITIES = _REGEX.search(_QUERY_SETUP_COMMODITIES)

def cleanup():
    return _QUERY_CLEANUP

def fetch_commodities():
    return _QUERY_FETCH_COMMODITIES

def setup_qc_table():
    return _QUERY_QC_SETUP

def teardown_qc_table():
    return _QUERY_QC_TDOWN

def insert_prices(prices):
    # Each price is a tuple of the format (commodity_guid, currency_guid, date, price)
    if len(prices) == 0:
        return None # Nothing to do

    return _repeat_replace(_QUERY_INSERT_PRICES, _MTXES_INSERT_PRICES, len(prices)), _flatten([(
        uuid.uuid5(uuid.UUID(hex = p[0]), p[2].isoformat()).hex,
        p[0],
        p[1],
        datetime.combine(p[2], time.time(0, 0, 0)),
        "user:price",
        "last",
        p[3].as_integer_ratio()[0],
        p[3].as_integer_ratio()[1]
    ) for p in prices])

def setup_commodities(commodities):
    if len(commodities) == 0:
        return None

    return _repeat_replace(_QUERY_SETUP_COMMODITIES, _MTXES_SETUP_COMMODITIES, len(commodities)), commodities
