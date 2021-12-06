# Wrapper for the AV API

import asyncio, time, warnings
from datetime import date, datetime, timedelta
from alpha_vantage import foreignexchange, timeseries, cryptocurrencies

class DatedDict():
    """
    A dynamic dictionary where keys are dates.
    In this context, if a value for a certain
    date doesn't exist, it will be looked up on
    the day before, and so on, until it's past the limit.

    This is useful to get price data during the weekends
    when the market is closed.
    """
    _ONEDAY = timedelta(days = 1)
    _RECURSION_DEPTH_LIMIT = 5

    def __init__(self) -> None:
        self.data = dict()

    def add(self, iso, val):
        dt = None
        if isinstance(iso, date):
            dt = iso
        else:
            dt = date.fromisoformat(iso)

        if self.data.get(dt, None) is not None:
            raise ValueError("This key already exists!")
        self.data[dt] = val
    
    def get(self, iso, _depth_limit = _RECURSION_DEPTH_LIMIT):
        dt = None
        if isinstance(iso, date):
            dt = iso
        else:
            dt = date.fromisoformat(iso)

        if self.data.get(dt) is not None:
            return self.data.get(dt)
        elif _depth_limit == 0:
            return None
        else:
            # Check on the day before
            return self.get(dt - self._ONEDAY, _depth_limit = _depth_limit - 1)
    
    def iter(self, end, begin):
        # Start with a depth of 0, then bump to default once the first element is found
        cdepth = self._RECURSION_DEPTH_LIMIT

        # We start in the date most in the future then go backwards
        for date in (end - timedelta(n) for n in range((end - begin).days + 1)):
            result = self.get(date, _depth_limit = cdepth)
            if cdepth == 0 and result is not None:
                # We actually hit the start of the list, we can fill in any bumps
                cdepth = self._RECURSION_DEPTH_LIMIT
            elif cdepth != 0 and result is None:
                # We're likely past the list
                cdepth = 0

            yield (date, result)

def av_rate_limit(f):
    def g(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ValueError as e:
            if str(e).find("premium") == -1:
                raise e

            dtnow = datetime.now().timestamp()
            dtt = dtnow - (dtnow % 60) + 60 # When the next full minute hits i.e. if rate limit is hit at 10:45:10, wait until 10:46:00
            warnings.warn(f"Endpoint has hit rate limit - holding on for {dtt - dtnow} seconds")
            time.sleep(dtt - dtnow)
            return g(*args, **kwargs)
    return g

def pack_into_dated_dict(key_sample = ""):
    if key_sample.strip() == "":
        raise ValueError("Empty key to query!")

    def meta(f):
        def g(*args, **kwargs):
            kw = ""
            dump, _ = f(*args, **kwargs)
            # Figure out the key we're looking for based on the sample
            for k in dump.get(list(dump.keys())[0]).keys():
                if k.startswith(key_sample):
                    print(k)
                    kw = k
                    break

            data = DatedDict()

            for date in dump.keys():
                data.add(date, float(dump.get(date).get(kw)))

            return data
        return g

    return meta

class AlphaVantageDriver():
    fx_driver = None
    cc_driver = None
    mkt_driver = None

    def __init__(self, apikey = "") -> None:
        if apikey is None or apikey.strip() == "":
            raise ValueError("No API Key provided!")
        
        self.fx_driver = foreignexchange.ForeignExchange(key = apikey, output_format = "json")
        self.cc_driver = cryptocurrencies.CryptoCurrencies(key = apikey, output_format = "json")
        self.mkt_driver = timeseries.TimeSeries(key = apikey, output_format = "json")

    @av_rate_limit
    @pack_into_dated_dict(key_sample = "4.")
    def fx(self, full_history = False, symbol = "", to = ""):
        x = self.fx_driver.get_currency_exchange_daily(from_symbol = symbol, to_symbol = to, outputsize = "full" if full_history else "compact")
        import pprint
        pprint.pprint(x)
        return x

    @av_rate_limit
    @pack_into_dated_dict(key_sample = "4a.")
    def crypto(self, symbol = "", to = "USD"):
        x = self.cc_driver.get_digital_currency_daily(symbol, to)
        import pprint
        pprint.pprint(x)
        return x

    @av_rate_limit
    @pack_into_dated_dict(key_sample = "4.")
    def mkt(self, full_history = False, symbol = ""):
        x = self.mkt_driver.get_daily(symbol = symbol, outputsize = "full" if full_history else "compact")
        import pprint
        pprint.pprint(x)
        return x
