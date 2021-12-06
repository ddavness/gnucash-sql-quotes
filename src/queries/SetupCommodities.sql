UPDATE commodities
    SET quote_flag = 0, quote_source = 'quotes.py'
    WHERE guid IN (/*?*/);
