-- Cleans all prices that match one of these:
-- The commodity is "controlled" by these scripts (quote source = quotes.py)
--  AND the source of the price is not user:price

DELETE FROM prices
    WHERE guid IN (
        SELECT p.guid FROM commodities c, prices p
            WHERE c.quote_source = 'quotes.py'
            AND p.source != 'user:price'
            AND c.guid = p.commodity_guid
    );
