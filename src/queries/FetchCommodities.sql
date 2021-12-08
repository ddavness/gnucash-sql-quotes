SELECT c.namespace, c.mnemonic, cc.mnemonic, c.guid, qc.currency_guid 
FROM commodities c, commodities cc, quote_currencies qc
    WHERE c.quote_source IN ('quotes.py', 'alphavantage', 'currency')
    AND c.guid = qc.commodity_guid
    AND cc.guid = qc.currency_guid;
