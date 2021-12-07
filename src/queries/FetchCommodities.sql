SELECT c.mnemonic, c.guid, c.namespace, qc.currency_guid
FROM commodities c, quote_currencies qc
    WHERE c.quote_source = 'quotes.py'
    AND c.guid = qc.guid;
