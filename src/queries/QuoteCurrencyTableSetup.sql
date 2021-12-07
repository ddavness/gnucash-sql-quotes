-- A table specific to this project that stores the currency each commodity is associated to

--  * For equity, the denominated currency will be looked up on Alphavantage
--  * For (crypto)currencies (namespace = "CURRENCY" or namespace = "CRYPTO"),
-- we use the currency of the root account.

CREATE TABLE IF NOT EXISTS quote_currencies (
    commodity_guid text(32) PRIMARY KEY NOT NULL,
    currency_guid text(32) NOT NULL,
    FOREIGN KEY (commodity_guid) REFERENCES commodities(guid),
    FOREIGN KEY (currency_guid) REFERENCES commodities(guid)
);
