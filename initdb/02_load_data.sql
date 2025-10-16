COPY candles (ticker, date, close, volume)
FROM '/docker-entrypoint-initdb.d/stock_candles.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    NULL ''
);

COPY companies (secid, description, founded, headquarters, employees, sector, ceo, link)
FROM '/docker-entrypoint-initdb.d/companies.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    NULL '',
    QUOTE '"',
    ESCAPE '"'
);