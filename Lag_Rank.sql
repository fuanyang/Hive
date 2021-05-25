--1. Create a database

CREATE DATABASE stock_db;
USE stock_db;


--2. Create a table for StockPrices.csv and skip first line as header

CREATE TABLE stock_prices(trading_date DATE, symbol STRING, open DOUBLE, close DOUBLE, low DOUBLE, high DOUBLE, volume INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");


--3. Load data from HDFS to created table

LOAD DATA INPATH '/user/soda000000gmail/StockPrices.csv' INTO TABLE stock_prices;


--4. Roll up daily prices to monthly prices

CREATE TABLE price_rev AS SELECT
YEAR(trading_date), MONTH(trading_date), symbol, ROUND(AVG(open),2), ROUND(AVG(close),2), ROUND(AVG(low),2), ROUND(AVG(high),2)
FROM stock_prices
GROUP BY YEAR(trading_date), MONTH(trading_date), symbol;


--5. Change the default column names to meaningful ones 

ALTER TABLE price_rev CHANGE `_c0` yr INT;
ALTER TABLE price_rev CHANGE `_c1` mon INT;
ALTER TABLE price_rev CHANGE `_c3` avg_open DOUBLE;
ALTER TABLE price_rev CHANGE `_c4` avg_close DOUBLE;
ALTER TABLE price_rev CHANGE `_c5` avg_low DOUBLE;
ALTER TABLE price_rev CHANGE `_c6` avg_high DOUBLE;
ALTER TABLE price_rev CHANGE `_c7` avg_volume INT;


--6. Create a table for StockCompanies.csv

CREATE TABLE stock_companies( symbol STRING, company STRING, sector STRING, sub_industry STRING, headquarter STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");


--7. Load data from HDFS to the table

LOAD DATA INPATH '/user/soda00000gmail/StockCompanies.csv' INTO TABLE stock_companies;


--8. Split headquarter location to state and city

CREATE TABLE company_st AS SELECT
symbol, company, sector, sub_industry, headquarter, SUBSTR(headquarter, INSTR(headquarter,";")+1) state
FROM stock_companies;


--9. Create a join table to connect stock prices with company info

CREATE TABLE stock_join( trading_yr INT, trading_mon INT, symbol STRING, company STRING, state STRING, sector STRING, sub_industry STRING, avg_close DOUBLE, avg_volume INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


--10. Insert data into created join table

INSERT OVERWRITE TABLE stock_join
SELECT a.yr, a.mon, a.symbol, b.company, b.state, b.sector, b.sub_industry, a.avg_close, a.avg_volumn
FROM price_rev a join company_st b ON a.symbol=b.symbol;


--11. Filter join table to keep only 2010, 2016 close prices

CREATE TABLE best_industry_by_state AS
SELECT state, sub_industry, trading_yr, AVG(avg_close) avg_yr_close 
FROM stock_join
WHERE trading_yr=2010 or trading_yr=2016
ORDER BY state, sub_industry, trading_yr;


--12. Use Lag() and Rank Over() to identify the best-growing industry in each state

SELECT rank_tb2.state, rank_tb2.sub_industry FROM(
  SELECT rank_tb.state, rank_tb.sub_industry, rank_tb.trading_yr, rank_tb.avg_yr_close,
  RANK() OVER(PARTITION BY state ORDER BY growth_after6yr DESC) rnk
  FROM best_industry_by_state
  ORDER BY state, sub_industry, trading_yr
  ) rank_tb
) rank_tb2
WHERE rank_tb2.rnk=1;

