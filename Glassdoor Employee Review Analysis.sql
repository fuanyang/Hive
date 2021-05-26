--1. Create a database 

DROP DATABASE IF EXISTS review_db2 CASCADE;
CREATE DATABASE review_db2;
USE review_db2;


--2. Create a table

CREATE TABLE review_tb2( index STRING, company STRING, location STRING, dates STRING, JobTitle STRING, Summary STRING, Pros STRING,
  Cons STRING, overallRatings STRING, WorkBalanceStars STRING, CultureStars STRING, CareerOpportunityStar STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


--3. Load data from HDFS to created table

LOAD DATA INPATH '/user/soda00000gmail/employee_review.csv' INTO TABLE review_tb2;


--4. Use regular expression to extract desired string in parentheses()

SELECT REGEXP_EXTRACT( location, '\\((.*?)\\)',1) FROM review_tb2
WHERE location LIKE '%(%'
LIMIT 5;


--5. Insert regular expression into conditional function statement

SELECT DISTINCT location,
  CASE WHEN location like 'none' THEN 'none'
       WHEN location like '%(%' THEN REGEXP_EXTRACT( location, '\\((.*?)\\)', 1)
       ELSE 'US'
  END AS country
FROM review_tb2
LIMIT 100;


--6. Fill missing values in the table with default values

CREATE TEMPORARY TABLE review_nonnull AS 
SELECT index, company, location, dates,
       REGEXP_REPLACE(OverallRatings, 'none', 4) nonull_OR,
       REGEXP_REPLACE(WorkBalanceStars, 'none', 3.2) nonull_WB,
       REGEXP_REPLACE(CultureStars, 'none', 4.1) nonull_CU,
       REGEXP_REPLACE(CareerOpportunityStar, 'none', 5) nonull_CO,
       REGEXP_REPLACE(BenefitStars, 'none', 4.7) nonull_BF
FROM review_tb2;


--7. Calculate median for all rating fields with newly added default values

SELECT
      ROUND(PERCENTILE_APPROX(CAST(nonull_OR as FLOAT), 0.5), 2) median_OR,
      ROUND(PERCENTILE_APPROX(CAST(nonull_WB as FLOAT), 0.5), 2) median_WB,
      ROUND(PERCENTILE_APPROX(CAST(nonull_CU as FLOAT), 0.5), 2) median_CS,
      ROUND(PERCENTILE_APPROX(CAST(nonull_CO as FLOAT), 0.5), 2) median_CO,
      ROUND(PERCENTILE_APPROX(CAST(nonull_BF as FLOAT), 0.5), 2) median_BF,
FROM review_nonull;



