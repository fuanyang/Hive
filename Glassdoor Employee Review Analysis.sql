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
