--1. Create a database

DROP DATABASE IF EXISTS review_db CASCADE;
CREATE DATABASE review_db;
USE review_db;


--2. Create a non-partitioned table in the database

CREATE TABLE review_tb( indec STRING, company STRING, location STRING, dates STRING, JobTitle STRING, Summary STRING, Pros STRING,
   Cons STRING, overallRatings STRING, WorkBalanceStars STRING, CultureStars STRING, CareerOpportunityStar STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES( "skip.header.line.count"="1");


--3. load HDFS data into created table

LOAD DATA INPATH '/user/soda00000gmail/employee_review.csv' INTO TABLE review_tb;


--4. Create partition table

CREATE TABLE review_ptb( index STRING, company STRING, location STRING, dates STRING, JobTitle STRING, Summary String, Pros STRING, 
  Cons STRING, overallRatings STRING, WorkBalanceStars STRING, CultureSTars STRING, CareerOpportunityStar STRING)
PARTITIONED BY( yr STRING);


--5. Load data into partition table

INSERT OVERWRITE TABLE review_ptb PARTITION(yr)
SELECT indext, company, location, dates, JobTitle, Summary, Pros, Cons, overallRatings, WorkBalanceStars, CultureStars, CareerOpportunityStar, SUBSTR(dates, -4) yr
FROM review_tb;


--6. Go to Hive directory to verify the result

dfs -ls hdfs://nameservice1/user/hive/warehouse/review_db.db/review_ptb
  
