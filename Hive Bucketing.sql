--1. Set environment for bucket insert and dynamic partition

SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition.mode=nonstrict;


--2. Create a new database

DROP DATABASE IF EXISTS review_db3 CASCADE;
CREATE DATABASE review_db3;
USE review_db3;


--3. Create a non-partitioned/non-bucketed table

CREATE TABLE review_tb3( index STRING, company STRING, location STRING, dates STRING, JobTitle STRING, Summary STRING,
  Pros STRING, Cons STRING, overallRatings STRING, WorkBalanceStars STRING, CultureStars STRING, CareerOpportunityStar String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


--4. Create a partitioned/bucketed table

CREATE TABLE review_bk( index STRING, company STRING, location STRING, dates STRING, JobTitle STRING, Summary STRING,
  Pros STRING, Cons STRING, overallRatings STRING, WorkBalanceStars STRING, CultureStars STRING, CareerOpportunityStar String)
PARTITIONED BY (country STRING)
CLUSTERED BY (dates) into 4 BUCKETS;


--5. Load data from HDFS to non-partitioned/non-bucketed table

LOAD DATA INPATH '/user/soda00000gmail/employee_review.csv' INTO TABLE review_tb3;


--6. Use case-when-then statement to convert location to corresponded country
     Insert the conditional statement in partitioned/bucketed table
     Verify partition results
     
INSERT OVERWRITE TABLE review_bk PARTITION (country)
SELECT index, company, location, dates, JobTitle, Summary, Pros, Cons, overallRatings, WorkBalanceStars, CultureStars, CareerOpportunityStar,
  (CASE WHEN location like 'none' THEN 'none
        WHEN location like '%(%' THEN REGEXP_EXTRACT( location, '\\((.*?)\\)', 1)
        ELSE 'US' 
   END) country
FROM review_tb3;
SHOW PARTITIONS review_bk;


--7. Get full address of review_bk 
     
DESCRIBE FORMATTED review_bk;


--8. Verify buckets are created in the subdirectory of the address 

dfs -ls hdfs://nameservice1/user/hive/warehouse/review_db3/review_bk/country=US

