--1. Load data to HDFS abd read the first 10 records

hdfs dfs -put movie_award.csv
hdfs dfs -cat movie_award.csv | head -10


--2. Create a database and a table in Hive, load data from HDFS to the table, and check the result

CREATE DATABASE movie_db;
USE movie_db;
CREATE TABLE movie_tb( director_name STRING, ceremony STRING, year INT, category STRING, outcome STRING, original_language STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA INPATH 'movie_award.csv' INTO TABLE movie_tb;
SELECT *  FROM movie_tb LIMIT 10;


--3. FInd directors who were nominated and have won awards in the year 2011

SELECT DISTINCT director_name FROM movie_tb 
WHERE outcome='Won' and year=2011
LIMIT 20;


--4. List all the award categories available in the Berlin International Film Festival

SELECT DISTINCT category FROM movie_tb 
WHERE ceremony='Berlin International Film Festival';


--5. Find directors who won the most awards

CREATE VIEW count_won( director_name, count_won) AS
SELECT director_name, COUNT(*) FROM movie_tb
WHERE outcome='Won'
GROUP BY director_name;

SELECT *  FROM count_won
ORDER BY count_won DESC
LIMIT 20;
