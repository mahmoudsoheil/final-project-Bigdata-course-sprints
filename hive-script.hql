Create database covid_db;
use covid_db;
CREATE TABLE IF NOT EXISTS covid_db.covid_staging 
(
 Country 			                STRING,
 Total_Cases   		                DOUBLE,
 New_Cases    		                DOUBLE,
 Total_Deaths                       DOUBLE,
 New_Deaths                         DOUBLE,
 Total_Recovered                    DOUBLE,
 Active_Cases                       DOUBLE,
 Serious		                  	DOUBLE,
 Tot_Cases                   		DOUBLE,
 Deaths                      		DOUBLE,
 Total_Tests                   		DOUBLE,
 Tests			                 	DOUBLE,
 CASES_per_Test                     DOUBLE,
 Death_in_Closed_Cases     	        STRING,
 Rank_by_Testing_rate 		        DOUBLE,
 Rank_by_Death_rate    		        DOUBLE,
 Rank_by_Cases_rate    		        DOUBLE,
 Rank_by_Death_of_Closed_Cases   	DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED as TEXTFILE
LOCATION '/user/cloudera/ds/COVID_HDFS_LZ'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.covid_ds_partitioned 
(
 Country 			                STRING,
 Total_Cases   		                DOUBLE,
 New_Cases    		                DOUBLE,
 Total_Deaths                       DOUBLE,
 New_Deaths                         DOUBLE,
 Total_Recovered                    DOUBLE,
 Active_Cases                       DOUBLE,
 Serious		                  	DOUBLE,
 Tot_Cases                   		DOUBLE,
 Deaths                      		DOUBLE,
 Total_Tests                   		DOUBLE,
 Tests			                 	DOUBLE,
 CASES_per_Test                     DOUBLE,
 Death_in_Closed_Cases     	        STRING,
 Rank_by_Testing_rate 		        DOUBLE,
 Rank_by_Death_rate    		        DOUBLE,
 Rank_by_Cases_rate    		        DOUBLE,
 Rank_by_Death_of_Closed_Cases   	DOUBLE
)
PARTITIONED BY (COUNTRY_NAME STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
LOCATION '/user/cloudera/ds/COVID_HDFS_PARTITIONED';

set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions = 100000;
set hive.exec.max.dynamic.partitions.pernode = 100000;

FROM covid_db.covid_staging
INSERT INTO TABLE covid_db.covid_ds_partitioned PARTITION(COUNTRY_NAME)
SELECT *,Country WHERE Country is not null and Country <> "" and Country <> "World";

CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.covid_final_output
( Country STRING, Deaths DOUBLE, Top_Death STRING, Tests DOUBLE, Top_Test STRING)
PARTITIONED BY (COUNTRY_NAME STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/user/cloudera/ds/COVID_FINAL_OUTPUT';

from covid_ds_partitioned insert into table covid_final_output PARTITION (COUNTRY_NAME)
SELECT Country, case when length(Deaths) = 0 then 0 else Deaths end,
rank() over(order by cast(regexp_replace(Deaths,",","") as double) desc),
case when length(Tests) = 0 then 0 else Tests end,
rank() over(order by cast(regexp_replace(Tests,",","") as double) desc), Country
where Country is not null and Country <> "" and Country <> "World";

insert overwrite local directory '/home/cloudera/output' row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' select * from covid_final_output;



