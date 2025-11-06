-------------------------------- Load dataset and copy to Hadoop server -------------------------


--Load cars.csv file
ychsiaoca@bigdata-m:~$ wget https://www.dropbox.com/s/rsrxro7r1c5a4i2/cars.csv

-- make directory in Hadoop
ychsiaoca@bigdata-m:~$ hadoop fs -mkdir /BigData

--move the cars.cve file from Local to Hadoop server 
hadoop fs -copyFromLocal /home/ychsiaoca/cars.csv /BigData/.


-------------------------------- Start Hive Shell & Create table -------------------------------

ychsiaoca@bigdata-m:~$ hive

--Create database
CREATE DATABASE cardata;

USE cardata;

--Create table

CREATE EXTERNAL TABLE IF NOT EXISTS used_cars_yichen (
maker STRING,
model STRING,
mileage INT,
manufacture_year STRING,
engine_displacement INT,
engine_power INT,
body_type STRING,
color_slug STRING,
stk_year STRING,
transmission STRING,
door_count INT,
seat_count INT,
fuel_type STRING,
date_created STRING,
datelastseen STRING,
price_eur FLOAT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/BigData/'
TBLPROPERTIES ('skip.header.line.count'='1','serialization.null.format' = '');


---------------------------------------- Data Cleaning: stage 2 ------------------------------------

-- 2-1. see how many missing values you have in each attribute
SELECT count(*) from used_cars_yichen where maker is NULL;
SELECT count(*) from used_cars_yichen where model is NULL;
SELECT count(*) from used_cars_yichen where mileage is NULL;
SELECT count(*) from used_cars_yichen where manufacture_year is NULL;
SELECT count(*) from used_cars_yichen where engine_displacement is NULL;
SELECT count(*) from used_cars_yichen where engine_power is NULL;
SELECT count(*) from used_cars_yichen where body_type is NULL;
SELECT count(*) from used_cars_yichen where color_slug is NULL;
SELECT count(*) from used_cars_yichen where stk_year is NULL;
SELECT count(*) from used_cars_yichen where transmission is NULL;
SELECT count(*) from used_cars_yichen where door_count is NULL;
SELECT count(*) from used_cars_yichen where seat_count is NULL;
SELECT count(*) from used_cars_yichen where fuel_type is NULL;
SELECT count(*) from used_cars_yichen where date_created is NULL;
SELECT count(*) from used_cars_yichen where datelastseen is NULL;
SELECT count(*) from used_cars_yichen where price_eur is NULL;


--***hive query to count how many 'non-NULL' value in specific column
SELECT count(maker) from used_cars_yichen;


--2-2.Remove any records that do not have a price

--There is no record without a price

CREATE TABLE used_cars_yichen_clean_2_2
AS SELECT *FROM used_cars_yichen
WHERE price_eur is not NULL;

DESCRIBE FORMATTED used_cars_yichen_clean_2_2;


--numRows；3552912


--2-3. Remove any records that do not have a model listed

--There are 1133361 records without a model

CREATE TABLE used_cars_yichen_clean_2_3
AS SELECT *FROM used_cars_yichen_clean_2_2
WHERE model is not NULL;

DESCRIBE FORMATTED used_cars_yichen_clean_2_3;

DELETE FROM used_cars_yichen
WHERE model is NULL;


--original total rows ('used_cars_yichen') = 3552912
--total rows of records with model ('used_cars_yichen_clean_2_2')= 2419551
----There are 1133361 records without a model

---------------------------------------- Data Cleaning: stage 3 ------------------------------------

--3-1.Group the price column and count the number of unique prices. 


CREATE TABLE used_cars_yichen_clean_3_1
AS SELECT price_eur FROM  used_cars_yichen_clean_2_3 
GROUP BY price_eur;

DESCRIBE FORMATTED used_cars_yichen_clean_3_1;

--numRows: 167243 (the number of unique prices)


--3-2. Do you notice if there is a single price that is repeating across the ads? 
--Remove records that have these prices.

CREATE TABLE used_cars_yichen_clean_3_2
AS SELECT price_eur, COUNT (price_eur) count_of_price  FROM  used_cars_yichen_clean_2_3 
GROUP BY price_eur;

SELECT price_eur, count_of_price FROM used_cars_yichen_clean_3_2
ORDER BY count_of_price DESC
LIMIT 10;

--price '1295.34' repeat 574753 times

CREATE TABLE used_cars_yichen_clean_3_2_a
AS SELECT * FROM  used_cars_yichen_clean_2_3 
WHERE price_eur != 1295.34;

DESCRIBE FORMATTED used_cars_yichen_clean_3_2_a;

--numRows: 1844798 (=2419551-574753)


---------------------------------------- Data Cleaning: stage 4 ------------------------------------

--4. Find all the records where the model does not have a maker value. 
--Based on the model, fill in the maker value to complete the record. 
--For example, if the model is listed as Civic but does not have a maker value, put “Honda” as the maker.


--make a model_maker_pair table first
CREATE TABLE model_maker_pair
AS SELECT DISTINCT(CONCAT(model, maker)) FROM used_cars_yichen_clean_3_2_a;

DESCRIBE FORMATTED model_maker_pair;

--there are 826 various model_maker combinations in the 'used_cars_yichen_clean_3_2_a' table

SELECT count(*) from used_cars_yichen_clean_3_2_a where maker is NULL;

--there is 0 rows without 'maker' 


----------------------------------------------///Cleaning Task 6///---------------------------------------------

--6. Of the remaining records, remove any other records which you feel are abnormal or cannot be trusted. 
--   This is an open ended question so use your own judgement and creativity.

--(6-1). Exclude rows with unreasonable price_eur
--(6-2). Exclude rows with unreasonable manufacture_year or missing (no early than 1908 - the year first car came out; no later than the year when the ad posted)
--(6-3). Exclude rows with unreasonable milage or missing (ex:0, 9999999)


--6-1. Exclude rows with unreasonable price_eur

--calculate the average and standard deviation by model_maker
CREATE TABLE average_price_by_model_maker
AS SELECT model, maker, AVG(price_eur) avg_price ,STDDEV(price_eur) sd_price FROM  used_cars_yichen_clean_3_2_a
GROUP BY model, maker;

DESCRIBE FORMATTED average_price_by_model_maker;

SELECT * FROM average_price_by_model_maker
ORDER BY avg_price DESC
LIMIT 10;

--(Average price by model_maker)
kangoo  renault 7.009004778337162E8     4.3545727108604965E10
impreza subaru  1.0991529486164475E7    4.030591851533493E8
berlingo        citroen 4248744.7203723835      2.5152784926156342E8
v8      audi    588181.6242049324       3839765.68180293
aventador       lamborghini     368228.04374951654      140258.30912585667
lancer  mitsubishi      305817.8635682135       7774780.251419145
carrera-gt      porsche 272783.0702237216       347487.460756318
a5      audi    255972.56859279692      1.3655389152282294E7
z8      bmw     245118.60092905405      61622.247922119735
xm      citroen 200067.5044852933       2304741.5361049357

--The average are serious biased, so this analysis exclude the unreasonable prices first.

-- MAX: high end car, 2021, very low mileage (<1,000) 
--(for example: 2021 Mercedes-Benz A-Class, mileage=999, $37,075 = $35766.07 euro)

--MIN: Most affordable car, 2000, very high mileage (400,000)
--(for example: 2000 Toyota 4Runner, mileage=400,000, $1380 = $1331.28 euro)

--https://www.consumerreports.org/cars/car-value-estimator/


--Reasonable used car price:
1331.28 < price_eur < 35766.07

--SELECT rows with reasonable prices
CREATE TABLE used_cars_yichen_clean_6_1
AS SELECT * FROM used_cars_yichen_clean_3_2_a
WHERE 1331.28 < price_eur AND price_eur < 35766.07;

DESCRIBE FORMATTED used_cars_yichen_clean_6_1;


--there are 1629858 rows in 'used_cars_yichen_clean_6_1'
--there are 214,940 rows with unreasonable prices are excluded (1844798 - 1629858)

--Let's view the agerage and standard deviation again

CREATE TABLE average_price_by_model_maker_2
AS SELECT model, maker, AVG(price_eur) avg_price ,STDDEV(price_eur) sd_price FROM  used_cars_yichen_clean_6_1
GROUP BY model, maker;

DESCRIBE FORMATTED average_price_by_model_maker_2;

SELECT * FROM average_price_by_model_maker_2
ORDER BY avg_price DESC
LIMIT 10;

--(Average price by model_maker) --2nd run
q70     infinity        33841.746744791664      1704.098946630235
continental-flying-spur bentley 33350.037760416664      1647.619011733293
qx70    infinity        32700.252734375 966.3578728525526
continental-gt  bentley 32396.80234375  1583.2688725579544
h1      hummer  31669.272194602272      4242.638410159205
i3      bmw     29990.86916613223       5404.980436169807
qx56    infinity        29925.0 3075.0
q45     infinity        29900.0 0.0
rs3     audi    29855.20176003196       5809.4125171969035
lx-570  lexus   29454.5498046875        45.4501953125

--6-2. Exclude rows with unreasonable manufacture_year or missing (no early than 1908 - the year first car came out; no later than the year when the ad posted)
--https://www.carsguide.com.au/car-advice/who-invented-the-first-car-and-when-was-it-made-76976


SELECT manufacture_year FROM used_cars_yichen_clean_6_1
GROUP BY manufacture_year
ORDER BY manufacture_year DESC
LIMIT 10;

992
991
990
99
988
974
965
960
959
958


SELECT manufacture_year FROM used_cars_yichen_clean_6_1
GROUP BY manufacture_year
ORDER BY manufacture_year
LIMIT 10;

0
10
1000
1001
1003
1006
1007
1009
1010


CREATE TABLE used_cars_yichen_clean_6_2
AS SELECT * FROM  used_cars_yichen_clean_6_1 
WHERE manufacture_year >= 1908 AND manufacture_year <= year(date_created);

DESCRIBE FORMATTED used_cars_yichen_clean_6_2;

--there are 1483586 rows in 'used_cars_yichen_clean_6_2'
--146,272 rows with a manufature_year < 1908 were excluded (1629858 - 1483586)


SELECT manufacture_year FROM used_cars_yichen_clean_6_2
GROUP BY manufacture_year
ORDER BY manufacture_year
LIMIT 10;



--6-3. Exclude rows with unreasonable milage or missing (ex:0, 9999999)

SELECT mileage FROM used_cars_yichen_clean_6_2
GROUP BY mileage
ORDER BY mileage
LIMIT 10;

NULL
0
1
2
3
4
5
6
7
8

SELECT mileage FROM used_cars_yichen_clean_6_2
GROUP BY mileage
ORDER BY mileage DESC
LIMIT 10;

9999999
9899800
9754500
9635898
9370350
9352000
9309100
9294000
9288000
9284000

--The mileage increases 10,000 miles (16,093 KM) to 12,000 (19,312 KM) per year for regular-use cars  
--(https://www.progressive.com/answers/used-car-mileage/). 

--Abnormal mileages (will be excluded):
--mileage < 16,093*0.5 KM = 8047 KM
--mileage > (year of ad – year of manufacture)*19,312 *2 KM

CREATE TABLE used_cars_yichen_clean_6_3
AS SELECT * FROM  used_cars_yichen_clean_6_2 
WHERE mileage >= 8047 AND mileage < (year(date_created)-manufacture_year)*19312*2;


DESCRIBE FORMATTED used_cars_yichen_clean_6_3;

--there are 1129441 rows in 'used_cars_yichen_clean_6_3'
--354145 rows with abnormal mileages were excluded (1483586 - 1129441)


SELECT mileage FROM used_cars_yichen_clean_6_3
GROUP BY mileage
ORDER BY mileage
LIMIT 10;

8047
8048
8049
8050
8051
8052
8053
8054
8055
8056

SELECT mileage FROM used_cars_yichen_clean_6_3
GROUP BY mileage
ORDER BY mileage DESC
LIMIT 10;

1940000
1234567
1129000
1111111
1000000
999999
997652
990000
970000
933534

----------------------------------------------///Cleaning Task 5///---------------------------------------------

--5. Find the average price for cars of different models and makers from an external source (cite this source) 
--   or make a best estimate on these values. Then write queries that will remove any records where prices are 
--   multiple factors above this price. The factor can be chosen by you based on your best judgement. 
--   For example, let’s say that Honda Civic price for 2015 with 100,000 Km is $5,000. 
--   Then remove any records for Honda Civic where price is more than 3 times this, 
--   ie price > $15,000. Note prices in the dataset are in Euros.


--create new column: mileage_level

--ref: High/Low mileage
--100,000 miles (160,934 KM) is considered a cut-off point for used cars
--https://www.progressive.com/answers/used-car-mileage/

CREATE TABLE used_cars_yichen_clean_5_1
AS SELECT *, 
CASE
WHEN mileage >= 160934  THEN 'High_mile'
ELSE 'Low_mile'
END AS mileage_level
FROM used_cars_yichen_clean_6_3;

SELECT COUNT(*) FROM used_cars_yichen_clean_5_1 WHERE mileage_level ='High_mile';

SELECT COUNT(*) FROM used_cars_yichen_clean_5_1 WHERE mileage_level ='Low_mile';

--number of 'High_mile':222527
--number of 'Low_mile':906914
--1129441 rows in 'used_cars_yichen_clean_5_1'


CREATE TABLE average_price_by_4_factors
AS SELECT model, maker, manufacture_year, mileage_level, AVG(price_eur) avg_price ,STDDEV(price_eur) sd_price 
FROM  used_cars_yichen_clean_5_1
GROUP BY model, maker, manufacture_year, mileage_level;

DESCRIBE FORMATTED average_price_by_4_factors;

--14084 rows in 'average_price_by_4_factors'

SELECT * FROM average_price_by_4_factors
LIMIT 20;


--join average and std to the table which will be used to analysis in next steps
--Left join

--left table: used_cars_yichen_clean_5_1
--Right table: average_price_by_4_factors


CREATE TABLE used_cars_yichen_clean_5_2
AS SELECT L.*, R.avg_price, R.sd_price
FROM used_cars_yichen_clean_5_1 L LEFT OUTER JOIN average_price_by_4_factors R
ON L.model = R.model AND L.maker = R.maker AND L.manufacture_year = R.manufacture_year AND L.mileage_level = R.mileage_level;

DESCRIBE FORMATTED used_cars_yichen_clean_5_2;

--there are 1129441 rows in 'used_cars_yichen_clean_5_2'


CREATE TABLE used_cars_yichen_clean_5_3
AS SELECT * FROM used_cars_yichen_clean_5_2
WHERE price_eur > (avg_price - 3*sd_price ) AND price_eur < (avg_price + 3*sd_price );

DESCRIBE FORMATTED used_cars_yichen_clean_5_3;

--there are 1114112 rows in 'used_cars_yichen_clean_5_3'
--15,329 outliers are excluded





--------------------------------////////Analysis///////////----------------------------------------


--/////////Part I. A snapshot of Luxury used cars///////////////////

--car details group by model_maker

CREATE TABLE model_maker_info
AS SELECT model, maker, COUNT(CONCAT(model, maker)) model_maker_count, 
AVG(price_eur), AVG(mileage), AVG(engine_displacement)  
FROM  used_cars_yichen_clean_5_3
GROUP BY model, maker;


DESCRIBE FORMATTED model_maker_info;

--LUXURY car details group by model_maker

CREATE TABLE model_maker_info_luxury
AS SELECT * FROM model_maker_info
WHERE maker IN ('audi', 'bmw', 'lexus', 'mercedes-benz', 'porsche', 'volvo', 'bentley')
ORDER BY model_maker_count DESC;

DESCRIBE FORMATTED model_maker_info_luxury;

--1. Makers with most luxury used car ads

SELECT maker, SUM(model_maker_count) quantity FROM model_maker_info_luxury
GROUP BY maker 
ORDER BY quantity DESC;


--2. The 20 luxury used cars with most ads

SELECT * FROM model_maker_info_luxury
LIMIT 20;

--////// Add dimention: YEAR ///////////

--car details group by model_maker_year

CREATE TABLE model_maker_info_yearly
AS SELECT model, maker, COUNT(CONCAT(model, maker)) model_maker_count, manufacture_year,
AVG(price_eur), AVG(mileage), AVG(engine_displacement)  
FROM  used_cars_yichen_clean_5_3
GROUP BY model, maker, manufacture_year;

DESCRIBE FORMATTED model_maker_info_yearly;

--numRows : 7287

--LUXURY car details group by model_maker_year
CREATE TABLE model_maker_info_luxury_yearly
AS SELECT * FROM model_maker_info_yearly
WHERE maker IN ('audi', 'bmw', 'lexus', 'mercedes-benz', 'porsche', 'volvo', 'bentley')
ORDER BY model_maker_count DESC;

DESCRIBE FORMATTED model_maker_info_luxury_yearly;

--numRows : 1307


--10 years LUXURY car details group by model_maker_year
CREATE TABLE model_maker_info_luxury_yearly_less10
AS SELECT * FROM model_maker_info_luxury_yearly
WHERE maker IN ('audi', 'bmw', 'lexus', 'mercedes-benz', 'porsche', 'volvo', 'bentley') AND manufacture_year >= 2012
ORDER BY model_maker_count DESC;

DESCRIBE FORMATTED model_maker_info_luxury_yearly_less10;

--numRows : 194

--Top LUXURY 20 cars less than 10 years by ads quantity

SELECT * FROM model_maker_info_luxury_yearly_less10
LIMIT 20;



--////////////////////////// TOP 10 used cars for driving sales ///////////////////////////

--Criteria
--a. Maker: Luxury car brands in this dataset include Audi, BMW, Lexus, Mercedes-Benz, Porsche, Volvo, Bentley.
--b. Manufacture year: >= 2012
--c. Mileage: < 16,093 KM.
--d. Fuel economy: Engine displacement <= 2000 ccm.
--e. Among those 20 cars with the highest ads quantity and meet criteria a, b, c, and d, 
--   choose 10 cars that have the lowest prices.
--f. The total ads quantity of the chosen 10 cars that meet criteria a, b, c, d, and e, 
--   should be higher than 3,180. 


CREATE TABLE model_maker_selection_1
AS SELECT * FROM used_cars_yichen_clean_5_3
Where maker IN ('audi', 'bmw', 'lexus', 'mercedes-benz', 'porsche', 'volvo', 'bentley') 
AND manufacture_year >= 2012
AND mileage < 16093
AND engine_displacement <= 2000;

DESCRIBE FORMATTED model_maker_selection_1;

CREATE TABLE model_maker_selection_2
AS SELECT model, maker, COUNT(CONCAT(model, maker)) model_maker_count,
AVG(price_eur), AVG(mileage), AVG(engine_displacement)  
FROM  model_maker_selection_1
GROUP BY model, maker
ORDER BY model_maker_count DESC;


DESCRIBE FORMATTED model_maker_selection_2;

--numRows: 29

SELECT * FROM model_maker_selection_2;

--/////////////////////////////////  Export CSV Files  ///////////////////////////////////

--Export model_maker_info_luxury (Export table to csv file from HDFS to Local)

INSERT OVERWRITE LOCAL DIRECTORY '/home/ychsiaoca/model_maker_info_luxury' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
select * from model_maker_info_luxury;

--download path: '/home/ychsiaoca/model_maker_info_luxury/000000_0'

INSERT OVERWRITE LOCAL DIRECTORY '/home/ychsiaoca/model_maker_info_luxury_yearly' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
select * from model_maker_info_luxury_yearly;

--download path: '/home/ychsiaoca/model_maker_info_luxury_yearly/000000_0'

INSERT OVERWRITE LOCAL DIRECTORY '/home/ychsiaoca/model_maker_selection_2' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
select * from model_maker_selection_2;

--download path: '/home/ychsiaoca/model_maker_selection_2/000000_0'





--How Much Mileage is Too Much?
--Let’s get straight to the point! The average mileage per year is about 24,000 kilometers.
--To see if a car's mileage is within a reasonable range, simply multiply 24,000 by the car's age 
--and see if the mileage reading on the odometer is higher or lower than that. You can also just 
--divide the car's odometer reading by its age to get the average reading.
--https://www.carpages.ca/blog/high-mileage-used-cars/



used_cars_yichen_clean_reasonable_price
select max(Salary) from employee_data;

SELECT MAX(mileage) FROM used_cars_yichen_clean_reasonable_price;
SELECT MIN(mileage) FROM used_cars_yichen_clean_reasonable_price;

--MAX(mileage) = 9999999
--MIN(mileage) = 0


CREATE TABLE count_manufacture_year
AS SELECT manufacture_year, COUNT (manufacture_year) count_manu_year  FROM  used_cars_yichen_clean_reasonable_price 
GROUP BY manufacture_year;

SELECT manufacture_year, count_manu_year FROM count_manufacture_year
ORDER BY count_manu_year DESC
LIMIT 20;

2015    260003
2012    129691
2011    122809
2014    104107
2013    87784
2010    86031
2007    78027
2008    77193
2009    77027
2006    73617
2005    66234
2016    60215
2004    58218
2003    49296
2002    39596
2001    33053
2000    24791
1999    17582
1998    10552
1997    5719
