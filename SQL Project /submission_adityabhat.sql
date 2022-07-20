/*

-----------------------------------------------------------------------------------------------------------------------------------
                                               Guidelines
-----------------------------------------------------------------------------------------------------------------------------------

The provided document is a guide for the project. Follow the instructions and take the necessary steps to finish
the project in the SQL file			
-----------------------------------------------------------------------------------------------------------------------------------

											Database Creation
                                               
-----------------------------------------------------------------------------------------------------------------------------------
*/

-- [1] To begin with the project, you need to create the database first
-- Write the Query below to create a Database
DROP DATABASE IF EXISTS VEHDB;
CREATE DATABASE VEHDB;

-- [2] Now, after creating the database, you need to tell MYSQL which database is to be used.
-- Write the Query below to call your Database
USE vehdb;

/*-----------------------------------------------------------------------------------------------------------------------------------

                                               Tables Creation
                                               
-----------------------------------------------------------------------------------------------------------------------------------*/

-- [3] Creating the tables:

/*Note:
---> To create the table, refer to the ER diagram and the solution architecture. 
---> Refer to the column names along with the data type while creating a table from the ER diagram.
---> If needed revisit the videos Week 2: Data Modeling and Architecture: Creating DDLs for Your Main Dataset and Normalized Datasets
---> While creating a table, make sure the column you assign as a primary key should uniquely identify each row.
---> If needed revisit the codes used to create tables for the gl_eats database. 
     This will help in getting a better understanding of table creation.*/
                                                  
/* List of tables to be created.

 Create a table temp_t, vehicles_t, order_t, customer_t, product_t, shipper_t */
 DROP TABLE IF EXISTS temp_t;                          

-- Creating table temp_t--
CREATE TABLE temp_t (
	SHIPPER_ID INTEGER,
    SHIPPER_NAME VARCHAR(50),
    SHIPPER_CONTACT_DETAILS VARCHAR(30),
    PRODUCT_ID INTEGER,
    VEHICLE_MAKER VARCHAR(60),
    VEHICLE_MODEL VARCHAR(60),
    VEHICLE_COLOR VARCHAR(60),
    VEHICLE_MODEL_YEAR INTEGER,
    VEHICLE_PRICE DECIMAL(14,2),
    QUANTITY INTEGER,
	DISCOUNT DECIMAL(4,2),
    CUSTOMER_ID INTEGER,
    CUSTOMER_NAME VARCHAR(25),
    GENDER VARCHAR(15),
    JOB_TITLE VARCHAR(50),
    PHONE_NUMBER VARCHAR(20),
    EMAIL_ADDRESS VARCHAR(50),
    CITY VARCHAR(25),
    COUNTRY VARCHAR(40),
    STATE VARCHAR(40),
    CUSTOMER_ADDRESS VARCHAR(50),
    ORDER_DATE DATE,
    ORDER_ID INTEGER,
    SHIP_DATE DATE,
    SHIP_MODE VARCHAR(25),
    SHIPPING VARCHAR(30),
    POSTAL_CODE INTEGER,
    CREDIT_CARD_TYPE VARCHAR(40),
    CREDIT_CARD_NUMBER BIGINT,
    CUSTOMER_FEEDBACK VARCHAR(20),
    QUARTER_NUMBER INTEGER,
	PRIMARY KEY (SHIPPER_ID,ORDER_ID,CUSTOMER_ID,PRODUCT_ID)
);
    
-- Creating table vehicles_t--
DROP TABLE IF EXISTS vehicles_t;
CREATE TABLE vehicles_t(
   SHIPPER_ID INTEGER,
    SHIPPER_NAME VARCHAR(50),
    SHIPPER_CONTACT_DETAILS VARCHAR(30),
    PRODUCT_ID INTEGER,
    VEHICLE_MAKER VARCHAR(60),
    VEHICLE_MODEL VARCHAR(60),
    VEHICLE_COLOR VARCHAR(60),
    VEHICLE_MODEL_YEAR INTEGER,
    VEHICLE_PRICE DECIMAL(14,2),
    QUANTITY INTEGER,
	DISCOUNT DECIMAL(4,2),
    CUSTOMER_ID INTEGER,
    CUSTOMER_NAME VARCHAR(25),
    GENDER VARCHAR(15),
    JOB_TITLE VARCHAR(50),
    PHONE_NUMBER VARCHAR(20),
    EMAIL_ADDRESS VARCHAR(50),
    CITY VARCHAR(25),
    COUNTRY VARCHAR(40),
    STATE VARCHAR(40),
    CUSTOMER_ADDRESS VARCHAR(50),
    ORDER_DATE DATE,
    ORDER_ID INTEGER,
    SHIP_DATE DATE,
    SHIP_MODE VARCHAR(25),
    SHIPPING VARCHAR(30),
    POSTAL_CODE INTEGER,
    CREDIT_CARD_TYPE VARCHAR(40),
    CREDIT_CARD_NUMBER BIGINT,
    CUSTOMER_FEEDBACK VARCHAR(20),
    QUARTER_NUMBER INTEGER,
	PRIMARY KEY (SHIPPER_ID,ORDER_ID,CUSTOMER_ID,PRODUCT_ID)
);
    
-- Creating table product_t--
DROP TABLE IF EXISTS product_t;
CREATE TABLE product_t(

    PRODUCT_ID INTEGER,
    VEHICLE_MAKER VARCHAR(60),
    VEHICLE_MODEL VARCHAR(60),
    VEHICLE_COLOR VARCHAR(60),
    VEHICLE_MODEL_YEAR INTEGER,
    VEHICLE_PRICE DECIMAL(14,2),
    PRIMARY KEY(PRODUCT_ID)
);

-- Creating table order_t--
DROP TABLE IF EXISTS order_t;
CREATE TABLE order_t(
	 ORDER_ID INTEGER,
	 CUSTOMER_ID INTEGER,
	 SHIPPER_ID INTEGER,
	 PRODUCT_ID INTEGER,
	 QUANTITY INTEGER,
	 VEHICLE_PRICE DECIMAL(14,2),
	 ORDER_DATE DATE,
	 SHIP_DATE DATE,
	 DISCOUNT DECIMAL(4,2),
	 SHIP_MODE VARCHAR(25),
	 SHIPPING VARCHAR(30),
	 CUSTOMER_FEEDBACK VARCHAR(20),
	 QUARTER_NUMBER INTEGER,
	 PRIMARY KEY(SHIPPER_ID,ORDER_ID,CUSTOMER_ID,PRODUCT_ID)
);

-- Creating table customer_t--
DROP TABLE IF EXISTS customer_t;
CREATE TABLE customer_t(
	CUSTOMER_ID INTEGER,
    CUSTOMER_NAME VARCHAR(25),
    GENDER VARCHAR(15),
    JOB_TITLE VARCHAR(50),
    PHONE_NUMBER VARCHAR(20),
    EMAIL_ADDRESS VARCHAR(50),
    CITY VARCHAR(25),
    COUNTRY VARCHAR(40),
    STATE VARCHAR(40),
    CUSTOMER_ADDRESS VARCHAR(50),
	POSTAL_CODE INTEGER,
    CREDIT_CARD_TYPE VARCHAR(40),
    CREDIT_CARD_NUMBER BIGINT,
    PRIMARY KEY(CUSTOMER_ID)
);

-- Creating table shipper_t--
DROP TABLE IF EXISTS shipper_t;
CREATE TABLE shipper_t(
    SHIPPER_ID INTEGER,
    SHIPPER_NAME VARCHAR(50),
    SHIPPER_CONTACT_DETAILS VARCHAR(30),
    PRIMARY KEY(SHIPPER_ID)
);

/*-----------------------------------------------------------------------------------------------------------------------------------

                                               Stored Procedures Creation
                                               
-----------------------------------------------------------------------------------------------------------------------------------*/

-- [4] Creating the Stored Procedures:

/*Note:

---> If needed revisit the video: Week 2: Data Modeling and Architecture: Introduction to Stored Procedures.
---> Also revisit the codes used to create stored procedures for the gl_eats database. 
	 This will help in getting a better understanding of the creation of stored procedures.*/

-- Syntax to create stored procedure-

-- To drop the stored procedure if already exists- 
DROP PROCEDURE IF EXISTS procedure_name;

-- Syntax to create a stored procedure-
DELIMITER $$ 
CREATE PROCEDURE procedure_name()
BEGIN
       INSERT INTO table_name (
	column_name1,
    column_name2,
    ..
    ..
    ..              
) SELECT * FROM table_name;
END;


/* List of stored procedures to be created.

   Creating the stored procedure for vehicles_p, order_p, customer_p, product_p, shipper_p*/
-- Stored procedure to populate the vehicles table--
DELIMITER $$ 
CREATE PROCEDURE vehicles_p()
BEGIN  
	INSERT INTO VEHDB.vehicles_t(
		SHIPPER_ID,
		SHIPPER_NAME,
		SHIPPER_CONTACT_DETAILS,
		PRODUCT_ID,
		VEHICLE_MAKER,
		VEHICLE_MODEL,
		VEHICLE_COLOR,
		VEHICLE_MODEL_YEAR,
		VEHICLE_PRICE,
		QUANTITY,
		DISCOUNT,
		CUSTOMER_ID,
		CUSTOMER_NAME,
		GENDER,
		JOB_TITLE,
		PHONE_NUMBER,
		EMAIL_ADDRESS,
		CITY,
		COUNTRY,
		STATE,
		CUSTOMER_ADDRESS,
		ORDER_DATE,
		ORDER_ID,
		SHIP_DATE,
		SHIP_MODE,
		SHIPPING,
		POSTAL_CODE,
		CREDIT_CARD_TYPE,
		CREDIT_CARD_NUMBER,
		CUSTOMER_FEEDBACK,
		QUARTER_NUMBER
    ) SELECT * FROM VEHDB.temp_t;
END;

-- Stored procedure to populate the products table--
DELIMITER $$ 
CREATE PROCEDURE products_p()
BEGIN  
	INSERT INTO product_t(
		PRODUCT_ID,
		VEHICLE_MAKER,
		VEHICLE_MODEL,
		VEHICLE_COLOR,
		VEHICLE_MODEL_YEAR,
		VEHICLE_PRICE
    ) SELECT DISTINCT
		PRODUCT_ID,
		VEHICLE_MAKER,
		VEHICLE_MODEL,
		VEHICLE_COLOR,
		VEHICLE_MODEL_YEAR,
		VEHICLE_PRICE
	  FROM vehicles_t WHERE PRODUCT_ID NOT IN (SELECT DISTINCT PRODUCT_ID FROM product_t);
END;

-- Stored procedure to populate the orders table--
DELIMITER $$ 
CREATE PROCEDURE orders_p(qtrnumber INTEGER)
BEGIN  
  INSERT INTO order_t(
	  ORDER_ID,
	  CUSTOMER_ID,
	  SHIPPER_ID,
	  PRODUCT_ID,
	  QUANTITY,
	  VEHICLE_PRICE,
	  ORDER_DATE,
	  SHIP_DATE,
	  DISCOUNT,
	  SHIP_MODE,
	  SHIPPING,
	  CUSTOMER_FEEDBACK ,
	  QUARTER_NUMBER
   )SELECT DISTINCT
	  ORDER_ID,
	  CUSTOMER_ID,
	  SHIPPER_ID,
	  PRODUCT_ID,
	  QUANTITY,
	  VEHICLE_PRICE,
	  ORDER_DATE,
	  SHIP_DATE,
	  DISCOUNT,
	  SHIP_MODE,
	  SHIPPING,
	  CUSTOMER_FEEDBACK ,
	  QUARTER_NUMBER
	FROM vehicles_t WHERE QUARTER_NUMBER=qtrnumber;
END;

-- Stored procedure to populate the customer table--
DELIMITER $$ 
CREATE PROCEDURE customers_p()
BEGIN  
   INSERT INTO customer_t(
		CUSTOMER_ID,
		CUSTOMER_NAME,
		GENDER,
		JOB_TITLE,
		PHONE_NUMBER,
		EMAIL_ADDRESS,
		CITY,
		COUNTRY,
		STATE,
		CUSTOMER_ADDRESS,
		POSTAL_CODE,
		CREDIT_CARD_TYPE,
		CREDIT_CARD_NUMBER
   ) SELECT DISTINCT 
		CUSTOMER_ID,
		CUSTOMER_NAME,
		GENDER,
		JOB_TITLE,
		PHONE_NUMBER,
		EMAIL_ADDRESS,
		CITY,
		COUNTRY,
		STATE,
		CUSTOMER_ADDRESS,
		POSTAL_CODE,
		CREDIT_CARD_TYPE,
		CREDIT_CARD_NUMBER
    FROM vehicles_t WHERE CUSTOMER_ID NOT IN (SELECT DISTINCT CUSTOMER_ID FROM customer_t);
END;

-- Stored procedure to populate the shipper table--
DELIMITER $$ 
CREATE PROCEDURE shipper_p()
BEGIN  
  INSERT INTO shipper_t(
  
	  SHIPPER_ID,
      SHIPPER_NAME,
      SHIPPER_CONTACT_DETAILS
  ) SELECT DISTINCT
      SHIPPER_ID,
      SHIPPER_NAME,
      SHIPPER_CONTACT_DETAILS
	FROM vehicles_t WHERE SHIPPER_ID NOT IN (SELECT DISTINCT SHIPPER_ID FROM shipper_t);
END;

/*-----------------------------------------------------------------------------------------------------------------------------------

                                               Data Ingestion
                                               
-----------------------------------------------------------------------------------------------------------------------------------*/

-- [5] Ingesting the data:
-- Note: Revisit the video: Week-2: Data Modeling and Architecture: Ingesting data into the main table

TRUNCATE temp_t;

LOAD DATA LOCAL INFILE "/Users/adityaravindrabhat/Desktop/PGP_UTAustin/SQL/Project/new_wheels_proj/Data/new_wheels_sales_qtr_4.csv" -- change this location to load another week
INTO TABLE temp_t
FIELDS TERMINATED by ','
OPTIONALLY ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Calling the stored procedures to ingest the data --
call vehicles_p();
call customers_p();
call products_p();
call shipper_p();
call orders_p(4);

/* Note: 

---> With the help of the above code, you can ingest the data into temp_t table by ingesting the quarterly data and by calling the stored 
     procedures you can ingest the data into separate table.
---> You have to run the above ingestion code 4 times as 4 quarters of data are present and you also need to call all the stored procedures 
     4 times. Please change the argument value while calling the stored procedure order_p(n). (n = 1,2,3,4)
---> If needed revisit the videos: Week 2: Data Modeling and Architecture: Ingesting data into the main table and Ingesting future weeks of data
---> Also revisit the codes used to ingest the data for the gl_eats database. 
     This will help in getting a better understanding of how to ingest the data into various respective tables.*/


/*-----------------------------------------------------------------------------------------------------------------------------------

                                               Views Creation
                                               
-----------------------------------------------------------------------------------------------------------------------------------*/

-- [6] Creating the views:

/*Note: 

---> If needed revisit the videos: Week-2: Data Modeling and Architecture: Creating views for answers to business questions
---> Also revisit the codes used to create views for the gl_eats database. 
	 This will help in getting a better understanding of the creation of views.*/

-- Syntax to create view-

-- To drop the views if already exists- 
DROP VIEW IF EXISTS view_name;

-- To create a view-
CREATE VIEW view_name AS
    SELECT
	n1.column_name1,
    n2.column_name2,
    ..
    ..
    ..
FROM table_name1 n1
	INNER JOIN table_name2 n2
	    ON n1.column_name1 = n2.column_name2;

-- List of views to be created are "veh_prod_cust_v" , "veh_ord_cust_v"

-- Creating a view for veh_prod_cust --
DROP VIEW IF EXISTS veh_ord_cust_v;

CREATE VIEW veh_ord_cust_v AS
	SELECT
		cust.CUSTOMER_ID,
        cust.CUSTOMER_NAME,
        cust.CITY,
        cust.STATE,
        cust.CREDIT_CARD_TYPE,
        ord.ORDER_ID,
        ord.SHIPPER_ID,
        ord.PRODUCT_ID,
        ord.QUANTITY,
        ord.VEHICLE_PRICE,
        ord.ORDER_DATE,
        ord.SHIP_DATE,
        ord.DISCOUNT,
        ord.CUSTOMER_FEEDBACK,
        ord.QUARTER_NUMBER
	FROM 
		customer_t AS cust
	JOIN order_t AS ord USING (CUSTOMER_ID);
			
-- Creating a view for veh_ord_cust --
DROP VIEW IF EXISTS veh_prod_cust_v;

CREATE VIEW veh_prod_cust_v AS
	SELECT
		cust.CUSTOMER_ID,
		cust.CUSTOMER_NAME,
		cust.CREDIT_CARD_TYPE,
        cust.STATE,
        ord.ORDER_ID,
        ord.CUSTOMER_FEEDBACK,
        prod.PRODUCT_ID,
        prod.VEHICLE_MAKER,
        prod.VEHICLE_MODEL,
        prod.VEHICLE_COLOR,
        prod.VEHICLE_MODEL_YEAR
	FROM 
		customer_t AS cust
	JOIN order_t AS ord USING (CUSTOMER_ID)
	JOIN product_t AS prod USING (PRODUCT_ID);
/*-----------------------------------------------------------------------------------------------------------------------------------

                                               Functions Creation
                                               
-----------------------------------------------------------------------------------------------------------------------------------*/

-- [7] Creating the functions:

/*Note: 

---> If needed revisit the videos: Week-2: Data Modeling and Architecture: Creating User Defined Functions
---> Also revisit the codes used to create functions for the gl_eats database. 
     This will help in getting a better understanding of the creation of functions.*/

-- Create the function calc_revenue_f

-- Syntax to create function-

-- Function to calculate the revenue of the order after discount --
DELIMITER $$  
CREATE FUNCTION calc_revenues_f (quantity INT, vehicle_price DECIMAL(14,2), discount DECIMAL(4,2) ) 
RETURNS DECIMAL
DETERMINISTIC  
BEGIN  
	DECLARE revenue DECIMAL(25,2);
    IF quantity =0 THEN
		SET revenue =0;
	ELSE
		SET revenue = quantity * vehicle_price *(1-discount);
	END IF;
    RETURN (revenue);

END;

-- Function to calculate the numbers of days between the order date and shipping date--
 

/*-----------------------------------------------------------------------------------------------------------------------------------
Note: 
After creating tables, stored procedures, views and functions, attempt the below questions.
Once you have got the answer to the below questions, download the csv file for each question and use it in Python for visualisations.
------------------------------------------------------------------------------------------------------------------------------------ 

  
  
-----------------------------------------------------------------------------------------------------------------------------------

                                                         Queries
                                               
-----------------------------------------------------------------------------------------------------------------------------------*/
  
/*-- QUESTIONS RELATED TO CUSTOMERS
     [Q1] What is the distribution of customers across states?
     Hint: For each state, count the number of customers.*/

SELECT
	COUNT(CUSTOMER_ID) AS CUST_PER_STATE,
    STATE
FROM 
	veh_ord_cust_v
GROUP BY 2
ORDER BY 
	CUST_PER_STATE DESC;
-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q2] What is the average rating in each quarter?
-- Very Bad is 1, Bad is 2, Okay is 3, Good is 4, Very Good is 5.

Hint: Use a common table expression and in that CTE, assign numbers to the different customer ratings. 
      Now average the feedback for each quarter. 

Note: For reference, refer to question number 10. Week-2: Hands-on (Practice)-GL_EATS_PRACTICE_EXERCISE_SOLUTION.SQL. 
      You'll get an overview of how to use common table expressions from this question.*/

WITH RATING_BUCKET AS
(
SELECT 
	CUSTOMER_FEEDBACK,
	QUARTER_NUMBER,
		CASE 
			WHEN CUSTOMER_FEEDBACK='Very Bad' THEN 1
            WHEN CUSTOMER_FEEDBACK='Bad' THEN 2
            WHEN CUSTOMER_FEEDBACK='Okay' THEN 3
            WHEN CUSTOMER_FEEDBACK='Good' THEN 4
            WHEN CUSTOMER_FEEDBACK='Very Good' THEN 5
		END AS NUMERIC_RATING
        FROM 
			veh_ord_cust_v
)
SELECT 
	 QUARTER_NUMBER,
	 AVG(NUMERIC_RATING) AS QUARTERLY_RATING
FROM 
	RATING_BUCKET
GROUP BY 1
ORDER BY 1;
-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q3] Are customers getting more dissatisfied over time?

Hint: Need the percentage of different types of customer feedback in each quarter. Use a common table expression and
	  determine the number of customer feedback in each category as well as the total number of customer feedback in each quarter.
	  Now use that common table expression to find out the percentage of different types of customer feedback in each quarter.
      Eg: (total number of very good feedback/total customer feedback)* 100 gives you the percentage of very good feedback.
      
Note: For reference, refer to question number 10. Week-2: Hands-on (Practice)-GL_EATS_PRACTICE_EXERCISE_SOLUTION.SQL. 
      You'll get an overview of how to use common table expressions from this question*/
      
WITH FEEDBACK_BUCKET AS 
(
	SELECT
		CUSTOMER_FEEDBACK,
		COUNT(CUSTOMER_FEEDBACK) AS FEEDBACK_PER_CATEGORY,
		QUARTER_NUMBER
	FROM
		 veh_ord_cust_v
	GROUP BY 3,1
    ORDER BY 3
),TOTAL_FEEDBACK AS
(
	SELECT 
		QUARTER_NUMBER,
		SUM(FEEDBACK_PER_CATEGORY) AS NO_FEEDBACK_QUARTER
	FROM 
		FEEDBACK_BUCKET
	GROUP BY 1
    
)
SELECT 
	    fb.CUSTOMER_FEEDBACK,
		fb.FEEDBACK_PER_CATEGORY,
		fb.QUARTER_NUMBER,
        tf.NO_FEEDBACK_QUARTER,
        round((fb.FEEDBACK_PER_CATEGORY/tf.NO_FEEDBACK_QUARTER)*100,2) AS FEEDBACK_PERCENTAGE
FROM 
	TOTAL_FEEDBACK tf 
    JOIN FEEDBACK_BUCKET fb ON fb.QUARTER_NUMBER=tf.QUARTER_NUMBER;

-- ---------------------------------------------------------------------------------------------------------------------------------

/*[Q4] Which are the top 5 vehicle makers preferred by the customer.

Hint: For each vehicle make what is the count of the customers.*/

SELECT
    VEHICLE_MAKER,
	COUNT(CUSTOMER_ID) AS CUST_COUNT_BY_MAKER 
FROM 
	veh_prod_cust_v
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;
-- ---------------------------------------------------------------------------------------------------------------------------------

/*[Q5] What is the most preferred vehicle make in each state?

Hint: Use the window function RANK() to rank based on the count of customers for each state and vehicle maker. 
After ranking, take the vehicle maker whose rank is 1.*/
WITH PREFERRED_MAKER AS
(
    SELECT
		STATE,
        VEHICLE_MAKER,
		COUNT(CUSTOMER_ID) AS CUST_COUNT,
		RANK()OVER(PARTITION BY STATE ORDER BY COUNT(CUSTOMER_ID) DESC) AS RNK
	FROM 
		veh_prod_cust_v
	GROUP BY 1,2
    ORDER BY 3 DESC
)
SELECT 
	*
FROM 
	PREFERRED_MAKER
WHERE
	RNK=1;
    
-- ---------------------------------------------------------------------------------------------------------------------------------

/*QUESTIONS RELATED TO REVENUE and ORDERS 

-- [Q6] What is the trend of number of orders by quarters?

Hint: Count the number of orders for each quarter.*/

SELECT 
	COUNT(ORDER_ID) AS NO_OF_ORDERS,
    QUARTER_NUMBER
FROM 
	veh_ord_cust_v
GROUP BY QUARTER_NUMBER
ORDER BY QUARTER_NUMBER;
-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q7] What is the quarter over quarter % change in revenue? 

Hint: Quarter over Quarter percentage change in revenue means what is the change in revenue from the subsequent quarter to the previous quarter in percentage.
      To calculate you need to use the common table expression to find out the sum of revenue for each quarter.
      Then use that CTE along with the LAG function to calculate the QoQ percentage change in revenue.
      
Note: For reference, refer to question number 5. Week-2: Hands-on (Practice)-GL_EATS_PRACTICE_EXERCISE_SOLUTION.SQL. 
      You'll get an overview of how to use common table expressions and the LAG function from this question.*/
WITH QoQ AS 
(
	SELECT
		QUARTER_NUMBER,
		SUM(calc_revenues_f(QUANTITY,VEHICLE_PRICE,DISCOUNT)) AS REVENUE
	FROM 
		 veh_ord_cust_v
	GROUP BY 1
)
SELECT
	QUARTER_NUMBER,
    REVENUE,
    LAG(REVENUE) OVER (ORDER BY QUARTER_NUMBER) AS PREVIOUS_QUARTER_REVENUE,
    ((REVENUE - LAG(REVENUE) OVER (ORDER BY QUARTER_NUMBER))/LAG(REVENUE) OVER(ORDER BY QUARTER_NUMBER) * 100) AS QTR_OVER_QTR_REVENUE
FROM
	QoQ;
-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q8] What is the trend of revenue and orders by quarters?

Hint: Find out the sum of revenue and count the number of orders for each quarter.*/

SELECT 
	QUARTER_NUMBER,
	SUM(calc_revenues_f(QUANTITY,VEHICLE_PRICE,DISCOUNT)) AS TOTAL_REVENUE,
    COUNT(ORDER_ID) AS NO_OF_ORDERS
FROM 
	veh_ord_cust_v
GROUP BY 1
ORDER BY 2 DESC;
-- ---------------------------------------------------------------------------------------------------------------------------------

/* QUESTIONS RELATED TO SHIPPING 
    [Q9] What is the average discount offered for different types of credit cards?

Hint: Find out the average of discount for each credit card type.*/
SELECT 
	ROUND(AVG(DISCOUNT),2) AS AVG_DISCOUNT,
    CREDIT_CARD_TYPE
FROM 
	veh_ord_cust_v
GROUP BY 2;
-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q10] What is the average time taken to ship the placed orders for each quarters?
   Use days_to_ship_f function to compute the time taken to ship the orders.

Hint: For each quarter, find out the average of the function that you created to calculate the difference between the ship date and the order date.*/

SELECT 
	QUARTER_NUMBER,
    AVG(days_to_ship_f(ORDER_DATE,SHIP_DATE)) AS AVG_SHIP_DAYS
FROM 
	veh_ord_cust_v
GROUP BY 1
ORDER BY 2;
-- --------------------------------------------------------Done----------------------------------------------------------------------
-- ----------------------------------------------------------------------------------------------------------------------------------



