# Load required libraries
library(DBI)
library(RSQLite)
library(RMySQL)
library(lubridate)

# SQLite database settings
dbfile <- "pharma.db"
con_sqlite <- dbConnect(RSQLite::SQLite(), dbfile)

# MySQL database settings
db_name_fh <- "sql7628203"
db_user_fh <- "sql7628203"
db_host_fh <- "sql7.freemysqlhosting.net"
db_pwd_fh <- "NPqraIa1dg"
db_port_fh <- 3306

# Connect to the MySQL database
con_mysql <- dbConnect(
  RMySQL::MySQL(),
  dbname = db_name_fh,
  host = db_host_fh,
  port = db_port_fh,
  user = db_user_fh,
  password = db_pwd_fh
)

# Load data from SQLite tables
reps.df <- dbGetQuery(con_sqlite, "SELECT * FROM reps")
customers.df <- dbGetQuery(con_sqlite, "SELECT * FROM customers")
products.df <- dbGetQuery(con_sqlite, "SELECT * FROM products")
salestxn.df <- dbGetQuery(con_sqlite, "SELECT * FROM salestxn")

#Drop tables before rerunning
dbExecute(con_mysql, "DROP TABLE IF EXISTS salestxn")
dbExecute(con_mysql, "DROP TABLE IF EXISTS reps")
dbExecute(con_mysql, "DROP TABLE IF EXISTS customers")
dbExecute(con_mysql, "DROP TABLE IF EXISTS products")


# Create dimension tables in MySQL
dbExecute(con_mysql, "
  CREATE TABLE reps (
    rep_id VARCHAR(255) PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    territory TEXT
  )
")

dbExecute(con_mysql, "
  CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name TEXT,
    country TEXT
  )
")

dbExecute(con_mysql, "
  CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT
  )
")

dbExecute(con_mysql, "
  CREATE TABLE salestxn (
   txn_id INTEGER PRIMARY KEY,
   product_id INTEGER,
   rep_id VARCHAR(255),
   customer_id INTEGER,
   sale_date TEXT,
   sale_amount REAL,
   FOREIGN KEY (product_id) REFERENCES products(product_id),
   FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
  )
")

# Populate dimension tables in MySQL
dbWriteTable(con_mysql, "reps", reps.df, append = TRUE, row.names = FALSE)
dbWriteTable(con_mysql, "customers", customers.df, append = TRUE, row.names = FALSE)
dbWriteTable(con_mysql, "products", products.df, append = TRUE, row.names = FALSE)
dbWriteTable(con_mysql, "salestxn", salestxn.df, append = TRUE, row.names = FALSE)

# Drop existing "product_facts" and "rep_facts" tables if they exist
dbExecute(con_mysql, "DROP TABLE IF EXISTS product_facts")
dbExecute(con_mysql, "DROP TABLE IF EXISTS rep_facts")

# Create and populate product_facts fact table using dimension tables and salestxn fact table
dbExecute(con_mysql, "
  CREATE TABLE product_facts AS
  SELECT
    pr.product_name,
    YEAR(STR_TO_DATE(st.sale_date, '%m/%d/%Y')) AS year,
    QUARTER(STR_TO_DATE(st.sale_date, '%m/%d/%Y')) AS quarter,
    c.country AS region,
    SUM(st.sale_amount) AS total_sold
  FROM
    salestxn st
    JOIN products pr ON st.product_id = pr.product_id
    JOIN customers c ON st.customer_id = c.customer_id
  GROUP BY
    pr.product_name, year, quarter, region
")

# Execute a query to retrieve all rows from the "product_facts" table
result_product_facts <- dbGetQuery(con_mysql, "SELECT * FROM product_facts")

# Print the retrieved data
print(result_product_facts)

dbExecute(con_mysql, "
  UPDATE salestxn
  SET rep_id = CONCAT('r', rep_id)
")

# Create and populate rep_facts fact table using dimension tables and salestxn fact table
dbExecute(con_mysql, "
 CREATE TABLE rep_facts AS
  SELECT
    r.first_name,
    r.last_name,
    YEAR(STR_TO_DATE(st.sale_date, '%m/%d/%Y')) AS year,
    QUARTER(STR_TO_DATE(st.sale_date, '%m/%d/%Y')) AS quarter,
    pr.product_name,
    SUM(st.sale_amount) AS total_sold
  FROM
    salestxn st
    JOIN reps r ON st.rep_id = r.rep_id
    JOIN products pr ON st.product_id = pr.product_id
  GROUP BY
    r.first_name, r.last_name, year, quarter, pr.product_name
")

# Execute a query to retrieve all rows from the "product_facts" table
result_rep_facts <- dbGetQuery(con_mysql, "SELECT * FROM rep_facts")

# Print the retrieved data
print(result_rep_facts)

# Execute a query to get the total sold for each quarter of 2020 for all products
query <- "
 SELECT
  quarter,
  SUM(total_sold) AS total_sold
FROM
  product_facts
WHERE
  year = 2020
GROUP BY
  quarter
"

result <- dbGetQuery(con_mysql, query)

# Print the result
print(result)

# Execute a query to get the total sold for each quarter of 2020 for 'Alaraphosol'
query <- "
SELECT
  quarter,
  SUM(total_sold) AS total_sold
FROM
  product_facts
WHERE
  year = 2020
  AND product_name = 'Alaraphosol'
GROUP BY
  quarter
"

result <- dbGetQuery(con_mysql, query)

# Print the result
print(result)

# Execute a query to get the product that sold the best in 2022
query <- "
SELECT
  product_name,
  SUM(total_sold) AS total_sold
FROM
  product_facts
WHERE
  year = 2020
GROUP BY
  product_name
ORDER BY
  total_sold DESC
LIMIT 1
"
result <- dbGetQuery(con_mysql, query)

# Print the result
print(result)

# Execute a query to retrieve total sales for each sales rep in 2020 from rep_facts
query <- "
  SELECT
    first_name,
    last_name,
    SUM(total_sold) AS total_sales
  FROM
    rep_facts
  WHERE
    year = 2020
  GROUP BY
    first_name, last_name
"

result_rep_sales_2022 <- dbGetQuery(con_mysql, query)

# Print the retrieved data
print(result_rep_sales_2022)

# Disconnect from MySQL
dbDisconnect(con_mysql)

# Disconnect from SQLite
dbDisconnect(con_sqlite)
