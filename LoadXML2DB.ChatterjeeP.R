# Load required libraries
library(DBI)
library(RSQLite)
library(XML)
library(xml2)

#-----------------------------------------------------------------------
xmlfile_reps <- "txn-xml/pharmaReps.xml"
xml_files <- c("txn-xml/pharmaSalesTxn-4k-B.xml")
xmlDOCU_reps <- xmlParse(xmlfile_reps)
dbfile <- "pharma.db"
con <- dbConnect(RSQLite::SQLite(), dbfile)

## Get the root of the XML tree and determine the number of reps
root_reps <- xmlRoot(xmlDOCU_reps)
RepSize <- xmlSize(root_reps)
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
# Define table names
table_names <- c("products", "reps", "customers", "salestxn")

# Drop tables if they exist
for (table_name in table_names) {
  dbExecute(con, paste0("DROP TABLE IF EXISTS ", table_name))
}

# Define table schemas and create tables
create_products_table <- "CREATE TABLE products (
  product_id INTEGER PRIMARY KEY,
  product_name TEXT
)"

create_reps_table <- "CREATE TABLE reps (
  rep_id TEXT PRIMARY KEY,
  first_name TEXT,
  last_name TEXT,
  territory TEXT
)"

create_customers_table <- "CREATE TABLE customers (
  customer_id INTEGER PRIMARY KEY,
  customer_name TEXT,
  country TEXT
)"

create_salestxn_table <- "CREATE TABLE salestxn (
  txn_id INTEGER PRIMARY KEY,
  product_id INTEGER,
  rep_id TEXT,
  customer_id INTEGER,
  sale_date TEXT,
  sale_amount REAL,
  FOREIGN KEY (rep_id) REFERENCES reps(rep_id),
  FOREIGN KEY (product_id) REFERENCES products(product_id),
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
)"

# Execute the CREATE TABLE statements
dbExecute(con, create_products_table)
dbExecute(con, create_reps_table)
dbExecute(con, create_customers_table)
dbExecute(con, create_salestxn_table)
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
reps.df <- data.frame(rep_id = vector(mode = "character", length = RepSize),
                      first_name = vector(mode = "character", length = RepSize),
                      last_name = vector(mode = "character", length = RepSize),
                      territory = vector(mode = "character", length = RepSize))


for(i in 1:RepSize)
{
  rep <- root_reps[[i]]
  
  rep_id <- xmlAttrs(rep)["rID"]
  first_name <- xmlValue(rep[[1]])
  last_name <- xmlValue(rep[[2]])
  territory <- xmlValue(rep[[3]])
  
  reps.df[i,]$rep_id <- rep_id
  reps.df[i,]$first_name <- first_name
  reps.df[i,]$last_name <- last_name
  reps.df[i,]$territory <- territory
}

dbWriteTable(con, "reps", reps.df, overwrite = TRUE)

# Execute a query to retrieve all rows from the "reps" table
result <- dbGetQuery(con, "SELECT * FROM reps")

# Print the retrieved data
print(result)
#-----------------------------------------------------------------------


#-----------------------------------------------------------------------
for (xmlfile_sales in xml_files) {
  xmlDOCU_sales <- xmlParse(xmlfile_sales)
  root_txn <- xmlRoot(xmlDOCU_sales)
  txnSize <- xmlSize(root_txn)
  
  # Create data frames to store customer and product information
  customers.df <- data.frame(customer_id = integer(),
                             customer_name = character(),
                             country = character())
  
  products.df <- data.frame(product_id = integer(),
                            product_name = character())
  
  for (i in 1:txnSize) {
    txn <- root_txn[[i]]
    
    customer_name <- xpathSApply(txn, ".//cust", xmlValue)
    country <- xpathSApply(txn, ".//country", xmlValue)
    
    # Check if the customer already exists in the dataframe
    if (!customer_name %in% customers.df$customer_name) {
      customers.df <- rbind(customers.df, data.frame(customer_name, country))
    }
  }
  
  # Populate products.df
  for (i in 1:txnSize) {
    txn <- root_txn[[i]]
    
    product_name <- xpathSApply(txn, ".//prod", xmlValue)
    
    # Check if the product already exists in the dataframe
    if (!product_name %in% products.df$product_name) {
      products.df <- rbind(products.df, data.frame(product_name))
    }
  }
}

# Assign customer id based on row numbers and add as the first column
customers.df$customer_id <- seq_len(nrow(customers.df))
customers.df <- customers.df[, c("customer_id", "customer_name", "country")]

# Assign product id based on row numbers and add as the first column
products.df$product_id <- seq_len(nrow(products.df))
products.df <- products.df[, c("product_id", "product_name")]

# Write data frames to "customers" and "products" tables
dbWriteTable(con, "customers", customers.df, overwrite= TRUE)
dbWriteTable(con, "products", products.df, overwrite = TRUE)


# Execute a query to retrieve all rows from the "customers" and "products" tables
result_customers <- dbGetQuery(con, "SELECT * FROM customers")
result_products <- dbGetQuery(con, "SELECT * FROM products")

# Print the retrieved data
print(result_customers)
print(result_products)
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
# Create data frame to store sales transactions information
salestxn.df <- data.frame(txn_id = integer(),
                          product_id = integer(),
                          rep_id = character(),
                          customer_id = integer(),
                          sale_date = character(),
                          sale_amount = numeric())


# Create a mapping of product_name to product_id and customer_name to customer_id
product_id_mapping <- setNames(products.df$product_id, products.df$product_name)
customer_id_mapping <- setNames(customers.df$customer_id, customers.df$customer_name)


# Populate salestxn.df from XML data
for (i in 1:txnSize) {
  txn <- root_txn[[i]]
  
  txn_id <- as.integer(xpathSApply(txn, ".//txnID", xmlValue))
  product_name <- xpathSApply(txn, ".//prod", xmlValue)[1]
  rep_id <- xpathSApply(txn, ".//repID", xmlValue)[1]
  customer_name <- xpathSApply(txn, ".//cust", xmlValue)[1]
  sale_date <- xpathSApply(txn, ".//date", xmlValue)[1]
  sale_amount <- as.numeric(xpathSApply(txn, ".//amount", xmlValue)[1])
  
  # Look up product_id and customer_id using the mapping
  product_id <- product_id_mapping[product_name]
  customer_id <- customer_id_mapping[customer_name]
  
  salestxn.df[i,]$txn_id <- txn_id
  salestxn.df[i,]$product_id <- product_id
  salestxn.df[i,]$rep_id <- rep_id
  salestxn.df[i,]$customer_id <- customer_id
  salestxn.df[i,]$sale_date <- sale_date
  salestxn.df[i,]$sale_amount <- sale_amount
}

# Write sales transactions data to the "salestxn" table in the database
dbWriteTable(con, "salestxn", salestxn.df, overwrite = TRUE)

#-----------------------------------------------------------------------
# loading the 2nd xml file
xml_files <- c("txn-xml/pharmaSalesTxn-4k-A.xml")


for (xmlfile_sales in xml_files) {
  xmlDOCU_sales <- xmlParse(xmlfile_sales)
  root_txn <- xmlRoot(xmlDOCU_sales)
  txnSize <- xmlSize(root_txn)
  
  # Create data frame to store sales transactions information
  salestxn.df <- data.frame(txn_id = integer(),
                            product_id = integer(),
                            rep_id = character(),
                            customer_id = integer(),
                            sale_date = character(),
                            sale_amount = numeric())
  
  
  # Create a mapping of product_name to product_id and customer_name to customer_id
  product_id_mapping <- setNames(products.df$product_id, products.df$product_name)
  customer_id_mapping <- setNames(customers.df$customer_id, customers.df$customer_name)
  
  
  # Populate salestxn.df from XML data
  for (i in 1:txnSize) {
    txn <- root_txn[[i]]
    
    txn_id <- as.integer(xpathSApply(txn, ".//txnID", xmlValue))
    product_name <- xpathSApply(txn, ".//prod", xmlValue)[1]
    rep_id <- xpathSApply(txn, ".//repID", xmlValue)[1]
    customer_name <- xpathSApply(txn, ".//cust", xmlValue)[1]
    sale_date <- xpathSApply(txn, ".//date", xmlValue)[1]
    sale_amount <- as.numeric(xpathSApply(txn, ".//amount", xmlValue)[1])
    
    # Look up product_id and customer_id using the mapping
    product_id <- product_id_mapping[product_name]
    customer_id <- customer_id_mapping[customer_name]
    
    salestxn.df[i,]$txn_id <- txn_id
    salestxn.df[i,]$product_id <- product_id
    salestxn.df[i,]$rep_id <- rep_id
    salestxn.df[i,]$customer_id <- customer_id
    salestxn.df[i,]$sale_date <- sale_date
    salestxn.df[i,]$sale_amount <- sale_amount
  }
}
# Write sales transactions data to the "salestxn" table in the database
dbWriteTable(con, "salestxn", salestxn.df, append = TRUE)


#-----------------------------------------------------------------------
# loading the 3rd xml file
xml_files <- c("txn-xml/pharmaSalesTxn-3k-C.xml")


for (xmlfile_sales in xml_files) {
  xmlDOCU_sales <- xmlParse(xmlfile_sales)
  root_txn <- xmlRoot(xmlDOCU_sales)
  txnSize <- xmlSize(root_txn)
  
  # Create data frame to store sales transactions information
  salestxn.df <- data.frame(txn_id = integer(),
                            product_id = integer(),
                            rep_id = character(),
                            customer_id = integer(),
                            sale_date = character(),
                            sale_amount = numeric())
  
  
  # Create a mapping of product_name to product_id and customer_name to customer_id
  product_id_mapping <- setNames(products.df$product_id, products.df$product_name)
  customer_id_mapping <- setNames(customers.df$customer_id, customers.df$customer_name)
  
  
  # Populate salestxn.df from XML data
  for (i in 1:txnSize) {
    txn <- root_txn[[i]]
    
    txn_id <- as.integer(xpathSApply(txn, ".//txnID", xmlValue))
    product_name <- xpathSApply(txn, ".//prod", xmlValue)[1]
    rep_id <- xpathSApply(txn, ".//repID", xmlValue)[1]
    customer_name <- xpathSApply(txn, ".//cust", xmlValue)[1]
    sale_date <- xpathSApply(txn, ".//date", xmlValue)[1]
    sale_amount <- as.numeric(xpathSApply(txn, ".//amount", xmlValue)[1])
    
    # Look up product_id and customer_id using the mapping
    product_id <- product_id_mapping[product_name]
    customer_id <- customer_id_mapping[customer_name]
    
    salestxn.df[i,]$txn_id <- txn_id
    salestxn.df[i,]$product_id <- product_id
    salestxn.df[i,]$rep_id <- rep_id
    salestxn.df[i,]$customer_id <- customer_id
    salestxn.df[i,]$sale_date <- sale_date
    salestxn.df[i,]$sale_amount <- sale_amount
  }
}
# Write sales transactions data to the "salestxn" table in the database
dbWriteTable(con, "salestxn", salestxn.df, append = TRUE)


#-----------------------------------------------------------------------
# loading the 4th xml file
xml_files <- c("txn-xml/pharmaSalesTxn-20-C.xml")


for (xmlfile_sales in xml_files) {
  xmlDOCU_sales <- xmlParse(xmlfile_sales)
  root_txn <- xmlRoot(xmlDOCU_sales)
  txnSize <- xmlSize(root_txn)
  
  # Create data frame to store sales transactions information
  salestxn.df <- data.frame(txn_id = integer(),
                            product_id = integer(),
                            rep_id = character(),
                            customer_id = integer(),
                            sale_date = character(),
                            sale_amount = numeric())
  
  
  # Create a mapping of product_name to product_id and customer_name to customer_id
  product_id_mapping <- setNames(products.df$product_id, products.df$product_name)
  customer_id_mapping <- setNames(customers.df$customer_id, customers.df$customer_name)
  
  
  # Populate salestxn.df from XML data
  for (i in 1:txnSize) {
    txn <- root_txn[[i]]
    
    txn_id <- as.integer(xpathSApply(txn, ".//txnID", xmlValue))
    product_name <- xpathSApply(txn, ".//prod", xmlValue)[1]
    rep_id <- xpathSApply(txn, ".//repID", xmlValue)[1]
    customer_name <- xpathSApply(txn, ".//cust", xmlValue)[1]
    sale_date <- xpathSApply(txn, ".//date", xmlValue)[1]
    sale_amount <- as.numeric(xpathSApply(txn, ".//amount", xmlValue)[1])
    
    # Look up product_id and customer_id using the mapping
    product_id <- product_id_mapping[product_name]
    customer_id <- customer_id_mapping[customer_name]
    
    salestxn.df[i,]$txn_id <- txn_id
    salestxn.df[i,]$product_id <- product_id
    salestxn.df[i,]$rep_id <- rep_id
    salestxn.df[i,]$customer_id <- customer_id
    salestxn.df[i,]$sale_date <- sale_date
    salestxn.df[i,]$sale_amount <- sale_amount
  }
}
# Write sales transactions data to the "salestxn" table in the database
dbWriteTable(con, "salestxn", salestxn.df, append = TRUE)


#-----------------------------------------------------------------------
# loading the 5th xml file
xml_files <- c("txn-xml/pharmaSalesTxn-20-B.xml")


for (xmlfile_sales in xml_files) {
  xmlDOCU_sales <- xmlParse(xmlfile_sales)
  root_txn <- xmlRoot(xmlDOCU_sales)
  txnSize <- xmlSize(root_txn)
  
  # Create data frame to store sales transactions information
  salestxn.df <- data.frame(txn_id = integer(),
                            product_id = integer(),
                            rep_id = character(),
                            customer_id = integer(),
                            sale_date = character(),
                            sale_amount = numeric())
  
  
  # Create a mapping of product_name to product_id and customer_name to customer_id
  product_id_mapping <- setNames(products.df$product_id, products.df$product_name)
  customer_id_mapping <- setNames(customers.df$customer_id, customers.df$customer_name)
  
  
  # Populate salestxn.df from XML data
  for (i in 1:txnSize) {
    txn <- root_txn[[i]]
    
    txn_id <- as.integer(xpathSApply(txn, ".//txnID", xmlValue))
    product_name <- xpathSApply(txn, ".//prod", xmlValue)[1]
    rep_id <- xpathSApply(txn, ".//repID", xmlValue)[1]
    customer_name <- xpathSApply(txn, ".//cust", xmlValue)[1]
    sale_date <- xpathSApply(txn, ".//date", xmlValue)[1]
    sale_amount <- as.numeric(xpathSApply(txn, ".//amount", xmlValue)[1])
    
    # Look up product_id and customer_id using the mapping
    product_id <- product_id_mapping[product_name]
    customer_id <- customer_id_mapping[customer_name]
    
    salestxn.df[i,]$txn_id <- txn_id
    salestxn.df[i,]$product_id <- product_id
    salestxn.df[i,]$rep_id <- rep_id
    salestxn.df[i,]$customer_id <- customer_id
    salestxn.df[i,]$sale_date <- sale_date
    salestxn.df[i,]$sale_amount <- sale_amount
  }
}
# Write sales transactions data to the "salestxn" table in the database
dbWriteTable(con, "salestxn", salestxn.df, append = TRUE)


#-----------------------------------------------------------------------
# loading the 6th xml file
xml_files <- c("txn-xml/pharmaSalesTxn-20-A.xml")


for (xmlfile_sales in xml_files) {
  xmlDOCU_sales <- xmlParse(xmlfile_sales)
  root_txn <- xmlRoot(xmlDOCU_sales)
  txnSize <- xmlSize(root_txn)
  
  # Create data frame to store sales transactions information
  salestxn.df <- data.frame(txn_id = integer(),
                            product_id = integer(),
                            rep_id = character(),
                            customer_id = integer(),
                            sale_date = character(),
                            sale_amount = numeric())
  
  
  # Create a mapping of product_name to product_id and customer_name to customer_id
  product_id_mapping <- setNames(products.df$product_id, products.df$product_name)
  customer_id_mapping <- setNames(customers.df$customer_id, customers.df$customer_name)
  
  
  # Populate salestxn.df from XML data
  for (i in 1:txnSize) {
    txn <- root_txn[[i]]
    
    txn_id <- as.integer(xpathSApply(txn, ".//txnID", xmlValue))
    product_name <- xpathSApply(txn, ".//prod", xmlValue)[1]
    rep_id <- xpathSApply(txn, ".//repID", xmlValue)[1]
    customer_name <- xpathSApply(txn, ".//cust", xmlValue)[1]
    sale_date <- xpathSApply(txn, ".//date", xmlValue)[1]
    sale_amount <- as.numeric(xpathSApply(txn, ".//amount", xmlValue)[1])
    
    # Look up product_id and customer_id using the mapping
    product_id <- product_id_mapping[product_name]
    customer_id <- customer_id_mapping[customer_name]
    
    salestxn.df[i,]$txn_id <- txn_id
    salestxn.df[i,]$product_id <- product_id
    salestxn.df[i,]$rep_id <- rep_id
    salestxn.df[i,]$customer_id <- customer_id
    salestxn.df[i,]$sale_date <- sale_date
    salestxn.df[i,]$sale_amount <- sale_amount
  }
}
# Write sales transactions data to the "salestxn" table in the database
dbWriteTable(con, "salestxn", salestxn.df, append = TRUE)

# Execute a query to retrieve all rows from the "salestxn" table
result_sales <- dbGetQuery(con, "SELECT * FROM salestxn")

# Print the retrieved data
print(result_sales)

# Disconnect the database
dbDisconnect(con)







