library(tidyverse)
library(sparklyr)
library(readxl)
Sys.setenv(JAVA_HOME="C:\\Program Files\\Java\\jdk1.8.0_202\\")
library("sparklyr")
sc <- spark_connect(master = "local",
 spark_home = "C:\\Users\\msi\\AppData\\Local\\spark\\spark-2.3.1-bin-hadoop2.7",
 version = "2.3.1",app_name = "retail_data_analysis")


# skip this step if it's been done in a previous analysis run
if(!file.exists("sales-transactions")){
 

# read the dataset into R
 sales_transactions <- read_excel("C:/Users/msi/Downloads/r/Online Retail.xlsx")
 
 

# basic check to see the data has been properly read
 head(sales_transactions)
 dim(sales_transactions)
 
 

# pushing the data into Spark DataFrame
 sales_transactions_tbl <- copy_to(sc, sales_transactions, "sales_transactions", overwrite = TRUE)
 

# mark the cancelled invoices, correct the country name, create InvoicePrefix indicator
 sales_transactions_tbl <- sales_transactions_tbl %>%
 mutate(InvoiceStatus = ifelse(InvoiceNo %in% c("581483", "541431", "556444"), "Cancelled", NA),
 Country = ifelse(Country == "EIRE", "Ireland", Country),
 InvoicePrefix = ifelse(substr(InvoiceNo,1,1) %in% letters | substr(InvoiceNo,1,1) %in% LETTERS, substr(InvoiceNo,1,1), NA))
 

# check if the table is available on Spark
 src_tbls(sc)
 

# save the Spark DataFrame to a Parquet file(s) for persistence across sessions (Spark applications) - to a local filesystem, current project Data/spark-warehouse 
subdirectory
 spark_write_parquet(sales_transactions_tbl, str_c("file:///", getwd(), "/sales-transactions"),mode = "overwrite")
}


# read from Parquet into in-memory Spark DataFrame
sales_transactions_tbl <- spark_read_parquet(sc, "sales_transactions", str_c("file:///", getwd(), "/sales-transactions"), mode = "overwrite")


# read from Parquet into in-memory Spark DataFrame
sales_transactions_tbl <- spark_read_parquet(sc, "sales_transactions", str_c("file:///", getwd(), "/sales-transactions"), mode = "overwrite")
sales_transactions_tbl %>%
 sample_n(10)
sales_transactions_tbl %>%
 summarise(n_records = n(),
 n_invoices = n_distinct(InvoiceNo),
 n_missing_inv_rec = sum(as.integer(is.na(InvoiceNo)),na.rm = TRUE), 
 n_customers = n_distinct(CustomerID),
 n_missing_cust_rec = sum(as.integer(is.na(CustomerID)),na.rm = TRUE))
sales_transactions_tbl %>%
 summarise(n_dist_stocks = n_distinct(StockCode), 
 n_missing_stocks = sum(as.integer(is.na(StockCode)),na.rm = TRUE), 
 n_dist_desc = n_distinct(Description), 
 n_missing_desc = sum(as.integer(is.na(Description)),na.rm = TRUE), 
 n_missing_quant = sum(as.integer(is.na(Quantity)),na.rm = TRUE),
 n_missing_prices = sum(as.integer(is.na(UnitPrice)),na.rm = TRUE))


# skip this step if it's been done in a previous analysis run
if(!file.exists("Data/spark-warehouse/invoices")){
 

# create a temp table on Spark which holds the aggregation result
 invoices_tbl <- sales_transactions_tbl %>%
 group_by(InvoiceNo, InvoiceStatus, CustomerID, Country) %>%
 summarise(InvoiceDate = max(InvoiceDate, na.rm = TRUE),
 LineItems = n_distinct(StockCode),
 ItemQuantity = sum(Quantity, na.rm = TRUE),
 InvoiceAmount = sum(Quantity * UnitPrice, na.rm = TRUE)) %>%
 mutate(InvoicePrefix = ifelse(substr(InvoiceNo,1,1) %in% letters | substr(InvoiceNo,1,1) %in% LETTERS, substr(InvoiceNo,1,1), NA))
 
 

# register the temp table in Spark
 sdf_register(invoices_tbl, "invoices")
 

# save the Spark DataFrame to a Parquet file(s) for persistence across sessions (Spark applications) - to a local filesystem, current project Data/spark-warehouse 
subdirectory
 spark_write_parquet(invoices_tbl,str_c("file:///", getwd(), "/invoices"),mode = "overwrite")
}


# read from Parquet into in-memory Spark DataFrame
invoices_tbl <- spark_read_parquet(sc, "invoices", str_c("file:///", getwd(), "/invoices"), mode = "overwrite")
invoices_tbl %>%
 summarise(n_rows = n(),
 n_distinct_invoices = n_distinct(InvoiceNo))
invoices_tbl %>%
 group_by(InvoiceNo) %>%
 summarise(n_rows = n()) %>%
 filter(n_rows > 1) %>%
 inner_join(invoices_tbl, by = "InvoiceNo") %>%
 arrange(desc(n_rows), InvoiceNo)


# calculate summary stats in Spark and pull the result into R
cancel_summary <- invoices_tbl %>%
 group_by(InvoicePrefix) %>%
 summarise(invoices = n(),
 amounts = sum(InvoiceAmount, na.rm = TRUE)) %>%
 mutate(InvoiceType = ifelse(is.na(InvoicePrefix), "Regular", ifelse(InvoicePrefix == "C", "Cancellation", InvoicePrefix))) %>%
 collect()


# use ggplot2 for data display - number of invoices by invoice type
ggplot(cancel_summary) +
 geom_bar(aes(x = InvoiceType, y = invoices), stat = "identity", position = "dodge", fill = "#08306b") +
 scale_y_continuous(labels = scales::format_format(big.mark = ".", decimal.mark = ",", scientific = FALSE)) +
 labs(x = "Invoice type", y = "Number of invoices")


# invoice amounts by invoice type
ggplot(cancel_summary) +
 geom_bar(aes(x = InvoiceType, y = amounts), stat = "identity", position = "dodge", fill = "#08306b") +
 scale_y_continuous(labels = scales::format_format(big.mark = ".", decimal.mark = ",", scientific = FALSE)) +
 labs(x = "Invoice type", y = "Invoice amounts (GBP)")
invoices_tbl %>%
 filter(InvoicePrefix == "C") %>%
 arrange(desc(InvoiceAmount)) %>%
 filter(rank(InvoiceAmount) <= 10)
invoices_tbl %>%
 filter(InvoicePrefix == "C") %>%
 select(InvoiceNo, InvoiceDate, InvoiceAmount) %>%
 transmute(CancelNo = InvoiceNo,
 CancelDate = InvoiceDate,
 CancelAmount = InvoiceAmount,
 InvoiceNo = substr(InvoiceNo, 2, 7)) %>%
 left_join(invoices_tbl, by = "InvoiceNo") %>%
 select(CancelNo, CancelDate, CancelAmount, InvoiceNo, InvoiceDate, InvoiceAmount) %>%
 arrange(CancelAmount) %>%
 filter(!is.na(InvoiceAmount))
invoices_tbl %>%
 filter(abs(InvoiceAmount) %in% c(168469.60, 77183.60, 38970.00, 22998.40, 17836.46, 16888.02, 16453.71)) %>%
 arrange(desc(abs(InvoiceAmount)))
invoices_tbl <- invoices_tbl %>%
 filter(is.na(InvoicePrefix) & is.na(InvoiceStatus))
invoices_tbl %>%
 mutate(SuspCustGroup = ifelse(is.na(CustomerID), "Suspected retail", "Suspected wholesale")) %>%
 group_by(LineItems, SuspCustGroup) %>%
 summarise(num_invoices = n(), invoice_amounts = sum(InvoiceAmount, na.rm = TRUE)) %>%
 
collect() %>%
 ggplot() +
 geom_bar(aes(x = LineItems, y = num_invoices), stat = "identity", fill = "#08306b") +
 coord_cartesian(xlim = c(0, 100), ylim = c(0, 2500)) +
 labs(x = "Line items on invoice", 
 y = "Number of invoices",
 fill = "Customer group") +
 scale_fill_brewer(palette = "Set1") +
 facet_grid(SuspCustGroup ~ .)
invoices_tbl <- invoices_tbl %>%
 mutate(CustomerGroup = ifelse(is.na(CustomerID), "Retail", "Wholesale"))
cust_group_stats <- invoices_tbl %>%
 group_by(CustomerGroup) %>%
 summarise(n_invoices = n(),
 invoice_amount = sum(InvoiceAmount, na.rm = TRUE)) %>%
 collect()
ggplot(cust_group_stats) +
 geom_bar(aes(x = CustomerGroup, y = n_invoices), stat = "identity", fill = "#08306b") +
 scale_y_continuous(labels = scales::format_format(big.mark = ".", decimal.mark = ",", scientific = FALSE)) +
 labs(x = "Customer group", 
 y = "N of invoices",
 title = "Number of invoices per customer group") +
 theme(plot.title = element_text(hjust = 0.5))
ggplot(cust_group_stats) +
 geom_bar(aes(x = CustomerGroup, y = invoice_amount), stat = "identity", fill = "#08306b") +
 scale_y_continuous(labels = scales::format_format(big.mark = ".", decimal.mark = ",", scientific = FALSE)) +
 labs(x = "Customer group", 
 y = "Invoice amounts (GBP)",
 title = "Invoice amounts per customer group") +
 theme(plot.title = element_text(hjust = 0.5))
library(leaflet)
library(rworldmap)


# aggregate revenue by country
sales_by_country <- invoices_tbl %>%
 group_by(Country) %>%
 summarise(Amount = sum(InvoiceAmount, na.rm = TRUE)) %>%
 collect


# join the revenue by country to the world map of countries
sPDF <- joinCountryData2Map(sales_by_country
 ,joinCode = "NAME"
 ,nameJoinColumn = "Country", verbose = FALSE)


# select only the countries which generated revenue
existing_countries <- subset(sPDF, !is.na(Amount))


# create spending classes for revenues per country
bins <- c(0, 50000, 100000, 150000, 200000, 250000, 300000, Inf)


# assign a color to each of the classes
pal <- colorBin("YlOrRd", domain = existing_countries$Amount, bins = bins)


# create labels with actual revenue amounts per country, for hover info
labels <- paste0("<strong>", existing_countries$Country, "</strong><br/>", 
 format(existing_countries$Amount, digits = 1, big.mark = ".", decimal.mark = ",", scientific = FALSE),
 " GBP") %>% lapply(htmltools::HTML)


# create the cloropleth map
leaflet(existing_countries) %>%
 addTiles() %>% # Add default OpenStreetMap map tiles
 addPolygons(
 fillColor = ~pal(Amount),
 weight = 1,
 opacity = 1,
 color = "white",
 dashArray = "3",
 
fillOpacity = 0.7,
 highlight = highlightOptions(
 weight = 2,
 color = "#666",
 dashArray = "",
 fillOpacity = 0.7,
 bringToFront = TRUE),
 label = labels,
 labelOptions = labelOptions(
 style = list("font-weight" = "normal", padding = "3px 8px"),
 textsize = "15px",
 direction = "auto")) %>% 
 addLegend(pal = pal, values = ~Amount, opacity = 0.7, title = NULL,
 position = "topright") %>%
 setView(17,34,2) 
library(ggplot2)
invoices_tbl %>%
 filter(CustomerGroup == "Wholesale") %>%
 group_by(CustomerID) %>%
 summarise(NumInvoices = n()) %>%
 ungroup() %>%
 group_by(NumInvoices) %>%
 summarise(NumCustomers = n()) %>%
 arrange(NumInvoices) %>%
 collect() %>%
 ggplot() +
 geom_freqpoly(aes(x = NumInvoices, y = NumCustomers), stat = "identity", colour = "#08306b") +
 labs(x = "Number of purchases",
 y = "Number of customers",
 title = "Wholesale customers by number of purchases") +
 scale_y_continuous(labels = scales::format_format(big.mark = ".", decimal.mark = ",", scientific = FALSE)) +
 theme(plot.title = element_text(hjust = 0.5))
invoices_tbl %>%
 filter(CustomerGroup == "Wholesale") %>%
 group_by(CustomerID) %>%
 summarise(NumInvoices = as.double(n())) %>%
 ungroup() %>%
 ft_bucketizer(input_col = "NumInvoices", output_col = "NumInvoicesDisc", splits = c(1,2,3,4,5,6,11,16,21,Inf)) %>%
 group_by(NumInvoicesDisc) %>%
 summarise(NumCustomers = n()) %>%
 arrange(NumInvoicesDisc) %>%
 collect() %>%
 mutate(NumInvoicesDisc = ordered(NumInvoicesDisc,
 levels = c(0,1,2,3,4,5,6,7,8),
 labels = c("1","2","3","4","5","6-10","11-15","16-20","21+")),
 Group = "Purchases") %>%
 ggplot() +
 geom_bar(aes(x = Group, fill = reorder(NumInvoicesDisc, desc(NumInvoicesDisc)), y = NumCustomers), stat = "identity", position = "stack") +
 labs(x = "",
 y = "Number of customers",
 title = "Wholesale customers by number of purchases",
 fill = "Number of purchases") +
 scale_y_continuous(labels = scales::format_format(big.mark = ".", decimal.mark = ",", scientific = FALSE)) +
 theme(plot.title = element_text(hjust = 0.5)) +
 scale_fill_brewer(palette = "Blues")
invoices_tbl %>%
 filter(CustomerGroup == "Wholesale") %>%
 group_by(CustomerID) %>%
 summarise(NumInvoices = n(),
 AmountSpent = sum(InvoiceAmount, na.rm = TRUE),
 FirstPurchase = min(InvoiceDate, na.rm = TRUE),
 AvgPurchaseValue = sum(InvoiceAmount, na.rm = TRUE) / n()) %>%
 ungroup() %>%
 filter(NumInvoices >= 4 & rank(desc(AmountSpent)) <= 10)
sales_transactions_tbl <- sales_transactions_tbl %>%
 filter(is.na(InvoicePrefix) & is.na(InvoiceStatus))
sales_transactions_tbl %>%
 summarise(n_dist_products = n_distinct(StockCode))
multiple_descriptions <- sales_transactions_tbl %>%
 group_by(StockCode) %>%
 summarise(n_desc = n_distinct(Description)) %>%
 filter(n_desc > 1)
sales_transactions_tbl %>%
 inner_join(multiple_descriptions, by = "StockCode") %>%
 select(n_desc, StockCode, Description) %>%
 group_by(n_desc, StockCode, Description) %>%
 summarise(n_rows = n()) %>%
 arrange(desc(n_desc), StockCode, desc(n_rows)) %>%
 head(100)
products_tbl <- sales_transactions_tbl %>%
 group_by(StockCode, Description) %>%
 summarise(n_rows = n()) %>%
 filter(rank(desc(n_rows)) == 1) %>%
 select(StockCode, Description)


# register the temp table in Spark 
sdf_register(products_tbl, "products")


# save the Spark DataFrame to a Parquet file(s) for persistence across sessions (Spark applications) - to a local filesystem, current project Data/spark-warehouse 
subdirectory
spark_write_parquet(products_tbl, str_c("file:///", getwd(), "/products"), mode = "overwrite")
sales_transactions_tbl <- sales_transactions_tbl %>%
 select(-Description) %>%
 inner_join(products_tbl, by = "StockCode")
products_tbl %>%
 arrange(StockCode) %>%
 head(100)
products_tbl %>%
 filter(substr(StockCode, 1, 5) %in% c("15056", "16161")) %>%
 arrange(StockCode)
products_tbl %>%
 filter(StockCode %in% c("16014", "16015", "16016"))
products_tbl %>%
 mutate(ProductType = ifelse(substr(StockCode, -1, 1) %in% letters | substr(StockCode, -1, 1) %in% LETTERS, "Variation", "Regular")) %>%
 group_by(ProductType) %>%
 summarise(n_rows = n()) %>%
 collect %>%
 ggplot() +
 geom_bar(aes(x = ProductType, y = n_rows), stat = "identity", position = "dodge", fill = "#08306b") +
 scale_y_continuous(labels = scales::format_format(big.mark = ".", decimal.mark = ",", scientific = FALSE)) +
 labs(x = "Product type", y = "Number of products")
sales_transactions_tbl %>%
 mutate(ProductType = ifelse(substr(StockCode, -1, 1) %in% letters | substr(StockCode, -1, 1) %in% LETTERS, "Variation", "Regular")) %>%
 group_by(ProductType) %>%
 summarise(SalesAmount = sum(Quantity * UnitPrice, na.rm = TRUE)) %>%
 collect %>%
 ggplot() +
 geom_bar(aes(x = ProductType, y = SalesAmount), stat = "identity", position = "dodge", fill = "#08306b") +
 scale_y_continuous(labels = scales::format_format(big.mark = ".", decimal.mark = ",", scientific = FALSE)) +
 labs(x = "Product type", y = "Sales amount (GBP)")
sales_transactions_tbl %>%
 group_by(StockCode, Description) %>%
 summarise(SalesAmount = sum(Quantity * UnitPrice, na.rm = TRUE)) %>%
 filter(rank(desc(SalesAmount)) <= 100) %>%
 arrange(desc(SalesAmount)) %>%
 head(100)
sales_transactions_tbl <- sales_transactions_tbl %>%
 filter(!StockCode %in% c("DOT", "POST", "M", "AMAZONFEE"))


# I'm looking at average price, because the price of a product can change during the year.
sales_transactions_tbl %>%
 group_by(StockCode, Description) %>%
 summarise(SalesAmount = sum(Quantity * UnitPrice, na.rm = TRUE),
 SalesQuantity = sum(Quantity, na.rm = TRUE),
 AvgPrice = mean(UnitPrice, na.rm = TRUE)) %>%
 ungroup() %>%
 mutate(SalesAmtPercOfTotal = round(SalesAmount / sum(SalesAmount, na.rm = TRUE) * 100, 2)) %>%
 arrange(desc(SalesAmount)) %>%
 mutate(CumRevenue = cumsum(SalesAmount),
 CumRevPerc = cumsum(SalesAmtPercOfTotal)) %>%
 filter(rank(desc(SalesAmount)) <= 100)
top_10_products <- sales_transactions_tbl %>%
 mutate(CustomerGroup = ifelse(is.na(CustomerID), "Retail", "Wholesale")) %>%
 group_by(Description, CustomerGroup) %>%
 summarise(SalesAmount = sum(Quantity * UnitPrice, na.rm = TRUE)) %>%
 ungroup() %>%
 group_by(CustomerGroup) %>%
 filter(rank(desc(SalesAmount)) <= 10) %>%
 collect
top_10_products %>%
 filter(CustomerGroup == "Wholesale") %>%
 ggplot() +
 geom_bar(aes(x = reorder(factor(Description), SalesAmount), y = SalesAmount), stat = "identity", fill = "#08306b") +
 scale_y_continuous(labels = scales::format_format(big.mark = ".", decimal.mark = ",", scientific = FALSE)) +
 labs(x = "Product", 
 y = "Sales amount (GBP)",
 title = "Top 10 sold products - wholesale") +
 coord_flip()
top_10_products %>%
 filter(CustomerGroup == "Retail") %>%
 ggplot() +
 geom_bar(aes(x = reorder(factor(Description), SalesAmount), y = SalesAmount), stat = "identity", fill = "#08306b") +
 scale_y_continuous(labels = scales::format_format(big.mark = ".", decimal.mark = ",", scientific = FALSE)) +
 labs(x = "Product", 
 y = "Sales amount (GBP)",
 title = "Top 10 sold products - retail") +
 coord_flip()
itemsets_tbl <- sales_transactions_tbl %>%
 select(InvoiceNo, Description) %>%
 distinct() %>%
 group_by(InvoiceNo) %>%
 summarise(items = collect_list(Description))


# let's look at how the prepared data looks like - one row, one invoice, and a list of its line items
head(itemsets_tbl)


# run the FPGrowth
fp_model <- ml_fpgrowth(itemsets_tbl, min_confidence = 0.5, min_support = 0.025)


# extract the derived frequent itemsets and reformat the data
freq_itemsets <- ml_freq_itemsets(fp_model) %>%
 collect %>%
 mutate(list_length = map_int(items, length)) %>%
 filter(list_length > 1) %>%
 arrange(desc(freq)) %>%
 mutate(itemset = map_chr(items, str_c, sep = "-", collapse = "-")) %>%
 select(-items, -list_length)


# display the itemsets
freq_itemsets
library(wordcloud)
wordcloud(freq_itemsets$itemset, freq_itemsets$freq, max.words = 20, scale=c(0.1, 3.0),rot.per = 0,
 colors=brewer.pal(8, "Dark2"), random.order = FALSE, random.color = FALSE, fixed.asp = FALSE)
library(networkD3)


# extract association rules
assoc_rules <- ml_association_rules(fp_model) %>%
 collect %>%
 mutate(antecedent = map_chr(antecedent, str_c, sep = " + ", collapse = " + ")) %>%
 mutate(consequent = map_chr(consequent, str_c, sep = " + ", collapse = " + "))


# create a list of distinct antecedents
ante <- assoc_rules %>%
 distinct(antecedent) %>%
 transmute(name = antecedent)


# create a list of distinct consequents, combine them with distinct antecedents to create a list of network nodes


# add a unique id to every node
nodes <- assoc_rules %>%
 distinct(consequent) %>%
 transmute(name = consequent) %>%
 bind_rows(ante) %>%
 distinct() %>%
 mutate(group = "1") %>%
 mutate(row_id = seq(from = 0, length.out = length(name)), size = 20)


# extract directed link information from association rules, and add corresponding node IDs
links <- assoc_rules %>%
 left_join(nodes, by = c("antecedent" = "name")) %>%
 mutate(antecedent_row_id = row_id) %>%
 select(-row_id) %>%
 left_join(nodes, by = c("consequent" = "name")) %>%
 mutate(consequent_row_id = row_id) %>%
 select(-row_id,-group.x,-group.y)


# create the network visual using the nodes and links
forceNetwork(Links = as.data.frame(links), Nodes = as.data.frame(nodes), Source = "antecedent_row_id",
 Target = "consequent_row_id", Value = "confidence", NodeID = "name",
 Group = "group", opacity = 0.9, arrows = TRUE, linkWidth = JS("function(d) { return d.value * 4; }"),
 Nodesize = "size", fontSize = 15, fontFamily = "arial", linkDistance = 100, charge = -30, bounded = TRUE,
 opacityNoHover = 0.5)
ratings_tbl <- sales_transactions_tbl %>%
 filter(!is.na(CustomerID)) %>%
 select(CustomerID, StockCode, Description, Quantity) %>%
 group_by(CustomerID, StockCode, Description) %>%
 summarise(Quantity = sum(Quantity, na.rm = TRUE)) %>%
 ungroup() %>%
 mutate(CustomerID = as.integer(CustomerID),
 StockID = as.integer(rank(StockCode)))


# let's look at how the prepared data looks like - one row, one item, and a quantity transformed into 1-10 rating
head(ratings_tbl, 250)


# How many ratings do I have?
ratings_tbl %>%
 summarise(row_count = n())


# create a stock ID and names table - to join it back to results
product_names_tbl <- ratings_tbl %>%
 select(StockID, StockCode, Description) %>%
 distinct()


# train the ALS. I'll set the regularization parameter to 0.1, set implicit preference to true to indicate to ALS that ratings are actually derived from other information, and set 
the cold start to drop, to get only the results where the recommender returns a recommendation.
als_model <- ml_als(ratings_tbl, rating_col = "Quantity", user_col = "CustomerID",
 item_col = "StockID", reg_param = 0.1,
 implicit_prefs = TRUE, alpha = 1, nonnegative = FALSE,
 max_iter = 10, num_user_blocks = 10, num_item_blocks = 10,
 checkpoint_interval = 10, cold_start_strategy = "drop")
top_5_recommended_products <- ml_recommend(als_model, type = "items", 5) %>%
 inner_join(product_names_tbl, by = "StockID") %>%
 select(-recommendations) %>%
 arrange(CustomerID, desc(rating))
top_5_recommended_products %>%
 head(100)
ratings_tbl %>%
 filter(CustomerID == 12353) %>%
 arrange(CustomerID, desc(Quantity)) %>%
 select(CustomerID, StockCode, Description, Quantity)
top_5_recommended_products %>%
 filter(CustomerID == 12353) %>%
 arrange(CustomerID, desc(rating)) %>%
 select(CustomerID, StockCode, Description, rating)
ratings_tbl %>%
 filter(CustomerID == 12361) %>%
 arrange(CustomerID, desc(Quantity)) %>%
 select(CustomerID, StockCode, Description, Quantity)
top_5_recommended_products %>%
 filter(CustomerID == 12361) %>%
 arrange(CustomerID, desc(rating)) %>%
 select(CustomerID, StockCode, Description, rating)
ratings_tbl %>%
 filter(CustomerID == 12367) %>%
 arrange(CustomerID, desc(Quantity)) %>%
 select(CustomerID, StockCode, Description, Quantity)
top_5_recommended_products %>%
 filter(CustomerID == 12367) %>%
 arrange(CustomerID, desc(rating)) %>%
 select(CustomerID, StockCode, Description, rating)
ratings_tbl %>%
 filter(CustomerID == 12401) %>%
 arrange(CustomerID, desc(Quantity)) %>%
 select(CustomerID, StockCode, Description, Quantity)
top_5_recommended_products %>%
 filter(CustomerID == 12401) %>%
 arrange(CustomerID, desc(rating)) %>%
 select(CustomerID, StockCode, Description, rating)
ratings_tbl %>%
 filter(CustomerID == 12441) %>%
 arrange(CustomerID, desc(Quantity)) %>%
 select(CustomerID, StockCode, Description, Quantity)
top_5_recommended_products %>%
 filter(CustomerID == 12441) %>%
 arrange(CustomerID, desc(rating)) %>%
 select(CustomerID, StockCode, Description, rating)
spark_disconnect(sc)
