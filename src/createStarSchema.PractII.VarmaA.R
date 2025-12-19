# ==============================================================================
# Program: createStarSchema.PractII.VarmaA.R
# Author: Ayush Varma
# Semester: Fall 2025
# Purpose: Create star schema for SportsTV streaming analytics datamart
# ==============================================================================

rm(list = ls())

# Install and load required packages
required_packages <- c("DBI", "RMySQL")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    install.packages(pkg)
    library(pkg, character.only = TRUE)
  }
}

# ------------------------------------------------------------------------------
# 1. DATABASE CONNECTION
# ------------------------------------------------------------------------------
cat("========================================\n")
cat("SPORTSTV ANALYTICS - STAR SCHEMA SETUP\n")
cat("========================================\n\n")

get_mysql_connection <- function() {
  dbConnect(RMySQL::MySQL(),
            host = Sys.getenv("MYSQL_HOST", "your-mysql-host.aivencloud.com"),
            port = as.integer(Sys.getenv("MYSQL_PORT", "15435")),
            dbname = Sys.getenv("MYSQL_DB", "defaultdb"),
            user = Sys.getenv("MYSQL_USER", "your-username"),
            password = Sys.getenv("MYSQL_PASSWORD", "your-password"))
}

cat("Connecting to Aiven MySQL database...\n")
mysql_conn <- get_mysql_connection()
cat("Successfully connected to MySQL\n\n")

# ------------------------------------------------------------------------------
# 2. CLEAN EXISTING SCHEMA
# ------------------------------------------------------------------------------
cat("Cleaning up existing schema...\n")

tables_to_drop <- c(
  "fact_streaming_summary",
  "dim_date",
  "dim_country",
  "dim_sport"
)

for (table in tables_to_drop) {
  tryCatch({
    dbExecute(mysql_conn, paste0("DROP TABLE IF EXISTS ", table))
    cat("  Dropped table (if existed):", table, "\n")
  }, error = function(e) {
    cat("  Note:", table, "did not exist\n")
  })
}

cat("\n")

# ------------------------------------------------------------------------------
# 3. CREATE DIMENSION TABLES
# ------------------------------------------------------------------------------
cat("Creating dimension tables...\n")
cat("-----------------------------------------\n")

# 3.1 Date Dimension - supports time-based analytics with drill-down
cat("Creating dim_date table...\n")

create_dim_date <- "
CREATE TABLE dim_date (
  date_id INT PRIMARY KEY,
  full_date DATE NOT NULL UNIQUE,
  year INT NOT NULL,
  quarter INT NOT NULL,
  month INT NOT NULL,
  week INT NOT NULL,
  day_of_month INT NOT NULL,
  day_of_week INT NOT NULL,
  
  INDEX idx_full_date (full_date),
  INDEX idx_year (year),
  INDEX idx_year_month (year, month),
  INDEX idx_year_quarter (year, quarter),
  INDEX idx_year_week (year, week)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

dbExecute(mysql_conn, create_dim_date)
cat("  dim_date table created\n")

# 3.2 Country Dimension
cat("Creating dim_country table...\n")

create_dim_country <- "
CREATE TABLE dim_country (
  country_id INT PRIMARY KEY,
  country_name VARCHAR(100) NOT NULL,
  
  INDEX idx_country_name (country_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

dbExecute(mysql_conn, create_dim_country)
cat("  dim_country table created\n")

# 3.3 Sport Dimension
cat("Creating dim_sport table...\n")

create_dim_sport <- "
CREATE TABLE dim_sport (
  sport_id INT AUTO_INCREMENT PRIMARY KEY,
  sport_name VARCHAR(100) NOT NULL UNIQUE,
  
  INDEX idx_sport_name (sport_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

dbExecute(mysql_conn, create_dim_sport)
cat("  dim_sport table created\n\n")

# ------------------------------------------------------------------------------
# 4. CREATE FACT TABLE WITH PARTITIONING
# ------------------------------------------------------------------------------
cat("Creating fact table with partitioning...\n")
cat("-----------------------------------------\n")

# Granularity: Daily (one row per date+country+sport combination)
# sport_name denormalized to avoid JOINs for common queries
# Note: Not using foreign keys because MySQL doesn't support FK on partitioned tables

cat("Creating fact_streaming_summary table...\n")

create_fact_table <- "
CREATE TABLE fact_streaming_summary (
  date_id INT NOT NULL,
  country_id INT NOT NULL,
  sport_name VARCHAR(100) NOT NULL,
  
  transaction_count INT DEFAULT 0,
  unique_user_count INT DEFAULT 0,
  total_minutes_streamed INT DEFAULT 0,
  completed_streams INT DEFAULT 0,
  avg_minutes_per_stream DECIMAL(10,2) DEFAULT 0,
  
  year INT NOT NULL,
  quarter INT NOT NULL,
  month INT NOT NULL,
  week INT NOT NULL,
  
  PRIMARY KEY (date_id, country_id, sport_name),
  
  INDEX idx_country (country_id),
  INDEX idx_sport (sport_name),
  INDEX idx_year_month (year, month),
  INDEX idx_year_quarter (year, quarter),
  INDEX idx_year_week (year, week),
  INDEX idx_year_country (year, country_id),
  INDEX idx_year_sport (year, sport_name),
  INDEX idx_full_coverage (year, month, country_id, sport_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE (date_id) (
    PARTITION p_before_2020 VALUES LESS THAN (20200101),
    PARTITION p_2020 VALUES LESS THAN (20210101),
    PARTITION p_2021 VALUES LESS THAN (20220101),
    PARTITION p_2022 VALUES LESS THAN (20230101),
    PARTITION p_2023 VALUES LESS THAN (20240101),
    PARTITION p_2024 VALUES LESS THAN (20250101),
    PARTITION p_2025 VALUES LESS THAN (20260101),
    PARTITION p_future VALUES LESS THAN MAXVALUE
)"

dbExecute(mysql_conn, create_fact_table)
cat("  fact_streaming_summary table created with partitioning\n\n")

# ------------------------------------------------------------------------------
# 5. VERIFY SCHEMA CREATION
# ------------------------------------------------------------------------------
cat("Verifying schema creation...\n")
cat("-----------------------------------------\n")

all_tables <- dbListTables(mysql_conn)
cat("Tables in database:\n")
for (table in all_tables) {
  col_query <- paste0("SELECT COUNT(*) as col_count 
                      FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_SCHEMA = 'defaultdb' 
                      AND TABLE_NAME = '", table, "'")
  col_info <- dbGetQuery(mysql_conn, col_query)
  
  cat(sprintf("  %-30s [%2d columns]\n", table, col_info$col_count))
}

cat("\n")

cat("Fact table structure (fact_streaming_summary):\n")
fact_structure <- dbGetQuery(mysql_conn, "
  SELECT 
    COLUMN_NAME as 'Column',
    DATA_TYPE as 'Type',
    IS_NULLABLE as 'Nullable',
    COLUMN_KEY as 'Key'
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = 'defaultdb' 
    AND TABLE_NAME = 'fact_streaming_summary'
  ORDER BY ORDINAL_POSITION
")
print(fact_structure, row.names = FALSE)

# ------------------------------------------------------------------------------
# 6. VERIFY PARTITIONING
# ------------------------------------------------------------------------------
cat("\n")
cat("Verifying table partitioning...\n")
cat("-----------------------------------------\n")

partition_info <- dbGetQuery(mysql_conn, "
  SELECT 
    PARTITION_NAME,
    PARTITION_ORDINAL_POSITION as Position,
    PARTITION_DESCRIPTION as 'Less Than',
    TABLE_ROWS as 'Rows'
  FROM INFORMATION_SCHEMA.PARTITIONS
  WHERE TABLE_SCHEMA = 'defaultdb' 
    AND TABLE_NAME = 'fact_streaming_summary'
  ORDER BY PARTITION_ORDINAL_POSITION
")

if (nrow(partition_info) > 0) {
  cat("Partitions created for fact_streaming_summary:\n")
  print(partition_info, row.names = FALSE)
  cat("\nPartitioning verified successfully\n")
} else {
  cat("Warning: No partitions found\n")
}

# ------------------------------------------------------------------------------
# 7. VERIFY INDEXES
# ------------------------------------------------------------------------------
cat("\n")
cat("Verifying indexes on fact table...\n")
cat("-----------------------------------------\n")

index_info <- dbGetQuery(mysql_conn, "SHOW INDEX FROM fact_streaming_summary")

unique_indexes <- unique(index_info$Key_name)
cat(sprintf("Total indexes created: %d\n", length(unique_indexes)))
cat("Index names:\n")
for (idx in unique_indexes) {
  cols <- index_info$Column_name[index_info$Key_name == idx]
  cat(sprintf("  - %s (%s)\n", idx, paste(cols, collapse = ", ")))
}

# ------------------------------------------------------------------------------
# 8. SUMMARY
# ------------------------------------------------------------------------------
cat("\n")
cat("========================================\n")
cat("STAR SCHEMA CREATION COMPLETE\n")
cat("========================================\n\n")

cat("Schema components created:\n")
cat("  1. dim_date              - Time dimension with date hierarchies\n")
cat("  2. dim_country           - Geographic dimension for countries\n")
cat("  3. dim_sport             - Sport reference dimension\n")
cat("  4. fact_streaming_summary - Pre-aggregated daily facts (partitioned)\n")
cat("\n")

cat("This schema supports:\n")
cat("  - Total/average streaming transactions by time period and country\n")
cat("  - Total/average streaming transactions by time period and sport\n")
cat("  - Total streaming time by time period, country, and sport\n")
cat("\n")

cat("Next step: Run loadAnalyticsDB.PractII.VarmaA.R to populate the schema\n")

# ------------------------------------------------------------------------------
# 9. CLEANUP
# ------------------------------------------------------------------------------
dbDisconnect(mysql_conn)
cat("\nDatabase connection closed.\n")
cat("========================================\n")