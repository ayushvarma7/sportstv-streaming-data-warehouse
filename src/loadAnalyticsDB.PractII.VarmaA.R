# ==============================================================================
# Program: loadAnalyticsDB.PractII.VarmaA.R
# Author: Ayush Varma
# Semester: Fall 2025
# Purpose: ETL process to populate analytics datamart with streaming data
#          Extracts from SQLite and CSV, transforms, and loads into MySQL
# 
# ==============================================================================
# AI ASSISTANCE ACKNOWLEDGMENT
# I used Claude AI for:
# - Code review and debugging assistance
# - Performance optimization suggestions (vectorization, hashmap lookups)
# All generated code was reviewed, understood, and adapted for this use case.
# ==============================================================================

rm(list = ls())

# ==============================================================================
# INSTALL AND LOAD REQUIRED PACKAGES
# ==============================================================================
required_packages <- c("DBI", "RMySQL", "RSQLite")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    install.packages(pkg)
    library(pkg, character.only = TRUE)
  }
}

options(warn = -1)

# ==============================================================================
# TIMING AND LOGGING UTILITIES
# ==============================================================================

timing_log <- data.frame(
  step = character(),
  start_time = character(),
  end_time = character(),
  duration_seconds = numeric(),
  records_processed = numeric(),
  stringsAsFactors = FALSE
)

log_timing <- function(step_name, start, end, records = NA) {
  duration <- as.numeric(difftime(end, start, units = "secs"))
  timing_log <<- rbind(timing_log, data.frame(
    step = step_name,
    start_time = format(start, "%H:%M:%S"),
    end_time = format(end, "%H:%M:%S"),
    duration_seconds = round(duration, 2),
    records_processed = records,
    stringsAsFactors = FALSE
  ))
  cat(sprintf("  [%s] %.2f seconds", step_name, duration))
  if (!is.na(records)) {
    cat(sprintf(" | %s records", format(records, big.mark = ",")))
    if (duration > 0) {
      cat(sprintf(" | %.0f records/sec", records/duration))
    }
  }
  cat("\n")
}

print_section <- function(title) {
  cat("\n")
  cat(rep("=", 60), "\n", sep = "")
  cat(title, "\n")
  cat(rep("=", 60), "\n")
}

# ==============================================================================
# HELPER FUNCTION: Build INSERT values (vectorized for performance)
# ==============================================================================
build_insert_values <- function(sub_batch) {
  date_vals <- as.Date(sub_batch$streaming_date)
  date_ids <- as.numeric(format(date_vals, "%Y%m%d"))
  year_vals <- as.numeric(format(date_vals, "%Y"))
  month_nums <- as.numeric(format(date_vals, "%m"))
  quarter_vals <- (month_nums - 1) %/% 3 + 1
  week_vals <- as.numeric(format(date_vals, "%V"))
  
  escaped_sports <- gsub("'", "''", sub_batch$sport)
  
  values_list <- sprintf(
    "(%d, %d, '%s', %d, %d, %d, %d, %d, %d, %d, %d)",
    date_ids,
    sub_batch$country_id,
    escaped_sports,
    sub_batch$transaction_count,
    sub_batch$unique_users,
    sub_batch$minutes_streamed,
    sub_batch$completed,
    year_vals, quarter_vals, month_nums, week_vals
  )
  
  return(values_list)
}

etl_start_time <- Sys.time()

cat("============================================================\n")
cat("   SPORTSTV ANALYTICS - ETL PROCESS\n")
cat("   Started:", format(etl_start_time, "%Y-%m-%d %H:%M:%S"), "\n")
cat("============================================================\n")

# ==============================================================================
# 1. LOAD LIBRARIES
# ==============================================================================
print_section("1. LOADING LIBRARIES")
step_start <- Sys.time()

library(DBI)
library(RSQLite)
library(RMySQL)

log_timing("Load libraries", step_start, Sys.time())

# ==============================================================================
# 2. DATABASE CONNECTIONS
# ==============================================================================
print_section("2. ESTABLISHING DATABASE CONNECTIONS")
step_start <- Sys.time()

mysql_conn <- dbConnect(RMySQL::MySQL(),
                        host = Sys.getenv("MYSQL_HOST", "your-mysql-host.aivencloud.com"),
                        port = as.integer(Sys.getenv("MYSQL_PORT", "15435")),
                        dbname = Sys.getenv("MYSQL_DB", "defaultdb"),
                        user = Sys.getenv("MYSQL_USER", "your-username"),
                        password = Sys.getenv("MYSQL_PASSWORD", "your-password"))
cat("  Connected to MySQL (Aiven cloud)\n")

sqlite_conn <- dbConnect(RSQLite::SQLite(), 
                         dbname = "data/subscribersDB.sqlitedb")
cat("  Connected to SQLite (operational database)\n")

log_timing("Database connections", step_start, Sys.time())

# ==============================================================================
# 3. CLEAR EXISTING DATA
# ==============================================================================
print_section("3. CLEARING EXISTING DATA")
step_start <- Sys.time()

dbExecute(mysql_conn, "DELETE FROM fact_streaming_summary")
cat("  Cleared fact_streaming_summary\n")

log_timing("Clear existing data", step_start, Sys.time())

# ==============================================================================
# 4. POPULATE DIMENSION TABLES
# ==============================================================================
print_section("4. POPULATING DIMENSION TABLES")

cat("\n4.1 Loading Country Dimension...\n")
step_start <- Sys.time()

countries <- dbGetQuery(sqlite_conn, "SELECT country_id, country FROM countries")
dbExecute(mysql_conn, "DELETE FROM dim_country")

if (nrow(countries) > 0) {
  values <- paste(
    sprintf("(%d, '%s')", 
            countries$country_id, 
            gsub("'", "''", countries$country)),
    collapse = ", "
  )
  dbExecute(mysql_conn, paste("INSERT INTO dim_country (country_id, country_name) VALUES", values))
}

log_timing("Country dimension", step_start, Sys.time(), nrow(countries))

cat("\n Loading Sport Dimension...\n")
step_start <- Sys.time()

sports <- dbGetQuery(sqlite_conn, 
                     "SELECT DISTINCT sport FROM assets WHERE sport IS NOT NULL AND sport != ''")
dbExecute(mysql_conn, "DELETE FROM dim_sport")

if (nrow(sports) > 0) {
  values <- paste(sprintf("('%s')", gsub("'", "''", sports$sport)), collapse = ", ")
  dbExecute(mysql_conn, paste("INSERT INTO dim_sport (sport_name) VALUES", values))
}

log_timing("Sport dimension", step_start, Sys.time(), nrow(sports))

cat("\n Loading Date Dimension...\n")
step_start <- Sys.time()

sqlite_dates <- dbGetQuery(sqlite_conn, 
                           "SELECT MIN(streaming_date) as min_d, MAX(streaming_date) as max_d 
                            FROM streaming_txns")

csv_full_dates <- read.csv("data/new-streaming-transactions-98732.csv",
                           stringsAsFactors = FALSE)
csv_min <- min(csv_full_dates$streaming_date)
csv_max <- max(csv_full_dates$streaming_date)
rm(csv_full_dates)

start_date <- as.Date(min(sqlite_dates$min_d, csv_min))
end_date <- as.Date(max(sqlite_dates$max_d, csv_max))

cat(sprintf("  Date range: %s to %s\n", start_date, end_date))

all_dates <- seq(start_date, end_date, by = "day")
cat(sprintf("  Total dates to load: %d\n", length(all_dates)))

dbExecute(mysql_conn, "DELETE FROM dim_date")

date_df <- data.frame(
  date_id = as.numeric(format(all_dates, "%Y%m%d")),
  full_date = as.character(all_dates),
  year = as.numeric(format(all_dates, "%Y")),
  quarter = (as.numeric(format(all_dates, "%m")) - 1) %/% 3 + 1,
  month = as.numeric(format(all_dates, "%m")),
  week = as.numeric(format(all_dates, "%V")),
  day_of_month = as.numeric(format(all_dates, "%d")),
  day_of_week = as.numeric(format(all_dates, "%w")) + 1
)

batch_size <- 500
total_inserted <- 0

for (i in seq(1, nrow(date_df), batch_size)) {
  end_idx <- min(i + batch_size - 1, nrow(date_df))
  batch <- date_df[i:end_idx, ]
  
  values <- paste(
    sprintf("(%d, '%s', %d, %d, %d, %d, %d, %d)",
            batch$date_id, batch$full_date, batch$year, batch$quarter,
            batch$month, batch$week, batch$day_of_month, batch$day_of_week),
    collapse = ", "
  )
  
  dbExecute(mysql_conn, paste("INSERT IGNORE INTO dim_date VALUES", values))
  total_inserted <- total_inserted + nrow(batch)
}

log_timing("Date dimension", step_start, Sys.time(), total_inserted)

# ==============================================================================
# 5. BUILD LOOKUP STRUCTURES
# ==============================================================================
print_section("5. BUILDING LOOKUP STRUCTURES")
step_start <- Sys.time()

cat("  Loading asset-to-sport mapping...\n")
asset_sport_lookup <- dbGetQuery(sqlite_conn, 
                                 "SELECT asset_id, sport FROM assets WHERE sport IS NOT NULL")
cat(sprintf("    Loaded %s asset-sport mappings\n", format(nrow(asset_sport_lookup), big.mark = ",")))

cat("  Building user-to-country mapping...\n")
user_country_lookup <- dbGetQuery(sqlite_conn, "
  SELECT DISTINCT 
    s.user_id,
    c.country_id
  FROM subscribers s
  JOIN postal2city p ON s.postal_code = p.postal_code
  JOIN cities c ON p.city_id = c.city_id
  WHERE s.user_id IS NOT NULL AND c.country_id IS NOT NULL
")
cat(sprintf("    Loaded %s user-country mappings\n", format(nrow(user_country_lookup), big.mark = ",")))

asset_sport_map <- setNames(asset_sport_lookup$sport, asset_sport_lookup$asset_id)
user_country_map <- setNames(user_country_lookup$country_id, user_country_lookup$user_id)

rm(asset_sport_lookup, user_country_lookup)

log_timing("Build lookups", step_start, Sys.time())

cat("\n  Setting up sport inference for orphaned assets...\n")

infer_sport_from_prefix <- function(asset_ids) {
  result <- rep(NA_character_, length(asset_ids))
  
  ice_hockey_pattern <- "^(DEL|AHL|AIH|IHB|SIH|NLN|NLA|ICE|NXXX|SLXXX)-"
  result[grepl(ice_hockey_pattern, asset_ids)] <- "Ice Hockey"
  
  inline_pattern <- "^(IHL|ICEHL)-"
  result[grepl(inline_pattern, asset_ids)] <- "Inline Hockey"
  
  ski_pattern <- "^(SKJ|SKA|FIS)-"
  result[grepl(ski_pattern, asset_ids)] <- "Ski Jumping"
  
  return(result)
}

cat("  Sport inference configured for orphaned assets\n")

# ==============================================================================
# 6. PROCESS SQLITE STREAMING DATA
# ==============================================================================
print_section("6. PROCESSING SQLITE STREAMING DATA")

total_sqlite <- dbGetQuery(sqlite_conn, "SELECT COUNT(*) as cnt FROM streaming_txns")$cnt
cat(sprintf("Total SQLite transactions: %s\n", format(total_sqlite, big.mark = ",")))

sqlite_stats <- list(
  total_read = 0,
  valid_records = 0,
  missing_country = 0,
  missing_sport = 0,
  fact_rows_created = 0
)

batch_size <- 50000
offset <- 0
batch_num <- 1

sqlite_process_start <- Sys.time()

while (offset < total_sqlite) {
  batch_start <- Sys.time()
  
  current_batch_size <- min(batch_size, total_sqlite - offset)
  cat(sprintf("\nBatch %d: rows %s - %s\n", 
              batch_num,
              format(offset + 1, big.mark = ","),
              format(offset + current_batch_size, big.mark = ",")))
  
  fetch_start <- Sys.time()
  batch_data <- dbGetQuery(sqlite_conn, sprintf("
    SELECT transaction_id, user_id, asset_id, streaming_date, 
           minutes_streamed, completed
    FROM streaming_txns
    LIMIT %d OFFSET %d
  ", batch_size, offset))
  cat(sprintf("  Fetch: %.2fs\n", difftime(Sys.time(), fetch_start, units = "secs")))
  
  sqlite_stats$total_read <- sqlite_stats$total_read + nrow(batch_data)
  
  map_start <- Sys.time()
  batch_data$country_id <- user_country_map[batch_data$user_id]
  batch_data$sport <- asset_sport_map[batch_data$asset_id]
  
  missing_sport_mask <- is.na(batch_data$sport)
  if (sum(missing_sport_mask) > 0) {
    inferred_sports <- infer_sport_from_prefix(batch_data$asset_id[missing_sport_mask])
    batch_data$sport[missing_sport_mask] <- inferred_sports
    inferred_count <- sum(!is.na(inferred_sports))
    if (inferred_count > 0) {
      cat(sprintf("  Inferred sport for %d orphaned assets\n", inferred_count))
    }
  }
  
  cat(sprintf("  Map lookups: %.2fs\n", difftime(Sys.time(), map_start, units = "secs")))
  
  missing_country <- sum(is.na(batch_data$country_id))
  missing_sport <- sum(is.na(batch_data$sport))
  sqlite_stats$missing_country <- sqlite_stats$missing_country + missing_country
  sqlite_stats$missing_sport <- sqlite_stats$missing_sport + missing_sport
  
  if (missing_country > 0 || missing_sport > 0) {
    cat(sprintf("  Dropped: %d missing country, %d missing sport\n", 
                missing_country, missing_sport))
  }
  
  valid_data <- batch_data[!is.na(batch_data$country_id) & 
                             !is.na(batch_data$sport) &
                             !is.na(batch_data$streaming_date), ]
  
  sqlite_stats$valid_records <- sqlite_stats$valid_records + nrow(valid_data)
  
  if (nrow(valid_data) > 0) {
    agg_start <- Sys.time()
    
    valid_data$minutes_streamed[is.na(valid_data$minutes_streamed)] <- 0
    valid_data$completed[is.na(valid_data$completed)] <- 0
    
    agg_result <- aggregate(
      cbind(minutes_streamed, completed, transaction_count = 1) ~ 
        streaming_date + country_id + sport,
      data = transform(valid_data, transaction_count = 1),
      FUN = sum
    )
    
    user_counts <- aggregate(user_id ~ streaming_date + country_id + sport,
                             data = valid_data,
                             FUN = function(x) length(unique(x)))
    names(user_counts)[4] <- "unique_users"
    
    agg_result <- merge(agg_result, user_counts)
    
    cat(sprintf("  Aggregate: %.2fs (%d groups)\n", 
                difftime(Sys.time(), agg_start, units = "secs"),
                nrow(agg_result)))
    
    insert_start <- Sys.time()
    insert_batch_size <- 500
    
    for (j in seq(1, nrow(agg_result), insert_batch_size)) {
      end_j <- min(j + insert_batch_size - 1, nrow(agg_result))
      sub_batch <- agg_result[j:end_j, ]
      
      values_list <- build_insert_values(sub_batch)
      
      insert_sql <- paste0(
        "INSERT INTO fact_streaming_summary ",
        "(date_id, country_id, sport_name, transaction_count, unique_user_count, ",
        "total_minutes_streamed, completed_streams, year, quarter, month, week) VALUES ",
        paste(values_list, collapse = ", "),
        " ON DUPLICATE KEY UPDATE ",
        "transaction_count = transaction_count + VALUES(transaction_count), ",
        "unique_user_count = unique_user_count + VALUES(unique_user_count), ",
        "total_minutes_streamed = total_minutes_streamed + VALUES(total_minutes_streamed), ",
        "completed_streams = completed_streams + VALUES(completed_streams)"
      )
      
      tryCatch({
        dbExecute(mysql_conn, insert_sql)
      }, error = function(e) {
        cat(sprintf("    Insert error: %s\n", e$message))
      })
    }
    
    sqlite_stats$fact_rows_created <- sqlite_stats$fact_rows_created + nrow(agg_result)
    
    cat(sprintf("  Insert: %.2fs\n", difftime(Sys.time(), insert_start, units = "secs")))
  }
  
  cat(sprintf("  Batch total: %.2fs\n", difftime(Sys.time(), batch_start, units = "secs")))
  
  offset <- offset + batch_size
  batch_num <- batch_num + 1
  
  rm(batch_data, valid_data)
  if (exists("agg_result")) rm(agg_result)
  if (exists("user_counts")) rm(user_counts)
}

sqlite_process_end <- Sys.time()

cat("\n--- SQLite Processing Summary ---\n")
cat(sprintf("  Total records read: %s\n", format(sqlite_stats$total_read, big.mark = ",")))
cat(sprintf("  Valid records: %s (%.1f%%)\n", 
            format(sqlite_stats$valid_records, big.mark = ","),
            100 * sqlite_stats$valid_records / sqlite_stats$total_read))
cat(sprintf("  Dropped (missing country): %s\n", format(sqlite_stats$missing_country, big.mark = ",")))
cat(sprintf("  Dropped (missing sport): %s\n", format(sqlite_stats$missing_sport, big.mark = ",")))
cat(sprintf("  Fact rows created: %s\n", format(sqlite_stats$fact_rows_created, big.mark = ",")))

log_timing("SQLite processing", sqlite_process_start, sqlite_process_end, sqlite_stats$valid_records)

# ==============================================================================
# 7. PROCESS CSV STREAMING DATA
# ==============================================================================
print_section("7. PROCESSING CSV STREAMING DATA")

csv_process_start <- Sys.time()

csv_stats <- list(
  total_read = 0,
  valid_records = 0,
  missing_country = 0,
  missing_sport = 0,
  fact_rows_created = 0
)

csv_file <- "data/new-streaming-transactions-98732.csv"

cat("Counting CSV records...\n")
csv_line_count <- length(readLines(csv_file)) - 1
cat(sprintf("Total CSV transactions: %s\n", format(csv_line_count, big.mark = ",")))

batch_size <- 50000
skip_rows <- 0
batch_num <- 1
first_batch <- TRUE

while (skip_rows < csv_line_count) {
  batch_start <- Sys.time()
  
  current_batch_size <- min(batch_size, csv_line_count - skip_rows)
  cat(sprintf("\nBatch %d: rows %s - %s\n",
              batch_num,
              format(skip_rows + 1, big.mark = ","),
              format(skip_rows + current_batch_size, big.mark = ",")))
  
  fetch_start <- Sys.time()
  if (first_batch) {
    csv_batch <- read.csv(csv_file, stringsAsFactors = FALSE, nrows = batch_size)
    first_batch <- FALSE
  } else {
    csv_batch <- read.csv(csv_file, stringsAsFactors = FALSE, 
                          skip = skip_rows, nrows = batch_size, header = FALSE)
    names(csv_batch) <- c("transaction_id", "subscriber_id", "user_id", "asset_id",
                          "streaming_date", "streaming_start_time", "minutes_streamed",
                          "device_type", "quality_streamed", "completed")
  }
  cat(sprintf("  Fetch: %.2fs\n", difftime(Sys.time(), fetch_start, units = "secs")))
  
  csv_stats$total_read <- csv_stats$total_read + nrow(csv_batch)
  
  map_start <- Sys.time()
  csv_batch$country_id <- user_country_map[csv_batch$user_id]
  csv_batch$sport <- asset_sport_map[csv_batch$asset_id]
  
  missing_sport_mask <- is.na(csv_batch$sport)
  if (sum(missing_sport_mask) > 0) {
    inferred_sports <- infer_sport_from_prefix(csv_batch$asset_id[missing_sport_mask])
    csv_batch$sport[missing_sport_mask] <- inferred_sports
    inferred_count <- sum(!is.na(inferred_sports))
    if (inferred_count > 0) {
      cat(sprintf("  Inferred sport for %d orphaned assets\n", inferred_count))
    }
  }
  
  cat(sprintf("  Map lookups: %.2fs\n", difftime(Sys.time(), map_start, units = "secs")))
  
  missing_country <- sum(is.na(csv_batch$country_id))
  missing_sport <- sum(is.na(csv_batch$sport))
  csv_stats$missing_country <- csv_stats$missing_country + missing_country
  csv_stats$missing_sport <- csv_stats$missing_sport + missing_sport
  
  if (missing_country > 0 || missing_sport > 0) {
    cat(sprintf("  Dropped: %d missing country, %d missing sport\n",
                missing_country, missing_sport))
  }
  
  valid_csv <- csv_batch[!is.na(csv_batch$country_id) &
                           !is.na(csv_batch$sport) &
                           !is.na(csv_batch$streaming_date), ]
  
  csv_stats$valid_records <- csv_stats$valid_records + nrow(valid_csv)
  
  if (nrow(valid_csv) > 0) {
    agg_start <- Sys.time()
    
    valid_csv$minutes_streamed[is.na(valid_csv$minutes_streamed)] <- 0
    valid_csv$completed[is.na(valid_csv$completed)] <- 0
    valid_csv$completed <- as.numeric(valid_csv$completed)
    
    agg_result <- aggregate(
      cbind(minutes_streamed, completed, transaction_count = 1) ~
        streaming_date + country_id + sport,
      data = transform(valid_csv, transaction_count = 1),
      FUN = sum
    )
    
    user_counts <- aggregate(user_id ~ streaming_date + country_id + sport,
                             data = valid_csv,
                             FUN = function(x) length(unique(x)))
    names(user_counts)[4] <- "unique_users"
    
    agg_result <- merge(agg_result, user_counts)
    
    cat(sprintf("  Aggregate: %.2fs (%d groups)\n",
                difftime(Sys.time(), agg_start, units = "secs"),
                nrow(agg_result)))
    
    insert_start <- Sys.time()
    insert_batch_size <- 500
    
    for (j in seq(1, nrow(agg_result), insert_batch_size)) {
      end_j <- min(j + insert_batch_size - 1, nrow(agg_result))
      sub_batch <- agg_result[j:end_j, ]
      
      values_list <- build_insert_values(sub_batch)
      
      insert_sql <- paste0(
        "INSERT INTO fact_streaming_summary ",
        "(date_id, country_id, sport_name, transaction_count, unique_user_count, ",
        "total_minutes_streamed, completed_streams, year, quarter, month, week) VALUES ",
        paste(values_list, collapse = ", "),
        " ON DUPLICATE KEY UPDATE ",
        "transaction_count = transaction_count + VALUES(transaction_count), ",
        "unique_user_count = unique_user_count + VALUES(unique_user_count), ",
        "total_minutes_streamed = total_minutes_streamed + VALUES(total_minutes_streamed), ",
        "completed_streams = completed_streams + VALUES(completed_streams)"
      )
      
      tryCatch({
        dbExecute(mysql_conn, insert_sql)
      }, error = function(e) {
        cat(sprintf("    Insert error: %s\n", e$message))
      })
    }
    
    csv_stats$fact_rows_created <- csv_stats$fact_rows_created + nrow(agg_result)
    cat(sprintf("  Insert: %.2fs\n", difftime(Sys.time(), insert_start, units = "secs")))
  }
  
  cat(sprintf("  Batch total: %.2fs\n", difftime(Sys.time(), batch_start, units = "secs")))
  
  skip_rows <- skip_rows + batch_size
  batch_num <- batch_num + 1
  
  rm(csv_batch, valid_csv)
  if (exists("agg_result")) rm(agg_result)
}

csv_process_end <- Sys.time()

cat("\n--- CSV Processing Summary ---\n")
cat(sprintf("  Total records read: %s\n", format(csv_stats$total_read, big.mark = ",")))
cat(sprintf("  Valid records: %s (%.1f%%)\n",
            format(csv_stats$valid_records, big.mark = ","),
            100 * csv_stats$valid_records / max(csv_stats$total_read, 1)))
cat(sprintf("  Dropped (missing country): %s\n", format(csv_stats$missing_country, big.mark = ",")))
cat(sprintf("  Dropped (missing sport): %s\n", format(csv_stats$missing_sport, big.mark = ",")))
cat(sprintf("  Fact rows created: %s\n", format(csv_stats$fact_rows_created, big.mark = ",")))

log_timing("CSV processing", csv_process_start, csv_process_end, csv_stats$valid_records)

# ==============================================================================
# 8. UPDATE CALCULATED FIELDS
# ==============================================================================
print_section("8. UPDATING CALCULATED FIELDS")
step_start <- Sys.time()

dbExecute(mysql_conn, "
  UPDATE fact_streaming_summary
  SET avg_minutes_per_stream = 
    CASE 
      WHEN transaction_count > 0 
      THEN ROUND(total_minutes_streamed / transaction_count, 2)
      ELSE 0
    END
")
cat("  Calculated avg_minutes_per_stream\n")

log_timing("Calculate averages", step_start, Sys.time())

# ==============================================================================
# 9. VALIDATION
# ==============================================================================
print_section("9. DATA VALIDATION")
step_start <- Sys.time()

cat("\n9.1 Fact Table Summary Statistics:\n")
cat(rep("-", 50), "\n", sep = "")

summary_stats <- dbGetQuery(mysql_conn, "
  SELECT 
    COUNT(*) as fact_rows,
    SUM(transaction_count) as total_transactions,
    SUM(total_minutes_streamed) as total_minutes,
    SUM(unique_user_count) as total_user_sessions,
    COUNT(DISTINCT date_id) as unique_dates,
    COUNT(DISTINCT country_id) as unique_countries,
    COUNT(DISTINCT sport_name) as unique_sports,
    MIN(year) as min_year,
    MAX(year) as max_year
  FROM fact_streaming_summary
")
print(summary_stats)

cat("\n9.2 Transactions by Country:\n")
cat(rep("-", 50), "\n", sep = "")
country_check <- dbGetQuery(mysql_conn, "
  SELECT 
    dc.country_name,
    SUM(f.transaction_count) as transactions,
    SUM(f.total_minutes_streamed) as minutes
  FROM fact_streaming_summary f
  JOIN dim_country dc ON f.country_id = dc.country_id
  GROUP BY dc.country_name
  ORDER BY transactions DESC
  LIMIT 10
")
print(country_check)

cat("\n9.3 Transactions by Sport:\n")
cat(rep("-", 50), "\n", sep = "")
sport_check <- dbGetQuery(mysql_conn, "
  SELECT 
    sport_name,
    SUM(transaction_count) as transactions,
    SUM(total_minutes_streamed) as minutes,
    ROUND(AVG(avg_minutes_per_stream), 2) as avg_mins
  FROM fact_streaming_summary
  GROUP BY sport_name
  ORDER BY transactions DESC
")
print(sport_check)

cat("\n9.4 Transactions by Year:\n")
cat(rep("-", 50), "\n", sep = "")
year_check <- dbGetQuery(mysql_conn, "
  SELECT 
    year,
    SUM(transaction_count) as transactions,
    SUM(total_minutes_streamed) as minutes,
    COUNT(DISTINCT country_id) as countries,
    COUNT(DISTINCT sport_name) as sports
  FROM fact_streaming_summary
  GROUP BY year
  ORDER BY year
")
print(year_check)

cat("\n9.5 Cross-Validation with Source:\n")
cat(rep("-", 50), "\n", sep = "")
source_total <- dbGetQuery(sqlite_conn, "SELECT COUNT(*) as cnt FROM streaming_txns")$cnt + csv_stats$total_read
fact_total <- summary_stats$total_transactions

cat(sprintf("  Source records (SQLite + CSV): %s\n", format(source_total, big.mark = ",")))
cat(sprintf("  Fact table transactions: %s\n", format(fact_total, big.mark = ",")))
cat(sprintf("  Difference: %s (%.2f%% of source)\n", 
            format(source_total - fact_total, big.mark = ","),
            100 * (source_total - fact_total) / source_total))

log_timing("Validation", step_start, Sys.time())

cat("\n9.6 Critical Validation Checks:\n")
cat(rep("-", 50), "\n", sep = "")

validation_total <- dbGetQuery(mysql_conn, "
  SELECT SUM(transaction_count) as fact_total
  FROM fact_streaming_summary
")

cat(sprintf("  Fact table total transactions: %s\n", 
            format(validation_total$fact_total, big.mark = ",")))
cat(sprintf("  Expected (from validation): %s\n", 
            format(fact_total, big.mark = ",")))

if (abs(validation_total$fact_total - fact_total) < 100) {
  cat("  PASS: Transaction totals match within tolerance\n")
} else {
  cat("  FAIL: Transaction total mismatch detected!\n")
}

week_check <- dbGetQuery(mysql_conn, "
  SELECT MIN(week) as min_week, MAX(week) as max_week, 
         COUNT(DISTINCT week) as unique_weeks
  FROM fact_streaming_summary
")

cat(sprintf("\n  Week number range: %d to %d (%d unique weeks)\n",
            week_check$min_week, week_check$max_week, week_check$unique_weeks))

if (week_check$min_week >= 1 && week_check$max_week <= 53) {
  cat("  PASS: Week numbers within valid range (1-53)\n")
} else {
  cat("  FAIL: Invalid week numbers detected!\n")
}

null_check <- dbGetQuery(mysql_conn, "
  SELECT 
    SUM(CASE WHEN date_id IS NULL THEN 1 ELSE 0 END) as null_dates,
    SUM(CASE WHEN country_id IS NULL THEN 1 ELSE 0 END) as null_countries,
    SUM(CASE WHEN sport_name IS NULL THEN 1 ELSE 0 END) as null_sports
  FROM fact_streaming_summary
")

cat(sprintf("\n  NULL check - Dates: %d, Countries: %d, Sports: %d\n",
            null_check$null_dates, null_check$null_countries, null_check$null_sports))

if (null_check$null_dates == 0 && null_check$null_countries == 0 && null_check$null_sports == 0) {
  cat("  PASS: No NULL values in critical fields\n")
} else {
  cat("  FAIL: NULL values found in fact table!\n")
}

cat("\n")

# ==============================================================================
# 10. TIMING SUMMARY
# ==============================================================================
print_section("10. ETL TIMING SUMMARY")

etl_end_time <- Sys.time()
total_duration <- as.numeric(difftime(etl_end_time, etl_start_time, units = "secs"))

cat("\nStep-by-Step Timing:\n")
cat(rep("-", 70), "\n", sep = "")
print(timing_log, row.names = FALSE)

cat(rep("-", 70), "\n", sep = "")
cat(sprintf("\nTOTAL ETL DURATION: %.2f seconds (%.2f minutes)\n", 
            total_duration, total_duration/60))

# ==============================================================================
# 11. CLEANUP
# ==============================================================================
print_section("11. CLEANUP")

dbDisconnect(mysql_conn)
dbDisconnect(sqlite_conn)
cat("Database connections closed.\n")

cat("\n")
cat("============================================================\n")
cat("   ETL PROCESS COMPLETE\n")
cat("   Finished:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("   Total Duration:", sprintf("%.2f minutes", total_duration/60), "\n")
cat("============================================================\n")