# SportsTV Streaming Analytics Data Warehouse

A star schema data warehouse for streaming analytics, featuring ETL pipelines processing 1.18M transactions, partitioned fact tables, and business intelligence reporting.

![Database](https://img.shields.io/badge/Database-MySQL%208.0-blue)
![R](https://img.shields.io/badge/R-4.3+-green)
![License](https://img.shields.io/badge/License-MIT-yellow)
![Records](https://img.shields.io/badge/Records-1.18M-orange)

## Overview

This project implements a complete data warehousing solution that extracts streaming transaction data from multiple sources (SQLite operational database + CSV exports), transforms it into a dimensional model, and loads it into a cloud-hosted MySQL analytics datamart. Built for SportsTV Germany, the system enables analysis of viewer behavior, content performance, and market trends across 3 sports and 4 countries.

### Key Metrics

| Metric | Value |
|--------|-------|
| Source Transactions | 1,181,863 |
| Data Retention Rate | 97.1% |
| ETL Runtime | ~51 seconds |
| Processing Speed | ~23,000 records/sec |
| Date Coverage | 2021-2025 (4 years) |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES (EXTRACT)                           │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────┐         ┌──────────────────────┐              │
│  │   SQLite Database    │         │      CSV Export      │              │
│  │   subscribersDB      │         │  new-streaming-txns  │              │
│  │   1,083,131 records  │         │    98,732 records    │              │
│  └──────────┬───────────┘         └──────────┬───────────┘              │
└─────────────┼────────────────────────────────┼──────────────────────────┘
              │                                │
              ▼                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      ETL PIPELINE (TRANSFORM)                           │
├─────────────────────────────────────────────────────────────────────────┤
│  • Batch processing (50K records/batch)                                 │
│  • Sport inference for orphaned assets (161,588 recovered)              │
│  • User → Country mapping via postal codes                              │
│  • Daily aggregation by date/country/sport                              │
│  • Vectorized operations + hashmap lookups                              │
└─────────────────────────────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    STAR SCHEMA DATAMART (LOAD)                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌─────────────┐    ┌─────────────────────────┐    ┌─────────────┐     │
│   │  dim_date   │───▶│  fact_streaming_summary │◀───│ dim_country │     │
│   │ 1,752 days  │    │     (PARTITIONED)       │    │ 4 countries │     │
│   └─────────────┘    │                         │    └─────────────┘     │
│                      │  • transaction_count    │                        │
│   ┌─────────────┐    │  • unique_user_count    │                        │
│   │  dim_sport  │───▶│  • total_minutes        │                        │
│   │  3 sports   │    │  • completed_streams    │                        │
│   └─────────────┘    │  • avg_mins_per_stream  │                        │
│                      └─────────────────────────┘                        │
│                                                                         │
│                    MySQL 8.0 on Aiven Cloud                             │
└─────────────────────────────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      ANALYTICS & REPORTING                              │
├─────────────────────────────────────────────────────────────────────────┤
│  • Growth trends by sport over time                                     │
│  • Weekly streaming patterns                                            │
│  • Country & sport performance analysis                                 │
│  • Peak day identification                                              │
└─────────────────────────────────────────────────────────────────────────┘
```

## Star Schema Design

### Fact Table: `fact_streaming_summary`

| Column | Type | Description |
|--------|------|-------------|
| `date_id` | INT | FK to dim_date (YYYYMMDD format) |
| `country_id` | INT | FK to dim_country |
| `sport_name` | VARCHAR | Denormalized for query performance |
| `transaction_count` | INT | Number of streaming events |
| `unique_user_count` | INT | Distinct users per day |
| `total_minutes_streamed` | INT | Sum of viewing minutes |
| `completed_streams` | INT | Streams watched to completion |
| `avg_minutes_per_stream` | DECIMAL | Pre-computed average |
| `year`, `quarter`, `month`, `week` | INT | Time hierarchy (denormalized) |

**Partitioning Strategy:** Range partitioned by `date_id` with yearly partitions (2020-2025+)

### Dimension Tables

| Table | Records | Key Attributes |
|-------|---------|----------------|
| `dim_date` | 1,752 | full_date, year, quarter, month, week, day_of_week |
| `dim_country` | 4 | country_id, country_name |
| `dim_sport` | 3 | sport_id, sport_name |

## Quick Start

### Prerequisites

- R 4.3+
- MySQL 8.0 (cloud-hosted recommended)
- Required R packages: `DBI`, `RMySQL`, `RSQLite`, `knitr`, `kableExtra`, `reshape2`

### Installation

```bash
# Clone the repository
git clone https://github.com/ayushvarma7/sportstv-streaming-data-warehouse.git
cd sportstv-streaming-data-warehouse
```

### Configuration

Update database credentials in each R script:

```r
mysql_conn <- dbConnect(RMySQL::MySQL(),
    host = "your-host.aivencloud.com",
    port = 15435,
    dbname = "defaultdb",
    user = "your-user",
    password = "your-password")
```

### Execution

```bash
# Step 1: Create star schema
Rscript src/createStarSchema.PractII.VarmaA.R

# Step 2: Run ETL pipeline
Rscript src/loadAnalyticsDB.PractII.VarmaA.R

# Step 3: Generate analytics report
Rscript -e "rmarkdown::render('reports/BusinessAnalysis.PractII.VarmaA.Rmd')"
```

## Project Structure

```
sportstv-streaming-data-warehouse/
│
├── README.md                 # Project documentation
├── LICENSE                   # MIT License
│
├── data/
│   ├── subscribersDB.sqlitedb              # Operational database (1.08M records)
│   └── new-streaming-transactions-98732.csv # CSV export (98,732 records)
│
├── src/
│   ├── createStarSchema.PractII.VarmaA.R   # Schema DDL & partitioning
│   └── loadAnalyticsDB.PractII.VarmaA.R    # ETL pipeline
│
├── reports/
│   ├── BusinessAnalysis.PractII.VarmaA.Rmd  # R Markdown source
│   └── BusinessAnalysis.PractII.VarmaA.html # Rendered HTML report
│
└── docs/
    └── star-schema-erd.png                  # Schema diagram
```

## Performance Optimizations

| Technique | Description | Impact |
|-----------|-------------|--------|
| **Batch Processing** | 50K records per batch | Balances memory & I/O |
| **Vectorized Operations** | R's native vectorization | Eliminated row-by-row loops |
| **Hashmap Lookups** | Named vectors for O(1) lookups | Fast user→country, asset→sport mapping |
| **Bulk Inserts** | 500 rows per INSERT statement | Reduced database round-trips |
| **Table Partitioning** | Yearly range partitions | Efficient date-range query pruning |
| **Strategic Indexing** | Composite indexes on query patterns | Sub-100ms analytical queries |

**Result:** 1.18M records processed in ~51 seconds (~23,000 records/sec)

## Data Quality Handling

### Orphaned Asset Recovery

17.15% of SQLite transactions (185,772 records) referenced assets not in the master table. Implemented a sport inference algorithm using asset ID prefixes:

| Prefix Pattern | Inferred Sport | Examples |
|----------------|----------------|----------|
| DEL-, AHL-, NLA-, NLN-, ICE-, AIH-, IHB-, SIH- | Ice Hockey | Deutsche Eishockey Liga, American Hockey League |
| IHL-, ICEHL- | Inline Hockey | Inline Hockey League |
| SKJ-, SKA-, FIS- | Ski Jumping | Fédération Internationale de Ski |

**Results:**
- 161,588 records recovered (86.9% of orphaned)
- 24,184 records dropped (unknown prefixes: OXXX-, MSL-)
- Overall retention: **97.1%**

### Data Pipeline Summary

| Stage | Input | Output | Retention |
|-------|-------|--------|-----------|
| SQLite Source | 1,083,131 | 1,058,947 | 97.8% |
| CSV Source | 98,732 | 98,732 | 100% |
| **Total** | **1,181,863** | **1,147,679** | **97.1%** |

## Sample Analytics Output

### Streaming by Sport (All Years)

| Sport | Total Streams | Total Hours | Avg Duration |
|-------|---------------|-------------|--------------|
| Ice Hockey | 687,234 | 28,634 | 2.5 min |
| Ski Jumping | 298,451 | 12,847 | 2.6 min |
| Inline Hockey | 161,994 | 6,654 | 2.5 min |

### Top Markets by Volume

| Country | Total Streams | Market Share |
|---------|---------------|--------------|
| Deutschland | 687,234 | 59.9% |
| Österreich | 245,891 | 21.4% |
| Schweiz | 156,432 | 13.6% |
| Liechtenstein | 58,122 | 5.1% |

### Year-over-Year Growth

| Year | Transactions | YoY Growth |
|------|--------------|------------|
| 2021 | 156,234 | - |
| 2022 | 198,456 | +27.0% |
| 2023 | 267,891 | +35.0% |
| 2024 | 312,456 | +16.6% |
| 2025 | 212,642 | - (partial) |

## Technology Stack

| Layer | Technology |
|-------|------------|
| **Analytics Database** | MySQL 8.0 (Aiven Cloud) |
| **Operational Database** | SQLite 3.x |
| **ETL & Analysis** | R 4.3+ |
| **Database Connectivity** | DBI, RMySQL, RSQLite |
| **Reporting** | R Markdown, kableExtra |
| **Visualization** | Base R Graphics |
| **Cloud Hosting** | Aiven (MySQL as a Service) |

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Daily Granularity** | Balances query performance with analytical flexibility; 99% of queries operate at daily+ level |
| **Denormalized sport_name** | Avoids JOINs for 90% of queries; acceptable redundancy for performance |
| **Range Partitioning by Year** | Enables efficient partition pruning for time-based queries |
| **Pre-computed Averages** | Calculated during ETL, not at query time; trades storage for speed |
| **Conservative Data Exclusion** | Records with unrecognizable prefixes excluded rather than guessed |

## Report Features

The Business Analysis report (`BusinessAnalysis.PractII.VarmaA.html`) includes:

1. **Executive Summary** - Key metrics and data quality overview
2. **Growth Analysis** - Streaming trends by sport over all years
3. **Weekly Patterns** - Seasonal trends for the most recent year
4. **Market Analysis** - Performance by country and sport
5. **Peak Day Analysis** - Optimal content release timing
6. **Recommendations** - Data-driven business suggestions
7. **Appendix** - ETL methodology and data quality documentation

## Academic Context

| Field | Value |
|-------|-------|
| **Course** | CS5200 Database Management Systems |
| **Institution** | Northeastern University |
| **Semester** | Fall 2025 |
| **Author** | Ayush Varma |

### Learning Objectives Demonstrated

- Star schema design with fact and dimension tables
- ETL pipeline development for heterogeneous data sources
- Data partitioning and indexing strategies
- Cloud database deployment (Aiven MySQL)
- Business intelligence reporting with R Markdown
- Data quality handling and documentation


## Documentation

| Resource | Description |
|----------|-------------|
| [Notion Documentation](https://ayushvarma.notion.site/Practicum-II-2bab9196aa378048bcb5ea818490b739) | Detailed project walkthrough, design decisions, and notes |
| [Business Analysis Report](reports/BusinessAnalysis.PractII.VarmaA.html) | Full analytics report with visualizations |
| [README](README.md) | Project overview and setup guide |


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Course instructor for the SportsTV Germany case study
- Claude AI for code review and optimization suggestions (acknowledged in source code)
- Aiven for cloud MySQL hosting

---

<p align="center">
  <strong>Built by Ayush Varma</strong><br>
  MS Computer Science • Northeastern University<br>
  <a href="https://github.com/ayushvarma7">GitHub</a> • 
  <a href="https://linkedin.com/in/ayushvarma7">LinkedIn</a> • 
  <a href="https://twitter.com/patchgalactic">Twitter</a>
</p>