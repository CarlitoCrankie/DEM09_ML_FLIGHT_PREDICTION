# Architecture Diagram Generation Instructions

## Scenario
Create a system architecture diagram for a flight price analysis data pipeline using Apache Airflow for orchestration and DBT for transformations, processing flight price data from Bangladesh through a medallion architecture (Bronze/Silver/Gold) with SCD Type 2 for historical tracking.

---

## System Overview

**Purpose**: Process and analyze flight price data for Bangladesh using a batch data pipeline with medallion architecture, implementing SCD Type 2 for historical tracking of fare changes.

**Architecture Pattern**: Medallion architecture (Bronze/Silver/Gold) with containerized components

**Key Technologies**:
- Docker & Docker Compose (orchestration)
- Apache Airflow 2.7.3 (workflow scheduling)
- MySQL 8.0 (staging database)
- PostgreSQL 15 (analytics database)
- DBT 1.7.4 (SQL transformations)
- Python 3.10 with Pandas (data processing)

---

## Components

### 1. **Docker Infrastructure**
- **Docker Network**: Internal bridge network
- **Purpose**: Isolates and connects all services
- **Volumes**:
  - `./dags` → `/opt/airflow/dags` (DAG files)
  - `./dbt_project` → `/opt/airflow/dbt_project` (DBT models)
  - `./data` → `/opt/airflow/data` (CSV source file)
  - `./logs` → `/opt/airflow/logs` (Airflow logs)

---

### 2. **Data Source**
- **File**: `Flight_Price_Dataset_of_Bangladesh.csv`
- **Location**: `./data/`
- **Records**: 57,000 rows
- **Columns**: 18

**Schema**:
```
- airline (string)
- source (string, IATA code)
- destination (string, IATA code)
- departure_time (timestamp)
- arrival_time (timestamp)
- duration (integer, minutes)
- stops (integer)
- class (string: Economy/Business/First)
- base_fare (decimal)
- total_fare (decimal)
- seasonality (string: Regular/Eid/Hajj/Winter Holidays)
- booking_lead_time (integer, days)
- booking_source (string: direct/agency/online)
- flight_number (string)
- aircraft_type (string)
- baggage_allowance (integer, kg)
- meal_included (boolean)
- wifi_available (boolean)
```

---

### 3. **Airflow Components**

#### 3a. Airflow Webserver
- **Container Name**: `airflow-webserver`
- **Port**: `8080`
- **Purpose**: Web UI for DAG management and monitoring
- **Credentials**:
  - Username: `admin`
  - Password: `admin`

**Features**:
```
• DAG visualization
• Task status monitoring
• Log viewing
• Manual trigger capability
• Connection management
```

#### 3b. Airflow Scheduler
- **Container Name**: `airflow-scheduler`
- **Purpose**: Executes DAG tasks according to schedule
- **DAG**: `flight_price_pipeline`

**Functions**:
```
• Task scheduling
• Dependency resolution
• Retry handling
• Task state management
```

---

### 4. **MySQL Staging Database**
- **Container Name**: `mysql-staging`
- **Port**: `3307` (external), `3306` (internal)
- **Database**: `flight_staging`
- **User**: `flight_user`
- **Password**: `flight_pass`
- **Initialization**: `./scripts/init_mysql.sql`

**Tables**:
```sql
Table: raw_flight_data
├── id (INT AUTO_INCREMENT PRIMARY KEY)
├── airline (VARCHAR(100))
├── source_code (VARCHAR(10))
├── source_name (VARCHAR(100))
├── destination_code (VARCHAR(10))
├── destination_name (VARCHAR(100))
├── departure_datetime (DATETIME)
├── arrival_datetime (DATETIME)
├── duration_hrs (DECIMAL(5,2))
├── stopovers (VARCHAR(20))
├── aircraft_type (VARCHAR(100))
├── travel_class (VARCHAR(50))
├── booking_source (VARCHAR(50))
├── base_fare_bdt (DECIMAL(12,2))
├── tax_surcharge_bdt (DECIMAL(12,2))
├── total_fare_bdt (DECIMAL(12,2))
├── seasonality (VARCHAR(50))
├── days_before_departure (INT)
├── source_file (VARCHAR(255))
├── loaded_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
└── PRIMARY KEY (id)

Table: validated_flight_data
├── id (INT AUTO_INCREMENT PRIMARY KEY)
├── (all raw_flight_data columns except id)
├── is_valid (TINYINT)
├── validation_errors (TEXT)
├── validation_warnings (TEXT)
├── raw_id (INT)
├── validated_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
├── PRIMARY KEY (id)
├── FOREIGN KEY (raw_id) REFERENCES raw_flight_data(id)
└── INDEX (is_valid), INDEX (raw_id)
```

**Purpose**:
```
• Landing zone for CSV data (raw_flight_data)
• Validation staging area (validated_flight_data)
• Separation of concerns: raw vs. validated
• Audit trail with timestamps and validation flags
```

---

### 5. **PostgreSQL Analytics Database**
- **Container Name**: `postgres-analytics`
- **Port**: `5433` (external), `5432` (internal)
- **Database**: `flight_analytics`
- **User**: `analytics_user`
- **Password**: `analytics_pass`
- **Initialization**: `./scripts/init_postgres.sql`

**Schemas and Objects**:
```
├── bronze schema (Raw layer)
│   └── validated_flights (57,000 rows)
│       ├── All validated columns from MySQL
│       ├── is_valid, validation_errors
│       ├── mysql_raw_id, mysql_validated_id
│       ├── mysql_loaded_at, bronze_loaded_at
│       └── NO transformations applied
│
├── silver schema (Cleaned layer)
│   ├── silver_cleaned_flights (57,000 rows)
│   │   ├── All bronze columns plus derived columns:
│   │   │   - route (source_code || '-' || destination_code)
│   │   │   - fare_category (Budget/Standard/Premium)
│   │   │   - booking_window (Last Minute/Standard/Advance/Early Bird)
│   │   │   - route_type (Domestic/International)
│   │   │   - is_peak_season (boolean)
│   │   ├── Standardized text (UPPER case, trim whitespace)
│   │   └── Valid records only (is_valid=1)
│   │
│   ├── flight_fare_snapshot (SCD Type 2, ~19,052 rows)
│   │   ├── Tracks: airline, route, class, seasonality combinations
│   │   ├── Metrics: avg_base_fare, avg_total_fare, booking_count
│   │   ├── SCD Columns: dbt_valid_from, dbt_valid_to, dbt_scd_id, dbt_updated_at
│   │   └── Strategy: check (monitors specific columns for changes)
│   │
│   └── route_fare_snapshot (SCD Type 2, ~152 rows)
│       ├── Tracks: route-level metrics
│       ├── Metrics: avg_fare, total_bookings, airline_count
│       ├── SCD Columns: dbt_valid_from, dbt_valid_to, dbt_scd_id, dbt_updated_at
│       └── Strategy: check
│
├── gold schema (KPI/Analytics layer)
│   ├── gold_avg_fare_by_airline (24 rows)
│   │   ├── airline, avg_fare, min_fare, max_fare, median_fare
│   │   ├── booking_count, market_share
│   │   └── Aggregation: GROUP BY airline
│   │
│   ├── gold_seasonal_fare_analysis (4 rows)
│   │   ├── seasonality, avg_fare, booking_count
│   │   ├── pct_diff_from_regular
│   │   └── Aggregation: GROUP BY seasonality
│   │
│   ├── gold_booking_count_by_airline (24 rows)
│   │   ├── airline, booking_source, booking_window
│   │   ├── booking_count, total_revenue
│   │   └── Aggregation: GROUP BY airline, booking_source, booking_window
│   │
│   ├── gold_popular_routes (152 rows)
│   │   ├── source, destination, airline
│   │   ├── booking_count, avg_fare, flight_count
│   │   └── Aggregation: GROUP BY route, airline (ordered by booking_count)
│   │
│   ├── gold_fare_by_class (3 rows)
│   │   ├── travel_class, avg_fare, min_fare, max_fare
│   │   ├── booking_count, revenue_share
│   │   └── Aggregation: GROUP BY travel_class
│   │
│   ├── gold_data_quality_report (13 rows)
│   │   ├── metric_name, metric_value, percentage
│   │   └── Tracks: categorical value distributions
│   │
│   ├── gold_fare_history (view)
│   │   ├── Joins flight_fare_snapshot with current data
│   │   ├── Shows historical fare changes over time
│   │   └── Includes valid_from, valid_to dates
│   │
│   └── gold_route_history (view)
│       ├── Joins route_fare_snapshot with current data
│       ├── Shows historical route metric changes
│       └── Includes valid_from, valid_to dates
│
└── audit schema (Monitoring/Logging layer)
    └── pipeline_runs (log entries)
        ├── run_id (uuid)
        ├── dag_id, task_id (varchar)
        ├── status (started/completed/failed)
        ├── rows_processed, rows_failed (int)
        ├── started_at, completed_at (timestamp)
        ├── error_message (text)
        └── metadata (jsonb)
```

---

### 6. **DBT Transformation Engine**
- **Location**: Runs inside Airflow container
- **Project Path**: `/opt/airflow/dbt_project`
- **Version**: 1.7.4
- **Database Profile**: `flight_analytics` (PostgreSQL)
- **Threads**: 4

**Configuration**:
```yaml
dbt_project.yml:
  name: flight_analytics
  version: 1.0.0
  config-version: 2
  
profiles.yml:
  flight_analytics:
    outputs:
      postgres:
        type: postgres
        host: postgres-analytics
        port: 5432
        database: flight_analytics
        schema: "{{ env_var('DBT_SCHEMA', 'analytics') }}"
```

**Models (9 total)**:

**Bronze Layer**:
- No models - data loaded directly from MySQL

**Silver Layer**:
- `silver_cleaned_flights.sql` (table, 57,000 rows)
  - Cleaning and standardization
  - Derived columns (route, fare_category, booking_window, etc.)
  - Source: bronze.validated_flights

**Gold Layer (6 KPI tables)**:
- `gold_avg_fare_by_airline.sql` (table, 24 rows)
- `gold_seasonal_fare_analysis.sql` (table, 4 rows)
- `gold_booking_count_by_airline.sql` (table, 24 rows)
- `gold_popular_routes.sql` (table, 152 rows)
- `gold_fare_by_class.sql` (table, 3 rows)
- `gold_data_quality_report.sql` (table, 13 rows)

**Views (2)**:
- `gold_fare_history.sql` (view)
  - Historical fare tracking via snapshot
- `gold_route_history.sql` (view)
  - Historical route metric tracking via snapshot

**Snapshots (2 total - SCD Type 2)**:

1. `flight_fare_snapshot.sql`
   ```yaml
   unique_key: [airline, source_code, destination_code, travel_class, seasonality]
   check_cols: [avg_base_fare, avg_total_fare, booking_count]
   strategy: check
   updated_at: current_timestamp
   ```
   - Records: ~19,052
   - Tracks fare changes over time
   - SCD Columns: dbt_valid_from, dbt_valid_to, dbt_scd_id, dbt_updated_at

2. `route_fare_snapshot.sql`
   ```yaml
   unique_key: [source_code, destination_code]
   check_cols: [avg_fare, total_bookings, airline_count]
   strategy: check
   updated_at: current_timestamp
   ```
   - Records: ~152
   - Tracks route metrics over time
   - SCD Columns: dbt_valid_from, dbt_valid_to, dbt_scd_id, dbt_updated_at

**Tests (22 total - all passing)**:
- Uniqueness tests on primary keys
- Not null tests on required fields
- Relationship tests for foreign keys
- Custom validation tests
- dbt_expectations (Great Expectations integration)

**Macros (1)**:
- `get_custom_schema.sql` - Custom schema naming macro

**Packages**:
- dbt-utils (utility functions)
- dbt-expectations (data quality)
- dbt-audit-helper (auditing utilities)

---

## Airflow DAG Tasks

### Task 1: Load CSV to MySQL
- **Task ID**: `load_csv_to_mysql`
- **Duration**: ~8 seconds
- **Input**: CSV file (57,000 records)
- **Output**: MySQL `raw_flight_data` table

**Process**:
```
1. Read CSV with Pandas
2. Rename columns to snake_case according to COLUMN_MAPPING
3. Convert data types (datetime, numeric)
4. Add source_file column
5. Check for NULL values (logging)
6. Truncate existing tables (with FK checks disabled)
7. Insert records in batches (BATCH_SIZE=5000)
8. Log pipeline event with row count and NULL statistics
```

**Return Value**:
```python
{
    'rows_loaded': 57000,
    'rows_with_issues': 0
}
```

---

### Task 2: Validate MySQL Data
- **Task ID**: `validate_mysql_data`
- **Duration**: ~10 seconds
- **Input**: MySQL `raw_flight_data`
- **Output**: MySQL `validated_flight_data`

**Validation Rules**:

**Hard Rules** (rows fail if not met):
```
• Required fields not null or empty:
  - airline, source_code, destination_code
• Positive fare values:
  - base_fare_bdt > 0
  - total_fare_bdt > 0
• Non-negative tax/surcharge:
  - tax_surcharge_bdt >= 0
• IATA code format (3 characters):
  - source_code, destination_code
• Positive duration:
  - duration_hrs > 0
• Positive booking lead time:
  - days_before_departure >= 1
```

**Soft Rules** (logged warnings but records pass):
```
• Unexpected seasonality values
  Known: Regular, Eid, Hajj, Winter, Winter Holidays
• Unexpected travel_class values
  Known: Economy, Business, First, First Class, Premium Economy
• Unexpected stopovers values
  Known: Direct, 1 Stop, 2 Stops
• Unexpected booking_source values
  Known: Direct Booking, Travel Agency, Online Website
• Unusual days_before_departure (> 365)
```

**Output Columns**:
```
All source columns plus:
• is_valid (1=valid, 0=invalid)
• validation_errors (concatenated error messages)
• validation_warnings (concatenated warning messages)
• raw_id (foreign key to raw_flight_data)
```

**Return Value**:
```python
{
    'total_rows': 57000,
    'valid_rows': 57000,
    'invalid_rows': 0,
    'rows_with_warnings': 0
}
```

---

### Task 3: Transfer to PostgreSQL Bronze
- **Task ID**: `transfer_to_postgres_bronze`
- **Duration**: ~12 seconds
- **Input**: MySQL `validated_flight_data` (valid records only)
- **Output**: PostgreSQL `bronze.validated_flights`

**Process**:
```
1. Extract validated data from MySQL with joins
   - Include raw_id and mysql_validated_id for traceability
   - Only where is_valid = 1
2. Convert TINYINT is_valid to BOOLEAN
3. Add bronze_loaded_at timestamp
4. Connect to PostgreSQL analytics database
5. Truncate existing bronze table
6. Insert records in batches (BATCH_SIZE=5000)
7. Verify row count matches source count
8. Log transfer statistics
```

**Return Value**:
```python
{
    'rows_transferred': 57000
}
```

---

### Task 4: Run DBT Transformations
- **Task ID**: `run_dbt_transformations`
- **Duration**: ~4 seconds
- **Input**: PostgreSQL `bronze.validated_flights`
- **Output**: Silver and Gold layers

**Execution Order**:
```
1. dbt run (--profiles-dir .)
   ├─ Creates/updates Silver models
   ├─ Creates/updates Gold models
   └─ Returns model execution results

2. dbt test (--profiles-dir .)
   ├─ Runs all data quality tests
   └─ Returns test results

3. dbt snapshot (--profiles-dir .)
   ├─ Updates flight_fare_snapshot
   ├─ Updates route_fare_snapshot
   └─ Returns snapshot execution results
```

**Error Handling**:
```
• If any step fails, logs error and raises exception
• Captures stdout/stderr (first 2000 chars)
• Records failed_step in metadata
• Prevents execution of subsequent steps
```

**Return Value**:
```python
{
    'dbt_run': 'success',
    'dbt_test': 'success',
    'dbt_snapshot': 'success'
}
```

---

## Medallion Architecture

### Bronze Layer
- **Schema**: `bronze`
- **Table**: `validated_flights`
- **Row Count**: 57,000
- **Purpose**: Raw validated data, unchanged from source

**Columns** (all source columns plus audit columns):
```
Core columns from MySQL:
• airline, source_code, source_name
• destination_code, destination_name
• departure_datetime, arrival_datetime
• duration_hrs, stopovers, aircraft_type
• travel_class, booking_source
• base_fare_bdt, tax_surcharge_bdt, total_fare_bdt
• seasonality, days_before_departure

Audit columns:
• is_valid (boolean)
• validation_errors (text)
• mysql_raw_id, mysql_validated_id
• mysql_loaded_at, bronze_loaded_at
```

---

### Silver Layer
- **Schema**: `silver`
- **Purpose**: Cleaned, standardized data with derived columns

**Main Table: silver_cleaned_flights**
- **Row Count**: 57,000 (valid records only)
- **Source**: bronze.validated_flights

**Transformations Applied**:
```
1. Text Standardization
   • Convert all text to UPPERCASE
   • Trim leading/trailing whitespace
   • Replace multiple spaces with single space

2. Derived Columns
   • route = source_code || '-' || destination_code
   • fare_category = CASE
       WHEN total_fare_bdt <= 5000 THEN 'Budget'
       WHEN total_fare_bdt <= 15000 THEN 'Standard'
       ELSE 'Premium'
     END
   • booking_window = CASE
       WHEN days_before_departure = 1 THEN 'Last Minute'
       WHEN days_before_departure <= 7 THEN 'Standard'
       WHEN days_before_departure <= 30 THEN 'Advance'
       ELSE 'Early Bird'
     END
   • route_type = CASE
       WHEN source_code IN (select code from domestic_airports) 
         AND destination_code IN (select code from domestic_airports)
         THEN 'Domestic'
       ELSE 'International'
     END
   • is_peak_season = seasonality IN ('Eid', 'Hajj', 'Winter Holidays')

3. Time Dimensions
   • departure_hour = EXTRACT(HOUR FROM departure_datetime)
   • departure_dow = EXTRACT(DOW FROM departure_datetime)
   • departure_date = DATE(departure_datetime)
```

**SCD Type 2 Snapshots**:

1. **flight_fare_snapshot** (~19,052 rows)
   ```
   Tracks: airline, source_code, destination_code, travel_class, seasonality
   Metrics aggregated at this grain:
   • avg_base_fare
   • avg_total_fare
   • booking_count
   • min_fare, max_fare
   • total_revenue
   
   SCD Columns:
   • dbt_valid_from (when record became current)
   • dbt_valid_to (when record was superseded)
   • dbt_scd_id (unique identifier for dimension)
   • dbt_updated_at (timestamp of latest update)
   ```

2. **route_fare_snapshot** (~152 rows)
   ```
   Tracks: source_code, destination_code
   Metrics aggregated at this grain:
   • avg_fare
   • total_bookings
   • airline_count
   • unique_classes
   
   SCD Columns:
   • dbt_valid_from, dbt_valid_to
   • dbt_scd_id, dbt_updated_at
   ```

---

### Gold Layer
- **Schema**: `gold`
- **Purpose**: Business-ready KPI tables for analytics and reporting

**Table 1: gold_avg_fare_by_airline** (24 rows)
```
Dimensions:
  • airline

Metrics:
  • avg_base_fare
  • avg_total_fare
  • min_fare
  • max_fare
  • median_fare
  • booking_count
  • market_share (%)

Grain: One row per airline
```

**Table 2: gold_seasonal_fare_analysis** (4 rows)
```
Dimensions:
  • seasonality (Regular, Eid, Hajj, Winter)

Metrics:
  • avg_fare
  • booking_count
  • pct_diff_from_regular
  • revenue_share

Grain: One row per season
```

**Table 3: gold_booking_count_by_airline** (24 rows)
```
Dimensions:
  • airline

Metrics:
  • total_bookings
  • total_revenue
  • avg_booking_value
  • booking_by_class (JSON)
  • booking_by_window (JSON)

Grain: One row per airline
```

**Table 4: gold_popular_routes** (152 rows)
```
Dimensions:
  • source_code, source_name
  • destination_code, destination_name
  • airline

Metrics:
  • booking_count
  • avg_fare
  • flight_count
  • bookings_rank (by route)

Grain: One row per route/airline combo, ordered by popularity
```

**Table 5: gold_fare_by_class** (3 rows)
```
Dimensions:
  • travel_class (Economy, Business, First)

Metrics:
  • avg_fare
  • min_fare
  • max_fare
  • booking_count
  • revenue_share

Grain: One row per class
```

**Table 6: gold_data_quality_report** (13 rows)
```
Metrics tracked:
  • record_count
  • valid_record_count
  • invalid_record_count
  • null_count_by_column (for nullable fields)
  • unique_airline_count
  • unique_route_count
  • pct_valid_records

Grain: Quality metrics (one row per metric)
```

**Views (2)**:

1. **gold_fare_history**
   ```
   Joins: gold_avg_fare_by_airline + flight_fare_snapshot
   Shows: Historical fare changes with valid_from/valid_to dates
   Columns: airline, avg_fare_current, avg_fare_previous, 
            change_pct, valid_from, valid_to
   ```

2. **gold_route_history**
   ```
   Joins: gold_popular_routes + route_fare_snapshot
   Shows: Historical route metric changes
   Columns: route, booking_count_current, booking_count_previous,
            change_pct, valid_from, valid_to
   ```

---

### Audit Schema
- **Schema**: `audit`
- **Purpose**: Pipeline monitoring and troubleshooting

**Table: pipeline_runs**
```
Columns:
  • id (serial primary key)
  • run_id (UUID - unique per DAG run)
  • dag_id (varchar - 'flight_price_pipeline')
  • task_id (varchar - task name)
  • status (varchar - 'started', 'completed', 'failed')
  • rows_processed (integer)
  • rows_failed (integer)
  • rows_with_warnings (integer)
  • started_at (timestamp)
  • completed_at (timestamp)
  • error_message (text, nullable)
  • metadata (JSONB - flexible attributes)

Metadata JSON fields:
  {
    "source_file": "...",
    "rows_with_null_values": 0,
    "validation_rate": "100%",
    "unexpected_values": {...},
    "dbt_results": {...}
  }
```

---

## Data Flow

### Step-by-Step Flow

```
1. CSV Source File
   ├─ Location: ./data/Flight_Price_Dataset_of_Bangladesh.csv
   ├─ Records: 57,000
   └─ Columns: 18

2. Airflow Task 1: Load CSV to MySQL
   ├─ Read CSV with Pandas
   ├─ Transform column names
   ├─ Insert to raw_flight_data
   └─ Duration: ~8 seconds

3. MySQL Staging (raw_flight_data)
   ├─ 57,000 rows
   └─ No transformations

4. Airflow Task 2: Validate Data
   ├─ Apply validation rules
   ├─ Set is_valid flag
   ├─ Record validation errors
   └─ Duration: ~10 seconds

5. MySQL Staging (validated_flight_data)
   ├─ 57,000 rows
   ├─ is_valid = 1 for all (100% pass rate)
   └─ Foreign key to raw_flight_data

6. Airflow Task 3: Transfer to PostgreSQL
   ├─ Extract valid records
   ├─ Convert boolean types
   ├─ Load to bronze.validated_flights
   └─ Duration: ~12 seconds

7. PostgreSQL Bronze Layer
   ├─ Schema: bronze
   ├─ Table: validated_flights
   └─ 57,000 rows

8. Airflow Task 4: DBT Run + Snapshot
   ├─ Execute Silver models
   ├─ Execute Gold models
   ├─ Update SCD snapshots
   ├─ Run tests
   └─ Duration: ~4 seconds

9. PostgreSQL Silver Layer
   ├─ Schema: silver
   ├─ Table: silver_cleaned_flights (57,000 rows)
   ├─ Snapshot: flight_fare_snapshot (19,052 rows)
   └─ Snapshot: route_fare_snapshot (152 rows)

10. PostgreSQL Gold Layer
    ├─ Schema: gold
    ├─ 6 KPI tables
    └─ 2 history views
```

---

## Connections

### Data Connections (Solid Arrows)

1. **CSV → Airflow (Task 1)**
   - Label: "Read CSV\n(Pandas)"
   - Color: Blue (#1976D2)
   - Style: Solid

2. **Airflow → MySQL (raw)**
   - Label: "Task 1\nLoad CSV\n~8s"
   - Color: Blue (#1976D2)
   - Style: Solid

3. **MySQL (raw) → MySQL (validated)**
   - Label: "Task 2\nValidate\n~10s"
   - Color: Blue (#1976D2)
   - Style: Solid

4. **MySQL → PostgreSQL (Bronze)**
   - Label: "Task 3\nTransfer\n~12s"
   - Color: Green (#388E3C)
   - Style: Solid

5. **Bronze → Silver**
   - Label: "DBT\nClean & Transform"
   - Color: Orange (#F57C00)
   - Style: Solid

6. **Silver → Gold**
   - Label: "DBT\nAggregate KPIs"
   - Color: Gold (#FFC107)
   - Style: Solid

### Control Connections (Dashed Arrows)

7. **Airflow Scheduler → Airflow Webserver**
   - Label: "Task Status"
   - Color: Gray (#616161)
   - Style: Dashed

8. **DBT → Silver Snapshots**
   - Label: "SCD Type 2\nHistorical Tracking"
   - Color: Purple (#7B1FA2)
   - Style: Dashed

9. **All Tasks → Audit Table**
   - Label: "Logging"
   - Color: Gray (#616161)
   - Style: Dashed

---

## Layout Rules

### Horizontal Layout (Left to Right)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Docker Environment                                  │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │                         AIRFLOW ORCHESTRATION                                ││
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐              ││
│  │  │ Task 1   │───▶│ Task 2   │───▶│ Task 3   │───▶│ Task 4   │              ││
│  │  │Load CSV  │    │Validate  │    │Transfer  │    │DBT Run   │              ││
│  │  └────┬─────┘    └────┬─────┘    └────┬─────┘    └────┬─────┘              ││
│  └───────┼───────────────┼───────────────┼───────────────┼──────────────────────┘│
│          │               │               │               │                       │
│          ▼               ▼               │               ▼                       │
│  ┌───────────────────────────────┐       │       ┌───────────────────────────┐  │
│  │       MySQL Staging           │       │       │   PostgreSQL Analytics    │  │
│  │  ┌─────────┐   ┌───────────┐  │       │       │                           │  │
│  │  │  raw_   │──▶│validated_ │  │───────┘       │  ┌───────┐  ┌───────┐    │  │
│  │  │ flight_ │   │ flight_   │  │               │  │Bronze │─▶│Silver │    │  │
│  │  │  data   │   │   data    │  │               │  └───────┘  └───┬───┘    │  │
│  │  └─────────┘   └───────────┘  │               │                 │        │  │
│  └───────────────────────────────┘               │                 ▼        │  │
│                                                  │            ┌───────┐     │  │
│  ┌───────────┐                                   │            │ Gold  │     │  │
│  │   CSV     │                                   │            └───────┘     │  │
│  │  Source   │                                   └───────────────────────────┘  │
│  └───────────┘                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Vertical Positioning

- **Top**: Airflow task flow (horizontal)
- **Left**: CSV source file
- **Center-Left**: MySQL staging (raw and validated tables)
- **Center-Right**: PostgreSQL analytics (medallion layers)
- **Bottom**: Audit logging

### Clustering

1. **Outer Cluster**: Docker Environment
   - Border: Dashed, Dark Gray (#37474F)
   - Background: Light Gray (#ECEFF1)
   - Label: "Docker Environment"

2. **Airflow Cluster**: Contains task flow
   - Border: Solid, Orange (#F57C00)
   - Background: Light Orange (#FFE0B2)
   - Label: "Airflow Orchestration"

3. **MySQL Cluster**: Contains staging tables
   - Border: Solid, Blue (#1976D2)
   - Background: Light Blue (#E3F2FD)
   - Label: "MySQL Staging :3307"

4. **PostgreSQL Cluster**: Contains medallion architecture
   - Border: Solid, Green (#388E3C)
   - Background: Light Green (#E8F5E9)
   - Label: "PostgreSQL Analytics :5433"

5. **Medallion Sub-clusters**:
   - Bronze: Background #D7CCC8
   - Silver: Background #CFD8DC
   - Gold: Background #FFF8E1

---

## Color Scheme

| Component | Background | Border | Text | Purpose |
|-----------|-----------|--------|------|---------|
| Docker Environment | `#ECEFF1` | `#37474F` | `#000000` | Infrastructure |
| CSV Source | `#E3F2FD` | `#1976D2` | `#000000` | Data source |
| Airflow | `#FFE0B2` | `#F57C00` | `#000000` | Orchestration |
| MySQL | `#E3F2FD` | `#1976D2` | `#000000` | Staging |
| PostgreSQL | `#E8F5E9` | `#388E3C` | `#000000` | Analytics |
| Bronze Layer | `#D7CCC8` | `#8D6E63` | `#000000` | Raw data |
| Silver Layer | `#CFD8DC` | `#607D8B` | `#000000` | Cleaned data |
| Gold Layer | `#FFF8E1` | `#FFC107` | `#000000` | KPIs |
| DBT | `#F3E5F5` | `#7B1FA2` | `#000000` | Transformations |
| Audit | `#FAFAFA` | `#9E9E9E` | `#000000` | Logging |

---

## Annotations

### CSV Source
```
CSV Source
Flight_Price_Dataset_of_Bangladesh.csv

• 57,000 records
• 18 columns
• Bangladesh flight data
```

### Airflow Webserver
```
Airflow Webserver
:8080

• DAG management UI
• Task monitoring
• Log viewing
• admin/admin
```

### Airflow Scheduler
```
Airflow Scheduler

• Task execution
• Dependency resolution
• Retry handling
```

### MySQL Staging
```
MySQL Staging
:3307

Database: flight_staging
Tables:
• raw_flight_data
• validated_flight_data
Foreign key relationship
```

### PostgreSQL Analytics
```
PostgreSQL Analytics
:5433

Database: flight_analytics
Schemas:
• bronze (raw)
• silver (cleaned)
• gold (KPIs)
• audit (logging)
```

### DBT
```
DBT 1.7.4

• 9 models
• 22 tests
• 2 SCD snapshots
• ~4s execution
```

### Bronze Layer
```
Bronze Layer
validated_flights

• 57,000 rows
• Raw validated data
• No transformations
```

### Silver Layer
```
Silver Layer
silver_cleaned_flights

• 57,000 rows
• Standardized text
• Derived columns
• SCD snapshots
```

### Gold Layer
```
Gold Layer

• 6 KPI tables
• 2 history views
• Business metrics
• Market analysis
```

---

## Performance Metrics to Include

Add a legend box showing:

```
Pipeline Metrics
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total Duration:   ~34 seconds
Source Records:   57,000
Valid Records:    57,000 (100%)
Airlines:         24
Unique Routes:    152
DBT Models:       9
DBT Tests:        22 (passing)
SCD Snapshots:    2

Task Breakdown:
• Load CSV:       ~8s
• Validate:       ~10s
• Transfer:       ~12s
• DBT Run:        ~4s
```

---

## Output Format Requirements

### File Naming
- Base name: `flight_price_architecture`
- PNG: `flight_price_architecture.png`
- DOT: `flight_price_architecture.dot`
- DRAWIO: `flight_price_architecture.drawio`

### Image Specifications
- **Format**: PNG
- **Resolution**: 300 DPI minimum
- **Size**: Optimized for A4 landscape (11.7" x 8.3")
- **Background**: White
- **Margins**: 0.5 inches all sides
- **Border**: None

### Text Requirements
- **Font Family**: Sans-serif (Helvetica, Arial)
- **Title**: 16pt bold
- **Component labels**: 12pt regular
- **Annotations**: 10pt regular
- **Port numbers**: 9pt italic
- **All text must be readable at 100% zoom**
- **No text overlap**

### Graph Attributes
```python
graph_attr = {
    "fontsize": "12",
    "fontname": "Helvetica",
    "bgcolor": "white",
    "pad": "0.5",
    "nodesep": "1.0",
    "ranksep": "1.5",
    "splines": "ortho",
    "rankdir": "LR",
    "concentrate": "false"
}
```

---

## Validation Checklist

Before finalizing, verify:

### Components
- [ ] CSV source file shown with record count
- [ ] Airflow Webserver shown with port (8080)
- [ ] Airflow Scheduler shown
- [ ] All 4 DAG tasks represented
- [ ] MySQL staging with both tables
- [ ] PostgreSQL with all 4 schemas
- [ ] DBT transformation engine indicated
- [ ] Bronze/Silver/Gold layers visible
- [ ] SCD snapshots shown in Silver layer
- [ ] Audit logging indicated

### Connections
- [ ] CSV → Task 1 arrow
- [ ] Task 1 → MySQL (raw) arrow
- [ ] Task 2 → MySQL (validated) arrow
- [ ] Task 3 → PostgreSQL (Bronze) arrow
- [ ] Task 4 → Silver/Gold arrows
- [ ] Bronze → Silver → Gold flow
- [ ] DBT → Snapshots connection

### Labels
- [ ] All arrows labeled with task names
- [ ] All ports displayed
- [ ] Row counts shown
- [ ] Durations indicated

### Visual
- [ ] Docker boundary visible
- [ ] Airflow cluster grouped
- [ ] MySQL cluster grouped
- [ ] PostgreSQL cluster grouped
- [ ] Medallion layers distinguished by color
- [ ] Color coding consistent
- [ ] No overlapping text
- [ ] All text readable

### Files
- [ ] PNG generated
- [ ] DOT generated
- [ ] DRAWIO generated
- [ ] All saved to docs/diagrams/

---

## Technology Versions

Include version information:

```
Technology Stack
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Docker Compose:   2.0+
Apache Airflow:   2.7.3
MySQL:            8.0
PostgreSQL:       15
DBT:              1.7.4
Python:           3.10
Pandas:           2.0+
```

---

## KPI Summary to Display

```
Gold Layer KPIs
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Avg Fare by Airline:     24 airlines
Seasonal Analysis:       4 seasons
Booking by Airline:      24 airlines
Popular Routes:          152 routes
Fare by Class:           3 classes
Data Quality Report:     13 categories
```

---

## SCD Type 2 Details

```
SCD Snapshots
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
flight_fare_snapshot:
• Tracks: airline/route/class/season
• Records: 19,052
• Columns: avg fares, booking count

route_fare_snapshot:
• Tracks: route-level metrics
• Records: 152
• Columns: avg fare, bookings, airlines

SCD Columns (auto-generated):
• dbt_valid_from
• dbt_valid_to
• dbt_scd_id
• dbt_updated_at
```

## Example Use Cases to Illustrate

### Use Case 1: Normal Pipeline Execution
```
1. Airflow scheduler triggers flight_price_pipeline DAG
2. Task 1 reads CSV file (57,000 records)
3. Task 1 loads data to MySQL raw_flight_data
4. Task 2 validates all records (100% pass rate)
5. Task 2 writes to MySQL validated_flight_data
6. Task 3 extracts valid records
7. Task 3 loads to PostgreSQL bronze.validated_flights
8. Task 4 runs DBT models (Silver transformations)
9. Task 4 runs DBT models (Gold aggregations)
10. Task 4 updates SCD snapshots
11. Task 4 runs 22 tests (all pass)
12. Pipeline completes in ~34 seconds
```

### Use Case 2: Data Validation Failure Handling
```
1. CSV contains record with null airline field
2. Task 2 validation detects missing required field
3. Record marked as is_valid = 0
4. Validation error logged to validation_errors column
5. Task 3 filters out invalid records (is_valid = 1 only)
6. Invalid records remain in MySQL for review
7. Pipeline continues with valid records
8. Audit table logs rows_failed count
```

### Use Case 3: SCD Type 2 Historical Tracking
```
1. Initial run: Airline X has avg_fare = 5000 BDT
2. Snapshot creates record with dbt_valid_from = 2024-01-01
3. Next run: Airline X avg_fare changes to 5500 BDT
4. Snapshot detects change in tracked column
5. Original record updated: dbt_valid_to = 2024-01-15
6. New record created: dbt_valid_from = 2024-01-15, dbt_valid_to = NULL
7. gold_fare_history view shows both versions
8. Analysts can query fare trends over time
```

### Use Case 4: Foreign Key Constraint Handling
```
1. Pipeline reruns (truncate and reload)
2. MySQL foreign key: validated_flight_data → raw_flight_data
3. Direct TRUNCATE fails due to FK constraint
4. Solution: Disable FK checks within transaction
5. Execute: SET FOREIGN_KEY_CHECKS = 0
6. Truncate validated_flight_data first
7. Truncate raw_flight_data second
8. Execute: SET FOREIGN_KEY_CHECKS = 1
9. Commit transaction
10. Load fresh data
```

### Use Case 5: Boolean Type Conversion
```
1. MySQL stores boolean as TINYINT (0 or 1)
2. Task 3 extracts data from MySQL
3. Pandas reads is_valid as integer (0/1)
4. PostgreSQL expects boolean (TRUE/FALSE)
5. Conversion applied: df['is_valid'] = df['is_valid'].astype(bool)
6. Data loads successfully to PostgreSQL
7. DBT models can use boolean logic directly
```

---

## Error Handling Patterns

### MySQL Connection Errors
```
Error: Can't connect to MySQL server
Solution:
1. Check container status: docker ps
2. Verify port mapping: 3307:3306
3. Check credentials in Airflow connection
4. Restart MySQL container if needed
```

### PostgreSQL Connection Errors
```
Error: Connection refused to PostgreSQL
Solution:
1. Check container status: docker ps
2. Verify port mapping: 5433:5432
3. Check credentials in Airflow connection
4. Verify database exists: flight_analytics
```

### DBT Model Failures
```
Error: DBT model compilation failed
Solution:
1. Check source table exists in bronze schema
2. Verify column names match sources.yml
3. Run dbt debug for connection test
4. Check profiles.yml configuration
```

### Schema Naming Issues
```
Error: DBT creates public_silver instead of silver
Solution:
1. Create custom macro: generate_schema_name
2. Override default schema concatenation
3. Return custom_schema_name directly
4. Macro location: macros/get_custom_schema.sql
```

---

## Monitoring and Observability

### Airflow UI Monitoring
```
URL: http://localhost:8080
Credentials: admin/admin

Monitor:
• DAG run status (success/failed/running)
• Task duration trends
• Task logs (stdout/stderr)
• Retry attempts
• Dependency graph
```

### Audit Table Queries
```sql
-- Recent pipeline runs
SELECT task_id, status, rows_processed, started_at
FROM audit.pipeline_runs
ORDER BY id DESC
LIMIT 10;

-- Failed tasks
SELECT task_id, error_message, started_at
FROM audit.pipeline_runs
WHERE status = 'failed'
ORDER BY id DESC;

-- Average task duration
SELECT task_id, AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_seconds
FROM audit.pipeline_runs
WHERE status = 'completed'
GROUP BY task_id;
```

### Data Quality Monitoring
```sql
-- Check row counts across layers
SELECT 'bronze.validated_flights' as table_name, COUNT(*) as rows 
FROM bronze.validated_flights
UNION ALL
SELECT 'silver.silver_cleaned_flights', COUNT(*) 
FROM silver.silver_cleaned_flights
UNION ALL
SELECT 'gold.gold_avg_fare_by_airline', COUNT(*) 
FROM gold.gold_avg_fare_by_airline;

-- Validation pass rate
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as valid,
    ROUND(100.0 * SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) / COUNT(*), 2) as pass_rate
FROM bronze.validated_flights;
```

---

## Diagram Component Details

### Airflow Task Nodes

Each task should be represented as a distinct node with:

```
Task 1: Load CSV to MySQL
┌─────────────────────────┐
│     Load CSV to MySQL   │
│                         │
│  • Read with Pandas     │
│  • Rename columns       │
│  • Insert to MySQL      │
│  • Duration: ~8s        │
│  • Rows: 57,000         │
└─────────────────────────┘

Task 2: Validate Data
┌─────────────────────────┐
│     Validate Data       │
│                         │
│  • Check required fields│
│  • Validate ranges      │
│  • Set is_valid flag    │
│  • Duration: ~10s       │
│  • Pass rate: 100%      │
└─────────────────────────┘

Task 3: Transfer to PostgreSQL
┌─────────────────────────┐
│  Transfer to PostgreSQL │
│                         │
│  • Extract valid records│
│  • Convert booleans     │
│  • Load to Bronze       │
│  • Duration: ~12s       │
│  • Rows: 57,000         │
└─────────────────────────┘

Task 4: DBT Run + Snapshot
┌─────────────────────────┐
│   DBT Run + Snapshot    │
│                         │
│  • Silver transforms    │
│  • Gold aggregations    │
│  • SCD snapshots        │
│  • Duration: ~4s        │
│  • Models: 9            │
└─────────────────────────┘
```

### Database Table Nodes

```
MySQL: raw_flight_data
┌─────────────────────────┐
│    raw_flight_data      │
│                         │
│  • 57,000 rows          │
│  • 18 source columns    │
│  • created_at timestamp │
│  • No validation        │
└─────────────────────────┘

MySQL: validated_flight_data
┌─────────────────────────┐
│  validated_flight_data  │
│                         │
│  • 57,000 rows          │
│  • is_valid flag        │
│  • validation_errors    │
│  • FK to raw_flight_data│
└─────────────────────────┘

PostgreSQL: bronze.validated_flights
┌─────────────────────────┐
│ bronze.validated_flights│
│                         │
│  • 57,000 rows          │
│  • Raw validated data   │
│  • loaded_at timestamp  │
│  • No transformations   │
└─────────────────────────┘

PostgreSQL: silver.silver_cleaned_flights
┌─────────────────────────┐
│silver.silver_cleaned_   │
│        flights          │
│  • 57,000 rows          │
│  • Standardized text    │
│  • Derived columns:     │
│    - route              │
│    - fare_category      │
│    - booking_window     │
│    - route_type         │
│    - is_peak_season     │
└─────────────────────────┘
```

### Gold Layer Table Nodes

```
┌─────────────────────────┐
│gold_avg_fare_by_airline │
│  • 24 rows (airlines)   │
│  • avg/min/max/median   │
│  • market_share_pct     │
└─────────────────────────┘

┌─────────────────────────┐
│gold_seasonal_fare_      │
│       analysis          │
│  • 4 rows (seasons)     │
│  • pct_diff_from_regular│
│  • by route_type/class  │
└─────────────────────────┘

┌─────────────────────────┐
│gold_booking_count_      │
│      by_airline         │
│  • 24 rows (airlines)   │
│  • by booking_source    │
│  • by booking_window    │
└─────────────────────────┘

┌─────────────────────────┐
│  gold_popular_routes    │
│  • 152 rows (routes)    │
│  • booking_count        │
│  • fare statistics      │
└─────────────────────────┘

┌─────────────────────────┐
│   gold_fare_by_class    │
│  • 3 rows (classes)     │
│  • Economy/Business/    │
│    First                │
│  • domestic/intl split  │
└─────────────────────────┘

┌─────────────────────────┐
│gold_data_quality_report │
│  • 13 rows (categories) │
│  • value distributions  │
│  • anomaly tracking     │
└─────────────────────────┘
```

### SCD Snapshot Nodes

```
┌─────────────────────────┐
│ silver.flight_fare_     │
│       snapshot          │
│                         │
│  • 19,052 rows          │
│  • SCD Type 2           │
│  • Tracks:              │
│    - airline            │
│    - route              │
│    - class              │
│    - seasonality        │
│  • Columns:             │
│    - avg_base_fare      │
│    - avg_total_fare     │
│    - booking_count      │
│  • dbt_valid_from/to    │
└─────────────────────────┘

┌─────────────────────────┐
│  silver.route_fare_     │
│       snapshot          │
│                         │
│  • 152 rows             │
│  • SCD Type 2           │
│  • Tracks:              │
│    - route              │
│  • Columns:             │
│    - avg_fare           │
│    - total_bookings     │
│    - airline_count      │
│  • dbt_valid_from/to    │
└─────────────────────────┘
```

---

## Arrow Specifications

### Primary Data Flow Arrows

| From | To | Label | Color | Style | Width |
|------|-----|-------|-------|-------|-------|
| CSV Source | Task 1 | "Read CSV (Pandas)" | #1976D2 | Solid | 2 |
| Task 1 | raw_flight_data | "Insert 57K rows" | #1976D2 | Solid | 2 |
| raw_flight_data | Task 2 | "Read for validation" | #1976D2 | Solid | 2 |
| Task 2 | validated_flight_data | "Write with is_valid" | #1976D2 | Solid | 2 |
| validated_flight_data | Task 3 | "Extract valid only" | #388E3C | Solid | 2 |
| Task 3 | bronze.validated_flights | "JDBC Insert" | #388E3C | Solid | 2 |
| bronze.validated_flights | Task 4 | "DBT source" | #F57C00 | Solid | 2 |
| Task 4 | silver.silver_cleaned_flights | "DBT transform" | #F57C00 | Solid | 2 |
| Task 4 | Gold tables | "DBT aggregate" | #FFC107 | Solid | 2 |

### Secondary Flow Arrows

| From | To | Label | Color | Style | Width |
|------|-----|-------|-------|-------|-------|
| Task 4 | flight_fare_snapshot | "SCD snapshot" | #7B1FA2 | Dashed | 1 |
| Task 4 | route_fare_snapshot | "SCD snapshot" | #7B1FA2 | Dashed | 1 |
| All Tasks | audit.pipeline_runs | "Logging" | #9E9E9E | Dashed | 1 |
| Airflow Scheduler | Airflow Webserver | "Status updates" | #616161 | Dashed | 1 |

### Medallion Layer Arrows

| From | To | Label | Color | Style | Width |
|------|-----|-------|-------|-------|-------|
| Bronze | Silver | "Clean & Transform" | #F57C00 | Bold | 3 |
| Silver | Gold | "Aggregate KPIs" | #FFC107 | Bold | 3 |

---

## Additional Notes

### Scalability Indicators

Show where scaling is possible:

```
MySQL Staging:
• Can add read replicas for validation queries
• Annotation: "Read replica capable"

PostgreSQL Analytics:
• Can partition large tables by date
• Annotation: "Partitioning supported"

Airflow:
• Can add more workers for parallel DAGs
• Annotation: "Celery executor available"

DBT:
• Can parallelize model execution
• Annotation: "threads: 4 configurable"
```

### Security Notes

Add disclaimer:

```
Note: Development configuration
Production requires:
• Encrypted connections (SSL/TLS)
• Secret management (Vault/AWS Secrets)
• Network policies
• Role-based access control
• Audit logging enabled
```

### Data Lineage Summary

```
Data Lineage
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source: CSV file (Kaggle dataset)
    ↓
Staging: MySQL (raw → validated)
    ↓
Bronze: PostgreSQL (validated_flights)
    ↓
Silver: PostgreSQL (cleaned_flights + snapshots)
    ↓
Gold: PostgreSQL (6 KPI tables + 2 views)

Transformations:
• Column renaming (CSV → MySQL)
• Data validation (MySQL)
• Boolean conversion (MySQL → PostgreSQL)
• Text standardization (Bronze → Silver)
• Derived columns (Bronze → Silver)
• Aggregations (Silver → Gold)
• SCD tracking (Silver snapshots)
```

---

## Final Diagram Requirements Summary

The completed diagram must show:

1. **Data Source**: CSV file with 57,000 records
2. **Orchestration**: Airflow with 4 sequential tasks
3. **Staging**: MySQL with raw and validated tables
4. **Analytics**: PostgreSQL with medallion architecture
5. **Transformations**: DBT with 9 models and 2 snapshots
6. **Data Flow**: Clear arrows showing data movement
7. **Metrics**: Row counts, durations, and pass rates
8. **Color Coding**: Distinct colors for each layer
9. **Ports**: All exposed ports labeled
10. **Docker Boundary**: Container environment visible

The diagram should be readable at 100% zoom, with no overlapping text, and exported in PNG, DOT, and DRAWIO formats to the `docs/diagrams/` directory.