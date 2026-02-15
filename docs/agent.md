# Agent Instructions: Flight Price Analysis Pipeline Architecture Diagram Generation

## Overview
This workspace contains tools to automatically generate architecture diagrams for a flight price analysis data pipeline. The system uses Apache Airflow for orchestration and DBT for transformations, processing flight price data from Bangladesh through a medallion architecture (Bronze/Silver/Gold) with SCD Type 2 for historical tracking. Diagrams are created using Python's `diagrams` library, rendered with GraphViz, and converted to editable draw.io format.

---

## Environment Setup

### Python Environment
- **Python Version**: 3.8+
- **Virtual Environment**: Recommended for isolation
- **Activation**: 
  - Windows: `.\venv\Scripts\activate`
  - Linux/Mac: `source venv/bin/activate`

### Installed Packages (Required Versions)
```
diagrams==0.24.4
graphviz==0.20.3
graphviz2drawio==1.1.0
```

### Initial Setup from Scratch
```bash
# 1. Create virtual environment
python -m venv venv

# 2. Activate environment
# Windows:
.\venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# 3. Install packages
pip install -r requirements.txt
```

### GraphViz Installation
- **Windows**: Download from https://graphviz.org/download/ and install to `C:\Program Files\Graphviz`
- **Linux**: `sudo apt-get install graphviz`
- **Mac**: `brew install graphviz`

**Critical**: Add GraphViz to PATH:
```bash
# Windows (PowerShell)
$env:PATH += ";C:\Program Files\Graphviz\bin"

# Linux/Mac
export PATH=$PATH:/usr/local/bin
```

### VS Code Extensions (Recommended)
- **Draw.io Integration**: `hediet.vscode-drawio` - For viewing/editing .drawio files
- **Docker**: `ms-azuretools.vscode-docker` - For container management

---

## Project Structure

```
flight_price_pipeline/
├── docs/
│   ├── diagrams/                      # Output directory for generated diagrams
│   │   ├── *.png                      # PNG image outputs
│   │   ├── *.dot                      # GraphViz DOT source files
│   │   └── *.drawio                   # Editable draw.io files
│   ├── agent.md                       # THIS FILE - Agent instructions
│   ├── instructions.md                # Complete architecture documentation
│   ├── requirements.txt               # Python dependencies for diagram generation
│   └── generate_architecture.py       # Main diagram generation script
├── docker-compose.yml                 # Container orchestration
├── Dockerfile.airflow                 # Custom Airflow image
├── README.md                          # Project overview
├── dags/
│   ├── flight_pipeline_dag.py         # Main Airflow DAG with 4 tasks
│   ├── flight_pipeline_dag copy.py    # Backup copy
│   ├── __pycache__/
│   └── utils/
│       ├── __init__.py
│       ├── logging_utils.py           # Logging and context utilities
│       └── __pycache__/
├── dbt_project/
│   ├── dbt_project.yml                # DBT configuration
│   ├── profiles.yml                   # Database connections
│   ├── dbt_packages/                  # Installed DBT packages
│   ├── models/
│   │   ├── sources.yml                # Source definitions (bronze.validated_flights)
│   │   ├── bronze/                    # Bronze layer (raw data)
│   │   ├── silver/                    # Silver layer (cleaned data)
│   │   └── gold/                      # Gold layer (KPI aggregations)
│   │       ├── gold_avg_fare_by_airline.sql
│   │       ├── gold_seasonal_fare_analysis.sql
│   │       ├── gold_booking_count_by_airline.sql
│   │       ├── gold_popular_routes.sql
│   │       ├── gold_fare_by_class.sql
│   │       ├── gold_data_quality_report.sql
│   │       ├── gold_fare_history.sql (view)
│   │       ├── gold_route_history.sql (view)
│   │       └── schema.yml
│   ├── snapshots/
│   │   ├── flight_fare_snapshot.sql   # SCD Type 2 - fare history
│   │   └── route_fare_snapshot.sql    # SCD Type 2 - route history
│   ├── macros/
│   │   └── get_custom_schema.sql      # Custom schema naming macro
│   ├── tests/                         # Data quality tests
│   ├── logs/                          # DBT execution logs
│   └── target/                        # DBT compiled artifacts
├── data/
│   └── Flight_Price_Dataset_of_Bangladesh.csv  # Source data (57,000 rows)
├── scripts/
│   ├── init_mysql.sql                 # MySQL initialization script
│   └── init_postgres.sql              # PostgreSQL initialization script
├── plugins/                           # Airflow plugins directory
├── logs/                              # Airflow execution logs
│   ├── dag_id=flight_price_pipeline/  # DAG-specific logs
│   ├── dag_processor_manager/
│   └── scheduler/
└── .env                               # Environment variables
│       └── get_custom_schema.sql      # Custom schema naming
├── data/
│   └── Flight_Price_Dataset_of_Bangladesh.csv
├── scripts/
│   ├── init_mysql.sql                 # MySQL initialization
│   └── init_postgres.sql              # PostgreSQL initialization
└── logs/                              # Airflow logs
```

---

## Diagram Generation Workflow

### Complete Process (3 Steps)

#### Step 1: Create Python Diagram Script
- Import required components from `diagrams` library
- Use proper icon names for Docker, Airflow, MySQL, PostgreSQL, Python
- Configure graph attributes for clean layout:
  ```python
  graph_attr = {
      "splines": "ortho",      # Orthogonal lines
      "nodesep": "1.0",        # Node spacing
      "ranksep": "1.5",        # Rank spacing
      "fontsize": "12",
      "bgcolor": "white",
      "pad": "0.5",
      "rankdir": "LR"          # Left to right flow
  }
  ```
- Use Cluster for Docker containers and logical grouping
- Set different background colors for different components
- Set output format: `outformat=["png", "dot"]`

#### Step 2: Run with GraphViz in PATH
```bash
cd docs/
python generate_architecture.py
```

#### Step 3: Convert DOT to Draw.io
This happens automatically in the script using:
```python
subprocess.run([
    "graphviz2drawio", 
    "diagrams/flight_price_architecture.dot", 
    "-o", 
    "diagrams/flight_price_architecture.drawio"
], check=True)
```

---

## Component Icons and Imports

### Required Imports
```python
from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.container import Docker
from diagrams.onprem.compute import Server
from diagrams.programming.language import Python
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.database import PostgreSQL, MySQL
from diagrams.generic.storage import Storage
from diagrams.generic.database import SQL
from diagrams.custom import Custom
```

### Component Mapping

| Component | Icon Class | Label |
|-----------|-----------|-------|
| CSV Source | `Storage` | "CSV Source\nFlight_Price_Dataset.csv\n57,000 records" |
| Airflow Webserver | `Airflow` | "Airflow Webserver\n:8080" |
| Airflow Scheduler | `Airflow` | "Airflow Scheduler" |
| MySQL Staging | `MySQL` | "MySQL Staging\n:3307" |
| PostgreSQL Analytics | `PostgreSQL` | "PostgreSQL Analytics\n:5433" |
| DBT | `Custom` or `Server` | "DBT\nTransformations" |
| Bronze Layer | `SQL` | "Bronze Layer\nvalidated_flights" |
| Silver Layer | `SQL` | "Silver Layer\ncleaned_flights\nSCD Snapshots" |
| Gold Layer | `SQL` | "Gold Layer\nKPI Tables" |

---

## Color Coding for Components

Use different background colors to distinguish component types:

```python
# Data Source Layer
source_cluster_attr = {
    "fontsize": "13",
    "bgcolor": "#E3F2FD",  # Light Blue
    "style": "rounded",
    "margin": "15",
    "label": "Data Source"
}

# Orchestration Layer
airflow_cluster_attr = {
    "fontsize": "13",
    "bgcolor": "#FFE0B2",  # Light Orange
    "style": "rounded",
    "margin": "15",
    "label": "Airflow Orchestration"
}

# Staging Layer
staging_cluster_attr = {
    "fontsize": "13",
    "bgcolor": "#FFF3E0",  # Light Peach
    "style": "rounded",
    "margin": "15",
    "label": "MySQL Staging"
}

# Analytics Layer
analytics_cluster_attr = {
    "fontsize": "13",
    "bgcolor": "#E8F5E9",  # Light Green
    "style": "rounded",
    "margin": "15",
    "label": "PostgreSQL Analytics"
}

# Medallion Architecture
bronze_cluster_attr = {
    "fontsize": "12",
    "bgcolor": "#D7CCC8",  # Bronze-ish
    "style": "rounded",
    "margin": "10",
    "label": "Bronze"
}

silver_cluster_attr = {
    "fontsize": "12",
    "bgcolor": "#CFD8DC",  # Silver-ish
    "style": "rounded",
    "margin": "10",
    "label": "Silver"
}

gold_cluster_attr = {
    "fontsize": "12",
    "bgcolor": "#FFF8E1",  # Gold-ish
    "style": "rounded",
    "margin": "10",
    "label": "Gold"
}

# Infrastructure Layer
docker_cluster_attr = {
    "fontsize": "14",
    "bgcolor": "#ECEFF1",  # Light Gray
    "style": "dashed",
    "margin": "20",
    "label": "Docker Environment"
}
```

---

## Data Flow Arrows

### Arrow Styles and Labels

```python
# Task 1: CSV to MySQL
Edge(label="Task 1\nLoad CSV", color="blue", style="solid")

# Task 2: Validation
Edge(label="Task 2\nValidate Data", color="blue", style="solid")

# Task 3: Transfer
Edge(label="Task 3\nTransfer to PostgreSQL", color="green", style="solid")

# Task 4: DBT transformations
Edge(label="Task 4\nDBT Run + Snapshot", color="purple", style="bold")

# Internal flows
Edge(label="raw_flight_data", color="gray", style="dashed")
Edge(label="validated_flight_data", color="gray", style="dashed")

# Medallion transitions
Edge(label="Bronze → Silver", color="orange", style="solid")
Edge(label="Silver → Gold", color="gold", style="solid")
```

---

## Architecture Layers

### Layer 1: Docker Infrastructure
- Outer container representing Docker environment
- Contains all services
- Ports: 8080 (Airflow), 3307 (MySQL), 5433 (PostgreSQL)

### Layer 2: Data Source
- CSV file: Flight_Price_Dataset_of_Bangladesh.csv
- 57,000 records, 18 columns

### Layer 3: Orchestration
- Airflow Webserver (UI and API)
- Airflow Scheduler (task execution)
- DAG: flight_price_pipeline

### Layer 4: Staging Database (MySQL)
- raw_flight_data table
- validated_flight_data table with is_valid flag
- Foreign key relationship between tables

### Layer 5: Analytics Database (PostgreSQL)
- Bronze schema: validated_flights
- Silver schema: cleaned_flights, SCD snapshots
- Gold schema: KPI aggregation tables
- Audit schema: pipeline_runs

### Layer 6: Transformation Engine (DBT)
- 9 models
- 22 tests
- 2 snapshots (SCD Type 2)

---

## Airflow DAG Tasks to Display

```python
# Task 1: Load CSV to MySQL
"Task 1: load_csv_to_mysql\n• Read CSV (57,000 rows)\n• Rename columns to snake_case\n• Convert data types\n• Insert to raw_flight_data\n• Duration: ~8 seconds"

# Task 2: Validate Data
"Task 2: validate_mysql_data\n• Validate required fields\n• Check positive fare values\n• Validate IATA codes (3 chars)\n• Check positive duration\n• Populate validated_flight_data\n• Duration: ~10 seconds"

# Task 3: Transfer to PostgreSQL
"Task 3: transfer_to_postgres_bronze\n• Extract valid records (is_valid=1)\n• Convert boolean types\n• Truncate bronze layer\n• Load to bronze.validated_flights\n• Duration: ~12 seconds"

# Task 4: DBT Transformations
"Task 4: run_dbt_transformations\n• dbt run (Silver & Gold models)\n• dbt test (Data quality tests)\n• dbt snapshot (SCD updates)\n• Duration: ~4 seconds"
```

---

## Medallion Architecture Details

### Bronze Layer
```python
"bronze.validated_flights\n• 57,000 rows\n• Raw validated data\n• All source columns preserved\n• is_valid, validation_errors flags\n• No transformations applied"
```

### Silver Layer
```python
"silver.silver_cleaned_flights\n• 57,000 rows (valid only)\n• Standardized text (UPPER, trim)\n• Derived columns:\n  - route (source-destination)\n  - fare_category\n  - booking_window\n  - route_type\n  - is_peak_season"

"silver.flight_fare_snapshot\n• SCD Type 2\n• ~19,052 records\n• Tracks fare changes over time\n• dbt_valid_from/to timestamps"

"silver.route_fare_snapshot\n• SCD Type 2\n• ~152 records\n• Tracks route metrics\n• dbt_valid_from/to timestamps"
```

### Gold Layer (6 KPI Tables)
```python
"gold.gold_avg_fare_by_airline (24 rows)\nAverage fare by airline\n\ngold.gold_seasonal_fare_analysis (4 rows)\nFare analysis by season\n\ngold.gold_booking_count_by_airline (24 rows)\nBooking volume by airline\n\ngold.gold_popular_routes (152 rows)\nTop routes by popularity\n\ngold.gold_fare_by_class (3 rows)\nFare statistics by class\n\ngold.gold_data_quality_report (13 rows)\nData quality metrics"
```

---

## Troubleshooting

### GraphViz Not Found
**Error**: `ExecutableNotFound: failed to execute 'dot'`

**Solution**: 
```bash
# Windows
$env:PATH += ";C:\Program Files\Graphviz\bin"

# Linux/Mac
export PATH=$PATH:/usr/local/bin
```

### Import Errors
**Error**: `cannot import name 'Airflow'`

**Solution**: Check available icons:
```python
from diagrams.onprem import workflow
print([x for x in dir(workflow) if not x.startswith('_')])
```

### Layout Issues
**Issue**: Cluttered or overlapping components

**Solutions**:
1. Adjust `nodesep` and `ranksep` in graph_attr
2. Change `rankdir` from "LR" to "TB" (top-bottom)
3. Simplify cluster nesting
4. Manually refine in draw.io after generation

---

## Output Files

Each diagram generation produces 3 files:

1. **PNG** (`flight_price_architecture.png`)
   - Static image for documentation

2. **DOT** (`flight_price_architecture.dot`)
   - GraphViz source (text format)
   - Version control friendly

3. **DRAWIO** (`flight_price_architecture.drawio`)
   - Editable in VS Code or draw.io
   - For manual refinement

**Location**: All files saved to `docs/diagrams/`

---

## Example Architecture Components

### Container Hierarchy
```
Docker Environment
├── Airflow Services
│   ├── Airflow Webserver (:8080)
│   └── Airflow Scheduler
│
├── MySQL Staging (:3307)
│   ├── raw_flight_data (57,000 rows)
│   └── validated_flight_data (57,000 rows, with validation flags)
│
├── PostgreSQL Analytics (:5433)
│   ├── bronze schema
│   │   └── validated_flights (57,000 rows)
│   ├── silver schema
│   │   ├── silver_cleaned_flights (57,000 rows)
│   │   ├── flight_fare_snapshot (SCD Type 2, ~19,052 records)
│   │   └── route_fare_snapshot (SCD Type 2, ~152 records)
│   ├── gold schema
│   │   ├── gold_avg_fare_by_airline
│   │   ├── gold_seasonal_fare_analysis
│   │   ├── gold_booking_count_by_airline
│   │   ├── gold_popular_routes
│   │   ├── gold_fare_by_class
│   │   ├── gold_data_quality_report
│   │   ├── gold_fare_history (view)
│   │   └── gold_route_history (view)
│   └── audit schema
│       └── pipeline_runs (audit trail)
│
└── DBT Transformation Engine (runs inside Airflow)
    ├── Models: 9
    ├── Tests: 22 (all passing)
    ├── Snapshots: 2 (SCD Type 2)
    └── Macros: 1 (custom schema naming)
```

### Data Flow
```
CSV File → Airflow Task 1 → MySQL (raw) → Airflow Task 2 → MySQL (validated)
    → Airflow Task 3 → PostgreSQL Bronze → Airflow Task 4 (DBT) → Silver → Gold
```

---

## Validation Checklist

Before finalizing the diagram, verify:

- [ ] CSV source file shown with record count (57,000)
- [ ] Airflow components shown (Webserver, Scheduler)
- [ ] All 4 DAG tasks represented with flow arrows
- [ ] MySQL staging with both tables (raw, validated)
- [ ] PostgreSQL with medallion architecture (Bronze/Silver/Gold)
- [ ] DBT transformation engine indicated
- [ ] SCD snapshots shown in Silver layer
- [ ] Port numbers displayed (8080, 3307, 5433)
- [ ] Docker boundary visible
- [ ] Color coding consistent across layers
- [ ] All text readable at 100% zoom
- [ ] PNG, DOT, and DRAWIO files generated

---

## Performance Metrics to Display

The diagram should illustrate:

- **Total Pipeline Duration**: ~34 seconds
- **Source Records**: 57,000
- **Valid Records**: 57,000 (100%)
- **Airlines**: 24
- **Unique Routes**: 152
- **DBT Models**: 9
- **DBT Tests**: 22 (all passing)
- **SCD Snapshots**: 2

---

## Next Steps After Generation

1. **Review PNG**: Check overall layout and readability
2. **Inspect DOT**: Verify all connections are correct
3. **Refine in Draw.io**: 
   - Adjust spacing
   - Add detailed annotations
   - Align components
   - Add legend if needed
4. **Export final PNG**: High resolution (300 DPI) for documentation
5. **Commit to Git**: Include all three formats