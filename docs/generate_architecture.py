#!/usr/bin/env python3
"""
Flight Price Analysis Pipeline Architecture Diagram Generator

Generates complete architecture diagrams following the specifications in:
- docs/instructions.md
- docs/agent.md

Outputs DOT format to docs/diagrams/ which can be converted to PNG using GraphViz
"""

import subprocess
import sys
from pathlib import Path


def generate_dot_file():
    """Generate the architecture diagram as a DOT file."""
    
    dot_content = '''digraph "Flight Price Analysis Pipeline Architecture" {
    graph [
        rankdir=LR,
        bgcolor=white,
        fontname=Helvetica,
        fontsize=12,
        label="Flight Price Analysis Pipeline Architecture",
        margin="0.5,0.5",
        nodesep=1.0,
        ranksep=1.5,
        splines=ortho,
        compound=true
    ]
    
    node [fontname=Helvetica, fontsize=11, shape=box, style=rounded, fillcolor=white, color=black]
    edge [fontname=Helvetica, fontsize=10, color="#7B8894"]
    
    // Docker Environment Cluster
    subgraph cluster_docker {
        label="Docker Environment"
        style=dashed
        bgcolor="#ECEFF1"
        penwidth=2
        margin=20
        
        // Data Source Layer
        subgraph cluster_datasource {
            label="Data Source Layer"
            style=rounded
            bgcolor="#E3F2FD"
            margin=15
            
            CSV [label="CSV Source\\nFlight_Price_Dataset.csv\\n\\n57,000 records\\n18 columns", shape=folder]
        }
        
        // Airflow Orchestration Layer
        subgraph cluster_airflow {
            label="Airflow Orchestration Layer"
            style=rounded
            bgcolor="#FFE0B2"
            margin=15
            
            WebServer [label="Airflow Webserver\\n:8080"]
            Scheduler [label="Airflow Scheduler"]
            
            Task1 [label="Task 1: Load CSV\\n~8 seconds\\n57,000 rows", shape=box]
            Task2 [label="Task 2: Validate Data\\n~10 seconds\\n100% pass rate", shape=box]
            Task3 [label="Task 3: Transfer to PostgreSQL\\n~12 seconds\\n57,000 rows", shape=box]
            Task4 [label="Task 4: DBT Run + Snapshot\\n~4 seconds\\n9 models", shape=box]
            
            Task1 -> Task2 [label="Task dependency", color="#1976D2", style=solid, penwidth=2]
            Task2 -> Task3 [label="Task dependency", color="#1976D2", style=solid, penwidth=2]
            Task3 -> Task4 [label="Task dependency", color="#1976D2", style=solid, penwidth=2]
            
            Scheduler -> WebServer [label="Task Status", style=dashed, color=gray]
        }
        
        // MySQL Staging Database
        subgraph cluster_mysql {
            label="MySQL Staging :3307"
            style=rounded
            bgcolor="#E3F2FD"
            margin=15
            
            RawTable [label="raw_flight_data\\n\\n57,000 rows\\nRaw unvalidated data", shape=cylinder]
            ValTable [label="validated_flight_data\\n\\n57,000 rows\\nis_valid flag", shape=cylinder]
            
            RawTable -> ValTable [label="Task 2\\nValidate", color="#1976D2", style=solid, penwidth=2]
        }
        
        // PostgreSQL Analytics Database
        subgraph cluster_postgres {
            label="PostgreSQL Analytics :5433"
            style=rounded
            bgcolor="#E8F5E9"
            margin=15
            
            // Bronze Layer
            subgraph cluster_bronze {
                label="Bronze Layer"
                style=rounded
                bgcolor="#D7CCC8"
                margin=10
                
                Bronze [label="validated_flights\\n\\n57,000 rows\\nRaw validated data\\nNo transformations", shape=cylinder]
            }
            
            // Silver Layer
            subgraph cluster_silver {
                label="Silver Layer"
                style=rounded
                bgcolor="#CFD8DC"
                margin=10
                
                Silver [label="silver_cleaned_flights\\n\\n57,000 rows\\nStandardized, derived columns", shape=cylinder]
                Snap1 [label="flight_fare_snapshot\\n\\n19,052 rows\\nSCD Type 2", shape=cylinder]
                Snap2 [label="route_fare_snapshot\\n\\n152 rows\\nSCD Type 2", shape=cylinder]
                
                Silver -> Snap1 [label="SCD Tracking", color="#7B1FA2", style=dashed]
                Silver -> Snap2 [label="SCD Tracking", color="#7B1FA2", style=dashed]
            }
            
            // Gold Layer
            subgraph cluster_gold {
                label="Gold Layer (KPI Tables)"
                style=rounded
                bgcolor="#FFF8E1"
                margin=10
                
                Gold1 [label="gold_avg_fare_by_airline\\n24 rows", shape=cylinder]
                Gold2 [label="gold_seasonal_fare_analysis\\n4 rows", shape=cylinder]
                Gold3 [label="gold_booking_count_by_airline\\n24 rows", shape=cylinder]
                Gold4 [label="gold_popular_routes\\n152 rows", shape=cylinder]
                Gold5 [label="gold_fare_by_class\\n3 rows", shape=cylinder]
                Gold6 [label="gold_data_quality_report\\n13 rows", shape=cylinder]
            }
            
            // Medallion Architecture Flow
            Bronze -> Silver [label="DBT Transform\\nClean & Standardize", color="#F57C00", style=solid, penwidth=2]
            Silver -> Gold1 [label="DBT Aggregate\\nKPI Creation", color="#FFC107", style=solid, penwidth=2]
            Silver -> Gold2 [color="#FFC107", style=solid]
            Silver -> Gold3 [color="#FFC107", style=solid]
            Silver -> Gold4 [color="#FFC107", style=solid]
            Silver -> Gold5 [color="#FFC107", style=solid]
            Silver -> Gold6 [color="#FFC107", style=solid]
        }
        
        // Data Flow Connections
        CSV -> Task1 [label="Task 1: Read CSV\\nwith Pandas", color="#1976D2", style=solid, penwidth=2]
        Task1 -> RawTable [label="Insert\\n57,000 rows", color="#1976D2", style=solid, penwidth=2]
        ValTable -> Task3 [label="Task 3: Extract\\nvalid records only", color="#388E3C", style=solid, penwidth=2]
        Task3 -> Bronze [label="Transfer via JDBC\\nBoolean conversion", color="#388E3C", style=solid, penwidth=2]
        Task4 -> Bronze [label="Task 4: DBT Source", color="#F57C00", style=solid]
        Task4 -> Silver [label="Task 4: DBT Run", color="#F57C00", style=solid]
    }
}'''
    
    return dot_content


def convert_dot_to_png(dot_file, png_file):
    """Convert DOT file to PNG using GraphViz."""
    try:
        subprocess.run(
            ["dot", "-Tpng", "-o", str(png_file), str(dot_file)],
            check=True,
            capture_output=True,
            timeout=30
        )
        return True
    except Exception as e:
        print(f"Note: PNG conversion failed ({e}), but DOT file is available")
        return False


def main():
    """Generate the architecture diagram."""
    
    # Setup paths
    script_dir = Path(__file__).parent
    diagrams_dir = script_dir / "diagrams"
    diagrams_dir.mkdir(exist_ok=True)
    
    print("=" * 80)
    print("Flight Price Analysis Pipeline - Architecture Diagram Generator")
    print("=" * 80)
    print()
    
    try:
        # Generate DOT content
        print("1. Generating DOT diagram...")
        dot_content = generate_dot_file()
        
        # Write DOT file
        dot_file = diagrams_dir / "flight_price_architecture.dot"
        dot_file.write_text(dot_content)
        print(f"✓ Created: flight_price_architecture.dot ({len(dot_content):,} bytes)")
        
        # Try to convert to PNG
        print()
        print("2. Attempting PNG conversion...")
        png_file = diagrams_dir / "flight_price_architecture.png"
        
        if convert_dot_to_png(dot_file, png_file):
            print(f"✓ Created: flight_price_architecture.png ({png_file.stat().st_size:,} bytes)")
            success = True
        else:
            print(f"✗ PNG conversion failed")
            success = False
        
        print()
        print("=" * 80)
        print("DIAGRAM GENERATION COMPLETE")
        print("=" * 80)
        print()
        print(f"Location: {diagrams_dir}")
        print()
        print("Files generated:")
        print(f"  ✓ flight_price_architecture.dot - GraphViz source format")
        
        if png_file.exists():
            print(f"  ✓ flight_price_architecture.png - PNG image")
        else:
            print()
            print("To manually generate PNG from DOT file:")
            print("  dot -Tpng flight_price_architecture.dot -o flight_price_architecture.png")
            print()
        
        return success
    
    except Exception as e:
        print(f"✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
