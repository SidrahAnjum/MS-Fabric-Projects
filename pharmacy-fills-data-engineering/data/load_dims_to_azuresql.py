"""
load_dims_to_azuresql.py

Loads the four SQL-sourced dimension CSVs (patients, prescribers,
pharmacies, plans) into the Azure SQL Database that simulates the
"operational database" source for this project. Run this once after
creating the tables (see the DDL in the guide), and re-run any time
you regenerate the dataset.

Usage:
    pip install pyodbc sqlalchemy pandas

    python load_dims_to_azuresql.py \
        --server pharmacy-demo-server.database.windows.net \
        --database pharmacy_opsdb \
        --username sqladmin \
        --password "<your-password>" \
        --data-dir ./pharmacy_fills_dataset
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

# entity file -> target table, matching dim_pipeline_config.csv source_path_or_table
TABLE_MAP = {
    "dim_patients.csv": "patients",
    "dim_prescribers.csv": "prescribers",
    "dim_pharmacies.csv": "pharmacies",
    "dim_plans.csv": "plans",
}


def build_engine(server, database, username, password, driver="ODBC Driver 18 for SQL Server"):
    conn_str = (
        f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}"
        f"?driver={driver.replace(' ', '+')}&Encrypt=yes&TrustServerCertificate=no"
    )
    return create_engine(conn_str, fast_executemany=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", required=True, help="e.g. pharmacy-demo-server.database.windows.net")
    parser.add_argument("--database", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--data-dir", default=".", help="Folder containing the dim_*.csv files")
    args = parser.parse_args()

    engine = build_engine(args.server, args.database, args.username, args.password)
    data_dir = Path(args.data_dir)

    for csv_name, table_name in TABLE_MAP.items():
        csv_path = data_dir / csv_name
        if not csv_path.exists():
            print(f"Skipping {csv_name} — not found in {data_dir}", file=sys.stderr)
            continue

        df = pd.read_csv(csv_path)

        with engine.begin() as conn:
            # Table already exists (created via the DDL in the guide) — append only,
            # so re-running this script doesn't fight the schema/primary keys.
            conn.execute(f"DELETE FROM dbo.{table_name}")
            df.to_sql(table_name, conn, schema="dbo", if_exists="append", index=False)

        print(f"Loaded {len(df)} rows into dbo.{table_name}")

    print("Done.")


if __name__ == "__main__":
    main()
