#!/usr/bin/env python3
import duckdb
import os

SOURCE = "/tmp/hive-data/artist_name_map_v1/*"
DEST   = "/tmp/hive-data/warehouse_discogs/artist_name_map_v1"

def main():
    con = duckdb.connect()

    print("Loading source artist_name_map_v1...")
    df = con.execute(f"SELECT * FROM '{SOURCE}'").df()
    print(f"Rows loaded: {len(df):,}")

    if os.path.exists(DEST):
        os.system(f"rm -rf {DEST}")
    os.makedirs(DEST, exist_ok=True)

    con.register("src", df)

    print("Writing Parquet to warehouse...")
    con.execute(f"""
        COPY src TO '{DEST}'
        (FORMAT PARQUET, COMPRESSION 'snappy');
    """)

    print("Done. Warehouse copy is clean and readable.")

if __name__ == "__main__":
    main()
