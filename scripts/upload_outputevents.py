import snowflake.connector
from pathlib import Path
from sf_env import snowflake_connect_kwargs

STAGE_FILE = 'outputevents.csv.gz'
TABLE_NAME = 'OUTPUTEVENTS_RAW'

def main():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        **snowflake_connect_kwargs(),
        schema='BRONZE',
        client_session_keep_alive=True
    )
    cur = conn.cursor()

    print(f"1. Creating {TABLE_NAME} table...")
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        subject_id   STRING,
        hadm_id      STRING,
        stay_id      STRING,
        caregiver_id STRING,
        charttime    STRING,
        storetime    STRING,
        itemid       STRING,
        value        STRING,
        valueuom     STRING
    )
    """)

    print("2. Creating Stage (if needed)...")
    cur.execute("CREATE STAGE IF NOT EXISTS AKI_RAW_DATA")

    file_path = (Path(__file__).resolve().parents[1] / "DATA" / "ICU DATA" / "outputevents.csv").as_posix()
    print(f"3. Uploading file://{file_path} to Snowflake Stage (parallel=8)...")
    cur.execute(f"""
        PUT 'file://{file_path}'
        @AKI_DB.BRONZE.AKI_RAW_DATA
        AUTO_COMPRESS=TRUE
        PARALLEL=8
        OVERWRITE=TRUE
    """)
    print("✅ Upload to Stage complete!")

    print(f"4. Running COPY INTO {TABLE_NAME}...")
    cur.execute(f"""
        COPY INTO {TABLE_NAME}
        FROM @AKI_DB.BRONZE.AKI_RAW_DATA/{STAGE_FILE}
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        )
        ON_ERROR = 'CONTINUE'
    """)

    result = cur.fetchall()
    print(f"✅ COPY INTO {TABLE_NAME} Complete!")
    for row in result:
        print(row)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
