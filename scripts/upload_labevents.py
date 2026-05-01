import snowflake.connector
from pathlib import Path
from sf_env import snowflake_connect_kwargs

def main():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        **snowflake_connect_kwargs(),
        schema='BRONZE',
        client_session_keep_alive=True
    )
    cur = conn.cursor()

    print("1. Creating LABEVENTS_RAW table...")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS LABEVENTS_RAW (
        labevent_id STRING,
        subject_id STRING,
        hadm_id STRING,
        specimen_id STRING,
        itemid STRING,
        order_provider_id STRING,
        charttime STRING,
        storetime STRING,
        value STRING,
        valuenum STRING,
        valueuom STRING,
        ref_range_lower STRING,
        ref_range_upper STRING,
        flag STRING,
        priority STRING,
        comments STRING
    )
    """)

    print("2. Chunking, Compressing, and Uploading 17GB file to Snowflake Stage...")
    print("(This may take 10-15 minutes. The Python connector splits it into parallel 50MB streams automatically!)")
    
    cur.execute("CREATE STAGE IF NOT EXISTS AKI_RAW_DATA")

    # Note: We must escape the space in 'Hospital Data' for the PUT command
    data_path = (Path(__file__).resolve().parents[1] / "DATA" / "Hospital Data" / "labevents.csv").as_posix()
    cur.execute(f"""
        PUT 'file://{data_path}'
        @AKI_DB.BRONZE.AKI_RAW_DATA
        AUTO_COMPRESS=TRUE
        PARALLEL=8
    """)

    print("3. Copying data from Stage into LABEVENTS_RAW table...")
    cur.execute("""
        COPY INTO LABEVENTS_RAW
        FROM @AKI_DB.BRONZE.AKI_RAW_DATA/labevents.csv.gz
        FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    """)

    print("✅ 17GB Upload Complete!")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
