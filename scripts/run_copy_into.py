import snowflake.connector
from sf_env import snowflake_connect_kwargs

def main():
    print("Connecting to Snowflake to execute COPY INTO...")
    conn = snowflake.connector.connect(
        **snowflake_connect_kwargs(),
        schema='BRONZE',
    )
    cur = conn.cursor()

    print("Executing COPY INTO with ON_ERROR='CONTINUE' and ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE...")
    cur.execute("""
        COPY INTO LABEVENTS_RAW
        FROM @AKI_DB.BRONZE.AKI_RAW_DATA/labevents.csv.gz
        FILE_FORMAT = (
            TYPE = CSV 
            SKIP_HEADER = 1 
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        )
        ON_ERROR = 'CONTINUE'
    """)
    
    # Fetch result to see how many rows loaded
    result = cur.fetchall()
    print("Result of COPY INTO:")
    for row in result:
        print(row)

    print("✅ COPY INTO Complete!")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
