from pyspark.sql.functions import col

def drop_duplicate_events(df, watermark_hours: int = 6):
    if "event_time" in df.columns and "event_id" in df.columns:
        return df.withWatermark("event_time", f"{watermark_hours} hours").dropDuplicates(["event_id"])
    return df
