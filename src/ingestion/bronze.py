from pyspark.sql.functions import col, lit, current_timestamp


def ingest_month_to_bronze(
    spark,
    raw_base_path,
    source_month,
    bronze_table
):
    """
    Ingest one monthly Parquet dataset into the Bronze Delta table.

    The function:
    1. Builds the raw data path for the requested month.
    2. Checks whether that month is already in Bronze.
    3. Reads the monthly Parquet file.
    4. Validates the source schema.
    5. Adds Bronze metadata.
    6. Appends the data to the Bronze Delta table.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    raw_base_path : str
        Base path of the Unity Catalog Volume.

    source_month : str
        Month to process, for example '2026-02'.

    bronze_table : str
        Target Bronze Delta table.

    Returns
    -------
    int
        Number of rows ingested.
    """

    # ---------------------------------------------------------
    # 1. Build the raw path
    # ---------------------------------------------------------

    raw_path = f"{raw_base_path}/raw/{source_month}/"

    print(f"Processing month: {source_month}")
    print(f"Raw path: {raw_path}")

    # ---------------------------------------------------------
    # 2. Check whether Bronze table already exists
    # ---------------------------------------------------------

    table_exists = spark.catalog.tableExists(bronze_table)

    # ---------------------------------------------------------
    # 3. Prevent duplicate monthly ingestion
    # ---------------------------------------------------------

    if table_exists:

        existing_rows = (
            spark.table(bronze_table)
            .filter(
                col("_source_month") == source_month
            )
            .limit(1)
            .count()
        )

        if existing_rows > 0:

            print(
                f"{source_month} already exists in Bronze."
            )

            print(
                "Skipping ingestion to prevent duplicates."
            )

            return 0

    # ---------------------------------------------------------
    # 4. Read the monthly Parquet file
    # ---------------------------------------------------------

    df_raw = spark.read.parquet(raw_path)

    print(
        f"Source columns: {len(df_raw.columns)}"
    )

    # ---------------------------------------------------------
    # 5. Validate schema if Bronze already exists
    # ---------------------------------------------------------

    if table_exists:

        bronze_columns = [
            column_name
            for column_name in spark.table(bronze_table).columns
            if column_name not in [
                "_source_month",
                "_source_file",
                "_ingested_at"
            ]
        ]

        source_columns = df_raw.columns

        if set(source_columns) != set(bronze_columns):

            missing_columns = (
                set(bronze_columns)
                - set(source_columns)
            )

            new_columns = (
                set(source_columns)
                - set(bronze_columns)
            )

            raise ValueError(
                "Source schema does not match Bronze schema.\n"
                f"Missing columns: {missing_columns}\n"
                f"New columns: {new_columns}"
            )

        print("Schema validation: PASSED")

    # ---------------------------------------------------------
    # 6. Add Bronze metadata
    # ---------------------------------------------------------

    df_bronze = (
        df_raw
        .withColumn(
            "_source_month",
            lit(source_month)
        )
        .withColumn(
            "_source_file",
            col("_metadata.file_path")
        )
        .withColumn(
            "_ingested_at",
            current_timestamp()
        )
    )

    # ---------------------------------------------------------
    # 7. Count rows before writing
    # ---------------------------------------------------------

    row_count = df_bronze.count()

    print(
        f"Rows to ingest: {row_count:,}"
    )

    # ---------------------------------------------------------
    # 8. Write to Bronze
    # ---------------------------------------------------------

    if table_exists:

        (
            df_bronze
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(bronze_table)
        )

    else:

        (
            df_bronze
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy("_source_month")
            .saveAsTable(bronze_table)
        )

    print(
        f"{source_month} successfully ingested "
        f"into Bronze."
    )

    return row_count