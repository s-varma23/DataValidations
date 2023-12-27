from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
from functools import reduce

def load_data(spark, path):
    data = spark.read.csv(path, header=True, inferSchema=True)
    return data

def is_potential_key_col(data, column):
    distinct_cnt = data.select(col(column).distinct().count())
    total_cnt = data.count()
    return distinct_cnt == total_cnt

def get_diff_cols(uat_data, prod_data):
    columns_not_common = set(uat_data.columns) ^ set(prod_data.columns)

    if len(columns_not_common) > 0:
        print("Additional column noticed in one of the dataframes:", columns_not_common)
        print("Proceeding the validation by excluding these extra columns")

    all_cols = set(uat_data.columns) & set(prod_data.columns)
    potential_key_columns = [column for column in all_cols if is_potential_key_col(uat_data, column)]

    if potential_key_columns:
        key_columns = potential_key_columns[0]
        other_columns = [ocol for ocol in all_cols if ocol != key_columns]

        print("Please find the key column identified for the dataframe:", key_columns)
        print("Please find the columns getting checked for discrepancies:", other_columns)

        conditions = [(col("bigplay." + c) != col("daas." + c)) for c in other_columns]
        column_names = [c for c in other_columns if uat_data.select(col(c)).exceptAll(prod_data.select(col(c))).count() > 0]

        if conditions and column_names:
            combined_conditions = reduce(lambda x, y: x | y, conditions, conditions[0])
            col_effect_expr = concat_ws(",", *[lit(c) for c in column_names])
            
            diff = uat_data.alias('bigplay').join(prod_data.alias('daas'), col("bigplay." + key_columns) == col("daas." + key_columns)) \
                .select(col("bigplay." + key_columns).alias(key_columns), col_effect_expr.alias("Column"),
                        *[col("bigplay." + c).alias("bigplay_" + c) for c in column_names] +
                        *[col("daas." + c).alias("daas_" + c) for c in column_names])

            return diff, key_columns
        else:
            print("No differences found in the dataframes")
            return None, key_columns
    else:
        print("No Potential Key columns found, check for duplicates in your dataframe")
        return None, None

def missing_records(bp, da, diff, key_column, spark):
    print("Running the missing records function")
    all_cols = bp.columns

    missing_records_bp = da.select(*all_cols).subtract(bp.select(*all_cols))
    missing_records_da = bp.select(*all_cols).subtract(da.select(*all_cols))

    missing_records_bp.createOrReplaceTempView("missing_records_bp")
    missing_records_da.createOrReplaceTempView("missing_records_da")

    q = """
    SELECT 'daas' as source, * FROM missing_records_bp
    UNION
    SELECT 'bigplay' as source, * FROM missing_records_da
    """

    missing_records = spark.sql(q)

    if diff is not None:
        diff_missed = diff.select(key_column).distinct()
        diff_missed.createOrReplaceTempView("diff_missed")
        missing_records.createOrReplaceTempView("missing_records")

        missed_records = spark.sql(
            f"SELECT DISTINCT m.* FROM missing_records m WHERE m.{key_column} NOT IN (SELECT {key_column} FROM diff_missed)"
        )

        if missed_records.count() > 0:
            print("Completed running the missing records function")
            return missed_records
    else:
        if missing_records.count() > 0:
            print("Completed running the missing records function")
            return missing_records

    print("Completed running missed records function, no missing records found")
    return None

def save_file(differences, username, type):
    print("Saving the file with the below detail:")
    print(username, type)
    output_file = f"file:///home/{username}/DataValidation_differences_{type}.csv"
    differences.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_file)

def main():
    parser = argparse.ArgumentParser(description="Compare data between Bigplay and Daas using pyspark")
    parser.add_argument("--bigplay_file", required=True, help="Path to the bigplay data file")
    parser.add_argument("--daas_file", required=True, help="Path to the Daas data file")
    parser.add_argument("--username", required=True, help="Please enter hadoop username")

    args = parser.parse_args()

    spark = SparkSession.builder.master('local').appName("DataValidation").config("spark.driver.memory", "10G") \
        .config("spark.dynamicAllocation.enabled", "true").config("spark.dynamicAllocation.initialExecutors", "8") \
        .config("spark.executor.memoryOverhead", "2g").config("spark.driver.memoryOverhead", "4gb") \
        .config("spark.kryoserializer.buffer.max", "1000").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    uat_data = load_data(spark, args.bigplay_file)
    prod_data = load_data(spark, args.daas_file)

    print("Checking for column differences…")
    differences, key_column = get_diff_cols(uat_data, prod_data)

    print("Checking for missing records…")
    missing_rec = missing_records(uat_data, prod_data, differences, key_column, spark)

    if differences is not None:
        print("Differences found:")
        differences.show(truncate=False)
        save_file(differences, args.username, "all_records")
        print("The file with the differences was generated successfully")
    else:
        print("No column discrepancies found in the two files")

    if missing_rec is not None:
        print("Missing records found:")
        save_file(missing_rec, args.username, "missing_records")
        print("The file with the missing records was generated successfully")
    else:
        print("No missing records found in the two files")

    spark.stop()

if __name__ == "__main__":
    main()
