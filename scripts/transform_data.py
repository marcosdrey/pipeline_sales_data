import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t


load_dotenv()

def create_connect_spark_session(uri):
    return SparkSession.builder \
        .appName("Sales Data Spark") \
        .config("spark.mongodb.read.connection.uri", uri) \
        .config("spark.mongodb.write.connection.uri", uri) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.3") \
        .getOrCreate()

def create_spark_dataframe(spark, db_name, collection_name):
    return (
        spark.read \
            .format("mongodb") \
            .option("database", db_name) \
            .option("collection", collection_name) \
            .load()
    )

def convert_dataframe_types(df):
    return df.withColumns({
        'YEAR_ID': df.YEAR_ID.cast(t.IntegerType()),
        'SALES': df.SALES.cast(t.DoubleType()),
        'QUANTITYORDERED': df.QUANTITYORDERED.cast(t.IntegerType()),
        'QTR_ID': df.QTR_ID.cast(t.IntegerType()),
        'PRICEEACH': df.PRICEEACH.cast(t.DoubleType()),
        'POSTALCODE': df.POSTALCODE.cast(t.IntegerType()),
        'ORDERNUMBER': df.ORDERNUMBER.cast(t.IntegerType()),
        'ORDERLINENUMBER': df.ORDERLINENUMBER.cast(t.IntegerType()),
        'ORDERDATE': f.to_date(df.ORDERDATE, 'M/d/yyyy H:m'),
        'MSRP': df.MSRP.cast(t.IntegerType()),
        'MONTH_ID': df.MONTH_ID.cast(t.IntegerType()),
    })

def convert_empty_strings_to_null(df):
    return df.na.replace("", None)

def main():
    uri = os.getenv('MONGO_ATLAS_URL')
    spark = create_connect_spark_session(uri)

    db_name = "db_sales"
    collection_name = "sales"

    df = create_spark_dataframe(spark, db_name, collection_name)
    print(f"O dataframe está carregado com {df.count()} linhas.")

    print("Transformando tipos dos dados...")
    df = convert_dataframe_types(df)
    print(f"Transformação de tipos concluída. Confira os tipos: ")
    df.printSchema()

    df = convert_empty_strings_to_null(df)
    print("Transformação de strings vazias para valores nulos concluída.")

    print("Exportando dataframe final para arquivos PARQUET...")
    df.write.parquet("data/parquet", mode="overwrite")

    print("Exportação concluída com sucesso.")
    spark.stop()

if __name__ == '__main__':
    main()
