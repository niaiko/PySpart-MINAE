from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
import os


def DBConnectSpark():
    sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.3.0 pyspark-shell'

    spark = SparkSession \
        .builder \
        .appName("Gestion des producteurs") \
        .master("local") \
        .config("spark.driver.extraClassPath", sparkClassPath) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def ReadTableWIthSPark(nomTable):
    spark = DBConnectSpark()
    table = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/base_test") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", nomTable) \
    .option("user", "postgres") \
    .option("password", "sitraka") \
    .load()
    return table
