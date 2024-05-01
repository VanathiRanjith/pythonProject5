import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, DateType, DecimalType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if __name__ == '__main__':
    os.environ['PYSPARK_PYTHON'] = sys.executable
    spark = SparkSession.builder.appName("p2").getOrCreate()

    patients_schema="subject_id int, gender string, anchor_age int, anchor_year string, anchor_year_group string, dod string"
    
    patients_df = spark.read.option("header",True).schema(patients_schema).csv(sys.argv[1])
    patients_df_new=patients_df.select("anchor_year").show(5)
    patients_df_new.write.mode("overwrite").option("header", True).csv(sys.argv[2])
