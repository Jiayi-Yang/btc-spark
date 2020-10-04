from .spark_config import sparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import DateType

def load_csv(path):
    return sparkSession.read.format("csv").option("header",True).option("inferSchema",True).load(path)

def agg_block(df):
    df.agg(func.sum("blockCount")).show()

def add_dt(df):
    df = df.withColumn("date", df['date'].cast(DateType()))
    df = df.withColumn("year", func.year(df['date']))\
           .withColumn("month", func.month(df['date']))
    return df

def agg_month(df):
    df = df\
        .groupby(df['year'],df['month'])\
        .agg(func.sum(df['blockSize']))\
        .sort(df['year'],df['month'])\
        .filter((df['year'] == '2018') | (df['year'] == '2019'))
    return df

df = load_csv("data/btc.csv")
df = add_dt(df)
print(df.schema)
print(f"total counts {df.count()}")
df = agg_month(df)
df.show()