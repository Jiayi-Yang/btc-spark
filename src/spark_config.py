from pyspark.sql import SparkSession
sparkSession = SparkSession.builder.appName("frugalops").master("local[*]").getOrCreate()