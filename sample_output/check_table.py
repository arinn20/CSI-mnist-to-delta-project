from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckTable").getOrCreate()

df = spark.table("mnist_images_delta")
df.show(10)

spark.stop()