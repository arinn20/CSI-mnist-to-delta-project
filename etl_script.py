import os
import random
from glob import glob
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Create Spark session with Delta support
builder = SparkSession.builder.appName("MNIST_Delta_ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Path to MNIST dataset (change if needed)
mnist_dir = "flat files/mnist_png/training/"

all_data = []

print("Reading images & sampling...")
for digit in range(10):
    folder = os.path.join(mnist_dir, str(digit))
    files = glob(os.path.join(folder, "*.png"))
    sampled_files = random.sample(files, 5)
    for file in sampled_files:
        with open(file, "rb") as f:
            img_bytes = f.read()
        all_data.append((digit, os.path.basename(file), img_bytes))
print(f"Total sampled files: {len(all_data)}")

# Convert to DataFrame
df = pd.DataFrame(all_data, columns=["digit", "filename", "image_bytes"])
spark_df = spark.createDataFrame(df)

# Save as Managed Delta Table (without compression)
print("Writing data to managed Delta table...")
spark_df.write.format("delta") \
    .mode("overwrite") \
    .option("compression", "none") \
    .saveAsTable("mnist_images_delta")

print("Delta table 'mnist_images_delta' created.")
spark.stop()