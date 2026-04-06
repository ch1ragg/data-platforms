from pyspark.sql import SparkSession

# Create Spark session with S3/MinIO configuration
spark = SparkSession.builder \
    .appName("TripAnalysis") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read CSV from MinIO
print("Reading trips.csv from MinIO...")
df = spark.read.csv("s3a://raw-data/trips.csv", header=True, inferSchema=True)

# Show what we read
print("Raw data:")
df.show()

# Transform: average fare and distance per city
print("Calculating average fare and distance per city...")
result = df.groupBy("city").agg(
    {"fare": "avg", "distance_km": "avg"}
)
result = result.withColumnRenamed("avg(fare)", "avg_fare") \
               .withColumnRenamed("avg(distance_km)", "avg_distance_km")

print("Results:")
result.show()

# Write results back to MinIO
print("Writing results to MinIO...")
result.write.mode("overwrite").csv("s3a://raw-data/trip-results", header=True)

print("Done!")
spark.stop()