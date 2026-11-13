from pyspark.sql import SparkSession
from pyspark.sql import functions as anto


# ---------- Start Spark ----------
spark = (
    SparkSession.builder
        .appName("ReadWriteS3")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
        .getOrCreate()
)


# ---------- 1. Read ----------
df = spark.read.csv("s3a://s3-data-bronze/anto/employee.csv", header=True, inferSchema=True)
print("Raw data:", df.count(), "rows")
df.show()


# ---------- 2. Clean ----------
df = df.dropDuplicates()                              # remove duplicate rows
df = df.dropna()                                      # remove rows with missing values
df = df.withColumn("name", anto.initcap(anto.trim("name")))              # John, Alice ...
df = df.withColumn("city", anto.initcap(anto.trim("city")))              # Chennai, Pune ...
df = df.withColumn("department", anto.trim("department"))


# ---------- 3. Validate ----------
df = df.filter(anto.col("salary") > 0)                # salary must be positive
print("Clean data:", df.count(), "rows")
print("Duplicate ids:", df.count() - df.select("id").distinct().count())


# ---------- 4. Transform ----------
df = df.withColumn("bonus", anto.col("salary") * 0.10)

df = df.withColumn(
    "salary_level",
    anto.when(anto.col("salary") < 50000, "Low")
        .when(anto.col("salary") < 70000, "Medium")
        .otherwise("High")
)

df.show()


# ---------- 5. Aggregate ----------
department_summary = df.groupBy("department").agg(
    anto.count("id").alias("employees"),
    anto.sum("salary").alias("total_salary"),
    anto.round(anto.avg("salary"), 2).alias("avg_salary"),
    anto.max("salary").alias("max_salary")
)

department_summary.show()


# ---------- 6. Write ----------
df.write.mode("overwrite").parquet("s3a://s3-data-silver/anto/")

department_summary.write.mode("overwrite").parquet(
    "s3a://s3-data-gold/DepartmentSummary/"
)

print("Done!")


spark.stop()