from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------- Start Spark ----------
spark = (
    SparkSession.builder
        .appName("JoinDemo")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
        .getOrCreate()
)


# ---------- 1. Read both datasets ----------
employee = spark.read.csv(
    "s3a://s3-data-bronze/anto/employee.csv",
    header=True,
    inferSchema=True
)

performance = spark.read.csv(
    "s3a://s3-data-bronze/anto/performance.csv",
    header=True,
    inferSchema=True
)

print("Employee rows:", employee.count())        # 25
print("Performance rows:", performance.count())  # 25


# ---------- 2. Clean both ----------
employee = employee.dropDuplicates().dropna()
employee = employee.withColumn(
    "name",
    F.initcap(F.trim("name"))
)

performance = performance.dropDuplicates().dropna()
performance = performance.filter(F.col("rating").between(0, 5))  # rating must be 0 to 5


# ---------- 3. Joins (all use the "id" column) ----------

# INNER JOIN: only employees that have a performance record
inner_df = employee.join(performance, on="id", how="inner")
print("INNER JOIN rows:", inner_df.count())  # 22
inner_df.show()


# LEFT JOIN: all employees, performance is empty (null) if missing
left_df = employee.join(performance, on="id", how="left")
print("LEFT JOIN rows:", left_df.count())  # 25
left_df.show(25)


# RIGHT JOIN: all performance records, employee is empty if missing
right_df = employee.join(performance, on="id", how="right")
print("RIGHT JOIN rows:", right_df.count())  # 25
right_df.show(25)


# FULL OUTER JOIN: everything from both sides
full_df = employee.join(performance, on="id", how="full")
print("FULL JOIN rows:", full_df.count())  # 28
full_df.show(28)


# LEFT ANTI JOIN: employees with NO performance record
no_review_df = employee.join(performance, on="id", how="left_anti")
print("Employees without performance record:")
no_review_df.show()  # ids 5, 13, 20


# ---------- 4. Aggregate on the joined data ----------
department_report = inner_df.groupBy("department").agg(
    F.count("id").alias("employees"),
    F.round(F.avg("rating"), 2).alias("avg_rating"),
    F.round(F.avg("salary"), 2).alias("avg_salary"),
    F.max("years_experience").alias("max_experience"),
)

department_report.show()


project_report = inner_df.groupBy("project").agg(
    F.count("id").alias("team_size"),
    F.sum("salary").alias("total_salary"),
).orderBy(F.col("team_size").desc())

project_report.show()


# ---------- 5. Write results ----------
inner_df.write.mode("overwrite").parquet(
    "s3a://s3-data-gold/EmployeePerformance/"
)

department_report.write.mode("overwrite").parquet(
    "s3a://s3-data-gold/DepartmentReport/"
)

no_review_df.write.mode("overwrite").parquet(
    "s3a://s3-data-gold/EmployeeWithoutPerformance/"
)

print("Done!")


spark.stop()