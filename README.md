# End-to-End Data Engineering Pipeline on AWS

## Project Overview

This project demonstrates an **end-to-end Data Engineering pipeline** built using **AWS EC2, Ubuntu Linux, PySpark, Apache Spark, and Amazon S3**.

The pipeline is executed on an **AWS EC2 instance running Ubuntu Linux**. It reads raw employee and performance datasets from an **Amazon S3 Bronze layer**, performs data cleaning, validation, transformation, joins, and aggregations using PySpark, and writes the processed data back to Amazon S3 in **Silver and Gold layers** using **Parquet format**.

# Architecture

```text
                    ┌──────────────────────┐
                    │    CSV Datasets      │
                    │                      │
                    │  • Employee Data     │
                    │  • Performance Data  │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Amazon S3 Bronze   │
                    │      Raw Data        │
                    └──────────┬───────────┘
                               │
                               ▼
              ┌──────────────────────────────┐
              │         AWS EC2              │
              │                              │
              │       Ubuntu Linux           │
              │                              │
              │   ┌──────────────────────┐   │
              │   │ PySpark / Spark      │   │
              │   │                      │   │
              │   │ • Data Cleaning      │   │
              │   │ • Validation         │   │
              │   │ • Transformation     │   │
              │   │ • Joins              │   │
              │   │ • Aggregations       │   │
              │   └──────────────────────┘   │
              └──────────────┬───────────────┘
                             │
                    ┌────────┴────────┐
                    ▼                 ▼
          ┌─────────────────┐  ┌─────────────────┐
          │   S3 Silver     │  │    S3 Gold      │
          │                 │  │                 │
          │ Cleaned &       │  │ Analytics &     │
          │ Transformed     │  │ Business        │
          │ Data            │  │ Reports         │
          └─────────────────┘  └─────────────────┘
```

# 🛠️ Technologies Used

- **AWS EC2**
- **Amazon S3**
- **Ubuntu Linux**
- **Python**
- **PySpark**
- **Apache Spark**
- **Hadoop AWS**
- **Parquet**
- **CSV**

# AWS Infrastructure

The data engineering pipeline was executed on an **AWS EC2 instance running Ubuntu Linux**.

The EC2 environment was used to:

- Run Linux commands through the Ubuntu terminal
- Configure the development environment
- Run PySpark applications
- Execute Apache Spark jobs
- Connect PySpark to Amazon S3
- Read raw data from Amazon S3
- Process and transform datasets
- Write processed data back to Amazon S3

The project uses the **`s3a://` protocol** to connect Apache Spark with Amazon S3.

# 🗂️ Project Structure

```text
End-to-End-Data-Engineering-Pipeline-on-AWS/
│
├── project.py
│   └── Employee data processing pipeline
│
├── project1.py
│   └── Employee and performance data join pipeline
│
├── employee.csv
│   └── Employee dataset
│
├── performance.csv
│   └── Employee performance dataset
│
└── README.md
```

# Data Lake Architecture

The project follows a simple **Bronze, Silver, and Gold data architecture**.

## 🥉 Bronze Layer — Raw Data

The Bronze layer contains the raw datasets.

```text
s3a://s3-data-bronze/
```

Datasets:

- Employee data
- Performance data

## 🥈 Silver Layer — Cleaned and Transformed Data

The Silver layer contains cleaned and transformed employee data.

```text
s3a://s3-data-silver/anto/
```

## 🥇 Gold Layer — Analytics and Reports

The Gold layer contains business-ready analytical datasets.

Examples include:

- Department Summary
- Employee Performance
- Department Report
- Employees Without Performance Records

```text
s3a://s3-data-gold/
```

# Pipeline 1: Employee Data Processing

## 1️⃣ Extract

The employee dataset is read from the Amazon S3 Bronze layer using PySpark.

```text
s3a://s3-data-bronze/anto/employee.csv
```

The dataset is loaded with:

- Header enabled
- Automatic schema inference

## 2️⃣ Data Cleaning

The pipeline performs the following data cleaning operations:

- Removes duplicate records
- Removes rows containing missing values
- Standardizes employee names
- Standardizes city names
- Removes unnecessary spaces from department values

## 3️⃣ Data Validation

The pipeline performs validation checks on the data.

### Salary Validation

Only records with valid positive salaries are retained.

```text
Salary > 0
```

### Duplicate ID Check

The pipeline checks for duplicate employee IDs.

## 4️⃣ Data Transformation

### Bonus Calculation

A bonus column is created based on employee salary.

```text
Bonus = Salary × 10%
```

### Salary Level

Employees are categorized into different salary levels.

| Salary                     | Salary Level |
| -------------------------- | ------------ |
| Less than 50,000           | Low          |
| 50,000 to less than 70,000 | Medium       |
| 70,000 and above           | High         |

## 5️⃣ Department Aggregation

A department-level summary is generated containing:

- Number of employees
- Total salary
- Average salary
- Maximum salary

## 6️⃣ Write Data to Amazon S3

### Silver Layer

The cleaned and transformed employee dataset is written in **Parquet format**.

```text
s3a://s3-data-silver/anto/
```

### Gold Layer

The department summary is written in **Parquet format**.

```text
s3a://s3-data-gold/DepartmentSummary/
```

# Pipeline 2: Employee Performance Analysis

## 1️⃣ Extract Datasets

Two datasets are read from the Amazon S3 Bronze layer.

### Employee Dataset

```text
s3a://s3-data-bronze/anto/employee.csv
```

### Performance Dataset

```text
s3a://s3-data-bronze/anto/performance.csv
```

## 2️⃣ Data Cleaning

### Employee Data

The pipeline:

- Removes duplicate records
- Removes missing values
- Standardizes employee names

### Performance Data

The pipeline:

- Removes duplicate records
- Removes missing values
- Validates performance ratings

Valid ratings:

```text
0 to 5
```

# Join Operations

The Employee and Performance datasets are joined using the **`id` column**.

## INNER JOIN

Returns only employees with matching performance records.

```text
Employee ∩ Performance
```

## LEFT JOIN

Returns all employee records and matching performance records.

Employees without performance records contain `NULL` values for performance information.

## RIGHT JOIN

Returns all performance records and matching employee information.

## FULL OUTER JOIN

Returns all records from both datasets.

## LEFT ANTI JOIN

Returns employees who do not have a matching performance record.

This is useful for identifying employees who have not received a performance review.

# Analytics and Aggregations

## Department Report

The pipeline generates department-level metrics:

- Number of employees
- Average performance rating
- Average salary
- Maximum years of experience

---

## Project Report

The pipeline generates project-level metrics:

- Team size
- Total salary

The report is ordered by team size.

# Output Datasets

The processed datasets are stored in Amazon S3 in **Parquet format**.

### Employee Performance

```text
s3a://s3-data-gold/EmployeePerformance/
```

### Department Report

```text
s3a://s3-data-gold/DepartmentReport/
```

### Employees Without Performance Records

```text
s3a://s3-data-gold/EmployeeWithoutPerformance/
```

### Department Summary

```text
s3a://s3-data-gold/DepartmentSummary/
```

# Complete Data Flow

```text
CSV Datasets
     │
     ▼
Amazon S3
Bronze Layer
     │
     ▼
AWS EC2
Ubuntu Linux
     │
     ▼
PySpark / Apache Spark
     │
     ├── Data Cleaning
     ├── Data Validation
     ├── Transformations
     ├── Join Operations
     └── Aggregations
     │
     ├───────────────────────┐
     ▼                       ▼
Amazon S3 Silver       Amazon S3 Gold
Cleaned Data           Analytics & Reports
```

# ▶️ How to Run the Project

## Prerequisites

Make sure you have:

- An AWS Account
- An AWS EC2 instance
- Ubuntu Linux
- Python
- PySpark
- Apache Spark
- Access to the required Amazon S3 buckets

## Step 1: Connect to AWS EC2

Connect to your AWS EC2 instance and open the Ubuntu terminal.

## Step 2: Verify PySpark

Verify that PySpark is available:

```bash
pyspark
```

## Step 3: Run the Employee Processing Pipeline

```bash
python project.py
```

This pipeline performs:

```text
Read Employee Data
        ↓
Clean Data
        ↓
Validate Data
        ↓
Transform Data
        ↓
Department Aggregation
        ↓
Write to Silver & Gold Layers
```

## Step 4: Run the Employee Performance Pipeline

```bash
python project1.py
```

This pipeline performs:

```text
Read Employee & Performance Data
                ↓
            Clean Data
                ↓
          Validate Data
                ↓
           Join Datasets
                ↓
            Aggregations
                ↓
         Write Gold Outputs
```

# Key Concepts Demonstrated

This project demonstrates practical experience with:

- AWS EC2
- Ubuntu Linux
- Linux terminal commands
- Amazon S3
- Cloud-based data processing
- PySpark DataFrames
- Apache Spark
- Data Lake architecture
- Bronze, Silver, and Gold layers
- Data ingestion
- Data cleaning
- Data validation
- Data transformation
- Feature engineering
- Inner Join
- Left Join
- Right Join
- Full Outer Join
- Left Anti Join
- Aggregations
- Parquet file format
- End-to-end data engineering workflows

# Author

**Newlin Anto Marian C**

Aspiring Data Engineer

**Skills:** Python | PySpark | SQL | Apache Spark | Hadoop | AWS | Amazon S3 | Linux

🔗 LinkedIn: www.linkedin.com/in/new-lin
