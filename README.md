# Employee-Performance-Analysis-Using-PySpark
## Project Summary
This project focuses on analyzing employee performance using PySpark, leveraging its capabilities for data processing, cleaning, and transformation. The objective is to create a structured data pipeline that ingests raw employee data, cleans it, and transforms it into a curated dataset for further analysis and reporting.
## Technologies Used
- pyspark
- snowflake
## Key Concepts:
### 1.	Data Ingestion:
  -Initialize SparkSession to create a Spark application.
 - Load relavant data into  Spark DataFrames from tables.
### 2.	Data Cleaning:
 - Drop columns that are not relevant to the analysis.
- Handle missing or inconsistent data by filling or removing null values.
- Standardize data formats for consistency.
### 3.	Silver Layer - Cleaned Data:
 - Save the cleaned data to the Silver layer for intermediate storage.
 - Use Delta Lake for versioning and efficient storage management.
### 4.	Transformations:
  -Perform aggregations and computations to derive insights from the cleaned data.
  - Calculate Average Performance Ratings: Aggregate the data to find the average performance rating for each department.
- Identify High-Performing Departments: Filter and rank departments based on their performance ratings.
- Track Salary Distributions: Analyze salary distributions across different departments and performance ratings.
- Save the curated data to the Gold layer, making it ready for final analysis.
### 5.	Gold Layer - Curated Data:
 - Save the transformed data to the Gold layer for final analysis.
 - Ensure the data is ready for business intelligence tools and reporting.
 ### 6. Snowflake Integration

- Utilize Snowflake Snowpipe for automated ingestion of curated data from CSV files into a single staging table in Snowflake.
- parse Json column table  into respective target  tables (performance_yr, avg_performance_dept, total_sales_dept, high_performers) for final analysis.

## Benefits:
-	Scalability: PySpark handles large datasets efficiently, making it suitable for enterprise-level data processing.
-	Efficiency: Delta Lake optimizes storage and provides robust version control.
-	Flexibility: PySpark allows for complex data transformations and aggregations to derive meaningful insights.
- Integration: The curated data in the Gold layer can be seamlessly integrated with business intelligence tools for reporting and decision-making.

By following these steps and leveraging PySpark's powerful data processing capabilities, the project demonstrates a comprehensive approach to managing and analyzing employee performance data, ensuring high data quality and actionable insights.

