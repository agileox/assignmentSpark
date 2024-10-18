import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Create Spark Session
val spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

// Read CSV file and cast data types
val df = spark.read
  .option("header", "true")
  .csv("/home/agileox/Project/assignmentSpark/data/flightData.csv")
  .withColumn("passengerId", col("passengerId").cast(IntegerType))
  .withColumn("flightId", col("flightId").cast(IntegerType))
  .withColumn("date", col("date").cast(DateType))

// Extract year and month, and group by them
val flightCountByMonthDF = df.groupBy(year(col("date")).alias("year"), month(col("date")).alias("month"))
  .agg(countDistinct("flightId").alias("uniqueFlightCount"))

// Show the grouped data
flightCountByMonthDF.show()

// Save the output to a file (e.g., in CSV format)
flightCountByMonthDF.write
  .option("header", "true") // Write the header
  .csv("/home/agileox/Project/assignmentSpark/output/flightCountByMonth.csv")

// Stop the Spark session
spark.stop()
