// the library required byt the code
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Create Spark Session
val spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

// Read CSV file and cast data types as it read the csv as text file
val df = spark.read
  .option("header", "true")
  .csv("/home/agileox/Project/assignmentSpark/data/flightData.csv")
  .withColumn("passengerId", col("passengerId").cast(IntegerType))
  .withColumn("flightId", col("flightId").cast(IntegerType))
  .withColumn("date", col("date").cast(DateType))

// Extract year and month, and group by them -> this is where the date being converted to month
val flightCountByMonthDF = df.groupBy(month(col("date")).alias("Months")) // the conversion function happen
  .agg(count("flightId").alias("Number_Of_Flights"))
  .orderBy(col("Months")) // Order by month

// Show the grouped data
// flightCountByMonthDF.show() // for showing the data in terminal but will remark for keeping it into csv file output

// Save the output to a file (e.g., in CSV format)
flightCountByMonthDF.coalesce(1)
  .write
  .option("header", "true") // Write the header
  .csv("/home/agileox/Project/assignmentSpark/output/No_Of_Flight_Each_Months") //output file location

// Stop the Spark session
spark.stop() // Stop the Spark context
System.gc() // Suggest garbage collection