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

val resultDf = df
  .groupBy("passengerId")
  .agg(collect_list("to").as("countries")) // Use collect_list directly
  .withColumn("filteredCountries", size(array_remove(array_distinct(col("countries")), "UK"))) // Use col() for column references
  .select("passengerId", "filteredCountries")

//resultDf.show()

// Save the output to a file (e.g., in CSV format)
resultDf.coalesce(1)
  .write
  .option("header", "true") // Write the header
  .csv("/home/agileox/Project/assignmentSpark/output/maxCountriesWithoutUK.csv")

// Stop the Spark session
spark.stop()
// Optionally clear cached RDDs if needed
// df.unpersist()
// spark.getPersistentRDDs.values.foreach(_.unpersist())
// Suggest garbage collection
System.gc()
