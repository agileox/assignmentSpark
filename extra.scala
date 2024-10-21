import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Initialize Spark session
val spark = SparkSession.builder.appName("PassengerPairs").getOrCreate()

// Read the CSV data
val df = spark.read
  .option("header", "true")
  .csv("/home/agileox/Project/assignmentSpark/data/flightData.csv")
  .withColumn("passengerId", col("passengerId").cast(IntegerType))
  .withColumn("flightId", col("flightId").cast(IntegerType))
  .withColumn("date", col("date").cast(DateType))

// Group by flightId to get the passengers for each flight along with flight details
val flightsDF = df.groupBy("flightId", "from", "to", "date") // Group by flightId, from, to, and date
  .agg(collect_list("passengerId").alias("passengers")) // Collect passengers for each flight

// Create pairs of passengers for each flight and count flights together
val passengerPairsDF = flightsDF
  .withColumn("passenger1", explode(col("passengers"))) // Explode to get individual passengers
  .withColumn("passenger2", explode(col("passengers"))) // Create pairs by exploding again
  .filter(col("passenger1") < col("passenger2")) // Keep unique pairs (to avoid duplicates)
  .groupBy("passenger1", "passenger2") // Group by passenger1 and passenger2
  .agg(
    count("flightId").alias("number_of_flights_together"), // Count how many flights they took together
    first("from").alias("from"), // Get the departure location
    first("to").alias("to"), // Get the destination location
    min("date").alias("from_date"), // Get the earliest date for the flight
    max("date").alias("to_date") // Get the latest date for the flight
  )

// Format the output columns to match desired output
val finalOutputDF = passengerPairsDF
  .select(
    col("passenger1"),
    col("passenger2"),
    col("number_of_flights_together"),
    concat(lit("from ("), col("from_date"), lit(")"), lit(" "), col("from")).alias("from (date)"), // Format the 'from' column
    concat(lit("to ("), col("to_date"), lit(")"), lit(" "), col("to")).alias("to (date)") // Format the 'to' column
  )

// Sort the results by number_of_flights_together in descending order
val sortedPassengerPairsDF = finalOutputDF
  .orderBy(desc("number_of_flights_together")) // Sort by number_of_flights_together in descending order

// Show the result
//sortedPassengerPairsDF.show(truncate = false)

// Save the output to a file (e.g., in CSV format)
sortedPassengerPairsDF.coalesce(1) // Ensure single file output
  .write
  .option("header", "true") // Write the header
  .csv("/home/agileox/Project/assignmentSpark/output/extra") // Specify output location

// Stop the Spark context and suggest garbage collection
spark.stop()
System.gc()
