import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

// Initialize Spark session
val spark = SparkSession.builder.appName("PassengerPairs").getOrCreate()

// Read the CSV data
val df = spark.read
  .option("header", "true")
  .csv("/home/agileox/Project/assignmentSpark/data/flightData.csv")
  .withColumn("passengerId", col("passengerId").cast(IntegerType))
  .withColumn("flightId", col("flightId").cast(IntegerType))
  .withColumn("date", col("date").cast(DateType))

// Group by flightId to get the passengers for each flight
val passengersPerFlightDF = df.groupBy("flightId")
  .agg(collect_list("passengerId").alias("passengers"))

// Create pairs of passengers for each flight and count flights together
val passengerPairsDF = passengersPerFlightDF
  .select(explode(col("passengers")).alias("passenger1"), col("passengers")) // Explode to get individual passengers
  .withColumn("passenger2", explode(col("passengers"))) // Create pairs by exploding again
  .filter(col("passenger1") < col("passenger2")) // Keep unique pairs (to avoid duplicates)
  .groupBy("passenger1", "passenger2")
  .agg(count("passenger2").alias("Number_Of_Flights_Together")) // Count how many flights they took together

// Sort the results by Number_Of_Flights_Together in descending order
val sortedPassengerPairsDF = passengerPairsDF
  .orderBy(desc("Number_Of_Flights_Together")) // Sort by Number_Of_Flights_Together in descending order

// Show the result
//sortedPassengerPairsDF.show(truncate = false)

// Save the output to a file (e.g., in CSV format)
sortedPassengerPairsDF.coalesce(1) // Ensure single file output
  .write
  .option("header", "true") // Write the header
  .csv("/home/agileox/Project/assignmentSpark/output/Passenger_Pairs_Flights_Together") // Specify output location

// Stop the Spark context and suggest garbage collection
spark.stop()
System.gc()
