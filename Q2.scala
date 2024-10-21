//library required for spark scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Create Spark Session
val spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

// Read CSV file for flights and cast data types
val flightsDF = spark.read
  .option("header", "true")
  .csv("/home/agileox/Project/assignmentSpark/data/flightData.csv") //read csv data location
  .withColumn("passengerId", col("passengerId").cast(IntegerType))
  .withColumn("flightId", col("flightId").cast(IntegerType))
  .withColumn("date", col("date").cast(DateType))

// Read the passengers CSV file
val passengersDF = spark.read
  .option("header", "true")
  .csv("/home/agileox/Project/assignmentSpark/data/passengers.csv") //read csv data location
  .withColumn("passengerId", col("passengerId").cast(IntegerType)) // Ensure passengerId is of the correct type

// Join the flights DataFrame with the passengers DataFrame
val joinedDF = flightsDF.join(passengersDF, Seq("passengerId"), "inner") // Inner join on passengerId - Join both table function

// Group by passengerId, firstName, and lastName, and count occurrences
val countByPassengerDF = joinedDF.groupBy("passengerId", "firstName", "lastName")
  .agg(count("*").alias("Number_of_Flights")) // Count the number of rows for each group
  .orderBy(desc("Number_of_Flights")) // Order by flightCount in descending order

// Show the grouped and counted data
//countByPassengerDF.show() //to show output in terminal - remark at the moment

// Optionally, save the count output to a file
countByPassengerDF.coalesce(1)
  .write
  .option("header", "true") // Write the header
  .csv("/home/agileox/Project/assignmentSpark/output/Most_Frequent_Fliers") // output location

// Stop the Spark session
spark.stop() // Stop the Spark context
System.gc() // Suggest garbage collection
