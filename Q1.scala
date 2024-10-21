import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FlightDataProcessing {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Flight Data Processing")
      .master("local[*]")  // Adjust for your cluster
      .getOrCreate()

    // Set file paths
    val inputFilePath = "/home/agileox/Project/assignmentSpark/data/flightData.csv"  // Replace with actual input file path
    val outputFolderPath = "/home/agileox/Project/assignmentSpark/output/flightData.csv"  // Replace with actual output folder path

    // Read the CSV file into a DataFrame
    val df = spark.read
      .option("header", "true")  // Assumes the CSV has a header
      .option("inferSchema", "true")  // To infer the data types
      .csv(inputFilePath)

    // Extract the month from the 'date' column
    val dfWithMonth = df.withColumn("month", date_format(col("date"), "yyyy-MM"))

    // Group by month and count the occurrences
    val monthlyCounts = dfWithMonth.groupBy("month").count()

    // Save the result to the output folder as CSV
    monthlyCounts.write
      .option("header", "true")
      .csv(outputFolderPath)

    // Stop the Spark session
    spark.stop()
  }
}