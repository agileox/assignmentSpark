// Import necessary libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object GroupByMonthExample {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Group By Month Example")
      .getOrCreate()

    // Define the schema for the CSV file
    val schema = new StructType()
      .add("passengerId", IntegerType, nullable = true)
      .add("flightId", IntegerType, nullable = true)
      .add("from", StringType, nullable = true)
      .add("to", StringType, nullable = true)
      .add("date", StringType, nullable = true)

    // Read the CSV file into a DataFrame
    val filePath = "/home/agileox/Project/assignmentSpark/data/flightData.csv" // Replace with your actual file path
    val df = spark.read
      .option("header", "true") // Assumes the CSV file has a header row
      .schema(schema)           // Using the defined schema
      .csv(filePath)

//    // Convert the 'date' column to a proper DateType and extract the month
//    val dfWithMonth = df
//      .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
//      .withColumn("month", date_format(col("date"), "yyyy-MM"))
//
//    // Group by the 'month' column and count the occurrences of 'passengerId'
//    val result = dfWithMonth
//      .groupBy("month")
//      .agg(count("passengerId").alias("total_passenger_count"))

    // Show the result in the console (optional)
    //result.show()
    df.show()

//    // Write the result to an output folder in CSV format
//    val outputPath = "/home/agileox/Project/assignmentSpark/output/output.out" // Replace with your actual output folder path
//    result.write
//      .option("header", "true") // Include header in the output
//      .csv(outputPath)

    // Stop the Spark session
    spark.stop()
  }
}