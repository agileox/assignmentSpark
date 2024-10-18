// Import necessary libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object GroupByMonthExample {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Q1 - Total number of flights")
      .master("local[*]") // Use local for testing
      .getOrCreate()

    // Define the schema for the CSV file
    val schema = new StructType()
      .add("passengerId", IntegerType, nullable = true)
      .add("flightId", IntegerType, nullable = true)
      .add("from", StringType, nullable = true)
      .add("to", StringType, nullable = true)
      .add("date", StringType, nullable = true)

    // Read the CSV file into a DataFrame
    val filePath = "/home/agileox/Project/assignmentSpark/data/flightData.csv" // replace with your file path
    val df = spark.read
      .option("header", "true") // assuming the file has a header
      .schema(schema)
      .csv(filePath)

    // Convert the 'date' column to a proper DateType and extract the month
    val dfWithMonth = df
      .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
      .withColumn("month", date_format(col("date"), "yyyy-MM"))

    // Group by the 'month' column and count the occurrences of 'passengerId'
    val result = dfWithMonth
      .groupBy("month")
      .agg(count("passengerId").alias("total_passenger_count"))

    // Show the result
    result.show()
    result.printSchema()

    // Stop the Spark session
    spark.stop()
  }
}

