import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object FlightDataAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flight Data Analysis")
      .master("local[*]") // Change this as needed for your cluster
      .getOrCreate()

    import spark.implicits._

    // Specify the path to your CSV file
    val csvFilePath = "/home/agileox/Project/assignmentSpark/data/flightData.csv" // Update this with your CSV file path

    // Read the CSV file into a DataFrame
    val df: DataFrame = spark.read
      .option("header", "true") // Indicate that the first row is a header
      .option("inferSchema", "true") // Automatically infer data types
      .csv(csvFilePath)

    // Print the schema to confirm the structure
    df.printSchema()

    // Define a UDF to calculate the maximum streak of countries without UK
    val maxStreakUDF = udf((countryList: Seq[String]) => {
      countryList.foldLeft((0, 0)) { case ((currentStreak, maxStreak), country) =>
        if (country == "uk") {
          (0, math.max(currentStreak, maxStreak)) // Reset and compare
        } else {
          (currentStreak + 1, maxStreak) // Increment current streak
        }
      }._2 // Return the max streak
    })

    // Group by passengerId and aggregate the 'to' column to a list
    val result = df.groupBy("passengerId")
      .agg(collect_list("to").as("countries"))
      .select($"passengerId", maxStreakUDF($"countries").as("maxCountriesWithoutUK"))

    // Show the result for all passengers
    result.show()

    // Save the result to a CSV file (optional)
    result.write
      .option("header", "true")
      .csv("/home/agileox/Project/assignmentSpark/output/maxCountriesWithoutUK.csv") // Update this path as needed

    // Stop the Spark session
    spark.stop()
  }
}
