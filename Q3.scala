import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Country Visit Analysis")
      .getOrCreate()

    import spark.implicits._

    // Read the data from CSV file
    // Make sure the CSV has columns: passengerId, from, to, date
    val filePath = "/home/agileox/Project/assignmentSpark/data/flightData.csv"  // Update with the actual file path
    val data = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // Combine "from" and "to" into a sequence of countries visited
    val flights = data.select($"passengerId", $"to".as("country"), $"date")
      .union(data.select($"passengerId", $"from".as("country"), $"date"))
      .orderBy("passengerId", "date")

    // Define a window partitioned by passengerId and ordered by date
    val windowSpec = Window.partitionBy("passengerId").orderBy("date")

    // Add a column that marks when a passenger revisits the UK
    val flightsWithUKFlag = flights
      .withColumn("isUK", when($"country" === "uk", 1).otherwise(0))
      .withColumn("uk_group", sum("isUK").over(windowSpec))

    // Now, filter out only sequences where the country is not UK
    val nonUKFlights = flightsWithUKFlag
      .filter($"country" =!= "uk")
      .withColumn("country_list", collect_set($"country").over(windowSpec.partitionBy($"uk_group")))

    // Find the maximum number of countries visited without revisiting the UK
    val result = nonUKFlights
      .groupBy("passengerId", "uk_group")
      .agg(size(collect_set($"country")).as("country_count"))
      .groupBy("passengerId")
      .agg(max($"country_count").as("Longest_Run"))

    // Show the results
    // result.show()

    // Save the output to a file (e.g., in CSV format)
    result.coalesce(1)
    .write
    .option("header", "true") // Write the header
    .csv("/home/agileox/Project/assignmentSpark/output/Longest_Run_Non_UK")

    // Stop the Spark session
    spark.stop()
    System.gc()