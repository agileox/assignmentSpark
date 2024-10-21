import org.apache.spark.sql.SparkSession

// Create a Spark session
val spark = SparkSession.builder()
  .appName("CSV Reader")
  .master("local[*]")  // Use all available cores
  .getOrCreate()

// Path to your CSV file
val csvFilePath = "/home/agileox/Project/assignmentSpark/data/flightData.csv"

// Read the CSV file into a DataFrame
val df = spark.read
  .option("header", "true")  // Use first line as header
  .option("inferSchema", "true")  // Infer schema automatically
  .csv(csvFilePath)

// Show the DataFrame in the terminal
df.show()

// Stop the Spark session
spark.stop()