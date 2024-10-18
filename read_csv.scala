import org.apache.spark.sql.SparkSession

// Create Spark Session
val spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

// Read CSV file
val df = spark.read.option("header", "true").csv("/home/agileox/Project/assignmentSpark/data/flightData.csv")

// Show data and schema
df.show()
df.printSchema()

// Stop the Spark session
spark.stop()

