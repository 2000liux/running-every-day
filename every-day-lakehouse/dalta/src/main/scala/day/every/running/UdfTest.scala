package day.every.running

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDate

object UdfTest {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel(LogLevel.ERROR.getLabel)


    val processDatePattern = "yyyy-MM-dd HH:mm:ss"
    val str = new SimpleDateFormat(processDatePattern).format(System.currentTimeMillis())
    /*  val timeFormatUDF = udf(
        (processDate:String) => {
          if (!StringUtils.isNotBlank(processDate)) {
            str
          } else
            LocalDate.now()
        }
      )*/

    val timeFormatUDF: UserDefinedFunction = udf(() => {
      LocalDate.now()
    })


//    val column = to_date(timeFormatUDF(), processDatePattern)

//    val timeFormatter = udf(() => {
//      if (StringUtils.isBlank(processDatePattern)) column else timeFormatUDF()
//    })


    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9981")
      .load()

    val jsonSchema = new StructType()
      .add("user_id", StringType, nullable = false)
      .add("event_type", StringType, nullable = false)
      .add("timestamp", TimestampType, nullable = false)

    import spark.implicits._

    val events = lines
      .select(from_json(col("value"), jsonSchema).as("event"))
      .selectExpr("event.user_id AS user_id", "event.event_type AS event_type",
        "event.timestamp AS timestamp")
      //      .withWatermark("timestamp", "10 seconds")
      .as[(String, String, Timestamp)]


    events.printSchema()

    val frame = events.withColumn("processDate", timeFormatUDF())

    frame.printSchema()
    frame.writeStream.format("console").option("truncate", "false").start()

    spark.streams.awaitAnyTermination();
  }

}
