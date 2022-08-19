package day.every.running.examples.review

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

import java.io.File

/**
 * delta lake update 跟 delete 功能验证
 * doc: https://www.yuque.com/liuxiangqian-umdwa/wh1ikk/wxy9yb
 */
object DeltaAndUpdateInstance {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("Quickstart")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel(LogLevel.ERROR.getLabel)
    val file = new File(s"/Users/suizi/Desktop/xql/clone/explore_3/lake_warehouse_examples/file/spark/delta_spark/custom-io/v1")
    if (file.exists()) FileUtils.deleteDirectory(file)

    // Create a table
    println("Creating a table")
    val path = file.getCanonicalPath

    createTable(spark, path)
    val deltaTable = DeltaTable.forPath(path)

    /* // Read table
     println("Reading the table")
     val df = spark.read.format("delta").load(path)
     df.show()

     // Upsert (merge) new data
     println("Upsert new data")
     val newData = spark.range(0, 20).toDF
     val deltaTable = DeltaTable.forPath(path)

     deltaTable.as("oldData")
       .merge(
         newData.as("newData"),
         "oldData.id = newData.id")
       .whenMatched
       .update(Map("id" -> col("newData.id")))
       .whenNotMatched
       .insert(Map("id" -> col("newData.id")))
       .execute()

     deltaTable.toDF.show()

     // Update table data
     println("Overwrite the table")
     data = spark.range(5, 10)
     data.write.format("delta").mode("overwrite").save(path)
     deltaTable.toDF.show()
  */

    // Update every even value by adding 100 to it
    //    println("Update to the table (add 100 to every even value)")
    //    deltaTable.update(
    //      condition = expr("id % 2 == 0"),
    //      set = Map("id" -> expr("id + 100")))
    //    deltaTable.toDF.show()

    // Delete every even value
    delete(deltaTable)


    /*
        // Read old version of the data using time travel
        print("Read old data using time travel")
        val df2 = spark.read.format("delta").option("versionAsOf", 0).load(path)
        df2.show()*/

    // Cleanup
    //    FileUtils.deleteDirectory(file)
    spark.stop()
  }

  def createTable(spark: SparkSession, path: String): Unit = {
    spark
      .range(0, 5)
      .write
      .format("delta")
      .save(path)
  }

  def update(spark: SparkSession, path: String, deltaTable: DeltaTable): Unit = {

    println("Overwrite the table")
    deltaTable.update(
      condition = expr("id % 2 == 0"),
      set = Map("id" -> expr("id + 100")))
    deltaTable.toDF.show()

  }

  def delete(deltaTable: DeltaTable): Unit = {
    deltaTable.delete(condition = expr("id % 2 == 0"))
    deltaTable.toDF.show()
  }

}
