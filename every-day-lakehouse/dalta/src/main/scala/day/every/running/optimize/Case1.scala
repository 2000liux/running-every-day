package day.every.running.optimize

import day.every.running.utils.SparkEnv
import io.delta.tables.DeltaTable

/**
 * 通过文件管理优化性能
 * https://help.aliyun.com/document_detail/188264.html
 */
object Case1 {

  import scala.util.Random

  val numRecords = 100 * 1000L
  val numFiles = 1000

  // 连接数据
  case class ConnRecord(src_ip: String, src_port: Int, dst_ip: String, dst_port: Int)

  // 生成随机的ip和port
  def randomIPv4(r: Random) = Seq.fill(4)(r.nextInt(256)).mkString(".")

  def randomPort(r: Random) = r.nextInt(65536)

  val path = "/Users/suizi/Desktop/xql/clone/explore_3/running-every-day/every-day-lakehouse/data/delta/databricks-delta-demo/ip_demo"
  val spark = SparkEnv.initSparkSession()

  val startTime = System.currentTimeMillis()

  // 生成一条随机的连接数据
  def randomConnRecord(r: Random) = ConnRecord(
    src_ip = randomIPv4(r), src_port = randomPort(r),
    dst_ip = randomIPv4(r), dst_port = randomPort(r))

  def main(args: Array[String]): Unit = {
    println(s"开始时间: = ${startTime}")
    //    firstStepWriteData()
    //    step2Query()
    //    step3OptimizeTable()
    //    step2Query()
    //    step4QueryTableCountInfo()
    //    step4_1QueryTable()
    //    step5_optimizeFields()
    //    step4QueryTableCountInfo()
    step6_vacuumTable()
    end()
  }


  def firstStepWriteData(): Unit = {
    import spark.implicits._
    // 生成10000个partition，每个partition中包含10000条连接数据
    val df = spark.range(0, numFiles, 1, numFiles).mapPartitions { it =>
      val partitionID = it.toStream.head
      val r = new Random(seed = partitionID)
      Iterator.fill((numRecords / numFiles).toInt)(randomConnRecord(r))
    }

    // 将数据保存到oss中，并基于数据建立table
    df.write
      .mode("overwrite")
      .format("delta")
      .save(path)
  }

  /**
   * 查询：157开头的源ip和216开头的目的ip的连接数量
   */
  def step2Query(): Unit = {
    spark.sql(s"create table conn_rand USING DELTA LOCATION '${path}';")
    spark.sql(s"SELECT COUNT(*) FROM conn_rand WHERE src_ip LIKE '157.%' AND dst_ip LIKE '216.%';").show()
  }

  /**
   * optimize 后大小变大了，但查询效率有提升 （快 1/10）
   */
  def step3OptimizeTable(): Unit = {
    spark.sql(s"OPTIMIZE delta.`${path}`;")
  }

  /**
   * 得到表文件的统计信息
   */
  def step4QueryTableCountInfo(): Unit = {
    spark.sql(s"create table conn_record USING DELTA LOCATION '${path}';")
    spark.sql("SELECT row_number() OVER (ORDER BY file) AS file_id," +
      "count(*) as numRecords, " +
      "min(src_ip), max(src_ip), " +
      "min(src_port),max(src_port), " +
      "min(dst_ip), max(dst_ip), " +
      "min(dst_port), max(dst_port) " +
      "FROM (" +
      "SELECT input_file_name() AS file, * FROM conn_record) GROUP BY file"
    ).show()
  }


  def step4_1QueryTable(): Unit = {
    spark.sql(s"create table conn_record USING DELTA LOCATION '${path}';")
    spark.sql("SELECT COUNT(*) FROM conn_record WHERE src_ip like '157.%' AND dst_ip like '216.%' AND src_port = 10000 AND dst_port = 10000;")
      .show()
  }

  def step5_optimizeFields(): Unit = {
    spark.sql(s"create table conn_record USING DELTA LOCATION '${path}';")
    spark.sql("OPTIMIZE conn_record ZORDER BY (src_ip, src_port, dst_ip, dst_port);")
  }

  def step6_vacuumTable(): Unit = {
    spark.sql(s"create table conn_record USING DELTA LOCATION '${path}';")
    spark.sql("vacuum conn_record;")

  }

  def end(): Unit = {
    val endTime = System.currentTimeMillis()
    println(s"结束时间: = ${System.currentTimeMillis()}")
    println(s"间隔:    = ${endTime - startTime}")
    spark.stop()
  }
}
