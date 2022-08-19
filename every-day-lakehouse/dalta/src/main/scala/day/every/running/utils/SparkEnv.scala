package day.every.running.utils

import org.apache.spark.sql.SparkSession

/**
 * @ClassName: SparkEnv
 * @Description:
 * @Author: suizi
 * @Date: 2022/7/6 21:00:15
 */
object SparkEnv {

  def localSimpleSparkSession(): SparkSession = {
    SparkSession.builder().master("local[*]").getOrCreate()
  }

  /**
   * 初始化 SparkSession
   */
  def initSparkSession(): SparkSession = {

    val conf = SparkConfig.getSparkConf

    //    conf.set("spark.driver.cores","10")
    //    conf.set("spark.driver.memory","10G")
    //    conf.set("spark.executor.memory","8G")
    //    conf.set("spark.executor.cores","8")


    val sessionConf: SparkSession.Builder = SparkSession
      .builder()
      .config(conf)

    implicit var session: SparkSession = null
    session = sessionConf.getOrCreate.newSession()
    SparkSession.setActiveSession(session)
    session
  }

  /**
   * 关闭SparkSession
   *
   * @param sparkSession sparkSession
   */
  def closeSession(sparkSession: SparkSession): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }

}
