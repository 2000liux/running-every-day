package day.every.running.utils

import org.apache.spark.SparkConf

/**
 * @Author: suizi
 */

object SparkConfig {

  /** 灵活配置Sparkconf
   * 获取 sparkConf
   *
   * @return
   */
  def getSparkConf: SparkConf = {
    //是否开启debug模式
    //    if (sparkDebug) {
    //      logger.info("开启DeBug模式")
    //    }

    // Spark configurations
    val conf = new SparkConf()

    // Override
    conf.set("spark.app.name", "cnpc-route" + System.currentTimeMillis())

    // Add defaults
    conf.setIfMissing("spark.sql.shuffle.partitions", "32")
    conf.getOption("spark.master") match {
      case Some(_) =>
      case None =>
        val master = "local[*]"
        //        System.setProperty("HADOOP_USER_NAME", "hadoop")
        conf.setMaster(master)
        //        conf.set("spark.sql.warehouse.dir", "file:///spark-warehouse")
        conf.set("spark.testing.memory", "471859200")
    }

    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf
  }


}
