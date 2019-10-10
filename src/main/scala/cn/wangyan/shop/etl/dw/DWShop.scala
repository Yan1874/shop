package cn.wangyan.shop.etl.dw

import cn.wangyan.shop.constant.ShopConstant
import cn.wangyan.shop.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

object DWShop {
  private val logger: Logger = LoggerFactory.getLogger(DWShop.getClass)

  def handleShopJob(sparkSession: SparkSession, appName: String, bdp_day: String, columns: ArrayBuffer[String], selectTablename: String, whereCondition: Column) = {
    val begin: Long = System.currentTimeMillis()

    try {
      //导入隐式转换
      import org.apache.spark.sql.functions._

      //设置缓存级别
      val storagelevel = ShopConstant.DEF_STORAGE_LEVEL
      //设置写入模式 overwrite
      val saveMode = SaveMode.Overwrite


      //从ODS层获取DW层目标客户主题
      val tableDf: DataFrame = SparkHelper.readTableDate(sparkSession, selectTablename, columns)
        .where(whereCondition)
        .repartition(ShopConstant.DEF_SOURCE_PARTITIOM)

      //将数据写入表中
      //SparkHelper.writeTableData(tableDf,ReleaseConstant.DW_RELEASE_CUSTOMER,saveMode)

      tableDf.show(10, false)

    } catch {
      case ex: Exception => {
        logger.error(ex.getMessage, ex)
      }
    } finally {
      println(s"任务处理时长：${appName},bdp_day=${bdp_day},${System.currentTimeMillis() - begin}")
    }

  }


  def handleJobs(appName: String, bdp_day_begin: String, bdp_day_end: String, columns: ArrayBuffer[String], selectTableName: String, whereCondition: Column): Unit = {
    var sparkSession: SparkSession = null

    try {

      val conf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        .setAppName(appName)
        .setMaster("local[*]")

      sparkSession = SparkHelper.createSparkSession(conf)
      val rangeDate: Seq[String] = SparkHelper.rangeDate(bdp_day_begin, bdp_day_end)

      for (bdp_day <- rangeDate) {
        handleShopJob(sparkSession, appName, bdp_day, columns, selectTableName, whereCondition)
      }
    } catch {
      case ex: Exception => {
        logger.error(ex.getMessage, ex)
      }

    }
  }

  def main(args: Array[String]): Unit = {
    val appName = "dw_shop_job"
    val bdp_day = "20191009"
    val bdp_day_begin = "20191009"
    val bdp_day_end = "20191009"

    //启动主题
    val customerColumns: ArrayBuffer[String] = DWShopColumnsHelper.SelectDWShop_actlog_launchColumns()
    val selectTableName = ShopConstant.ODS_USER_ACTION_LOG

    //设置where过滤条件 status=01 bdp_day=
    val whereCondition = (col(ShopConstant.DEF_PARTITION) === lit(bdp_day)
      and col(ShopConstant.COL_SHOP_ACTION) === lit("launch"))

    handleJobs(appName, bdp_day_begin, bdp_day_end, customerColumns, selectTableName, whereCondition)

  }
}