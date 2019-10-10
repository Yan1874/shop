package cn.wangyan.shop.etl.dw

import scala.collection.mutable.ArrayBuffer

object DWShopColumnsHelper {
  def SelectDWShop_actlog_launchColumns(): ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()

    columns.+=("action")
    columns.+=("session_id")
    columns.+=("device_id")
    columns.+=("user_id")
    columns.+=("os")
    columns.+=("os_version")
    columns.+=("manufacturer")
    columns.+=("carrier")
    columns.+=("network_type")
    columns.+=("area_code")
    columns.+=("longitude")
    columns.+=("latitude")
    columns.+=("ct")





  }

}
