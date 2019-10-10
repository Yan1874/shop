package cn.wangyan.shop.constant

import org.apache.spark.storage.StorageLevel

object ShopConstant {

  val COL_SHOP_ACTION: String = "action"


  val DEF_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK
  val DEF_PARTITION = "bdp_day"
  val DEF_SOURCE_PARTITIOM = 4

  //ods
  val ODS_USER_ACTION_LOG = "ods_shop.ods_01_user_action_log"

}
