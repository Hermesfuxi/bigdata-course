package bigdata.hermesfuxi.spark.example.wordCount.dao

import scala.beans.BeanProperty

// "oid":"o124", "cid": 2, "money": 200.0, "longitude":117.397128,"latitude":38.916527
@BeanProperty
case class OrderData(oid: String, cid: String, money: Double, longitude: Double, latitude: Double)
