package bigdata.hermesfuxi.spark.streaming.order

import scala.beans.BeanProperty

// {"oid":"o124", "cid": 2, "money": 200.0, "longitude":117.397128,"latitude":38.916527}
@BeanProperty
case class OrderBean(
                      var oid: String,
                      var cid: Int,
                      var money: Double,
                      var longitude: Double,
                      var latitude: Double,
                    )
