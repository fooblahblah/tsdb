package tsdb.model

import play.api.libs.functional.syntax._
import play.api.libs.json._
import play.api.libs.json.Format._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._

case class Metric(name: String, value: Double, timestamp: Option[Long] = None)

object ModelImplcits {

 implicit val metricFormat: Format[Metric] = (
      (__ \ "name").format[String] ~
      (__ \ "value").format[Double] ~
      (__ \ "createdAt").formatNullable[Long])(Metric.apply, unlift(Metric.unapply))

}
