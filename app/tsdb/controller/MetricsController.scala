package tsdb.controller

import play.api._
import play.api.data._
import play.api.data.Forms._
import play.api.data.validation.Constraints._
import play.api.libs.json._
import play.api.mvc._
import tsdb.Global
import tsdb.api.TSDB
import tsdb.model.Metric
import tsdb.model.ModelImplcits._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait MetricsController extends Controller {

  def tsdb: TSDB


  def store = Action(parse.tolerantJson) { implicit request =>
    val metric = request.body.as[Metric]
    Async {
      tsdb.write(metric.name, metric.timestamp.getOrElse(System.currentTimeMillis), metric.value) map { _ =>
        Ok
      }
    }
  }


  def query(targets: List[String], from: Option[String] = None, until: Option[String] = None) = Action { implicit request =>
    if(targets.isEmpty) BadRequest("One or more target parameters must be specified")
    else {
      Logger.info(targets.toString)
      Ok(JsNull)
    }
  }

}


object MetricsController extends MetricsController {
  val tsdb = Global.tsdb
}