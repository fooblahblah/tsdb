package tsdb

import play.api.GlobalSettings
import play.api.Play
import play.api.Play.current
import tsdb.api.TSDB

object Global extends GlobalSettings {

  lazy val config = Play.configuration

  lazy val tsdb = TSDB()
}