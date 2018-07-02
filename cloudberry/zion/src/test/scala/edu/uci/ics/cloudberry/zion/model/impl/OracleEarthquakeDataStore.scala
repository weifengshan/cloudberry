
package edu.uci.ics.cloudberry.zion.model.impl

import edu.uci.ics.cloudberry.zion.model.schema._

object OracleEarthquakeDataStore {
  val DatasetName = "Qz_9130"
  val TimeFieldName = "STARTDATE"
  val EarthquakeSchema = Schema("earthquake",
    Seq(
      TimeField(TimeFieldName),
      StringField("STATIONID"),
      StringField("POINTID"),
      StringField("ITEMID"),
      StringField("SAMPLERATE"),
      StringField("STARTDATE")
    ),
    Seq(
      StringField("OBSVALUE")
    ),
    Seq(StringField("STATIONID"),StringField("POINTID"),StringField("ITEMID"), StringField("SAMPLERATE"), StringField("STARTDATE")),
    TimeField(TimeFieldName)
  )
}
