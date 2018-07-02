package edu.uci.ics.cloudberry.zion.model.impl

import edu.uci.ics.cloudberry.zion.model.datastore.IDataConn
import edu.uci.ics.cloudberry.zion.model.schema.TimeField
import play.api.libs.ws.WSResponse
import play.api.libs.json.{Json, _}
import play.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.Breaks.{break, breakable}
import java.sql.{Connection, DriverManager, _}
import java.lang._

import oracle.sql.TIMESTAMP


class OracleConn(url: String)(implicit ec: ExecutionContext) extends IDataConn {
  val defaultQueryResponse = Json.toJson(Seq(Seq.empty[JsValue]))
  //oracledb.url = "jdbc:oracle:thin:@//128.195.52.124:1521/orcl?user=system&password=Bigdata1"
  //var oracleurl = url.substring(0,url.indexOf('?'))
  var username :String = "system"
  var password :String= "Bigdata1"

  Class.forName("oracle.jdbc.driver.OracleDriver");
  val connection: Connection = DriverManager.getConnection(url,username,password)

  def removeLimitWord(query: String): String = {
    val pos = query.toLowerCase().indexOf("limit")
    println("\r\n====="+query+",limit position="+pos)
    if (pos >0)
      return query.substring(0, pos)
    else
      return query
  }

  def post(query: String): Future[WSResponse] = {
    throw new UnsupportedOperationException
  }

  def postQuery(query: String): Future[JsValue] = query match {
    case OracleConn.metaName if query.contains(OracleConn.metaName)  => postBerryQuery(removeLimitWord(query))
    //case _ => postGeneralQueryMysql(removeLimitWord(query))
    case _ => postGeneralQuery(removeLimitWord(query))
  }

  protected def postGeneralQuery1(query: String): Future[JsValue] = {
    postGeneralQueryMysql(query)
  }
  protected def postGeneralQueryMysql(query: String): Future[JsValue] = {
    val statement = connection.createStatement
    val result = statement.executeQuery(query)
    val resultMetadata = result.getMetaData
    val columnCount = resultMetadata.getColumnCount
    var qJsonArray: JsArray = Json.arr()
    while (result.next) {
      var columnId = 0
      var rsJson: JsObject = Json.obj()
      breakable {
        for (columnId <- 1 to columnCount) {
          val columnLabel = resultMetadata.getColumnLabel(columnId)
          val value = result.getObject(columnLabel)
          println("\r\n...."+columnLabel+"="+value.toString+",");
          value match {
            case int: Integer =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsNumber(int.toInt))
            case boolean: java.lang.Boolean =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsBoolean(boolean))
            case date: Date =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(TimeField.TimeFormat.print(date.getTime)))
            case time: Time =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(TimeField.TimeFormat.print(time.getTime)))
            case timestamp: Timestamp =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(TimeField.TimeFormat.print(timestamp.getTime)))
            case ts: TIMESTAMP =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(TimeField.TimeFormatForSQL.print(ts.dateValue().getTime)))
            case long: Long =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsNumber(long.toLong))
            case double: Double =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsNumber(double.toDouble))
            case float: Float =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsNumber(float.asInstanceOf[BigDecimal]))
            case short: Short =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsNumber(short.toInt))
            case decimal: BigDecimal =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsNumber(decimal))
            case str: String =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(str))
            case blob: Blob => //large data
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(blob.toString))
            case byte: Byte =>
              rsJson = rsJson ++ Json.obj(columnLabel -> byte.toByte)
            case _ =>

              Logger.warn(s" oracle type of value $value is not detectd")
              break
          }
        }
        qJsonArray = qJsonArray :+ rsJson
        System.out.println("qJsonArrayyyyyyyyyyy="+Json.toJson(qJsonArray).toString())
      }
    }
    Future(Json.toJson(qJsonArray))
  }

  protected def postGeneralQuery(query: String): Future[JsValue] = {
    val statement = connection.createStatement
    System.out.println("\r\n-----------postGeneralQuery="+query)
    val result = statement.executeQuery(query)
    var x ="";
    val resultMetadata = result.getMetaData
    val columnCount = resultMetadata.getColumnCount
    var qJsonArray: JsArray = Json.arr()
    while (result.next) {
      var columnId = 0
      var rsJson: JsObject = Json.obj()
      breakable {
        for (columnId <- 1 to columnCount) {
          val columnLabel = resultMetadata.getColumnLabel(columnId)
          val valueType = resultMetadata.getColumnTypeName(columnId)
          val value = result.getObject(columnLabel)

          //ojdbc 7 version
          //columnLabel=T,value=1,valueType=NUMBER
//          qJsonArray=columnLabel=RID,value=6B13D4DD16917836E05521A2B13383E7,valueType=VARCHAR2
//          columnLabel=STARTDATE,value=20100101,valueType=VARCHAR2
//          columnLabel=STATIONID,value=1,valueType=VARCHAR2
//          columnLabel=POINTID,value=1,valueType=VARCHAR2
//          columnLabel=ITEMID,value=1,valueType=VARCHAR2
//          columnLabel=SAMPLERATE,value=1,valueType=VARCHAR2
//          columnLabel=OBSVALUE,value=null,valueType=CLOB
//          columnLabel=T1,value=null,valueType=CHAR
//          columnLabel=T2,value=null,valueType=NUMBER
//          columnLabel=T3,value=null,valueType=NUMBER
//          columnLabel=T4,value=null,valueType=DATE
//          columnLabel=T5,value=null,valueType=LONG
//          columnLabel=T7,value=null,valueType=NVARCHAR2
//          columnLabel=T8,value=null,valueType=ROWID
//          columnLabel=T9,value=null,valueType=NCHAR
//          columnLabel=T11,value=null,valueType=CLOB
//          columnLabel=T12,value=null,valueType=NCLOB
//          columnLabel=T13,value=null,valueType=BLOB
//          columnLabel=T14,value=null,valueType=INTERVALDS
//          columnLabel=T15,value=null,valueType=TIMESTAMP WITH TIME ZONE

          //x = x +"columnLabel="+columnLabel+",value="+value+",valueType="+valueType+"\r\n"
          //System.out.println("\r\n--------x="+x+"------------\r\n")
          valueType match {
            case "NUMBER" =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsNumber(value.toString.toDouble))
            case "DATE" =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(value.toString))
            case "TIMESTAMP" =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(value.toString))
            case "BLOB" =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(value.toString))
//            case "TIMESTAMP" =>
//              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(TimeField.TimeFormat.print(timestamp.getTime)))
            case "LONG" =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsNumber(value.toString.toLong))
//            case "ROWID" =>
//              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(value.toString))
//            case "TIMESTAMP WITH TIME ZONE" =>
//              rsJson = rsJson ++ Json.obj(columnLabel -> JsNumber(value.asInstanceOf[BigDecimal]))
//            case "INTERVALDS" =>
//              rsJson = rsJson ++ Json.obj(columnLabel -> JsNumber(value.toInt))
            case "NCHAR" =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(value.toString))
            case "CHAR" =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(value.toString))
            case "VARCHAR2" =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(value.toString))
            case "NVARCHAR2" =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(value.toString))
            case "NCLOB" => //large data
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(value.toString))
            case "CLOB" =>
              rsJson = rsJson ++ Json.obj(columnLabel -> JsString(value.toString))
            case _ =>
              Logger.warn(s"type of value $value is not detectd")
              break
          }
          //rsJson = rsJson ++Json.obj(columnLabel -> JsNumber(value.toString().toInt))
          qJsonArray = qJsonArray :+ rsJson
         // System.out.println("rsJson="+rsJson)
        }
      }
    }
    System.out.println("qJsonArrayxxxxxxx="+Json.toJson(qJsonArray).toString())
    Future(Json.toJson(qJsonArray))
  }

  protected def postBerryQuery(query: String): Future[JsValue] = {

    val statement = connection.createStatement
    System.out.println("\r\n-----------postBerryQuery=="+ query)
    val result = statement.executeQuery(query)
    var qJsonArray: JsArray = Json.arr()
    while (result.next) {
      var rsJson: JsObject = Json.obj()
      val name = result.getObject("name")
      val schema = result.getObject("schema")
      val stats = result.getObject("stats")
      val dataInterval = result.getObject("dataInterval")
      rsJson = rsJson ++ Json.obj("name" -> JsString(name.asInstanceOf[String]))
      rsJson = rsJson ++ Json.obj("schema" -> Json.parse(schema.toString))
      rsJson = rsJson ++ Json.obj("stats" -> Json.parse(stats.toString))
      rsJson = rsJson ++ Json.obj("dataInterval" -> Json.parse(dataInterval.toString))
      qJsonArray = qJsonArray :+ rsJson
    }
    Future(Json.toJson(qJsonArray))
  }

  def postControl(query: String) = {
    //Oracle sql can not end with ;
    System.out.println("\r\n-----------postControl="+query)
    val statement = connection.createStatement
    query.trim().split(";\n").foreach {
      case q => if (!q.isEmpty)   {
        if (q.endsWith(";")) {
          val sql = q.substring(0,q.length-1);
          statement.executeUpdate(sql);
        }
        else
          {
            statement.executeUpdate(q);
          }
      }
    }
    Future(true)
  }


  def postControlmysql(query: String) = {
    val statement = connection.createStatement
    query.split(";\n").foreach {
      case q => statement.executeUpdate(q)
    }
    Future(true)
  }

}

object OracleConn {
  val metaName = "berrymeta"
  val usertable = "user_tables"
}
