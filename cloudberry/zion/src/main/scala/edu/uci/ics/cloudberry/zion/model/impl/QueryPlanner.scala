package edu.uci.ics.cloudberry.zion.model.impl

import java.security.MessageDigest

import edu.uci.ics.cloudberry.zion.model.schema._
import org.joda.time.{DateTime, Interval}
import play.api.libs.json._

class QueryPlanner {

  import QueryPlanner._

  def makePlan(query: Query, source: DataSetInfo, views: Seq[DataSetInfo]): (Seq[Query], IMerger) = {

    //TODO currently only get the best one
    val bestView = selectBestView(findMatchedViews(query, source, views))
    splitQuery(query, source, bestView)
  }

  // Return whether there is matched views for a query, and it is used by the ViewStatusClient
  def requestViewForQuery(query: Query, source: DataSetInfo, views: Seq[DataSetInfo]): Boolean = {
    findMatchedViews(query, source, views).nonEmpty
  }

  // Find the matched views for a query
  def findMatchedViews(query: Query, source: DataSetInfo, views: Seq[DataSetInfo]): Seq[DataSetInfo] = {
    views.filter(view => view.createQueryOpt.exists(vq => vq.canSolve(query, source.schema)))
  }

  def suggestNewView(query: Query, source: DataSetInfo, views: Seq[DataSetInfo]): Seq[CreateView] = {
    //TODO currently only suggest the keyword subset views
    if (views.exists(v => v.createQueryOpt.exists(vq => vq.canSolve(query, source.schema)))) {
      Seq.empty[CreateView]
    } else {
      val keywordFilters = query.filter.filter(f => f.field.dataType == DataType.Text)
      keywordFilters.flatMap { kwFilter =>
        kwFilter.values.map { wordAny =>
          val word = wordAny.asInstanceOf[String]
          val wordFilter = FilterStatement(kwFilter.field, None, Relation.contains, Seq(word))
          val wordQuery = Query(query.dataset, Seq.empty, Seq.empty, Seq(wordFilter), Seq.empty, None, None)
          CreateView(getViewKey(query.dataset, word), wordQuery)
        }
      }
    }
  }

  def calculateMergeFunc(query: Query, schema: AbstractSchema): IMerger = {
    //TODO the current logic is very simple, all queries has to be the isomorphism.
    if (query.lookup.nonEmpty) {
      ???
    }

    query.globalAggr match {
      case Some(statement) =>
        val aggrMap = Map(statement.aggregate.as.name -> statement.aggregate.func)
        return Merger(Seq.empty, aggrMap, Map.empty, Set.empty, None)
      case None =>
    }

    val keys = Seq.newBuilder[String]
    val aggrValues = Map.newBuilder[String, AggregateFunc]
    query.groups match {
      case Some(groupStats) =>
        keys ++= groupStats.bys.map(key => key.as.getOrElse(key.field).name)
        aggrValues ++= groupStats.aggregates.map(v => v.as.name -> v.func)
      case None =>
    }

    val orderOn = Map.newBuilder[String, SortOrder.Value]
    val project = Set.newBuilder[String]
    var limitOpt: Option[Int] = None

    query.select match {
      case Some(select) =>
        orderOn ++= (select.orderOn.map(_.name).zip(select.order).toMap)
        project ++= select.fields.map(_.name)
        limitOpt = Some(select.limit)
      case None =>
    }

    Merger(keys.result, aggrValues.result, orderOn.result, project.result, limitOpt)
  }

  private def selectBestView(matchedViews: Seq[DataSetInfo]): Option[DataSetInfo] = {
    if (matchedViews.isEmpty) {
      None
    } else {
      Some(matchedViews.min(Ordering.by((info: DataSetInfo) => info.stats.cardinality)))
    }
  }

  private def splitQuery(query: Query, source: DataSetInfo, bestView: Option[DataSetInfo]): (Seq[Query], IMerger) = {
    bestView match {
      case None => (Seq(query), Unioner)
      case Some(view) =>
        if (!source.schema.isInstanceOf[Schema]) {
          throw new IllegalArgumentException("Lookup dataset " + source.schema.getTypeName + " cannot split query.")
        }
        val schema = source.schema.asInstanceOf[Schema]
        val queryInterval = query.getTimeInterval(schema.timeField).getOrElse(new Interval(new DateTime(0), DateTime.now()))
        val viewInterval = new Interval(new DateTime(0), view.stats.lastModifyTime)
        val unCovered = getUnCoveredInterval(viewInterval, queryInterval)

        val seqBuilder = Seq.newBuilder[Query]

        //TODO if select project fields from groupby results, postpone the project to the merge stage.

        //TODO here is a very simple assumption that the schema is the same, what if the schema are different?
        val viewFilters = view.createQueryOpt.get.filter
        val newFilter = query.filter.filterNot(qf => viewFilters.exists(vf => qf.covers(vf)))
        seqBuilder += query.copy(dataset = view.name, filter = newFilter)
        for (interval <- unCovered) {
          seqBuilder += query.setInterval(schema.timeField, interval)
        }
        (seqBuilder.result(), calculateMergeFunc(query, schema))
    }
  }

}

object QueryPlanner {

  def getUnCoveredInterval(dataInterval: Interval, queryInterval: Interval): Seq[Interval] = {
    val intersect = dataInterval.overlap(queryInterval)
    if (intersect == null) {
      return Seq(queryInterval)
    }
    val intervals = scala.collection.mutable.ArrayBuffer.empty[Interval]
    if (queryInterval.getStartMillis < intersect.getStartMillis) {
      intervals += new Interval(queryInterval.getStartMillis, intersect.getStartMillis)
    }
    if (intersect.getEndMillis < queryInterval.getEndMillis) {
      intervals += new Interval(intersect.getEndMillis, queryInterval.getEndMillis)
    }
    intervals
  }

  def getViewKey(sourceName: String, keyword: String): String = {
    sourceName + "_" + MessageDigest.getInstance("MD5").digest(keyword.getBytes("UTF-8")).map("%02x" format _).mkString
  }

  def unionAll(responses: TraversableOnce[JsValue]): JsArray = {
    if (responses.size == 1) return responses.toSeq.head.asInstanceOf[JsArray]
    val builder = Seq.newBuilder[JsValue]
    responses.foreach { jsValue =>
      builder ++= jsValue.asInstanceOf[JsArray].value
    }
    JsArray(builder.result())
  }

  trait IMerger {
    def apply(jsons: TraversableOnce[JsValue]): JsArray
  }

  case class Merger(keys: Seq[String],
                    aggrValues: Map[String, AggregateFunc],
                    orderOn: Map[String, SortOrder.Value],
                    project: Set[String],
                    limitOpt: Option[Int]) extends IMerger {
    override def apply(jsons: TraversableOnce[JsValue]) =
      merge(jsons.map(_.asInstanceOf[JsArray]).toSeq, keys, aggrValues, orderOn, project, limitOpt)
  }

  case object Unioner extends IMerger {
    override def apply(jsons: TraversableOnce[JsValue]) = unionAll(jsons)
  }

  def merge(jsArrays: Seq[JsArray],
            keys: Seq[String],
            aggrValues: Map[String, AggregateFunc],
            orderOn: Map[String, SortOrder.Value],
            project: Set[String],
            limitOpt: Option[Int]): JsArray = {
    if (jsArrays.isEmpty) return JsArray()
    val jsons = jsArrays.filter(_.value.nonEmpty)

    val mergedArray: JsArray = if (aggrValues.nonEmpty && jsons.size > 1) {
      mergeValues(jsons, keys, aggrValues)
    } else {
      unionAll(jsons)
    }

    val ordered: JsArray = if (orderOn.nonEmpty) {
      orderJsArray(mergedArray, orderOn)
    } else {
      mergedArray
    }

    val projected: JsArray = if (project.nonEmpty) {
      projectArray(ordered, project)
    } else {
      ordered
    }

    limitOpt match {
      case Some(limit) => JsArray(projected.value.slice(0, limit))
      case None => projected
    }
  }

  private def mergeValues(jsons: Seq[JsArray], keys: Seq[String], aggrValues: Map[String, AggregateFunc]): JsArray = {
    if (keys.isEmpty) {
      //e.g. global aggregation functions
      val size = jsons.head.value.size
      require(jsons.forall(_.value.size == size))
      return JsArray(
        0 to size map { i =>
          val row = jsons.map(_.value(i))
          row.reduce((l, r) => aggrRecord(l.asInstanceOf[JsObject], r.asInstanceOf[JsObject], aggrValues))
        })
    }

    val map = scala.collection.mutable.Map[Seq[JsValue], JsObject]()
    map ++= jsons.head.value.map { r =>
      val k = keys.map(r \ _).map(_.get)
      k -> r.as[JsObject]
    }

    for (json <- jsons.tail) {
      for (r <- json.value) {
        val ks = keys.map(r \ _).map(_.get)

        map.get(ks) match {
          case Some(oldObj) => map += ks -> aggrRecord(oldObj, r.as[JsObject], aggrValues)
          case None => map += ks -> r.as[JsObject]
        }
      }
    }
    JsArray(map.values.toSeq)
  }

  private def aggrRecord(left: JsObject, right: JsObject, aggrValues: Map[String, AggregateFunc]): JsObject = {
    val updatedObj = JsObject(aggrValues.map { case (name, func) =>
      func match {
        case Count | Sum =>
          name -> JsNumber((left \ name).as[JsNumber].value + (right \ name).as[JsNumber].value)
        case Max => ???
        case Min => ???
        case topk: TopK => ???
        case Avg => ???
        case DistinctCount => ???
      }
    })
    left ++ updatedObj
  }

  private def orderJsArray(mergedArray: JsArray, orderOn: Map[String, SortOrder.Value]): JsArray = {
    //ref http://stackoverflow.com/q/39152687/2598198
    def lt[A <: JsValue : Ordering](left: A, right: A, order: SortOrder.Value): Boolean = {
      import Ordering.Implicits._
      if (left < right) {
        order == SortOrder.ASC
      } else if (left > right) {
        order == SortOrder.DSC
      } else {
        false
      }
    }

    def ltObj(left: JsValue, right: JsValue): Boolean = {

      implicit val jsNumberOrdering: Ordering[JsNumber] = Ordering.by(_.value)
      implicit val jsStringOrdering: Ordering[JsString] = Ordering.by(_.value)

      orderOn.foreach { case (name, order) =>
        ((left \ name).get, (right \ name).get) match {
          case (lNum: JsNumber, rNum: JsNumber) =>
            if (lNum.value != rNum.value) {
              return lt(lNum, rNum, order)
            }
          case (lStr: JsString, rStr: JsString) =>
            if (lStr.value != rStr.value) {
              return lt(lStr, rStr, order)
            }
          case _ => ???
        }
      }
      false // equal
    }

    JsArray(mergedArray.value.sortWith(ltObj))
  }

  private def projectArray(jsArray: JsArray, project: Set[String]): JsArray = {
    JsArray(jsArray.value.map(obj => JsObject(obj.asInstanceOf[JsObject].fields.filter(e => project.contains(e._1)))))
  }

//hand avg function for ProgressiveSolver
  def handleAvg(mergedResults:Seq[JsArray]):Seq[JsArray] =
  {   
    val newmergedresults = mergedResults.map(
      x => {
        val y = x.value.map(r => {
          var mergfield : List[Array[String]]=List()
          //System.out.println("--------------------------------r=-----------"+r)
          val record = r.as[JsObject]
          if (mergfield.size ==0)
          {
            record.keys.map(field =>{
              if (field.startsWith("__count__") || field.startsWith("__sum__"))
              {
                val realfield = field.replaceAll("__sum__","").replaceAll("__count__","")
                val newfield=  Array(realfield,"__count__"+realfield,"__sum__"+realfield)
                var bExist =false
                mergfield.map(a =>{
                  if (a(0) == realfield)
                  {
                    bExist =true;
                  }
                })
                if (! bExist)
                  mergfield =  newfield +: mergfield
              }
            })
          }
          //merge sum and count to avg
          if (mergfield.size  >0)
          {
            var outjson = r.toString()
            mergfield.map(f =>{
              val count = (r \ f(1)).as[JsNumber]
              val sum = (r \ f(2)).as[JsNumber]
              val avg = (sum.toString().toDouble *1.0) / count.toString().toDouble
             // System.out.println("=====count="+count+",sum="+sum+",avg="+avg)
              //remove count
              outjson =  outjson.replaceAll("\""+f(1)+"\":"+count+",", "")
              //replace sum with avg
              outjson =  outjson.replaceAll("\""+f(2)+"\":"+sum, "\""+f(0)+"\":"+avg)
            })
            Json.parse(outjson)
          }
          else
            r

        })
        JsArray(y)
      }
    )
    //System.out.println("--------------------------------newmergedresults=-----------"+newmergedresults)
    newmergedresults
  }

  //hand avg function for RESTSolver
  def handleAvg(mergedResults:JsArray):JsValue =
  {
    //handle avg function, merge the __sum__ and __count__ fields into one   field
    //{"dd":"2007-09","__count__v":5,"__sum__v":46.9} ==> {"dd":"2007- 09","v":46.9/5}
    val y = mergedResults.value.map(rows => {
      rows.as[JsArray].value.map( r => {
        var mergfield : List[Array[String]]=List()
        //System.out.println("--------------------------------r=-----------"+r)
        val record = r.as[JsObject]
        if (mergfield.size ==0)
        {
          record.keys.map(field =>{
            if (field.startsWith("__count__") || field.startsWith("__sum__") )
            {
              val realfield = field.replaceAll("__sum__","").replaceAll("__count__","")
              val newfield=  Array(realfield,"__count__"+realfield,"__sum__"+realfield)
              var bExist =false
              mergfield.map(a =>{
                if (a(0) == realfield)
                {
                  bExist =true;
                }
              })
              if (! bExist)
                mergfield =  newfield +: mergfield
            }
          })
        }
        //merge sum and count to avg
        if (mergfield.size > 0) {
          var outjson = r.toString()
          mergfield.map(f =>{
            val count = (r \ f(1)).as[JsNumber]
            val sum = (r \ f(2)).as[JsNumber]
            val avg = (sum.toString().toDouble *1.0) / count.toString().toDouble
           // System.out.println("=====count="+count+",sum="+sum+",avg="+avg)
            //remove count
            outjson =  outjson.replaceAll("\""+f(1)+"\":"+count+",", "")
            //replace sum with avg
            outjson =  outjson.replaceAll("\""+f(2)+"\":"+sum, "\""+f(0)+"\":"+avg)
          })
          Json.parse(outjson)
        }
        else
          r
      })
    })
    Json.parse(Json.toJson(y).toString())
  }

}
