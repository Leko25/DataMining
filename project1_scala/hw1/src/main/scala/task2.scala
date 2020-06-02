import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import Array._
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Sorting.quickSort
import scala.util.control.Breaks._

//noinspection DuplicatedCode
object task2 extends Serializable {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1e9d + "sec")
    result
  }

  def loadBusiness(jsonObj: JObject): (String, Array[String]) = {
    implicit val formats = DefaultFormats
    val catArr:ArrayBuffer[String] = ArrayBuffer()
    if ((jsonObj \ "categories") != JNull &&(jsonObj \ "categories") != JNothing) {
      if (!(jsonObj \ "categories").extract[String].contentEquals("")) {
        val categories = (jsonObj \ "categories").extract[String].split(",")
        for (category <- categories) {
          catArr += category.trim()
        }
      }
    }
    ((jsonObj \ "business_id").extract[String], catArr.toArray)
  }

  def loadReviews(jsonObj: JObject): (String, (Double, Int)) = {
    implicit val formats = DefaultFormats
    val businessId = (jsonObj \ "business_id").extract[String]
    val stars = (jsonObj \ "stars").extract[Double]
    (businessId, (stars, 1))
  }

  def mapCategory(tuple: (Array[String], (Double, Int))): Array[(String, (Double, Int))] = {
    val catArr: Array[String] = tuple._1
    val starsCount: (Double, Int) = tuple._2
    val catMap: ArrayBuffer[(String, (Double, Int))] = ArrayBuffer()
    for (value <- catArr) {
      catMap += ((value, starsCount))
    }
    catMap.toArray
  }

  type BusinessType = Map[String, Array[String]]
  type ReviewType = Map[String, (Double, Int)]
  def loadBusinessJson(businessFile: String): (BusinessType, ReviewType) = {
    implicit val formats = DefaultFormats
    var idCat = Map[String, Array[String]]()
    var catCount = Map[String, (Double, Int)]()

    val source = Source.fromFile(businessFile)
    for (line <- source.getLines()) {
      val jsonObj = parse(line)

      if ((jsonObj \"categories") != JNull && (jsonObj \ "categories") != JNothing) {
        if (!(jsonObj \ "categories").extract[String].contentEquals("")) {
          val categories = (jsonObj \ "categories").extract[String].split(",")
          val catArr: ArrayBuffer[String] = ArrayBuffer()
          for (category <- categories) {catArr += category.trim()}
          idCat += (jsonObj \ "business_id").extract[String] -> catArr.toArray

          for (category <- catArr) {
            if (!(catCount isDefinedAt category)) {
              catCount += category -> (0.0, 0)
            }
          }
        }
      }
    }
    source.close()
    (idCat, catCount)
  }

  def mapBusinessReviews(reviewFile: String, idCat: task2.BusinessType, catCount: task2.ReviewType): scala.collection.mutable.Map[String, (Double, Int)] = {
    implicit val formats = DefaultFormats
    var newCatCount = scala.collection.mutable.Map[String, (Double, Int)]()

    val source = Source.fromFile(reviewFile)
    for (line <- source.getLines()) {

      val jsonObj = parse(line)

      if (idCat isDefinedAt (jsonObj \ "business_id").extract[String]) {
        val catArr: Array[String] = idCat((jsonObj \ "business_id").extract[String])

        for (category <- catArr) {
          var star: Double = if (newCatCount.contains(category)) newCatCount(category)._1 else catCount(category)._1
          star += (jsonObj \ "stars").extract[Double]

          var count: Int = if (newCatCount.contains(category)) newCatCount(category)._2 else catCount(category)._2
          count += 1

          newCatCount += category -> (star, count)
        }
      }
    }
    source.close()
    newCatCount
  }

  def sortTopAvg(results: Array[(String, Double)], n: Int): String = {
    var avgMap = Map[Double, Array[String]]()

    for (i <- results) {
      if (!(avgMap isDefinedAt i._2)) {
        avgMap += (i._2 -> Array(i._1))
      } else {
        val tempArr: Array[String] = avgMap(i._2)
        val tempBuffer = Array(i._1)
        avgMap += (i._2 -> concat(tempArr, tempBuffer))
      }
    }

    //Resort map by key
    val avgMapSorted = ListMap(avgMap.toSeq.sortWith(_._1 > _._1):_*)
    var count = n

    var str = "["

    breakable {
      for ((k, v) <- avgMapSorted) {
        if (count == 0) {
          break
        }
        quickSort(v) //sort array
        for (i <- v) {
          if (count == 0) {
            break
          }
          str += "[\"" + i + "\", " + k + "], "
          count -= 1
        }
      }
    }
    str = str.substring(0, str.length - 2)
    str += "]"
    str
  }

  def sparkImplementation(reviewFile: String, businessFile: String, n: Int): String = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Task2")

    val businessJson = sc.textFile(businessFile)
    val reviewJson = sc.textFile(reviewFile)

    val businessRDD = businessJson.map(x => parse(x).asInstanceOf[JObject]).map(loadBusiness)
    val reviewsRDD = reviewJson.map(x => parse(x).asInstanceOf[JObject]).map(loadReviews)

    // Natural Join Business and Reviews
    val businessReviews = businessRDD.join(reviewsRDD).cache()

    val categoryStars = businessReviews.filter(x => x._2._1.length != 0)
      .map(x => mapCategory(x._2))
      .flatMap(x => x)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).cache()

    // Compute the average
    val avgStars = categoryStars.mapValues(x => x._1/x._2).sortBy(_._2, ascending = false)
    val results = avgStars.collect()

    val outDict = "result" -> parse(sortTopAvg(results, n))
    compact(render(outDict))
  }

  def implementation(reviewFile: String, businessFile: String, n: Int): String = {
    val (idCat, catCount) = loadBusinessJson(businessFile)

    val joinedCatCount = mapBusinessReviews(reviewFile, idCat, catCount)

    var avgCatDict = Map[String, Double]()

    for ((k, v) <- joinedCatCount) {
      if (v._2 == 0) {
        avgCatDict += k -> 0.0
      } else {
        avgCatDict += k -> v._1/v._2
      }
    }

    //Sort Average Count Dictionary by average value in Descending order
    val avgCatDictSorted = ListMap(avgCatDict.toSeq.sortWith(_._2 > _._2):_*)

    //Convert Map[String, Double] to Array[(String, Double)]
    val avgCatArr: ArrayBuffer[(String, Double)] = ArrayBuffer()

    for ((k, v) <- avgCatDictSorted) {
      avgCatArr += ((k, v))
    }

    val results: String = sortTopAvg(avgCatArr.toArray, n)

    val outDict = "result" -> parse(results)
    compact(render(outDict))
  }


  def main(args: Array[String]): Unit = {

    // Unpack Arguments
    val Array(reviewFile, businessFile, outputFile, ifSpark, n) = args

    var result = ""

    if (ifSpark.contentEquals("spark")) {
      result = time(sparkImplementation(reviewFile, businessFile, n.toInt))
    } else {
      result = time(implementation(reviewFile, businessFile, n.toInt))
    }

    val writer = new FileWriter(outputFile)
    writer.write(result)
    writer.close()
  }
}
