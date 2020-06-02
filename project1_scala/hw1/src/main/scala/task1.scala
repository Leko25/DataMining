import java.io.FileWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{FileSystemNotFoundException, Files, Paths}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import scala.collection.mutable.ListBuffer
import scala.util.Sorting.quickSort
import Array._
import scala.collection.immutable.ListMap

//noinspection DuplicatedCode
object task1 extends Serializable {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1e9d + "sec")
    result
  }

  def loadStopWords(stopWordsFile: String): Option[Array[String]] = {
    try {
      val bytesArray = Files.readAllBytes(Paths.get(stopWordsFile))
      val resultString = new String(bytesArray, StandardCharsets.UTF_8)
      Some(resultString.split("\\s+"))
    } catch {
      case x: FileSystemNotFoundException => {
        println("Invalid file path for Stop words")
        None
      }
    }
  }

  type reviewsType = ((Int, Int), (String, Int), String)
  def mapJsonObj(obj: JObject): reviewsType = {
    implicit val formats = DefaultFormats

    var dateString = (obj \ "date").extract[String]
    var year: Int = dateString.split("-")(0).toInt

    var userId: String = (obj \ "user_id").extract[String]

    var text: String = (obj \ "text").extract[String]

    ((year, 1), (userId, 1), text)
  }

  def filterPunctuation(str: String): Array[String] = {
    val words = str.replaceAll("\\p{Punct}", "")
    words.split("\\s+")
  }

  def sortValues(tuples: Array[(String, Int)]): String = {
    var tempMap = Map[Int, Array[String]]()

    for (i <- tuples) {
      if (!(tempMap isDefinedAt i._2)) {
        tempMap += (i._2 -> Array(i._1))
      } else {
        val tempArr: Array[String] = tempMap(i._2)
        val tempBuffer = Array(i._1)
        tempMap += (i._2 -> concat(tempArr, tempBuffer))
      }
    }

    //Resort map by key
    val tempMapSorted = ListMap(tempMap.toSeq.sortWith(_._1 > _._1):_*)

    var result = "["

    for ((k, v) <- tempMapSorted) {
      quickSort(v) //sort array
      for (i <- v) {
        result += "[\"" + i + "\", " + k + "], "
      }
    }
    result = result.substring(0, result.length - 2)
    result += "]"
    result
  }

  def getResults(inputFile: String, stopWordsFile: String, y: Int, m: Int, n: Int): String = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Task1")

    val lines = sc.textFile(inputFile)
    val rdd = lines.map(x => parse(x).asInstanceOf[JObject]).map(x => mapJsonObj(x)).cache()

    // Determine the reviews in a given year
    val reviewsYear = rdd.filter(x => x._1._1 == y)
      .map(x => x._1).
      reduceByKey((x, y) => x + y)

    // Determine the top m users who have large number of reviews and their count
    val userReviews = rdd.map(x => x._2)
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)

    val totalReviews = rdd.count()
    val uniqueUsers = rdd.map(x => x._2._1).distinct().count()
    val topUsers = userReviews.take(m)

    // Load stopWords
    val stopWords = loadStopWords(stopWordsFile).getOrElse(Array[String]())

    val textRDD = rdd.flatMap(x => filterPunctuation(x._3))
      .map(x => (x.toLowerCase(), 1))
      .filter(x => x._1.matches("[a-zA-Z]+") && (stopWords.indexOf(x._1) == -1)).cache()


    val topWordsRDD = textRDD.reduceByKey((x, y) => x + y, 8)
      .sortBy(_._2, ascending = false)

    val topWords = topWordsRDD.take(n)
    val topWordsListBuffer: ListBuffer[String] = ListBuffer()

    for (i <- topWords) {
      topWordsListBuffer += i._1
    }

    var topWordsList = topWordsListBuffer.toList
    topWordsList = topWordsList.sorted


    val out_dict = ("A" -> totalReviews) ~ ("B" -> reviewsYear.collect()(0)._2) ~
      ("C" -> uniqueUsers) ~ ("D" -> parse(sortValues(topUsers))) ~ ("E" -> topWordsList)

    compact(render(out_dict))
  }
  def main(args: Array[String]): Unit = {
    // Read input arguments
    val Array(inputFile, outputFile, stopWordsFile, y, m, n) = args

    val result = time(getResults(inputFile, stopWordsFile, y.toInt, m.toInt, n.toInt))

    val writer = new FileWriter(outputFile)
    writer.write(result)
    writer.close()
  }
}
