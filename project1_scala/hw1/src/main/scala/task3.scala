import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import scala.collection.mutable.ListBuffer

//noinspection DuplicatedCode
object task3 extends Serializable {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1e9d + "sec")
    result
  }

  def mapBusinessID(jsonObj: JObject): (String, Int) = {
    implicit val formats = DefaultFormats
    ((jsonObj \ "business_id").extract[String], 1)
  }

  def default(inputFile: String, numPartitions: Int, n: Int): String = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Task3")

    val lines = sc.textFile(inputFile).coalesce(numPartitions).cache()

    val rdd = lines.map(x => parse(x).asInstanceOf[JObject]).map(mapBusinessID)

    val totalPartitions = rdd.partitions.length

    // Fetch partitions array and convert it to a list
    val nItems = rdd.glom().map(x => x.length).collect()
    val nItemsList: ListBuffer[Int] = ListBuffer()

    for (item <- nItems) {
      nItemsList += item
    }

    val businessSum = rdd.reduceByKey((x, y) => x + y)

    val popularBusinesses = businessSum.filter(x => x._2 > n)

    val results = popularBusinesses.collect()

    // Write results as [[String1, Int1], [String2, Int2]]

    var strResult = "["
    for (value<- results) {
      strResult += "[\"" + value._1 + "\", " + value._2 + "], "
    }
    strResult = strResult.substring(0, strResult.length - 2)
    strResult += "]"

    val outputDict = ("n_partitions" -> totalPartitions) ~
      ("n_items" -> nItemsList.toList) ~ ("result" -> parse(strResult))
    compact(render(outputDict))
  }

  def custom(inputFile: String, numPartitions: Int, n: Int): String = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Task3")

    val lines = sc.textFile(inputFile)

    val rdd = lines.map(x => parse(x).asInstanceOf[JObject])
      .map(mapBusinessID)
      .partitionBy(new HashPartitioner(numPartitions)).cache()

    val totalPartitions = rdd.partitions.length

    val nItems = rdd.glom().map(x => x.length).collect()
    val nItemsList: ListBuffer[Int] = ListBuffer()

    for (item <- nItems) {
      nItemsList += item
    }

    val businessSum = rdd.reduceByKey((x, y) => x + y, 8).cache()

    val popularBusinesses = businessSum.filter(x => x._2 > n)

    val results = popularBusinesses.collect()

    // Write results as [[String1, Int1], [String2, Int2]]

    var strResult = "["
    for (value<- results) {
      strResult += "[\"" + value._1 + "\", " + value._2 + "], "
    }
    strResult = strResult.substring(0, strResult.length - 2)
    strResult += "]"

    val outputDict = ("n_partitions" -> totalPartitions) ~
      ("n_items" -> nItemsList.toList) ~ ("result" -> parse(strResult))
    compact(render(outputDict))
  }


  def main(args: Array[String]): Unit = {

    //Unpack arguments
    val Array(inputFile, outputFile, partitionType, numPartitions, n) = args
    var result = ""

    if (partitionType.contentEquals("default")) {
      result = time(default(inputFile, numPartitions.toInt, n.toInt))
    } else {
      result = time(custom(inputFile, numPartitions.toInt, n.toInt))
    }

    val writer = new FileWriter(outputFile)
    writer.write(result)
    writer.close()
  }
}
