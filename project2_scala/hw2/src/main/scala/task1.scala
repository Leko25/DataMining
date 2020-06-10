import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.util.Sorting.quickSort
import scala.util.control.Breaks._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

//noinspection DuplicatedCode
class Bitmap(val n: Int) {
  private var bitVector: Array[Int] = Array.fill[Int]((n >> 5) + 1)(0)

  def get(pos: Int): Boolean = {
    val idx = pos >> 5

    val bitNum = pos & 0x1F

    val flag = (bitVector(idx) & ( 1 << bitNum)) != 0

    flag
  }

  def setBit(pos: Int): Unit = {
    val idx = pos >> 5

    val bitNum = pos & 0x1F

    bitVector(idx) |= 1 << bitNum
  }
}

//noinspection DuplicatedCode
object task1 {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("Duration: " + (t1 - t0)/1e9d)
    result
  }

  def convertValuesToList(values: Iterable[String]): Array[String] = {
    val data = values.toArray
    val mySet = collection.mutable.Set[String]()
    for (i <- data) {
      mySet += i
    }
    mySet.toArray
  }

  def getChunkSupport(partitionSize: Int, support: Int, basketSize: Long): Double = {
    Math.ceil(support * partitionSize/basketSize.toInt)
  }

  def getSingletons(itemSets: Array[String], freqItemSet: List[List[String]]): List[String] = {
    //TODO: PLEASE confirm types
    val freqItemSetList: ArrayBuffer[String] = ArrayBuffer()
    for (pairs <- freqItemSet) {
      for (pair <- pairs)
        freqItemSetList += pair
    }
    val intersectionSet: Set[String] = itemSets.toSet.intersect(freqItemSetList.toSet)
    val intersectionArr = intersectionSet.toArray
    quickSort(intersectionArr)
    intersectionArr.toList
  }

  def pcy(baskets: Iterator[Array[String]], basketSize: Long, bucketSize: Long, support: Int): Iterator[List[List[String]]] = {
    val basketsArr = baskets.toArray
    val partitionSize = basketsArr.length

    val ps: Double = getChunkSupport(partitionSize, support, basketSize)

    var (freqItems, bitmap) = firstPass(basketsArr, ps, bucketSize)

    var candidateDict = collection.mutable.Map[Int, List[List[String]]]()
    var idx = 1

    var singlesList: ListBuffer[List[String]] = ListBuffer()
    for (v <- freqItems) {
      singlesList += List(v)
    }
    candidateDict += (idx -> singlesList.toList)
    breakable {
      while (true) {
        idx += 1
        var pairsCount = collection.mutable.Map[String, Int]()
        var freqPairs = ListBuffer[List[String]]()
        var hashTable = collection.mutable.Map[Long, Int]()

        for (basket <- basketsArr) {
          //TODO: Please confirm types
          val itemSets = getSingletons(basket, candidateDict(1))
          val pairs = itemSets.combinations(idx)
          val nextPairs = itemSets.combinations(idx + 1)

          for (pair <- pairs) {
            val hashVal = getHash(pair, bucketSize)
            if (bitmap.get(hashVal.toInt)) {
              val key = pair.mkString(" ")
              if (!(pairsCount isDefinedAt key)) {
                pairsCount += (key -> 1)
              } else {
                var count = pairsCount(key)
                count += 1
                pairsCount += (key -> count)
              }
            }
          }

          for (nextPair <- nextPairs) {
            val hashVal = getHash(nextPair, bucketSize)
            if (!(hashTable isDefinedAt hashVal)) {
              hashTable += (hashVal -> 1)
            } else {
              var count = hashTable(hashVal)
              count += 1
              hashTable += (hashVal -> count)
            }
          }
        }

        //Get frequent pairs
        for ((k, v) <- pairsCount) {
          if (v >= ps)
            freqPairs += k.split("\\s+").toList
        }

        // Break loop if no candidate pairs exist
        if (freqPairs.isEmpty)
          break

        // Add frequent pairs to candidate list
        candidateDict += (idx -> freqPairs.toList)

        // Create new bit vector
        val tempBitmap = new Bitmap(bucketSize.toInt)
        for ((k, v) <- hashTable) {
          if (v >= ps)
            tempBitmap.setBit(k.toInt)
        }

        // Reset bit vector
        bitmap = tempBitmap
      }
    }
    val result: ListBuffer[List[String]] = ListBuffer()
    for ((_, v) <- candidateDict) {
      for (pairs <- v) {
        result += pairs
      }
    }
    Iterator(result.toList)
  }

  def permutationSet(args: Array[String]): Array[String] = {
    quickSort(args)
    args
  }

  def getHash(args: List[String], bucketSize: Long): Long = {
    var hashSum = 0
    for (arg <- args) {
      hashSum += arg.toInt
    }
    val hash = hashSum.hashCode() % bucketSize
    hash
  }

  def firstPass(basketsArr: Array[Array[String]], ps: Double, bucketSize: Long): (collection.mutable.Set[String], Bitmap) = {

    var singlesCount = collection.mutable.Map[String, Int]()
    var hashTable = collection.mutable.Map[Long, Int]()
    val freqItemSets = collection.mutable.Set[String]()

    for (items <- basketsArr) {
      for (item <- items) {
        if (!(singlesCount isDefinedAt item)) {
          singlesCount += (item -> 1)
        } else {
          var count = singlesCount(item)
          count += 1
          singlesCount += (item -> count)
        }
      }

      // Hash pairs
      for (idx1 <- 0 until items.length - 1) {
        for (idx2 <- (idx1 + 1) until items.length) {
          val pairs = permutationSet(Array(items(idx1), items(idx2)))
          val hashVal: Long = getHash(pairs.toList, bucketSize)
          if (!(hashTable isDefinedAt hashVal)) {
            hashTable += (hashVal -> 1)
          } else {
            var count = hashTable(hashVal)
            count += 1
            hashTable += (hashVal -> count)
          }
        }
      }
    }

    // Get frequent item sets
    for ((k, v) <- singlesCount) {
      if (v >= ps) {
        freqItemSets += k
      }
    }

    // Convert hash table to bitmap
    val bitmap = new Bitmap(bucketSize.toInt)
    for((k, v) <- hashTable) {
      if (v >= ps) {
        bitmap.setBit(k.toInt)
      }
    }
    (freqItemSets, bitmap)
  }

  def lexicographical(pairs: List[String]): List[String] = {
    val sortedPairs = pairs.sorted
    sortedPairs
  }

  def son(itemSets: Array[String], candidates: Array[List[String]]): List[(List[String], Int)] = {
    var itemsCount = collection.mutable.Map[List[String], Int]()
    for (candidate <- candidates) {
      if (candidate.toSet.subsetOf(itemSets.toSet)) {
        if (!(itemsCount isDefinedAt candidate)) {
          itemsCount += (candidate -> 1)
        } else {
          var count = itemsCount(candidate)
          count += 1
          itemsCount += (candidate -> count)
        }
      }
    }
    itemsCount.toList
  }

  def makeString(listItems: Array[List[String]]): String = {
    var resultStr = ""
    var idx = 1
    for (listItem <- listItems) {
      var stream = "("
      if (listItem.length == idx) {
        for (i <- listItem.indices) {
          if (listItem(i).contentEquals(listItem.last))
            stream += "\'" + listItem(i) + "\'),"
          else
            stream += "\'" + listItem(i) + "\', "
        }
        resultStr += stream
      } else {
        idx += 1
        resultStr = resultStr.substring(0, resultStr.length - 1) + "\n\n"

        for (i <- listItem.indices) {
          if (listItem(i).contentEquals(listItem.last))
            stream += "\'" + listItem(i) + "\'),"
          else
            stream += "\'" + listItem(i) + "\', "
        }
        resultStr += stream
      }
    }
    resultStr
  }

  def myFileWriter(candidates: Array[List[String]], freqItemSets: Array[List[String]], outputFile: String): Unit = {
    var resultStr = "Candidates:\n"

    val candidatesStr = makeString(candidates)
    resultStr += candidatesStr

    resultStr = resultStr.substring(0, resultStr.length - 1) + "\n\n"
    resultStr += "Frequent Itemsets:\n"

    val freqItemSetsStr = makeString(freqItemSets)
    resultStr += freqItemSetsStr

    resultStr = resultStr.substring(0, resultStr.length - 1) + "\n"

    val writer = new FileWriter(outputFile)
    writer.write(resultStr)
    writer.close()
  }

  def driver(caseNumber: Int, support: Int, inputFile: String, outputFile: String): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Task1")
    val lines = sc.textFile(inputFile, 1)

    if (caseNumber == 1) {
      val rdd = lines.map(x => (x.split(",")(0), x.split(",")(1)))
        .filter(x => !x._1.contentEquals("user_id"))
        .groupByKey()
        .mapValues(x => convertValuesToList(x))
        .map(x => x._2).cache()

      val basketSize = rdd.count()
      val bucketSize = basketSize * 2

      val candidates = rdd.mapPartitions(chunk => pcy(chunk, basketSize, bucketSize, support))
        .flatMap(pairs => pairs)
        .distinct().sortBy(pairs => (pairs.length, pairs.toString)).collect()

      val freqItemSets = rdd.flatMap(itemSet => son(itemSet, candidates))
        .reduceByKey((x, y) => x + y)
        .filter(pairs => pairs._2 >= support)
        .sortBy(pairs => (pairs._1.length, pairs._1.toString)).map(pairs => pairs._1).collect()

      myFileWriter(candidates, freqItemSets, outputFile)
    } else {
      val rdd = lines.map(x => (x.split(",")(1), x.split(",")(0)))
        .filter(x => !x._1.contentEquals("business_id"))
        .groupByKey()
        .mapValues(x => convertValuesToList(x))
        .map(x => x._2).cache()

      val basketSize = rdd.count()
      val bucketSize = 100

      val candidates = rdd.mapPartitions(chunk => pcy(chunk, basketSize, bucketSize, support))
        .flatMap(pairs => pairs)
        .distinct().sortBy(pairs => (pairs.length, pairs.toString)).collect()

      val freqItemSets = rdd.flatMap(itemSet => son(itemSet, candidates))
        .reduceByKey((x, y) => x + y, 8)
        .filter(pairs => pairs._2 >= support)
        .sortBy(pairs => (pairs._1.length, pairs._1.toString)).map(pairs => pairs._1).collect()

      myFileWriter(candidates, freqItemSets, outputFile)
    }
  }

  def main(args: Array[String]): Unit = {
    val Array(caseNumber, support, inputFile, outputFile) = args
    time(driver(caseNumber.toInt, support.toInt, inputFile, outputFile))
  }
}
