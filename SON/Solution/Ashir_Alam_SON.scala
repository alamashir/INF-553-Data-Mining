import java.io._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import util.control.Breaks._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{HashMap, ListBuffer}
import org.apache.spark.rdd.RDD

object task1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val start = System.currentTimeMillis()

    val sparkConf = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val raw = sc.textFile("/Users/ashiralam/Downloads/inf553_assignment3/Data/yelp_reviews_large.txt")
    //raw.take(10).foreach(println)
    var supportthreshold  = 120000


    //data.foreach(println)
    val data = raw.map(row => row.split(","))

    //data.take(100).foreach(println)


    //baskets creation
    
    var baskets = data.map(x => (x(0), x(1))).groupByKey().map(_._2.toSet)
    //baskets.foreach(println)
    var n = raw.getNumPartitions
    //println(n)
    var threshhold = 1
    if (supportthreshold/n >threshhold){
      threshhold = supportthreshold/n
    }

    //all sets
    var sets = baskets.mapPartitions(chunk => {
      apriori(chunk, threshhold)
    }).map(x=>(x,1)).reduceByKey((v1,v2)=>1).map(_._1).collect()
    
    val output_counts = baskets.mapPartitionsWithIndex((index, x) => {
        var listOfLists = ListBuffer.empty[(List[String])]
        x.toList.map(p => listOfLists += p._2.toList).iterator
        countRealOccurences(listOfLists, v, index).toIterator
      }).reduceByKey((a, b) => a + b).filter(m => m._2.toDouble >= supp).map(elem => elem._1).sortBy(elem => elem.size)





    //println(sets.size)
      final val numPartitions = new IntParam(
    this, "numPartitions", "the number of partitions", ParamValidators.ltEq(1)
  )
  setDefault(numPartitions -> 1)

  /** @group expertGetParam */
  def getNumPartitions: Int = $(numPartitions)

  
  final val delimiter = new Param[String](
    this, "delimiter", "delimiter"
  )
  setDefault(delimiter -> ",")

  
  def getDelimiter: String = $(delimiter)
}

    sort(sets).foreach(println)

    val br = sc.broadcast(sets)

    var secondmap = baskets.mapPartitions(chunk => {
      var chunklist = chunk.toList
      var out = List[(Set[String],Int)]()
      for (i<- chunklist){
        for (j<- br.value){
          if (j.forall(i.contains)){
            out = Tuple2(j,1) :: out
          }
        }
      }
      out.iterator
    })
    var last = secondmap.reduceByKey(_+_).filter(_._2 >= supportthreshold).map(_._1).map(x => (x.size,x)).collect()
    val max = last.maxBy(_._1)._1
    val writer = new PrintWriter(new File("result.txt"))
    ar finalOutput = ""
      var sizeSeen = 1;
      var ts = listAns.map(e => {
        var lala = "(";

        if (e.size > sizeSeen) {
          sizeSeen = e.size
          lala = "\n("
          finalOutput = finalOutput.stripSuffix(", ")
        }
        for ((x, i) <- e.zipWithIndex) {

          lala += x
          if (i != e.size - 1) {
            lala += ", "
          }


        }
        lala += "), "
        finalOutput += lala
        lala
      })
      ts.size
      finalOutput = finalOutput.stripSuffix(", ")
    writer.write(pr)
    writer.close()
    tosort.foreach(println)
    val end = System.currentTimeMillis()
    println("Time: " + (end - start)/1000 + " secs")
  }




  def sort[A : Ordering](col: Seq[Iterable[A]]) = col.sorted
  def apriori(basket: Iterator[Set[String]], threshold:Int): Iterator[Set[String]] = {

    var chunklist = basket.toList

    // frequent singleton items
    val single = chunklist.flatten.groupBy(identity).mapValues(_.size).filter(x => x._2 >= threshold).keySet

    var size = 2
    // freq to store the candidate single items for next round
    var freq = single
    // hashmap to store all the valid candidate set
    var hmap = Set.empty[Set[String]]
    // store all the frequent single items to hmap
    for (i <- single){
      hmap = hmap + Set(i)
    }
    // res to store the candidate single items after each round
    var res = Set.empty[String]
    // size of hmap
    var hmapsize = hmap.size

    while (freq.size >= size) {
      var candidate = freq.subsets(size)
      for (tmp <- candidate) {
        breakable {
          var counter = 0
          for (i <- chunklist) {
            if (tmp.subsetOf(i)) {
              counter = counter + 1
              if (counter >= threshold) {
                res = res ++ tmp
                hmap = hmap + tmp
                break
              }
            }
          }
        }
      }

      

    }
    hmap.iterator

  }



}
