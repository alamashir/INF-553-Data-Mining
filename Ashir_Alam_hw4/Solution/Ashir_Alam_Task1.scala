import java.io.{File, PrintWriter}
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.IOException

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.collection.mutable

object Task1 {

  def main(args: Array[ String ]): Unit = {

    val start_time = System.currentTimeMillis()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("task2").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val raw = sc.textFile("/Users/ashiralam/Downloads/INF553_Assignment4/Data/yelp_reviews_clustering_small.txt")

    val feature = 'W'

    // number of clusters
    val num_cluster = 5

    // number of iterations
    val max_iter = 20

      
    val unique = raw.flatMap(a=> a.split(" ")).map(x=>(x,1))

    val dict = unique_words.map{case (x,y) => (x,0.0)}
      
      
      
          val res1 = cluster_features.map(a => featureNames.zip(a).sortBy(_._2).reverse.map(_._1).take(10))
    val resize1 = cluster_features.map(a => featureNames.zip(a).sortBy(_._2).reverse.map(_._1).size)
    val sb = new StringBuilder()

    sb.append("{\"algorithm\":" + "\"K-means\"" +",\"WSSE\":" + WSSE + ",\"clusters\":[")

    for(i <- resize1.indices ){
      var terms = "["
      for(j <- res1(i).indices){
        if(j!=0){
          terms += ", "
        }
        terms += "\"" + res1(i)(j) + "\""
        //terms += "{"+"\ res1(i)(j) "}"
      }
      terms += "]"
      if(i!=0){
        sb.append(", ")
      }
      //sb.append(s"{id:$i,size:${resize1(i)},error:${cluster_SSE(i)},terms:$terms}")
      //sb.append("{id:"+i + ",size:"+ {resize1(i)}+ ",error:"+ {cluster_SSE(i)} +",terms:"+terms+"}")
      sb.append("{\"id\":"+(i+1) + ",\"size\":"+ {resize1(i)}+ ",\"error\":"+ {cluster_SSE(i)} +",\"terms\":"+terms+"}")
    }
    val b = sb.append("]}")

    println(b)
    println(s"result: ${sb.toString()}")

    println(res1)
    println(resize1)
    println(cluster_SSE)



    println("WSSE :"+WSSE)
    println("Time taken :"+(System.currentTimeMillis() - start_time)/1000)



      val bw = new BufferedWriter(new FileWriter("result.json"))

        bw.write(b.toString)
        bw.flush()
       bw.close()
      
      
      
  }
    
    
    
    def tf(term: String, doc: mutable.Map[String, Int]): Double = {
        
    var wordCount = 0d
    doc.foreach(x => wordCount += x._2)
    doc(term) / wordCount
  }

  def idf(term: String, allDocs: ArrayBuffer[mutable.Map[String, Int]]): Double = {
    var n = 0d
    allDocs.foreach(doc => {
      if (doc.contains(term)) n += 1
    })

    Math.log10(allDocs.length / n)
  }

  def tfIdf(word: String, docIndex: Int, allDocs: ArrayBuffer[Map[String, Int]]): Double = {
    val doc = allDocs(docIndex)
    tf(word, doc) * idf(word, allDocs)
  }
    
    def Centroid(wt:Map[String, Double]) extends FeatureVector {

  // number of occurrences of each unique noun
  lazy val weightedTerms:Map[String, Double] = wt

  lazy val weightedSum:Double = scala.math.sqrt(weightedTerms.map( (wt) => wt._2 * wt._2 ).sum)

}
    def KMeanCluster(files:List[(String, String)], k:Int)
    {
    
    val documentMap = mutable.Map[Document, Cluster]()

  val clusters:List[Cluster] = selectRandomInitialCluster(k, documents)(documentMap)

  val vectorSpace = VectorSpace()
  documents.foreach( d => vectorSpace.addDimension(d.uniqueNouns))
  logger.info("Vector space dimensions: %s".format(vectorSpace.dimensions.size))

  val mathUtils = MathUtils(vectorSpace)

  def doCluster():List[Cluster] = {
    for ( i <- 0 until iterations) {
      assignDocumentsToClusters()
      updateCentroids()
    }
    clusters
  }
        
 val weightedTerms:Map[String, Double] = nonUniqueWeightedTerms.foldLeft(nonUniqueWeightedTerms) {
    (acc, pair:(String, Double)) =>
      acc.find( currentItem => isPlural(currentItem._1, pair._1)) match {
        case Some((item, value)) =>
          val nacc = acc - pair._1
          val newWeight = value + pair._2
          nacc + (item -> newWeight)
        case None => acc
      }
  }.toMap

 }
    
    val geterror:
    
 def calculateNewCentroid() {
    if (docs.size > 0)
      centroid = Centroid(docs.map( _.weightedTerms ).reduce(_ |+| _).mapValues( _ / docs.size.asInstanceOf[Double]))
  }
    
    
      private def runAlgorithm(
      data: RDD[VectorWithNorm],
      instr: Option[Instrumentation]): KMeansModel = {

    val sc = data.sparkContext

    val initStartTime = System.nanoTime()

    val distanceMeasureInstance = DistanceMeasure.decodeFromString(this.distanceMeasure)

    val centers = initialModel match {
      case Some(kMeansCenters) =>
        kMeansCenters.clusterCenters.map(new VectorWithNorm(_))
      case None =>
        if (initializationMode == KMeans.RANDOM) {
          initRandom(data)
        } else {
          initKMeansParallel(data, distanceMeasureInstance)
        }
    }


      
      val collected = data.mapPartitions { points =>
        val thisCenters = bcCenters.value
        val dims = thisCenters.head.vector.size

        val sums = Array.fill(thisCenters.length)(Vectors.zeros(dims))
        val counts = Array.fill(thisCenters.length)(0L)

        points.foreach { point =>
          val (bestCenter, cost) = distanceMeasureInstance.findClosest(thisCenters, point)
          costAccum.add(cost)
          distanceMeasureInstance.updateClusterSum(point, sums(bestCenter))
          counts(bestCenter) += 1
        }

        counts.indices.filter(counts(_) > 0).map(j => (j, (sums(j), counts(j)))).iterator
      }.reduceByKey { case ((sum1, count1), (sum2, count2)) =>
        axpy(1.0, sum2, sum1)
        (sum1, count1 + count2)
      }.collectAsMap()

      if (iteration == 0) {
        instr.foreach(_.logNumExamples(collected.values.map(_._2).sum))
      }

      val newCenters = collected.mapValues { case (sum, count) =>
        distanceMeasureInstance.centroid(sum, count)
      }

      bcCenters.destroy(blocking = false)

      // Update the cluster centers and costs
      converged = true
      newCenters.foreach { case (j, newCenter) =>
        if (converged &&
          !distanceMeasureInstance.isCenterConverged(centers(j), newCenter, epsilon)) {
          converged = false
        }
        centers(j) = newCenter
      }

      cost = costAccum.value
      iteration += 1
    }

    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
    logInfo(f"Iterations took $iterationTimeInSeconds%.3f seconds.")

    if (iteration == maxIterations) {
      logInfo(s"KMeans reached the max number of iterations: $maxIterations.")
    } else {
      logInfo(s"KMeans converged in $iteration iterations.")
    }

    logInfo(s"The cost is $cost.")

    new KMeansModel(centers.map(_.vector), distanceMeasure, cost, iteration)
  }

def getEuclideanDistance(vector_1:List[Double],vector_2:List[Double]): Double =
  {
    val a = vector_1 zip vector_2
    val b = zippedVector.map({case(x,y) => (x-y)*(x-y)}).sum
    return math.sqrt(distance)
  }
}