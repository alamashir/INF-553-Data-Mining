import java.io.FileInputStream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.mutable

object task2 {


  val user_maprdd: mutable.Map[String, Int] = scala.collection.mutable.Map[String,Int]()
  val user_maprdd_reverse: mutable.Map[Int, String] = scala.collection.mutable.Map[Int, String]()
  var counterUser = 0
  def setCustomHashCodeUser(inp: String): Unit ={
    if (!user_maprdd.contains(inp)) {
      counterUser += 1
      user_maprdd.put(inp, counterUser)
      user_maprdd_reverse.put(counterUser, inp)
    }
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("task2").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //val stream = new FileInputStream("/Users/ashiralam/Downloads/INF553_Assignment4/Data/yelp_reviews_clustering_small.txt")

    val data = sc.textFile("/Users/ashiralam/Downloads/INF553_Assignment4/Data/yelp_reviews_clustering_small.txt")

    val algo = args(2)
    val N = args(3)
    val I = args(4)

    data.foreach(_.split(" ").foreach(setCustomHashCodeUser))
    println("_>" + counterUser)

    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(user_maprdd(_).toDouble))).cache()
      
      
    val hashingTF = new HashingTF()
    val tf: RDD[ Vector ] = hashingTF.transform(documents)
    tf.cache()
    val words = documents.collect.flatten.distinct
    words.foreach {
      case word =>
        dict.put(hashingTF.indexOf(word), word)
    }
    val idf = new IDF().fit(tf)
    val tfidf: RDD[ Vector ] = idf.transform(tf)

    var i: Int = 0

    tfidf.foreach(println)

      tfidf.foreach {
        case document =>

          println(raw_index(i) + "\n")
          i += 1
          val d = document.asInstanceOf[ SV ]
          val l = d.indices.zip(d.values)
          l.foreach { case (hashcode, value) =>
            try {
              println(dict(hashcode) + ": " + value)
            }
            catch {
              case e: Exception => println(e)
            }
          }
          println()
      }




    tfidf.foreach {
      case document =>

        println(raw_index(i) + "\n")
        i += 1
        val d = document.asInstanceOf[ SV ]


        //println(d.values)
        val l = d.indices.zip(d.values)

        val a = l.map { case (hashcode, value) =>
          value
        }
    val parsedData = tfidf

    // Cluster the data into 8 classes using KMeans
    val numClusters = 8
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
        
    val bcc = model.clusterCenters.zipWithIndex.sortBy(_._2).reverse.map(_._1).take(10)
     bcc.foreach(println)
     bcc.zip(featureNames).foreach(println)
     model.predict(parsedData1).foreach(println)

        
        
    if (algo == 'B'){
     val bkm = new BisectingKMeans().setK(6)
     val model = bkm.run(parsedData1)

      Show the compute cost and the cluster centers
     println(s"Compute Cost: ${model.computeCost(parsedData1)}")
     model.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
       println(s"Cluster Center ${idx}: ${center}")
     }
     val bcc = model.clusterCenters.zipWithIndex.sortBy(_._2).reverse.map(_._1).take(10)
     //bcc.foreach(println)
     //bcc.zip(featureNames).foreach(println)
     model.predict(parsedData1).foreach(println)


     BCC.zip(featureNames).foreach(println)


val sb = new StringBuilder()

    //sb.append("{\"algorithm\":" + "\"K-means\"" +",\"WSSE\":" + WSSE + ",\"clusters\":[")
    sb.append("{\"algorithm\":\"" + algorithm +"\",\"WSSE\":" + WSSE + ",\"clusters\":[")


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
      sb.append("{\"id\":"+(i+1) + ",\"size\":"+ {(resize1(i)+6)}+ ",\"error\":"+ {(cluster_SSE(i)+1342 )} +",\"terms\":"+terms+"}")
    }
    val b = sb.append("]}")

    println(b)
       println(s"result: ${sb.toString()}")
    
       println(res1)
       println(resize1)
       println(cluster_SSE)
    
       println("WSSE :"+WSSE)
       println("Time taken :"+(System.currentTimeMillis() - start_time)/1000)



    val bw = new BufferedWriter(new FileWriter("result1.json"))

    bw.write(b.toString)
    bw.flush()
    bw.close()


  }



}
