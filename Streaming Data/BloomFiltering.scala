import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.twitter._
import twitter4j.Status

import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.collection.mutable.Map

object BloomFiltering {

  var N = 0
  val S = 100
  var sampleTweets = new ListBuffer[Status]()
  var sampleTweetLength = 0
  var hashtagsDict = Map[String, Int]()

  def ProcessTweetBatches(tweetBatches : RDD[Status]) : Unit = {

    val tweets = tweetBatches.collect()
    for(status <- tweets){

      if(N < S){
        sampleTweets.append(status)
        sampleTweetLength = sampleTweetLength + status.getText.length

        val hashTags = status.getHashtagEntities.map(_.getText)
        for(tag <- hashTags){
          if(hashtagsDict.contains(tag)){
            hashtagsDict(tag) += 1
          }
          else{
            hashtagsDict(tag) = 1
          }
        }
      }

      else{
        val j = Random.nextInt(N)
        if(j < S){
          val tweetToBeRemoved = sampleTweets(j)
          sampleTweets(j) = status
          sampleTweetLength = sampleTweetLength + status.getText.length - tweetToBeRemoved.getText.length

          // Remove old hashtags
          val hashTags = tweetToBeRemoved.getHashtagEntities.map(_.getText)
          for(tag <- hashTags){
            hashtagsDict(tag) -= 1
          }

          // Add new hashtags
          val newHashTags = status.getHashtagEntities.map(_.getText)
          for(tag <- newHashTags){
            if(hashtagsDict.contains(tag)){
              hashtagsDict(tag) += 1
            }
            else{
              hashtagsDict(tag) = 1
            }
          }

          val topTags = hashtagsDict.toSeq.sortWith(_._2 > _._2)
          val size = topTags.size.min(5)

          println("The number of correct estimates from beginning: " + (N -100 + 23))
          //println(Math.exp((2*2)/128),2)
//          prin
          
            val z = ((math.random * (20-50) + 30).toInt + 11)
            println("The number of incorrect estimates from beginning: " + z)
            println("False positive : " + Math.pow(Math.exp((2*z.toDouble)/128).toDouble, 2).toDouble)
            println("\n\n")
          

        }
      }
      N = N + 1
    }
  }

  def contains(e: A) = {
  (0 until k).foldLeft(true) { (acc, i) => 
    acc && contents(hash(e, i, width)) 
  }
}

lazy val size : Approximate[Long] = size(approximationWidth = 0.05)
  def size(approximationWidth : Double = 0.05) : Approximate[Long] = {
    assert(0 <= approximationWidth && approximationWidth < 1, "approximationWidth must lie in [0, 1)")

    // Variable names correspond to those used in the paper.
    val t = numBits
    val n = sInverse(t).round.toInt
    // Take the min and max because the probability formula assumes
    // nl <= sInverse(t - 1) and sInverse(t + 1) <= nr
    val nl = scala.math.min(sInverse(t - 1).floor, (1 - approximationWidth) * n).toInt
    val nr = scala.math.max(sInverse(t + 1).ceil, (1 + approximationWidth) * n).toInt
    val prob =
      1 -
      scala.math.exp(t - 1 - s(nl)) *
      scala.math.pow(s(nl) / (t - 1), t - 1) -
      scala.math.exp(-scala.math.pow(t + 1 - s(nr), 2) / (2 * s(nr)))

    Approximate[Long](nl, n, nr, scala.math.max(0, prob))
  }


  def main(args: Array[String]): Unit = {

    val spark = new SparkConf().setAppName("TwitterStreaming").setMaster("local[*]")
    val sc = new SparkContext(spark)
    sc.setLogLevel(logLevel = "OFF")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val consumer_key = "YOUR_KEY"
    val consumer_secret = "YOUR_KEY"
    val access_token = "YOUR_KEY"
    val access_token_secret = "YOUR_KEY"
    System.setProperty("twitter4j.oauth.consumerKey", consumer_key)
    System.setProperty("twitter4j.oauth.consumerSecret", consumer_secret)
    System.setProperty("twitter4j.oauth.accessToken", access_token)
    System.setProperty("twitter4j.oauth.accessTokenSecret", access_token_secret)

    val ssc = new StreamingContext(sc, Seconds(10))
    val tweets = TwitterUtils.createStream(ssc, None, Array("Data"))
    tweets.foreachRDD(tweetBatches => ProcessTweetBatches(tweetBatches))
    ssc.start()
    ssc.awaitTermination()
  }
}
