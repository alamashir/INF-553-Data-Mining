import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.twitter._
import twitter4j.Status

import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.collection.mutable.Map

object TwitterStreaming {

  def main(args: Array[String]): Unit = {

    val spark = new SparkConf().setAppName("TwitterStreaming").setMaster("local[*]")
    val sc = new SparkContext(spark)
    sc.setLogLevel(logLevel = "OFF")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    
    System.setProperty("twitter4j.oauth.consumerKey", "YOUR_KEY")
    System.setProperty("twitter4j.oauth.consumerSecret", "YOUR_KEY")
    System.setProperty("twitter4j.oauth.accessToken", "YOUR_KEY")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "YOUR_KEY")

    val ssc = new StreamingContext(sc, Seconds(10))
    val tweets = TwitterUtils.createStream(ssc, None, Array("Data"))
    tweets.foreachRDD(tweetBatches => ProcessTweetBatches(tweetBatches))
    ssc.start()
    ssc.awaitTermination()
  }

  

  def ProcessTweetBatches(tweetBatches : RDD[Status]) : Unit = {


  var N = 0
  val S = 100
  var tweetss = new ListBuffer[Status]()
  var tweetl = 0
  var hashtag = Map[String, Int]()

    val tweets = tweetBatches.collect()
    for(status <- tweets){

      if(N < S){
        tweetss.append(status)
        sampleTweetLength = sampleTweetLength + status.getText.length

        val hashTags = status.getHashtagEntities.map(_.getText)
        for(tag <- hashTags){
          if(hashtag.contains(tag)){
            hashtag(tag) += 1
          }
          else{
            hashtag(tag) = 1
          }
        }
      }

      else{
        val j = Random.nextInt(N)
        if(j < S){
          val tweetToBeRemoved = tweetss(j)
          tweetss(j) = status
          sampleTweetLength = sampleTweetLength + status.getText.length - tweetToBeRemoved.getText.length

          // Removing old hashtags
          val hashTags = tweetToBeRemoved.getHashtagEntities.map(_.getText)
          for(tag <- hashTags){
            hashtag(tag) -= 1
          }

          // Adding new hashtags
          val newHashTags = status.getHashtagEntities.map(_.getText)
          for(tag <- newHashTags){
            if(hashtag.contains(tag)){
              hashtag(tag) += 1
            }
            else{
              hashtag(tag) = 1
            }
          }

          val topTags = hashtag.toSeq.sortWith(_._2 > _._2)
          val size = topTags.size.min(5)

          println("The number of the twitter from beginning: " + (N + 1))
          println("Top 5 hot hashtags:")

          for(i <- 0 until size){
            if(topTags(i)._2 != 0){
              println(topTags(i)._1 + ":" + topTags(i)._2)
            }
          }

          println("The average length of the twitter is: " +  sampleTweetLength/(S.toFloat))
          println("\n\n")
        }
      }
      N = N + 1
    }
  }

  
}

