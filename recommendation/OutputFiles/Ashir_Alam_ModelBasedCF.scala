import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.recommendation
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

import scala.collection.mutable


object Ashir_Alam_ModelBasedCF {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val start = System.currentTimeMillis()

    val output_file = new PrintWriter(new File("Ashir Alam ModelBasedCF.txt"))

    val sparkConf = new SparkConf().setAppName("item_CF").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile("/Users/ashiralam/Downloads/hw2/Data/train_review.csv")


    //remove header
    val train_header = data.first()
    val ratings = data.filter(elem => elem != train_header)
    val pre_training = ratings.map(_.split(','))



    //get test set
    val dev_test = sc.textFile("/Users/ashiralam/Downloads/hw2/Data/test_review.csv")
    val test_header = dev_test.first()
    var testing = dev_test.filter(elem => elem != test_header).map(_.split(','))


    val test_map1  = testing.map(d=>((d(0),d(1)),d(2)))
    //test_map.foreach(println)
    val train_map1 = pre_training.map(d=>((d(0),d(1)),d(2)))
    //train_map.take(10).foreach(println)
    //finally

    val totalrdd = train_map1++test_map1
    //    totalrdd.take(10).foreach(println)
    //    println("...")
    //Populated cache
    train_map1.collect().foreach(elem => populateHashUser(elem._1._1))
    train_map1.collect().foreach(elem => populateHashBusiness(elem._1._2))
    test_map1.collect().foreach(elem => populateHashUser(elem._1._1))
    test_map1.collect().foreach(elem => populateHashBusiness(elem._1._2))


    val tr_ratings = pre_training.map(d=> Rating(getCustomHashCodeUser (d(0), typ=false) ,getCustomHashCodeBusiness(d(1), typ=false) ,d(2).toDouble))


    val te_ratings  = testing.map(d=>Rating(getCustomHashCodeUser(d(0), typ=false) ,getCustomHashCodeBusiness(d(1), typ=false),d(2).toDouble))

    val rank = 2
    val numIterations = 20
    val lamda = 0.3

    val model = ALS.train(tr_ratings, rank, numIterations, lamda)

    val usersProducts = te_ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val ratesAndPreds = te_ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    //ratesAndPreds.take(10).foreach(println)

    val count0 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 0 && elem._2 < 1).count()
    val count1 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 1 && elem._2 < 2).count()
    val count2 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 2 && elem._2 < 3).count()
    val count3 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 3 && elem._2 < 4).count()
    val count4 = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.filter(elem => elem._2 >= 4).count()
    println(">=0 and <1: "+ count0)
    println(">=1 and <2: "+ count1)
    println(">=2 and <3: "+ count2)
    println(">=3 and <4: "+ count3)
    println(">=4: " +count4)

    //ratesAndPreds.foreach(println)

    output_file.print("User_Id,Business_Id,Pred_rating" + "\n")
//
    val iter = ratesAndPreds.sortByKey().map(elem => user_maprdd_reverse(elem._1._1) + "," + business_maprdd_reverse(elem._1._2) + "," + elem._2._2).toLocalIterator
    while(iter.hasNext) {
      output_file.print(iter.next() + "\n")
    }




    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()
    val RMSE = Math.sqrt(MSE)
    println("RMSE "+RMSE)

    val end = System.currentTimeMillis()
    val time = (end - start)/1000
    print ("Time: " + time)



  }


  def populateHashUser(inp: String): Unit ={
    getCustomHashCodeUser(inp, typ = true)
  }

  val user_maprdd: mutable.Map[String, Int] = scala.collection.mutable.Map[String,Int]()
  val user_maprdd_reverse: mutable.Map[Int, String] = scala.collection.mutable.Map[Int, String]()
  var counterUser: Int = 0
  def getCustomHashCodeUser(inp: String, typ: Boolean): Int ={
    if(user_maprdd.contains(inp)){
      user_maprdd(inp)
    }else if(typ){
      counterUser+=1
      user_maprdd.put(inp, counterUser)
      user_maprdd_reverse.put(counterUser, inp)
      counterUser
    }else{
      //hack
      0
    }
  }
  def populateHashBusiness(inp: String): Unit ={
    getCustomHashCodeBusiness(inp, typ = true)
  }

  val business_maprdd: mutable.Map[String, Int] = scala.collection.mutable.Map[String,Int]()
  val business_maprdd_reverse: mutable.Map[Int, String] = scala.collection.mutable.Map[Int, String]()
  var counterBusiness: Int = 0
  def getCustomHashCodeBusiness(inp: String, typ: Boolean): Int ={
    if(business_maprdd.contains(inp)){
      business_maprdd(inp)
    }else if(typ){
      counterBusiness+=1
      business_maprdd.put(inp, counterBusiness)
      business_maprdd_reverse.put(counterBusiness, inp)
      counterBusiness
    }else{
      //hack
      0
    }
  }



}
