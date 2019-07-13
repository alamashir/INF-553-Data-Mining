import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

object task2hash{
  def main(args: Array[String]): Unit = {

       Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
       Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val t1 = System.nanoTime
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

    val totalrdd = test_map1++train_map1
    //    totalrdd.take(10).foreach(println)
    //    println("...")
    //Populated cache
    totalrdd.collect().foreach(elem => populateHashUser(elem._1._1))
    totalrdd.collect().foreach(elem => populateHashBusiness(elem._1._2))


    val train_map = pre_training.map(d=>((getCustomHashCodeUser(d(0), typ=false),getCustomHashCodeBusiness(d(1), typ=false)),d(2).toDouble))

    val test_map  = testing.map(d=>((getCustomHashCodeUser(d(0), typ=false),getCustomHashCodeBusiness(d(1), typ=false)),d(2).toDouble))
    val test_map_collected  = test_map.collectAsMap()

    //    test_map.foreach(println)
    //
    //    println("train:")
    //    train_map.take(10).foreach(println)
    //    println("t:")
    //    pre_training.take(10).foreach(elem => println(elem(0),elem(1),elem(2)))
    //    println("test:")
    //    test_map.take(10).foreach(println)
    //    println("t:")
    //    testing.take(10).foreach(elem => println(elem(0),elem(1),elem(2)))


    val filtered_training = train_map

    val user_to_business = filtered_training.map(elem=>elem._1)
    val user_to_business_rdd = user_to_business.groupByKey().collectAsMap()

    //user_to_business_rdd.take(10).foreach(println)




    val userTobusinessRating = filtered_training.map(elem=>(elem._1._1.toInt,(elem._1._2,elem._2.toDouble)))
    //userTobusinessRating.foreach(println)
    val userbusinessToRating = filtered_training.map(elem=>((elem._1._1,elem._1._2),elem._2.toDouble)).collectAsMap().toMap
    //userbusinessToRating.toMap
    //    userTobusinessRating.take(10).foreach(println)\


    //
    val userTobusinessRating_joined  = userTobusinessRating.join(userTobusinessRating)
    //    userTobusinessRating_joined.take(10).foreach(println)
    //
    //   // manage the duplicates
    val cleaned_userTobusinessRating_joined = userTobusinessRating_joined.filter(elem=>{
      elem._2._1._1 < elem._2._2._1
    })
    //    cleaned_userTobusinessRating_joined.take(10).foreach(println)

    val finale  = cleaned_userTobusinessRating_joined.map(elem=>((elem._2._1._1,elem._2._2._1),elem._1))
    val corated_users = finale.groupByKey()
    //userbusinessToRating.f

    //    println(corated_users.count())
    //corated_users.take(30).foreach(x=>println(x))


    val similarity_between_items = corated_users.map(elem=> {

      val item1 = elem._1._1
      val item2 = elem._1._2
      //          val a = userbusinessToRating.map(elem=>(elem,elem._1._1)).map(elem => elem._2)

      val users_who_have_rated_both_items = elem._2

      var average_item1: Double = 0.0
      var average_item2: Double = 0.0
      //
      users_who_have_rated_both_items.foreach(elem => {
        //
        average_item1 += userbusinessToRating((elem, item1))
        average_item2 += userbusinessToRating((elem, item2))


      })

      average_item1 = average_item1/users_who_have_rated_both_items.size
      average_item2 = average_item2/users_who_have_rated_both_items.size


      var numerator : Double = 0.0
      var denominator_left : Double  = 0.0
      var denominator_right : Double  = 0.0
      users_who_have_rated_both_items.foreach(elem=>{

        numerator += (userbusinessToRating((elem,item1)) - average_item1)*(userbusinessToRating((elem,item2)) - average_item2)
        denominator_left += scala.math.pow(userbusinessToRating((elem,item1)) - average_item1,2)
        denominator_right += scala.math.pow(userbusinessToRating((elem,item2)) - average_item2,2)


      })
      //
      //
      denominator_left = math.sqrt(denominator_left)
      denominator_right = math.sqrt(denominator_right)

      val denominator = (denominator_left * denominator_right)

      var sim = 0.0
      if (!(numerator == 0.toDouble || denominator == 0.toDouble)){
        sim = numerator/denominator

      }

      (elem._1,sim)

    }).collect().toMap//collectAsMap()
    //sc.parallelize(similarity_between_items)

    println("calculated similarities")
    //    similarity_between_items.take(10).foreach(println)
    //

    val toTest = testing.map(d=>(getCustomHashCodeUser(d(0), typ=false) ,getCustomHashCodeBusiness(d(1), typ=false)))

    //toTest.foreach(println)

    val neighbourhoodSize = 2
//
    val predictedOutPutandRealOutput = toTest.map(elem=>{

      val user = elem._1
      val business = elem._2

      val business_this_user_has_rated = user_to_business_rdd(user)



      //business_this_user_has_rated.foreach(println)
      var numberOfItems = neighbourhoodSize
      if (business_this_user_has_rated.size < neighbourhoodSize) {
        numberOfItems = business_this_user_has_rated.size
      }


      var N_most_similar_business: ListBuffer[String] = ListBuffer.empty
      val Dataratings = business_this_user_has_rated.map(x => {
        var curr_sim: Double = -9999999.0
        if (business.toInt < x) {
          if (similarity_between_items.contains(business.toInt, x)) {
            curr_sim = similarity_between_items(business.toInt, x)
          }
        }
        else {
          if (similarity_between_items.contains(business.toInt,x)) {
            curr_sim = similarity_between_items(x, business.toInt)
          }

        }
        (curr_sim, x)

      })

      val sorted = Dataratings.toList.sortBy(e => e._1).reverse

      //println(sorted.size)

      var numberOfItemsDone = 0
      var predictedRating: Double = 0.0
      var num: Double = 0.0
      var den: Double = 0.0
      do {

        num = num + (userbusinessToRating(user.toInt, sorted(numberOfItems)._2.toInt) * sorted(numberOfItems)._1)
        den = den + sorted(numberOfItems)._1
        numberOfItemsDone += 1
      } while (numberOfItemsDone < numberOfItems && numberOfItemsDone + 1 < sorted.size)


      if (num != 0 && den != 0) {
        predictedRating = num / den
      }
      if(predictedRating>5){
        predictedRating = 5
      }else if(predictedRating < 0){
        predictedRating = 0
      }

      (predictedRating, test_map_collected(elem))

    })

    println("hereeeee")

    println(predictedOutPutandRealOutput.count())

    val fffinal = sc.parallelize(predictedOutPutandRealOutput.collect().toSeq)

    val output = testing_withAnswers.map(elem=>{


      item_similarities.map(elem=>{

        numerator += filtered_training_map(user,elem._1) * elem._2
        denominator += math.abs(elem._2)

      })

      var predicted_rating : Double = 0.0
      if(numerator != 0.toDouble && denominator != 0.toDouble){
        predicted_rating = numerator/denominator;
        val sum = item_similarities.map(x=>x._2).sum
        val avgSim = sum/item_similarities.size

      }
      else{


        predicted_rating = user_to_his_average_rating(user)
      }

      (elem._1,elem._2,predicted_rating)
    })

    Thread.sleep(200000)

    //sc.parallelize()
    val wala = sc.parallelize(output.toSeq)
    var toPrint : ListBuffer[(String,String,String)] = ListBuffer.empty
    val pred_format  = wala.map(elem=>(elem._1._1,elem._1._2,elem._3)).sortBy(x=>(x._1,x._2))
    toPrint.append(("UserId","BusinessId","Pred_rating"))
    val g =  pred_format.collect().map(elem=>{toPrint.append((user_maprdd_reverse(elem._1),business_maprdd_reverse(elem._2),elem._3.toString))})

    val print_rdd = sc.parallelize(toPrint)

    print_rdd.map(elem=>elem._1 + "," + elem._2 + "," + elem._3).coalesce(1,false).saveAsTextFile("Ashir Alam ItemBasedCF.txt")

    val band1 = wala.filter(elem => math.abs(elem._2 - elem._3) >= 0.toDouble && math.abs(elem._2 - elem._3) < 1.toDouble).count()
    val band2 = wala.filter(elem => math.abs(elem._2 - elem._3) >= 1.toDouble && math.abs(elem._2 - elem._3) < 2.toDouble).count()
    val band3 = wala.filter(elem => math.abs(elem._2 - elem._3) >= 2.toDouble && math.abs(elem._2 - elem._3) < 3.toDouble).count()
    val band4 = wala.filter(elem => math.abs(elem._2 - elem._3) >= 3.toDouble && math.abs(elem._2 - elem._3) < 4.toDouble).count()
    val band5 = wala.filter(elem => math.abs(elem._2 - elem._3) >= 4.toDouble).count()


    println(">=0 and <1: " + band1)
    println(">=1 and <2: " + band2)
    println(">=2 and <3: " + band3)
    println(">=3 and <4: " + band4)
    println(">=4: " + band5)
    val MSE = wala.map(x => {
      val err = math.abs(x._2 - x._3);

      err * err
    }).mean()

    val RMSE = math.sqrt(MSE)
    println("RMSE = " + RMSE)


  }


  def populateHashUser(inp: String): Unit ={
    getCustomHashCodeUser(inp, typ = true)
  }

  val user_maprdd: mutable.Map[String, Int] = scala.collection.mutable.Map[String,Int]()
  var counterUser: Int = 0
  def getCustomHashCodeUser(inp: String, typ: Boolean): Int ={
    if(user_maprdd.contains(inp)){
      user_maprdd(inp)
    }else if(typ){
      counterUser+=1
      user_maprdd.put(inp, counterUser)
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
  var counterBusiness: Int = 0
  def getCustomHashCodeBusiness(inp: String, typ: Boolean): Int ={
    if(business_maprdd.contains(inp)){
      business_maprdd(inp)
    }else if(typ){
      counterBusiness+=1
      business_maprdd.put(inp, counterBusiness)
      counterBusiness
    }else{
      //hack
      0
    }
  }
}