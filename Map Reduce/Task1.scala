import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd
import org.apache.spark.sql.SQLContext


object Task1 {
  def format(inp: (String, Int)): String = {
    var delim = ""
    if(inp._1.contains(',')){
      delim = "\""
    }
    "\n" + delim + inp._1 + delim + "," + inp._2
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local[*]")
        .getOrCreate()


    val df = spark.read.format("csv").option("header", "true").load(args(0))
    val c = df.rdd.repartition(4)
    var start = System.currentTimeMillis()
    var p = c.filter(args => args(52) != "NA" && args(52) != "0").groupBy(args => args(3)).map(f => (f._1.asInstanceOf[String], f._2.size)).collect().sortBy(_._1)

    var sum = 0

    //println(System.currentTimeMillis() - start)
    //start = System.currentTimeMillis()

//    newshaz.foreach(println)
    p.foreach(sum += _._2)
    var str = "Total," + sum
    p.foreach(str += format(_))

    val pw = new PrintWriter(new File(args(1)))
    pw.write(str)
    pw.close()




  }
}