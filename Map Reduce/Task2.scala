import java.io.{File, PrintWriter}
import java.io._

import org.apache.spark.{HashPartitioner, Partition, SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession



object Task2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local[2]")
        .getOrCreate()

    import spark.implicits._


//    val sc = new SparkContext(new SparkConf().setAppName("task2").setMaster("local[2]"))
    val df = spark.read.format("csv").option("header", "true").load(args{0})
    val raw = df.filter(args => args(52) != "NA" && args(52) != "0")

    val standard = raw.rdd.map(f => (f(0).asInstanceOf[String], f(3).asInstanceOf[String]))
    val partition = raw.repartition(2, $"Country").rdd.map(f => (f(3).asInstanceOf[String], f(0).asInstanceOf[String]))

    var res = new StringBuilder("standard,")

    //Time start
    val start = System.currentTimeMillis()

    standard.reduceByKey((a, b) => a+b)
    //Time end
    val end = System.currentTimeMillis() - start


    standard.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
      .toDF("partition_number","number_of_records")
      .collect().foreach(f => res.append(f.get(1) + ","))

    res.append(end)

    res.append("\npartition,")



    //Time start
    val start2 = System.currentTimeMillis()
    partition.reduceByKey((a, b) => a+b)
    //Time end
    val end2 = System.currentTimeMillis() - start2

    partition.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
      .toDF("partition_number","number_of_records")
      .collect().foreach(f => res.append(f.get(1) + ","))

    res.append(end2)



    //println(res)

    //val file = "Ashir_Alam_task2.csv"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args(1))))
    for (x <- res) {
      writer.write(x)  // however you want to format it
    }

    writer.close()

  }
}
