
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.functions._





object Task3 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local[*]")
        .getOrCreate()
    import spark.implicits._



    val df = spark.read.format("csv").option("header", "true").load(args(0))
    //val c = df.rdd
    var a = df.select("Country", "SalaryType", "Salary")
    a = a.filter(c => c.getAs[String](2) != "NA" && c.getAs[String](2) != "0")

    a = a.withColumn("Salary", regexp_replace($"Salary", ",", ""))
//  a = a.withColumn("Salary", regexp_replace($"Salary", ".00", ""))

    a = a.withColumn("SalaryType", regexp_replace($"SalaryType", "NA", "1"))
    a = a.withColumn("SalaryType", regexp_replace($"SalaryType", "Yearly", "1"))
    a = a.withColumn("SalaryType", regexp_replace($"SalaryType", "Monthly", "12"))
    a = a.withColumn("SalaryType", regexp_replace($"SalaryType", "Weekly", "52"))

    a = a.withColumn("TotalSalary", when($"Salary"*$"SalaryType" < 2147483647, ($"Salary"*$"SalaryType").cast(DecimalType(16,0))).otherwise(2147483647))

    var r = a.groupBy("Country").agg(count("Country"), min("TotalSalary"), max("TotalSalary"), (sum("TotalSalary")/count("Country")).cast(DecimalType(18,2)))//.orderBy(asc("Country"))


    //import sqlContext.implicits._

    var w= r.sort($"Country".asc).coalesce(1)

    w.write.csv(args(1))




    //newDF.show(Integer.MAX_VALUE)
  }
}