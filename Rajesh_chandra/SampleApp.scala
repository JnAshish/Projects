/* SimpleApp.scala */
//import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime

object SimpleApp {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp")
      .enableHiveSupport()
      .getOrCreate()
    try {
      
      println("Proceeding with data file")
      val dataDF = spark.read.format("csv")
        //.schema(input6)
        .option("delimiter", "\u0007")
        .option("ignoreLeadingWhiteSpace", "True")
        .option("ignoreTrailingWhiteSpace", "True")
        .option("multiline", "True")
        .option("escape", "\u000D")
      //  .load("C:/Users/rajatpancholi1008/Desktop/test/Pandey1.txt")
        .load("file:///home/azimukangda5500/Project-2/spark/SOURCE1/pump_PROD4_ACCT_ACCOUNTS_2018-10-23_08-31-38_00000_data.dsv.gz")
      println("Data file Found")

      //dataDF.write.format("csv").mode("overwrite").save("file:///home/azimukangda5500/Project-2/spark/SOURCE1/output/csv/")
      println("Proceeding with schema file")
      val input = spark.sparkContext.textFile("file:///home/azimukangda5500/Project-2/spark/SOURCE1/PROD4.ACCT_ACCOUNTS.schema")
      //val input = spark.sparkContext.textFile("C:/Users/rajatpancholi1008/Desktop/test/Schema.txt")
      
      //input.collect.foreach(println)
      println("Schema file found")

      val input2 = input.map { x =>
        val w = x.split(":")
        val columnName = w(0).trim()
        val raw = w(1).trim()
        (columnName, raw)
      }

      val input3 = input2.map { x =>
        val x2 = x._2.replaceAll(";", "")
        (x._1, x2)
      }

      val input4 = input3.map { x =>
        val pattern1 = ".*int\\d{0,}".r
        val pattern2 = ".*string\\[.*\\]".r
        val pattern3 = ".*timestamp\\[.*\\]".r
        val raw1 = pattern1 replaceAllIn (x._2, "int")
        val raw2 = pattern2 replaceAllIn (raw1, "string")
        val raw3 = pattern3 replaceAllIn (raw2, "timestamp")
        val raw4 = x._1 + " " + raw3
        raw4
      }

      val input5 = "create table if not exists temp123 (" + input4.collect().toList.mkString(",") + ") stored as parquetfile"

      //Table created in hive default database
      //spark.sql("drop table if exists temp123")
      spark.sql(input5)
      dataDF.write.insertInto("temp123")
      //val hiveOut = spark.sql("select * from temp123")
      //hiveOut.coalesce(1).write.format("parquet").mode("overwrite").save("file:///home/azimukangda5500/Project-2/spark/SOURCE1/output/parquet/extract_date=" + java.time.LocalDateTime.now)

  } catch {
      case e: Throwable => println("File Not Found")
    } finally {
      spark.stop()
    }
  }
}
