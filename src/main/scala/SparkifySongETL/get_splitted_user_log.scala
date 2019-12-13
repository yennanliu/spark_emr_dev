package SparkifySongETL

import org.apache.spark._
// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object get_splitted_user_log {
  def main(args: Array[String]): Unit = {
    //setup spark
    val conf = new SparkConf().setMaster("local[*]")
                              .setAppName("get_splitted_user_log")
    val spark: SparkSession = SparkSession.builder
                                          .config(conf)
                                          .getOrCreate()

    var input_data = "s3a://sparkify-song-data/log-data" 
    var output_data = "s3a://sparkify-song-data/scala_etl_output/get_splitted_user_log" 
    //var log_data = input_data +  "/*/*/*/*.json"
    var log_data = input_data +  "/2018/11/*.json"

    print (">>>>>> load from S3")
    var df = spark.read.json(log_data)
    df.createOrReplaceTempView("log_data_table")

    var logs_table = spark.sql("""
                            SELECT distinct log.* 
                            FROM log_data_table log
                            WHERE userid IS NOT NULL
                        """)

    print (">>>>>> write to S3")
    logs_table.write
               .mode("overwrite")
               .partitionBy("userid")
               .parquet(output_data)
    spark.stop()

  }
}