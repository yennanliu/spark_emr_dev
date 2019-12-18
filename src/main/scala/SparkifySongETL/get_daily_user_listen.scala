package SparkifySongETL

import org.apache.spark._
// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_list

object get_daily_user_listen {
  def main(args: Array[String]): Unit = {
    //setup spark
    val conf = new SparkConf().setMaster("local[*]")
                              .setAppName("get_daily_user_listen")
    val spark: SparkSession = SparkSession.builder
                                          .config(conf)
                                          .getOrCreate()

    var input_data = "s3a://sparkify-song-data/log-data" 
    var output_data = "s3a://sparkify-song-data/scala_etl_output/get_daily_user_listen" 
    //var log_data = input_data +  "/*/*/*/*.json"
    var log_data = input_data +  "/2018/11/*.json"

    print (">>>>>> load from S3")
    var df = spark.read.json(log_data)
    df.createOrReplaceTempView("log_data_table")

    var listened_log = spark.sql("""
                            SELECT 
                            to_date(to_timestamp(ts/1000)) as datetime, 
                            *
                            FROM log_data_table
                            WHERE userid IS NOT NULL
                            AND artist IS NOT NULL
                            AND page = 'NextSong'
                        """)
    // register df as tmp table
    listened_log.createOrReplaceTempView("listened_log")

    listened_log.show()
    print (">>>>>> write to S3")
    listened_log.write
                 .mode("overwrite")
                 .partitionBy("datetime")
                 .parquet(output_data)
    spark.stop()

  }
}