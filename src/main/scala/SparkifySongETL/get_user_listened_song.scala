package SparkifySongETL

import org.apache.spark._
// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_list

object get_user_listened_song {
  def main(args: Array[String]): Unit = {
    //setup spark
    val conf = new SparkConf().setMaster("local[*]")
                              .setAppName("get_user_listened_song")
    val spark: SparkSession = SparkSession.builder
                                          .config(conf)
                                          .getOrCreate()

    var input_data = "s3a://sparkify-song-data/log-data" 
    var output_data = "s3a://sparkify-song-data/scala_etl_output/get_user_listened_song" 
    //var log_data = input_data +  "/*/*/*/*.json"
    var log_data = input_data +  "/2018/11/*.json"

    print (">>>>>> load from S3")
    var df = spark.read.json(log_data)
    df.createOrReplaceTempView("log_data_table")

    var listened_log = spark.sql("""
                            SELECT distinct 
                            log.userid,
                            log.artist
                            FROM log_data_table log
                            WHERE userid IS NOT NULL
                            AND artist IS NOT NULL
                        """)

    var user_id_list = spark.sql("""
                            SELECT distinct userid
                            FROM log_data_table log
                            WHERE userid IS NOT NULL
                        """)

    // register df as tmp table
    user_id_list.createOrReplaceTempView("user_id_list")
    listened_log.createOrReplaceTempView("listened_log")

    // currently I can't find the "songid" at the log table,
    // so here I use listented astist instead of songid
    // will do a fix in the future
    var listen_history = spark.sql("""
                            SELECT
                            u.userid as userid,
                            l.artist as artist
                            FROM 
                            user_id_list u 
                            INNER JOIN
                            listened_log l
                            ON 
                            u.userid = l.userid 
                        """)
    // RDD 

    // df 
    val user_listened = listen_history.groupBy("userId")                        
                      .agg(collect_list("artist")
                      .alias("listed_artist"))

    user_listened.show()
    print (">>>>>> write to S3")
    user_listened.write
                 .mode("overwrite")
                 .partitionBy("userId")
                 .parquet(output_data)
    spark.stop()

  }
}