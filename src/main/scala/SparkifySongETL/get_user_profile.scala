package SparkifySongETL

import org.apache.spark._
// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object get_user_profile {
  def main(args: Array[String]): Unit = {
    //setup spark
    val conf = new SparkConf().setMaster("local[*]")
                              .setAppName("get_user_profile")
    val spark: SparkSession = SparkSession.builder
                                          .config(conf)
                                          .getOrCreate()

    var input_song_data = "s3a://sparkify-song-data/song-data" 
    var input_log_data = "s3a://sparkify-song-data/log-data" 
    var output_data = "s3a://sparkify-song-data/scala_etl_output/get_user_profile" 
    
    var log_data = input_log_data +  "/2018/11/*.json"

    print (">>>>>> load from S3")
    var df_log = spark.read.json(log_data)
    //filter by actions for song plays
    //df_log = df_log.filter("page != NextSong")
    df_log.createOrReplaceTempView("log_data_table")

    //extract columns from joined song and log datasets to create songplays table 
    var songplays_table = spark.sql("""
                                SELECT 
                                logT.userId as user_id,
                                logT.firstname as firstname,
                                logT.lastname as lastname,
                                logT.location as location,
                                logT.userAgent as user_agent,
                                logT.auth as last_auth, 
                                max(ts) as last_ts,
                                max(registration) as last_registration
                                FROM log_data_table logT
                                GROUP BY 1,2,3,4,5,6
                            """)

    print (">>>>>> write to S3")
    songplays_table.write
                   .mode("overwrite")
                   .parquet(output_data)
    spark.stop()

  }
}