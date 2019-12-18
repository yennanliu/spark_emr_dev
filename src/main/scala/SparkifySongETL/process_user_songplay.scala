package SparkifySongETL

import org.apache.spark._
// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object process_user_songplay {
  def main(args: Array[String]): Unit = {
    //setup spark
    val conf = new SparkConf().setMaster("local[*]")
                              .setAppName("process_user_songplay")
    val spark: SparkSession = SparkSession.builder
                                          .config(conf)
                                          .getOrCreate()

    var input_song_data = "s3a://sparkify-song-data/song-data" 
    var input_log_data = "s3a://sparkify-song-data/log-data" 
    var output_data = "s3a://sparkify-song-data/scala_etl_output/process_user_songplay" 
    
    //var song_data = input_data +  "/*/*/*/*.json"
    var song_data = input_song_data +  "/A/A/*/*.json"
    var log_data = input_log_data +  "/2018/11/*.json"

    print (">>>>>> load from S3")
    var df_song = spark.read.json(song_data)
    df_song.createOrReplaceTempView("song_data_table")

    var df_log = spark.read.json(log_data)
    //filter by actions for song plays
    //df_log = df_log.filter("page != NextSong")
    df_log.createOrReplaceTempView("log_data_table")

    //extract columns from joined song and log datasets to create songplays table 
    var songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(logT.ts/1000) as start_time,
                                month(to_timestamp(logT.ts/1000)) as month,
                                year(to_timestamp(logT.ts/1000)) as year,
                                logT.userId as user_id,
                                logT.level as level,
                                songT.song_id as song_id,
                                songT.artist_id as artist_id,
                                logT.sessionId as session_id,
                                logT.location as location,
                                logT.userAgent as user_agent
                                FROM log_data_table logT
                                JOIN song_data_table songT on logT.artist = songT.artist_name and logT.song = songT.title
                            """)
    songplays_table.show()
    print (">>>>>> write to S3")
    songplays_table.write
                   .mode("overwrite")
                   .partitionBy("year", "month")
                   .parquet(output_data)
    spark.stop()

  }
}