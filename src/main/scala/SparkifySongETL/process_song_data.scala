package SparkifySongETL

import org.apache.spark._
// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object process_song_data {
  def main(args: Array[String]): Unit = {
    //setup spark
    val conf = new SparkConf().setMaster("local[*]")
                              .setAppName("process_song_data")
    val spark: SparkSession = SparkSession.builder
                                          .config(conf)
                                          .getOrCreate()

    var input_data = "s3a://sparkify-song-data/song-data" 
    var output_data = "s3a://sparkify-song-data/scala_etl_output/process_song_data" 
    //var song_data = input_data +  "/*/*/*/*.json"
    var song_data = input_data +  "/A/A/A/*.json"

    print (">>>>>> load from S3")
    var df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data_table")

    var songs_table = spark.sql("""
                            SELECT sdtn.song_id, 
                            sdtn.title,
                            sdtn.artist_id,
                            sdtn.year,
                            sdtn.duration
                            FROM song_data_table sdtn
                            WHERE song_id IS NOT NULL
                        """)

    print (">>>>>> write to S3")
    songs_table.write
               .mode("overwrite")
               .partitionBy("year", "artist_id")
               .parquet(output_data)
    spark.stop()

  }
}