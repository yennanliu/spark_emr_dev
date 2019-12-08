package SparkifySongETL

import org.apache.spark._
// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object get_top_artist_song {
  def main(args: Array[String]): Unit = {
    //setup spark
    val conf = new SparkConf().setMaster("local[*]")
                              .setAppName("get_top_artist_song")
    val spark: SparkSession = SparkSession.builder
                                          .config(conf)
                                          .getOrCreate()

    var input_data = "s3a://sparkify-song-data/log-data" 
    var output_data = "s3a://sparkify-song-data/scala_etl_output/get_top_artist_song" 
    //var log_data = input_data +  "/*/*/*/*.json"
    var log_data = input_data +  "/2018/11/*.json"

    print (">>>>>> load from S3")
    var df = spark.read.json(log_data)
    df.createOrReplaceTempView("log_data_table")

    var top_artist_table = spark.sql("""
                            SELECT 
                            arsist, 
                            count(*) as arsist_count
                            FROM log_data_table log
                            WHERE song IS NOT NULL
                            AND arsist IS NOT NULL
                            GROUP BY 1 
                            ORDER BY 2 DESC 
                            LIMIT 1000
                        """)

    var top_song_table = spark.sql("""
                            SELECT 
                            song, 
                            count(*) as song_count
                            FROM log_data_table log
                            WHERE song IS NOT NULL
                            AND arsist IS NOT NULL
                            GROUP BY 1 
                            ORDER BY 2 DESC 
                            LIMIT 1000
                        """)

    print (">>>>>> write to S3")
    top_artist_table.write
               .mode("overwrite")
               .parquet(output_data)
    top_song_table.write
               .mode("overwrite")
               .parquet(output_data)
    spark.stop()

  }
}