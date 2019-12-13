package SparkifySongETL

import org.apache.spark._
// import required spark classes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object get_cleaned_log_data {
  def main(args: Array[String]): Unit = {
    //setup spark
    val conf = new SparkConf().setMaster("local[*]")
                              .setAppName("get_cleaned_log_data")
    val spark: SparkSession = SparkSession.builder
                                          .config(conf)
                                          .getOrCreate()

    var input_data = "s3a://sparkify-song-data/log-data" 
    var output_data = "s3a://sparkify-song-data/scala_etl_output/get_cleaned_log_data" 
    //var log_data = input_data +  "/*/*/*/*.json"
    var log_data = input_data +  "/2018/11/*.json"

    print (">>>>>> load from S3")
    var df = spark.read.json(log_data)
    df.createOrReplaceTempView("log_data_table")

    var logs_table = spark.sql("""
                            SELECT distinct log.* 
                            FROM log_data_table log
                            WHERE song IS NOT NULL
                            AND gender IS NOT NULL
                            AND artist IS NOT NULL
                            AND firstname IS NOT NULL
                            AND lastname IS NOT NULL
                            AND length IS NOT NULL
                            AND location IS NOT NULL
                            AND useragent IS NOT NULL
                            AND userid IS NOT NULL
                        """)

    print (">>>>>> write to S3")
    logs_table.write
               .mode("overwrite")
               .parquet(output_data)
    spark.stop()

  }
}