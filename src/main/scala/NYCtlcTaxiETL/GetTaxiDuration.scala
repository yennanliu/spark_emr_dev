package NYCtlcTaxiETL

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetTaxiDuration {

    def main(args: Array[String]) {

      lazy val sparksession = SparkSession.builder
              .appName("GetTaxiDuration")
              .config("spark.master", "local[*]")
              .getOrCreate()

        import sparksession.implicits._

        sparksession.conf.set("spark.sql.session.timeZone", "America/New_York")

        var zone_data = "s3a://nyc-tlc-taxi/zone/taxi+_zone_lookup.csv"

        // sample dataset
        var yellow_trip_data = "s3a://nyc-tlc-taxi/yellow_trip/dt=2014-01/*.csv" 
        var green_trip_data = "s3a://nyc-tlc-taxi/green_trip/dt=2014-01/*.csv"
        var fhv_trip_data = "s3a://nyc-tlc-taxi/fhv_trip/dt=2014-01/*.csv"
        var outout_data = "s3a://nyc-tlc-taxi/scala_etl_output/GetTaxiDuration"

        val yellowEvents = sparksession.read
          .option("header","true")
          .option("inferSchema", "true")
          .option("enforceSchema", "false")
          .option("timeStampFormat", "yyyy-MM-dd HH:mm:ss")
          .option("columnNameOfCorruptRecord", "error")
          .csv(yellow_trip_data) 
          .withColumn("duration", unix_timestamp($" dropoff_datetime").minus(unix_timestamp($" pickup_datetime")))
          .withColumn("minute_rate",$" trip_distance".divide($"duration") * 60)
          .withColumnRenamed("pickup_datetime","pickup_datetime")

        yellowEvents.show()
        yellowEvents.cache()

        print (">>>>>>>>>> SAVE OUTPUT  (parquet)")

        yellowEvents.repartition(1)
        .sortWithinPartitions($" pickup_datetime")
        .write
        .mode("OVERWRITE")
        .partitionBy(" pickup_datetime")
        .parquet(outout_data + "/allEvents")
    
                        } 
        }