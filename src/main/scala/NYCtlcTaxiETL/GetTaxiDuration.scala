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

        yellowEvents.columns
        yellowEvents.show()
        yellowEvents.cache()

        print (">>>>>>>>>> RENAME COLUMNS (avoid contains invalid character(s) among error)")

        var yellowEvents_ = yellowEvents.withColumnRenamed("vendor_id", "vendor_id_")
                                    .withColumnRenamed(" pickup_datetime", "pickup_datetime_")
                                    .withColumnRenamed(" dropoff_datetime", "dropoff_datetime_")
                                    .withColumnRenamed(" pickup_longitude", "pickup_longitude_")
                                    .withColumnRenamed(" pickup_latitude", "pickup_latitude_")
                                    .withColumnRenamed(" dropoff_longitude", " dropoff_longitude_")
                                    .withColumnRenamed(" dropoff_latitude", "dropoff_latitude_")
                                    .withColumnRenamed(" trip_distance", "trip_distance_")

        print (">>>>>>>>>> SAVE OUTPUT  (parquet)")

        yellowEvents.repartition(1)
        .select("vendor_id_", 
                "pickup_datetime_", 
                "dropoff_datetime_", 
                "pickup_longitude_", 
                "pickup_latitude_",
                "dropoff_longitude_",
                "dropoff_latitude_",
                "trip_distance_",
                "duration",
                "minute_rate")
        .write
        .mode("OVERWRITE")
        .partitionBy("pickup_datetime_")
        .parquet(outout_data + "/allEvents")
                        } 
        }